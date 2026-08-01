package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// pgTableOIDsKey holds the PG-local TableID→oid bindings (a PG addendum to the
// engine-neutral metadata catalog, which has no oid column). Restore binds each
// table by oid — invariant across RENAME — so a relation renamed or dropped by a
// crash between a DDL's physical commit and persistSchemaEvent never blocks
// rebinding to its durable (last-appended) catalog entry.
const pgTableOIDsKey = "pg_table_oids"

// encodeTableOIDs serializes the in-memory catalog's TableID→oid map (16-byte id
// + big-endian uint32 oid per entry). Written in persistSchemaEvent so it tracks
// every schema change atomically with the catalog rows.
func encodeTableOIDs(cat *catalog) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(cat.byID)))
	for tid, ti := range cat.byID {
		buf = append(buf, tid[:]...)
		buf = binary.BigEndian.AppendUint32(buf, ti.oid)
	}
	return buf
}

func decodeTableOIDs(b []byte) map[crdt.TableID]uint32 {
	m := map[crdt.TableID]uint32{}
	if len(b) == 0 {
		return m
	}
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return m
	}
	b = b[k:]
	for i := uint64(0); i < n && len(b) >= 20; i++ {
		var tid crdt.TableID
		copy(tid[:], b[:16])
		m[tid] = binary.BigEndian.Uint32(b[16:20])
		b = b[20:]
	}
	return m
}

// pgColAttrsKey holds the PG-local per-column physical attributes (type,
// NOT NULL, default, GENERATED/IDENTITY, PK membership) of every catalog table —
// the second PG addendum to the engine-neutral catalog, which stores none of
// them. Its purpose is crash-window fidelity: buildAlterTableOps derives an
// ALTER by diffing the live catalog against the cached tableInfo, so if restore
// primed those attributes from the live relation, a DDL that committed in
// Postgres but crashed before persistSchemaEvent would read as "no change" and
// silently never replicate. Recording them here keeps the cache at the last
// SHIPPED shape, which is what the diff has to compare against.
const pgColAttrsKey = "pg_col_attrs"

// colAttrs is one column's physical attributes as last shipped.
type colAttrs struct {
	typeName  string
	def       string
	identity  byte
	notNull   bool
	generated bool
	isPK      bool
}

func appendStr(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

func takeStr(b []byte) (string, []byte, bool) {
	n, k := binary.Uvarint(b)
	if k <= 0 || uint64(len(b[k:])) < n {
		return "", nil, false
	}
	return string(b[k : k+int(n)]), b[k+int(n):], true
}

// encodeColAttrs serializes the in-memory catalog's per-column attributes,
// keyed by the stable TableID/ColumnID (never attnum, which a re-add reuses
// differently on each node). Written in persistSchemaEvent, so it tracks the
// cache exactly.
func encodeColAttrs(cat *catalog) []byte {
	buf := binary.AppendUvarint(nil, uint64(len(cat.byID)))
	for tid, ti := range cat.byID {
		buf = append(buf, tid[:]...)
		buf = binary.AppendUvarint(buf, uint64(len(ti.cols)))
		for _, c := range ti.cols {
			buf = append(buf, c.cid[:]...)
			buf = appendStr(buf, c.typeName)
			buf = appendStr(buf, c.def)
			var flags byte
			if c.notNull {
				flags |= 1
			}
			if c.generated {
				flags |= 2
			}
			if c.isPK {
				flags |= 4
			}
			buf = append(buf, c.identity, flags)
		}
	}
	return buf
}

// decodeColAttrs reverses encodeColAttrs. A truncated or absent blob (a node
// whose metadata predates it) yields whatever was parsed so far; those tables
// fall back to the live relation, which is the behaviour before this record
// existed.
func decodeColAttrs(b []byte) map[crdt.TableID]map[crdt.ColumnID]colAttrs {
	out := map[crdt.TableID]map[crdt.ColumnID]colAttrs{}
	nt, k := binary.Uvarint(b)
	if k <= 0 {
		return out
	}
	b = b[k:]
	for i := uint64(0); i < nt; i++ {
		if len(b) < 16 {
			return out
		}
		var tid crdt.TableID
		copy(tid[:], b[:16])
		b = b[16:]
		nc, k := binary.Uvarint(b)
		if k <= 0 {
			return out
		}
		b = b[k:]
		cols := make(map[crdt.ColumnID]colAttrs, nc)
		for j := uint64(0); j < nc; j++ {
			if len(b) < 16 {
				return out
			}
			var cid crdt.ColumnID
			copy(cid[:], b[:16])
			b = b[16:]
			var a colAttrs
			var ok bool
			if a.typeName, b, ok = takeStr(b); !ok {
				return out
			}
			if a.def, b, ok = takeStr(b); !ok {
				return out
			}
			if len(b) < 2 {
				return out
			}
			a.identity, a.notNull = b[0], b[1]&1 != 0
			a.generated, a.isPK = b[1]&2 != 0, b[1]&4 != 0
			b = b[2:]
			cols[cid] = a
		}
		out[tid] = cols
	}
	return out
}

// applyColAttrs sets one table's columns to their last-shipped attributes.
// Columns with no record keep whatever the caller bound (the live relation).
func applyColAttrs(ti *tableInfo, attrs map[crdt.ColumnID]colAttrs) {
	for _, c := range ti.cols {
		a, ok := attrs[c.cid]
		if !ok {
			continue
		}
		c.typeName, c.def, c.identity = a.typeName, a.def, a.identity
		c.notNull, c.generated, c.isPK = a.notNull, a.generated, a.isPK
	}
}

// restoreSchemaCatalog rebuilds the in-memory OID⇄stable-ID map for
// DDL-created tables from the durable metadata catalog and restores schema_seq
// (§10), so a restart resumes from the persisted schema head rather than
// replaying the whole schema log from 0. The bootstrap tables (Config.Tables)
// are already in the catalog from introspectCatalog; this adds the DDL-created
// delta that persistSchemaEvent recorded.
//
// The metadata catalog stores engine-neutral fields (stable id, current name,
// ordinal) — never Postgres oid/attnum/type. Each active table is bound by oid
// (from the PG-local pgTableOIDsKey map) and its columns by attnum; see
// bindRestoredTable for why restore reflects the durable (last-appended) catalog
// rather than the live physical schema, which is what keeps a crash mid-DDL from
// silently swallowing the pending change.
func (e *Engine) restoreSchemaCatalog(ctx context.Context) error {
	if e.cfg.Meta == nil || e.cfg.SchemaLog == nil {
		return nil
	}
	if seq, ok, err := e.cfg.Meta.GetSchemaSeq(); err != nil {
		return fmt.Errorf("restore schema_seq: %w", err)
	} else if ok {
		e.schemaSeq.Store(seq)
	}
	snap, err := e.cfg.Meta.LoadCatalogSnapshot()
	if err != nil {
		return fmt.Errorf("load catalog snapshot: %w", err)
	}
	oidBlob, _, err := e.cfg.Meta.GetMeta(pgTableOIDsKey)
	if err != nil {
		return fmt.Errorf("load table oids: %w", err)
	}
	oidByTID := decodeTableOIDs(oidBlob)

	colsByTable := map[crdt.TableID][]metadata.ColumnEntry{}
	for _, c := range snap.Columns {
		if c.State == metadata.StateActive {
			colsByTable[c.TableID] = append(colsByTable[c.TableID], c)
		}
	}
	pkByTable := map[crdt.TableID][]metadata.KeyEntry{}
	// Non-PK unique keys (§5), grouped by (table, key). DropUniqueKey writes a
	// single dropped marker (ordinal 0, zero column id); treat the whole key as
	// dropped if any of its rows is non-active, mirroring catalog.Reload.
	type uqID struct {
		t crdt.TableID
		k crdt.KeyID
	}
	uqMembers := map[uqID][]metadata.KeyEntry{}
	uqDropped := map[uqID]bool{}
	for _, k := range snap.Keys {
		if k.KeyID == (crdt.KeyID{}) {
			if k.State == metadata.StateActive {
				pkByTable[k.TableID] = append(pkByTable[k.TableID], k)
			}
			continue
		}
		id := uqID{k.TableID, k.KeyID}
		if k.State != metadata.StateActive {
			uqDropped[id] = true
			continue
		}
		uqMembers[id] = append(uqMembers[id], k)
	}
	uqByTable := map[crdt.TableID][]restoredUniqueKey{}
	for id, members := range uqMembers {
		if uqDropped[id] {
			continue
		}
		sort.Slice(members, func(i, j int) bool { return members[i].Ordinal < members[j].Ordinal })
		uqByTable[id.t] = append(uqByTable[id.t], restoredUniqueKey{keyID: id.k, members: members})
	}
	for _, te := range snap.Tables {
		if te.State != metadata.StateActive {
			continue
		}
		if ti := e.cat.byID[te.ID]; ti != nil {
			// Already bound from the live relation (a bootstrap table that also
			// rode the schema log). Its merge unit is a recorded fact too, so a
			// crash-window REPLICA IDENTITY flip must not reach arbitration before
			// the cluster knows about it.
			if te.DefaultClockGroup == metadata.ClockGroupCell {
				ti.clockGroup = metadata.ClockGroupCell
			} else {
				ti.clockGroup = metadata.ClockGroupRow
			}
			continue
		}
		if err := e.bindRestoredTable(ctx, te, oidByTID[te.ID], colsByTable[te.ID], pkByTable[te.ID], uqByTable[te.ID]); err != nil {
			return err
		}
	}
	// Last-shipped column attributes over the live ones every path above bound,
	// so buildAlterTableOps diffs against what peers know rather than against a
	// physical schema a crash may have left ahead of the schema log.
	attrsBlob, _, err := e.cfg.Meta.GetMeta(pgColAttrsKey)
	if err != nil {
		return fmt.Errorf("load column attrs: %w", err)
	}
	for tid, attrs := range decodeColAttrs(attrsBlob) {
		if ti := e.cat.byID[tid]; ti != nil {
			applyColAttrs(ti, attrs)
		}
	}
	return nil
}

// restoredUniqueKey is one non-PK unique key reassembled from the durable
// metadata catalog at restore, members in ordinal (tuple) order.
type restoredUniqueKey struct {
	keyID   crdt.KeyID
	members []metadata.KeyEntry
}

// bindRestoredTable reconstructs one table's tableInfo from its DURABLE metadata
// catalog entry — the last state appended to the schema chain, which is what
// peers know — NOT the live physical schema. It binds by oid (rename-invariant,
// from the pgTableOIDsKey map) so a since-renamed/dropped relation never blocks
// Open, takes names/columns/PK from the recorded entry, and merges only the
// live PG attributes (type/not-null/default/generated/identity, matched by
// attnum) that apply needs but the engine-neutral catalog does not store.
//
// Reflecting the recorded — not the live — state is the crux: Postgres commits
// DDL before the sidecar appends its schema event, so a crash in that window
// leaves the physical schema ahead. The recovery path that closes the gap is
// capture re-delivering the un-pruned syzy_ddl_intent rows, whose ops are
// re-derived by DIFFING the live catalog against this cached tableInfo
// (buildAlterTableOps/buildDropOp). Binding to live would erase that diff and
// the pending RENAME/DROP would silently never replicate — a divergence worse
// than the loud Open failure this rebinding replaced. Column ATTRIBUTES are
// merged from the live relation only as a floor: restoreSchemaCatalog overwrites
// them with the last-shipped values (pgColAttrsKey) wherever it has them, so a
// crash-window ALTER COLUMN is a real diff here rather than an invisible one.
func (e *Engine) bindRestoredTable(ctx context.Context, te metadata.TableEntry, oid uint32, cols []metadata.ColumnEntry, pk []metadata.KeyEntry, uniqueKeys []restoredUniqueKey) error {
	if oid == 0 {
		// No persisted oid (a table predating the oid map): best-effort resolve by
		// the recorded name. If it was since renamed/dropped the lookup fails and
		// oid stays 0 — the table binds metadata-only (no live attrs), still safe.
		if o, err := relationOID(ctx, e.apply, appliedSchema, te.Name); err == nil {
			oid = o
		}
	}
	// Live attributes by attnum. Empty when the relation is gone (dropped in a
	// crash window) or oid is unknown — the recorded columns then bind without PG
	// attributes, which is all the impending DROP needs.
	pgcols, err := introspectColumns(ctx, e.apply, oid)
	if err != nil {
		return fmt.Errorf("restore table %s columns: %w", te.Name, err)
	}
	byAttnum := make(map[int]pgColumn, len(pgcols))
	for _, pc := range pgcols {
		byAttnum[pc.attnum] = pc
	}
	// The merge unit comes from the RECORDED entry like every other catalog
	// fact here, not from live pg_class: it is what peers know, and a crash
	// between the physical ALTER and the schema-log append must not flip this
	// node's arbitration rule ahead of the cluster's.
	group := te.DefaultClockGroup
	if group != metadata.ClockGroupCell {
		group = metadata.ClockGroupRow
	}
	ti := &tableInfo{schema: appliedSchema, name: te.Name, oid: oid, tid: te.ID,
		byName: map[string]*colInfo{}, clockGroup: group}
	for _, c := range cols {
		ci := &colInfo{name: c.Name, cid: c.ColumnID, attnum: c.Ordinal + 1, // attnum is 1-based; ordinal is 0-based
			counter: c.ClockGroup == metadata.ClockGroupCounter}
		if pc, ok := byAttnum[ci.attnum]; ok {
			ci.typeName, ci.notNull, ci.def = pc.typeName, pc.notNull, pc.def
			ci.generated, ci.identity, ci.isPK = pc.generated, pc.identity, pc.pkpos > 0
		}
		ti.cols = append(ti.cols, ci)
		ti.byName[ci.name] = ci
	}
	// PK in key order. Every recorded PK column is still in ti.cols (PK columns
	// cannot be dropped — buildAlterTableOps rejects that), so ti.pk is complete.
	sort.Slice(pk, func(i, j int) bool { return pk[i].Ordinal < pk[j].Ordinal })
	for _, k := range pk {
		if ci := ti.colByID(k.ColumnID); ci != nil {
			ti.pk = append(ti.pk, ci)
		}
	}
	// Non-PK unique keys (§5) in tuple order; a key whose member column went
	// missing (cannot happen — a unique key's columns can't be dropped without
	// dropping the key) is skipped rather than binding a partial key.
	for _, uk := range uniqueKeys {
		cols := make([]*colInfo, 0, len(uk.members))
		complete := true
		for _, m := range uk.members {
			ci := ti.colByID(m.ColumnID)
			if ci == nil {
				complete = false
				break
			}
			cols = append(cols, ci)
		}
		if complete && len(cols) > 0 {
			ti.uniqueKeys = append(ti.uniqueKeys, &uniqueKey{
				keyID: uk.keyID, cols: cols,
				coordinated: len(uk.members) > 0 && uk.members[0].Coordinated,
			})
		}
	}
	// Rebind each key's backing-index OID (not stored in the engine-neutral
	// metadata catalog) so a later DROP INDEX on the originator still maps to its
	// key. On a follower there is no physical unique index, so the OID stays 0.
	if len(ti.uniqueKeys) > 0 {
		oids, err := liveUniqueIndexOIDs(ctx, e.apply, ti)
		if err != nil {
			return fmt.Errorf("restore table %s unique index oids: %w", te.Name, err)
		}
		for _, uk := range ti.uniqueKeys {
			uk.indexOID = oids[uniqueKeySig(uk.cols)]
		}
	}
	// If a bootstrap table was dropped then recreated via DDL, introspectCatalog
	// already bound this oid to its name-derived id; the recreate's allocated id
	// is the cluster-authoritative one. Evict the stale entry so the relation
	// maps to a SINGLE id — otherwise a delayed peer changeset addressed to the
	// old id would still resolve (by id) and write to the new table, causing
	// silent divergence.
	if old := e.cat.byOID[oid]; old != nil && old.tid != ti.tid {
		delete(e.cat.byID, old.tid)
	}
	e.cat.addTable(ti)
	return nil
}
