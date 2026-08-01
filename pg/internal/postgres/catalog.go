package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// catalog maps the replicated tables to stable CRDT IDs and is the adapter's
// OID⇄stable-ID map (§6): it is indexed by relation OID as well as stable
// TableID. Bootstrap tables (Config.Tables) are seeded here at introspect with
// name-derived IDs (every node pre-creates them identically, so the hashes
// agree without a schema log); tables created through replicated DDL get a
// fresh allocated ID the op-builder records here (addTable) and trims on drop
// (dropTable). It is mutated only on the single orchestrator/capture goroutine,
// so it needs no lock.
type catalog struct {
	byID  map[crdt.TableID]*tableInfo
	byOID map[uint32]*tableInfo
	// coordUnique mirrors Config.CoordinatedUnique: DDL admission marks an
	// all-NOT-NULL unique key Coordinated instead of rejecting it.
	coordUnique bool
	// coordIdx is the reservation endpoint's copy of the coordinated-key
	// metadata. Unlike the rest of the catalog it IS read off this
	// goroutine — by the pgwire endpoint serving Postgres backends — so it
	// carries its own lock. Nil when coordinated uniqueness is disabled.
	coordIdx *coordIndex
}

type tableInfo struct {
	schema, name string
	oid          uint32 // relation OID (the RelationMessage / intent key)
	tid          crdt.TableID
	cols         []*colInfo // attnum order
	byName       map[string]*colInfo
	pk           []*colInfo // PK columns, key order

	// clockGroup is the table's merge unit: metadata.ClockGroupRow (whole-row
	// LWW, the default) or ClockGroupCell (per-column LWW + counter columns).
	// It mirrors pg_class.relreplident — REPLICA IDENTITY FULL is the opt-in,
	// and the capability per-column capture runs on (cell.go).
	clockGroup string

	// uniqueKeys lists the active non-PK unique keys (UNIQUE constraints /
	// CREATE UNIQUE INDEX) the apply path's loser-null arbitration (§5) reads.
	// Followers bind these in-catalog only — they do NOT create a physical
	// UNIQUE constraint; arbitration is the sole convergence mechanism (a
	// follower constraint would 23505 the apply txn before arbitration ran).
	uniqueKeys []*uniqueKey
}

// uniqueKey is one active non-PK unique key on a tableInfo. keyID matches the
// syzy_key row's key_id; cols are the member columns in declared (tuple) order.
type uniqueKey struct {
	keyID crdt.KeyID
	cols  []*colInfo
	// coordinated marks a CP key (all members NOT NULL): guaranteed by
	// construction through reserve-before-commit against the cluster
	// registry, with NO physical index on any node (coordinated.go).
	// Skipped by loser-null arbitration.
	coordinated bool
	// indexOID is the backing unique index's OID on the node that physically
	// holds it (the originator). It maps a standalone DROP INDEX back to this
	// key (buildDropOp). Zero on followers, which bind the key in-catalog only.
	indexOID uint32
}

type colInfo struct {
	name      string
	typeName  string // format_type(), used to cast text values on apply
	cid       crdt.ColumnID
	isPK      bool
	attnum    int    // PG physical column number; the rename-stable diff key (§6)
	notNull   bool   // replicated column attributes, carried so the ALTER catalog
	def       string // diff (§6) can detect — and cleanly reject — an attribute-only
	generated bool   // change it cannot yet represent as a typed catalog operation.
	identity  uint8  // attidentity: 0/'a'/'d'; 'a' (GENERATED ALWAYS) needs OVERRIDING SYSTEM VALUE on apply.
	counter   bool   // declared syzy_counter: the cell merges by summation (cell.go).
}

// table resolves a stable id to its tableInfo, or nil. Nil-receiver-safe: the
// deterministic fold fixtures drive a capturer with no catalog at all.
func (c *catalog) table(tid crdt.TableID) *tableInfo {
	if c == nil {
		return nil
	}
	return c.byID[tid]
}

// addTable indexes ti by both its stable ID and its OID.
func (c *catalog) addTable(ti *tableInfo) {
	c.byID[ti.tid] = ti
	c.byOID[ti.oid] = ti
}

// dropTable removes ti from both indexes.
func (c *catalog) dropTable(ti *tableInfo) {
	delete(c.byID, ti.tid)
	delete(c.byOID, ti.oid)
}

// colByID returns the column with the given stable id, or nil.
func (ti *tableInfo) colByID(cid crdt.ColumnID) *colInfo {
	for _, c := range ti.cols {
		if c.cid == cid {
			return c
		}
	}
	return nil
}

// removeColumn drops ci from the table's column list and name index. A PK column
// is never dropped (the diff rejects that), so ti.pk needs no adjustment.
func (ti *tableInfo) removeColumn(ci *colInfo) {
	delete(ti.byName, ci.name)
	kept := ti.cols[:0]
	for _, c := range ti.cols {
		if c != ci {
			kept = append(kept, c)
		}
	}
	ti.cols = kept
}

// removeUniqueKey drops the key with the given id from ti.uniqueKeys.
func (ti *tableInfo) removeUniqueKey(keyID crdt.KeyID) {
	kept := ti.uniqueKeys[:0]
	for _, uk := range ti.uniqueKeys {
		if uk.keyID != keyID {
			kept = append(kept, uk)
		}
	}
	ti.uniqueKeys = kept
}

// uniqueKeyByIndexOID finds the tracked unique key whose backing index has the
// given OID — the originator's map from a dropped index to the key it backed.
// Returns (nil, nil) if no tracked key owns that index.
func (c *catalog) uniqueKeyByIndexOID(oid uint32) (*tableInfo, *uniqueKey) {
	if oid == 0 {
		return nil, nil
	}
	for _, ti := range c.byID {
		for _, uk := range ti.uniqueKeys {
			if uk.indexOID == oid {
				return ti, uk
			}
		}
	}
	return nil, nil
}

// deriveTableID / deriveColumnID hash names so both nodes agree on stable IDs
// without (yet) a schema-log catalog.
func deriveTableID(schema, name string) crdt.TableID {
	h := sha256.Sum256([]byte("T\x00" + schema + "\x00" + name))
	var t crdt.TableID
	copy(t[:], h[:16])
	return t
}

func deriveColumnID(schema, table, col string) crdt.ColumnID {
	h := sha256.Sum256([]byte("C\x00" + schema + "\x00" + table + "\x00" + col))
	var c crdt.ColumnID
	copy(c[:], h[:16])
	return c
}

// introspectCatalog builds the catalog for the bootstrap tables (Config.Tables)
// from pg_catalog. Their stable IDs are name-derived — every node pre-creates
// them identically, so the hashes agree without a schema log (DDL-created tables
// instead get allocated IDs the op-builder records; see ddl_catalog.go).
func introspectCatalog(ctx context.Context, conn *pgx.Conn, tables []string) (*catalog, error) {
	cat := &catalog{byID: map[crdt.TableID]*tableInfo{}, byOID: map[uint32]*tableInfo{}}
	for _, qname := range tables {
		schema, name := splitQName(qname)
		var oid uint32
		var replident string
		if err := conn.QueryRow(ctx, `
			SELECT c.oid, c.relreplident::text FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
			WHERE ns.nspname = $1 AND c.relname = $2`, schema, name).Scan(&oid, &replident); err != nil {
			return nil, fmt.Errorf("introspect %s: %w", qname, err)
		}
		pgcols, err := introspectColumns(ctx, conn, oid)
		if err != nil {
			return nil, err
		}
		if err := rejectUnreplicable(schema, name, pgcols); err != nil {
			return nil, err
		}
		if err := rejectCounterShape(ctx, conn, schema, name, oid, replIdentByte(replident), pgcols); err != nil {
			return nil, err
		}
		ti := &tableInfo{schema: schema, name: name, oid: oid, tid: deriveTableID(schema, name),
			byName: map[string]*colInfo{}, clockGroup: clockGroupForReplIdent(replIdentByte(replident))}
		var pks []pkEntry
		for _, pc := range pgcols {
			ci := pc.colInfo(deriveColumnID(schema, name, pc.name))
			ti.cols = append(ti.cols, ci)
			ti.byName[ci.name] = ci
			if pc.pkpos > 0 {
				pks = append(pks, pkEntry{pos: pc.pkpos, ci: ci})
			}
		}
		if len(pks) == 0 {
			return nil, fmt.Errorf("postgres: table %s has no primary key", qname)
		}
		buildPK(ti, pks)
		cat.addTable(ti)
	}
	return cat, nil
}

func splitQName(q string) (schema, name string) {
	if i := strings.IndexByte(q, '.'); i >= 0 {
		return q[:i], q[i+1:]
	}
	return "public", q
}
