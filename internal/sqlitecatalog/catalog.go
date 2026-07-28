// Package catalog is the SQLite catalog implementation. It maps SQLite table
// and column names to stable
// crdt.TableID / crdt.ColumnID values used on the wire. The catalog is
// the in-memory view of the metadata's syzy_table / syzy_column /
// syzy_key tables. IDs are 16-byte random values allocated when a
// table or column is first introduced; renames/drops do not change
// IDs.
//
// At producer startup, the catalog is loaded from the metadata via
// LoadFromMeta. On a fresh metadata that already has a populated
// app.db, callers (syzy_init or its test analogue) seed the catalog
// with SeedFromSchema, which walks sqlite_master and allocates IDs
// for every replicated table.
//
// After a DDL apply, callers invoke Catalog.Reload to pick up the
// metadata's updated state.
package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	corecatalog "github.com/wjordan/syzy/catalog"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

// Column carries the per-column entry of a Table. ID is the wire-stable
// crdt.ColumnID; Ordinal is the 0-based position in the table; PKPos is
// 0 for non-PK columns and 1-indexed for PK columns in PK order.
//
// PKDefault is populated only on the buildTable / SeedFromSchema path,
// which reads `pragma_table_xinfo.dflt_value` directly. After a metadata-
// only Reload it stays zero (PKDefaultNone) until something repopulates
// it from app.db; the producer does this implicitly when it (re)builds
// the catalog from the live schema.
type Column struct {
	Name      string
	ID        crdt.ColumnID
	Ordinal   int
	PKPos     int
	PKDefault PKDefault
	// Collation is the column's text collating sequence (crdt.CollBinary
	// default). Used by admission to compile partial-index predicates and
	// to reject non-BINARY unique-key members.
	Collation crdt.Collation
	// CreateSeq/DropSeq bracket the column's live schema-chain range
	// (DropSeq 0 = still active). Carried so TableAtSeq can reconstruct
	// the column layout a journal record was captured under.
	CreateSeq uint64
	DropSeq   uint64
	// ClockGroup is syzy_column.clock_group. 'counter' marks a declared
	// counter column (sqlite/docs/DDL.md#counter-columns): concurrent writes merge
	// by summation, never stamp-arbitrated. Empty reads as 'row'.
	ClockGroup string
}

// Counter reports whether this is a declared counter column.
func (c Column) Counter() bool { return c.ClockGroup == metadata.ClockGroupCounter }

// liveAt reports whether the column was part of the table's physical
// layout at the given schema-chain position.
func (c Column) liveAt(seq uint64) bool {
	return c.CreateSeq <= seq && (c.DropSeq == 0 || c.DropSeq > seq)
}

// Table is one replicated table's catalog entry. PK is the subset of
// Columns flagged as primary-key, ordered by PKPos. Only active tables
// (state='active') appear in Tables() / Table(); dropped tables remain
// queryable via TableByID for the apply path's "skip dropped table" rule.
type Table struct {
	Name    string
	ID      crdt.TableID
	Columns []Column
	PK      []Column

	// UniqueKeys lists the active non-PK unique keys (UNIQUE constraints,
	// CREATE UNIQUE INDEX). Each entry's Columns are in declared order.
	// Used by the apply path's loser-null arbitration; see sqlite/docs/DDL.md#unique-keys.
	UniqueKeys []UniqueKey

	// dropped is true if syzy_table.state = 'dropped'. Active lookups
	// hide dropped tables; ID lookups return them so the apply path can
	// distinguish "dropped" (deterministic skip) from "unknown" (error).
	dropped bool

	// allColumns is the full column history — active and dropped —
	// sorted by ordinal, feeding TableAtSeq's layout reconstruction.
	// historyReliable is false when any tombstone predates ordinal/
	// create_seq preservation (legacy drops clobbered them), making
	// pre-drop layouts unreconstructable for this table.
	allColumns      []Column
	historyReliable bool

	// clockGroup is syzy_table.default_clock_group: 'row' (whole-row
	// LWW register) or 'cell' (per-column LWW; concurrent updates to
	// disjoint columns merge). See CRDT.md#layers.
	clockGroup string

	// hasCounters caches whether any active column is a declared
	// counter column, so the apply hot path can skip per-column scans
	// on counter-free tables.
	hasCounters bool
}

// UniqueKey is one active non-PK unique key on a Table. KeyID matches
// the syzy_key row's key_id. Columns are in declared order (the same
// order the unique-index tuple presents).
type UniqueKey struct {
	KeyID   crdt.KeyID
	Columns []Column
	// Coordinated is true for a CP (NOT NULL UNIQUE) key enforced by
	// reservation before commit; false for an eventual loser-null key.
	// See sqlite/docs/DDL.md#unique-keys.
	Coordinated bool
	// Predicate is the compiled WHERE clause of a partial unique index;
	// its zero value (nil Root) means a total key (every row
	// participates). Only set for coordinated keys. See sqlite/docs/DDL.md#unique-keys.
	Predicate crdt.UniquePredicate
}

// Dropped reports whether this table has been tombstoned via DROP TABLE.
// Inbound DML for a dropped table is a deterministic no-op; the apply
// path advances the frontier without writing.
func (t *Table) Dropped() bool { return t.dropped }

// CellGroup reports whether this table arbitrates updates per column
// (default_clock_group = 'cell') rather than per row.
func (t *Table) CellGroup() bool { return t.clockGroup == metadata.ClockGroupCell }

// HasCounters reports whether any active column is a declared counter
// column (sqlite/docs/DDL.md#counter-columns).
func (t *Table) HasCounters() bool { return t.hasCounters }

// ClockGroup returns the table's default clock group. Empty (pre-
// migration metadata) reads as 'row'.
func (t *Table) ClockGroup() string {
	if t.clockGroup == "" {
		return metadata.ClockGroupRow
	}
	return t.clockGroup
}

// Catalog is the in-memory mapping. Construct with LoadFromMeta.
// Reload pulls the latest catalog state after a DDL apply.
type Catalog struct {
	mu     sync.RWMutex
	sc     *metadata.Store
	byName map[string]*Table       // active tables only
	byID   map[crdt.TableID]*Table // active + dropped
	// hasCoordinated caches whether any active table has a coordinated
	// (CP) unique key, so the commit-time reserve fast-path can skip the
	// touch-buffer scan when no coordinated keys exist.
	hasCoordinated bool
	// schemaSeq mirrors meta.schema_seq as of the last Reload. The
	// producer stamps it into journal records at append so the drain
	// can decode each record under its capture-time layout.
	schemaSeq atomic.Uint64
}

// SchemaSeq returns the schema-chain position as of the last Reload.
func (c *Catalog) SchemaSeq() uint64 { return c.schemaSeq.Load() }

// AdvanceSchemaSeq records a metadata-only schema event without rebuilding the
// table maps.
func (c *Catalog) AdvanceSchemaSeq(seq uint64) {
	for {
		current := c.schemaSeq.Load()
		if current >= seq || c.schemaSeq.CompareAndSwap(current, seq) {
			return
		}
	}
}

// LoadFromMeta builds a Catalog from the metadata's DDL catalog
// tables. Returns an empty (zero-table) Catalog if the catalog is
// fresh.
func LoadFromMeta(sc *metadata.Store) (*Catalog, error) {
	c := &Catalog{sc: sc}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadForRuntime loads catalog identity for a normal SQLite runtime open.
func LoadForRuntime(app *sqlitebridge.Conn, sc *metadata.Store) (*Catalog, error) {
	return SeedFromSchema(app, sc)
}

// Reload re-reads the metadata catalog and rebuilds the in-memory maps.
// Producer DDL apply calls this after committing a metadata UPSERT so
// subsequent capture sees the new schema.
func (c *Catalog) Reload() error {
	if c.sc == nil {
		return errors.New("catalog: nil metadata (call LoadFromMeta)")
	}
	snap, err := c.sc.LoadCatalogSnapshot()
	if err != nil {
		return err
	}
	byName := map[string]*Table{}
	byID := map[crdt.TableID]*Table{}
	for _, te := range snap.Tables {
		t := &Table{Name: te.Name, ID: te.ID, dropped: te.State == metadata.StateDropped, clockGroup: te.DefaultClockGroup, historyReliable: true}
		byID[te.ID] = t
		if !t.dropped {
			byName[te.Name] = t
		}
	}
	for _, ce := range snap.Columns {
		t, ok := byID[ce.TableID]
		if !ok {
			continue
		}
		col := Column{
			Name:       ce.Name,
			ID:         ce.ColumnID,
			Ordinal:    ce.Ordinal,
			Collation:  ce.Collation,
			CreateSeq:  ce.CreateSeq,
			DropSeq:    ce.DropSeq,
			ClockGroup: ce.ClockGroup,
		}
		if ce.State == metadata.StateDropped {
			// A legacy drop clobbered name/ordinal/create_seq, so this
			// table's pre-drop layouts cannot be reconstructed.
			if ce.Name == "" {
				t.historyReliable = false
			}
			t.allColumns = append(t.allColumns, col)
			continue
		}
		if col.Counter() {
			t.hasCounters = true
		}
		t.Columns = append(t.Columns, col)
		t.allColumns = append(t.allColumns, col)
	}
	// uniqueMembers groups non-PK key rows by (TableID, KeyID) so we can
	// reassemble their member lists in ordinal order after the column
	// table is fully populated.
	type uniqMemberKey struct {
		t crdt.TableID
		k crdt.KeyID
	}
	uniqueMembers := map[uniqMemberKey][]metadata.KeyEntry{}
	uniqueDropped := map[uniqMemberKey]struct{}{}
	for _, ke := range snap.Keys {
		t, ok := byID[ke.TableID]
		if !ok {
			continue
		}
		if ke.KeyID == metadata.PKKeyID {
			if ke.State == metadata.StateDropped {
				continue
			}
			// Stamp PKPos onto the matching Column entry.
			for i, col := range t.Columns {
				if col.ID == ke.ColumnID {
					t.Columns[i].PKPos = ke.Ordinal + 1
				}
			}
			continue
		}
		k := uniqMemberKey{t: ke.TableID, k: ke.KeyID}
		if ke.State == metadata.StateDropped {
			// DropUniqueKey writes a single dropped marker; treat the
			// whole key as dropped even if other ordinals still show
			// active. Belt-and-braces against partial-drop bugs.
			uniqueDropped[k] = struct{}{}
			continue
		}
		uniqueMembers[k] = append(uniqueMembers[k], ke)
	}
	for k := range uniqueDropped {
		delete(uniqueMembers, k)
	}
	for _, t := range byID {
		// Sort Columns by Ordinal so iteration order matches schema.
		sortColumnsByOrdinal(t.Columns)
		t.PK = pkColumns(t.Columns)
		// History sorts (Ordinal, CreateSeq): a drop frees its ordinal
		// for the next ADD, so two entries can share one ordinal; their
		// live ranges are disjoint and liveAt picks the right one.
		sortColumnsByOrdinalThenSeq(t.allColumns)
	}
	for k, entries := range uniqueMembers {
		t, ok := byID[k.t]
		if !ok {
			continue
		}
		// Sort members by ordinal so the unique-key column list is in
		// declared order (matches the SQLite index column order).
		sortKeyEntriesByOrdinal(entries)
		// coordinated and predicate are key-group properties: all member
		// rows carry the same values, so the first entry is authoritative.
		uk := UniqueKey{KeyID: k.k, Coordinated: entries[0].Coordinated}
		if len(entries[0].Predicate) > 0 {
			pred, err := crdt.DecodeUniquePredicate(entries[0].Predicate)
			if err != nil {
				return fmt.Errorf("catalog: decode predicate for key %x: %w", k.k, err)
			}
			uk.Predicate = pred
		}
		for _, ke := range entries {
			col, ok := t.ColumnByID(ke.ColumnID)
			if !ok {
				// Column dropped or never landed; the unique key is
				// effectively dead. Skip the whole key — a partial
				// member list would mis-arbitrate.
				uk.Columns = nil
				break
			}
			uk.Columns = append(uk.Columns, col)
		}
		if len(uk.Columns) > 0 {
			t.UniqueKeys = append(t.UniqueKeys, uk)
		}
	}
	hasCoordinated := false
	for _, t := range byID {
		// Stable order on KeyID so iteration is deterministic.
		sortUniqueKeysByID(t.UniqueKeys)
		if !t.dropped {
			for _, uk := range t.UniqueKeys {
				if uk.Coordinated {
					hasCoordinated = true
				}
			}
		}
	}
	seq := uint64(0)
	if v, ok, err := c.sc.GetSchemaSeq(); err != nil {
		return fmt.Errorf("catalog: read schema_seq: %w", err)
	} else if ok {
		seq = v
	}
	c.mu.Lock()
	c.byName = byName
	c.byID = byID
	c.hasCoordinated = hasCoordinated
	c.schemaSeq.Store(seq)
	c.mu.Unlock()
	return nil
}

// TableAtSeq returns a view of t whose Columns/PK reflect the physical
// column layout at schema-chain position seq — what a journal record
// stamped with that seq was captured under. Returns (t, true) when the
// layout matches the current one, a synthetic read-only Table when it
// differs, and (nil, false) when this table's history cannot be
// reconstructed (a legacy drop tombstone lost its ordinal/create_seq).
func (c *Catalog) TableAtSeq(t *Table, seq uint64) (*Table, bool) {
	if seq >= c.SchemaSeq() {
		return t, true
	}
	live := make([]Column, 0, len(t.allColumns))
	for _, col := range t.allColumns {
		if col.liveAt(seq) {
			live = append(live, col)
		}
	}
	if len(live) == len(t.Columns) {
		same := true
		for i := range live {
			if live[i].ID != t.Columns[i].ID {
				same = false
				break
			}
		}
		if same {
			return t, true
		}
	}
	if !t.historyReliable {
		return nil, false
	}
	// Stamp PKPos/PKDefault from the current active columns (PK columns
	// cannot be dropped, so every PK member exists in both views).
	for i := range live {
		if cur, ok := t.ColumnByID(live[i].ID); ok {
			live[i].PKPos = cur.PKPos
			live[i].PKDefault = cur.PKDefault
		}
	}
	return &Table{
		Name:        t.Name,
		ID:          t.ID,
		Columns:     live,
		PK:          pkColumns(live),
		dropped:     t.dropped,
		clockGroup:  t.clockGroup,
		hasCounters: hasCounterColumns(live),
	}, true
}

// HasCoordinatedKeys reports whether any active table declares a
// coordinated (CP) unique key. The commit-time reserve path uses this to
// skip its touch-buffer scan when the schema has none.
func (c *Catalog) HasCoordinatedKeys() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hasCoordinated
}

// Table returns the catalog entry for an active table by name, or
// (nil, false). Dropped tables are not visible via this lookup.
func (c *Catalog) Table(name string) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.byName[name]
	return t, ok
}

// TableBytes is Table without the string allocation.
func (c *Catalog) TableBytes(name []byte) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.byName[string(name)]
	return t, ok
}

// TableByID returns the catalog entry for a stable TableID. Returns
// dropped tables too (callers must check Dropped()) so the apply path
// can deterministically skip late DML against tombstoned tables.
func (c *Catalog) TableByID(id crdt.TableID) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.byID[id]
	return t, ok
}

// Tables returns every active replicated table, in schema order.
func (c *Catalog) Tables() []*Table {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Table, 0, len(c.byName))
	for _, t := range c.byName {
		out = append(out, t)
	}
	return out
}

// Column returns the Column entry for (table, column), or (zero, false).
func (t *Table) Column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// ColumnByID returns the Column entry for a stable ColumnID.
func (t *Table) ColumnByID(id crdt.ColumnID) (Column, bool) {
	for _, c := range t.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// HistoricColumnByID resolves a ColumnID through the full column
// history, including dropped tombstones. Used by structural
// reconciliation, which must recover a dropped column's name to check
// whether the physical DROP actually reached app.db — the active-only
// ColumnByID reads an applied catalog drop as "column unknown". A
// legacy tombstone that lost its name still returns found with
// Name == ""; callers must treat that as unresolvable.
func (t *Table) HistoricColumnByID(id crdt.ColumnID) (Column, bool) {
	if c, ok := t.ColumnByID(id); ok {
		return c, true
	}
	for _, c := range t.allColumns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// ColumnIndexByID returns the Columns slice index for a stable
// ColumnID. Distinct from Column.Ordinal: ordinals can be sparse after
// a non-trailing DROP COLUMN (a drop tombstones its ordinal; survivors
// keep theirs), while slice indexes are always dense 0..len-1 — the
// shape SQL built by iterating Columns actually has.
func (t *Table) ColumnIndexByID(id crdt.ColumnID) (int, bool) {
	for i, c := range t.Columns {
		if c.ID == id {
			return i, true
		}
	}
	return 0, false
}

// SeedFromSchema is the bootstrap path: walk app.db's sqlite_master,
// allocate IDs for every replicated table/column/PK, and write them to
// the metadata. Used by syzy_init when an existing app.db already has
// schema; pre-existing rows acquire implicit (cl=0, base_hlc=0) clocks
// per ARCHITECTURE.md. No-op when the metadata already holds catalog
// rows. Returns the post-seed Catalog.
func SeedFromSchema(app *sqlitebridge.Conn, sc *metadata.Store) (*Catalog, error) {
	snap, err := sc.LoadCatalogSnapshot()
	if err != nil {
		return nil, err
	}
	if len(snap.Tables) > 0 {
		// Already seeded. Metadata carries stable identity and logical shape,
		// while native PK defaults live only in app.db; rebuild both before
		// callers install runtime allocators or accept writes.
		c, err := LoadFromMeta(sc)
		if err != nil {
			return nil, err
		}
		if err := c.RefreshPKDefaults(app); err != nil {
			return nil, err
		}
		return c, nil
	}
	names, err := tableNames(app)
	if err != nil {
		return nil, err
	}
	type seededTable struct {
		entry metadata.TableEntry
		cols  []metadata.ColumnEntry
		pk    []metadata.KeyEntry
	}
	var staged []seededTable
	built := map[string]*Table{}
	for _, name := range names {
		if !replicated(name) {
			continue
		}
		t, err := buildTable(app, name)
		if err != nil {
			return nil, fmt.Errorf("catalog: build %q: %w", name, err)
		}
		if len(t.PK) == 0 {
			return nil, fmt.Errorf("catalog: table %q has no PRIMARY KEY (replicated tables require one)", name)
		}
		// Counter columns require the admission checks and the cell
		// clock-group derivation that only the replicated DDL path
		// runs; adopting one silently as a row-LWW register would
		// change its merge semantics. Reject loudly.
		if col, ok := counterTypedColumn(app, name); ok {
			return nil, fmt.Errorf("catalog: table %q column %q: COUNTER columns must be created through replicated DDL, not adopted from a pre-existing schema", name, col)
		}
		built[name] = t
		st := seededTable{
			entry: metadata.TableEntry{
				ID:                t.ID,
				Name:              t.Name,
				State:             metadata.StateActive,
				DefaultClockGroup: metadata.ClockGroupRow,
				CreateSeq:         0, // implicit pre-cluster origin
			},
		}
		for _, col := range t.Columns {
			// LIMITATION: the seed path reads pragma_table_xinfo, which does
			// not expose collation, so an adopted column is recorded as
			// BINARY even if its DDL declared NOCASE/RTRIM. A coordinated
			// UNIQUE or partial-index predicate later created on such a
			// column would use BINARY where SQLite uses the real collation —
			// a divergence. Capturing it needs an rqlite/sql parse of the
			// table's sqlite_master SQL (as admission does); deferred because
			// it only bites the adopt + non-BINARY + unique/partial
			// intersection. Tables created through syzy DDL capture collation
			// correctly at admission.
			st.cols = append(st.cols, metadata.ColumnEntry{
				TableID:    t.ID,
				ColumnID:   col.ID,
				Name:       col.Name,
				Ordinal:    col.Ordinal,
				State:      metadata.StateActive,
				ClockGroup: metadata.ClockGroupRow,
				CreateSeq:  0,
			})
		}
		for _, pk := range t.PK {
			st.pk = append(st.pk, metadata.KeyEntry{
				TableID:   t.ID,
				KeyID:     metadata.PKKeyID,
				ColumnID:  pk.ID,
				Ordinal:   pk.PKPos - 1,
				State:     metadata.StateActive,
				CreateSeq: 0,
			})
		}
		staged = append(staged, st)
	}
	if err := sc.WithTx(func(tx *metadata.Tx) error {
		for _, st := range staged {
			if err := tx.UpsertTable(st.entry); err != nil {
				return err
			}
			for _, col := range st.cols {
				if err := tx.UpsertColumn(col); err != nil {
					return err
				}
			}
			for _, pk := range st.pk {
				if err := tx.UpsertKey(pk); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("catalog: seed metadata: %w", err)
	}
	c, err := LoadFromMeta(sc)
	if err != nil {
		return nil, err
	}
	c.refreshPKDefaultsFromBuilt(built)
	return c, nil
}

// refreshPKDefaultsFromBuilt republishes each catalog table with the
// PKDefault values carried by a freshly-built counterpart. Used by paths
// that load via the metadata (which doesn't persist PKDefault) but have
// an app.db conn handy to derive it.
//
// Copy-on-write, not in-place patching. Readers take a *Table from
// Table()/TableByID()/Tables() and release c.mu before reading its
// Columns, so holding the write lock here stops nothing: writing into a
// published table races every reader that already has the pointer, and
// a Column is several words wide, so a reader can see a new PKDefault
// paired with a stale Name. Building replacements and swapping the map
// entries keeps a published *Table immutable for its whole life — the
// invariant Reload already depends on.
func (c *Catalog) refreshPKDefaultsFromBuilt(built map[string]*Table) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, src := range built {
		old, ok := c.byName[name]
		if !ok {
			continue
		}
		defs := make(map[string]PKDefault, len(src.Columns))
		for _, col := range src.Columns {
			defs[col.Name] = col.PKDefault
		}
		next := *old // shallow copy; the patched slices are replaced below
		next.Columns = withPKDefaults(old.Columns, defs)
		next.PK = withPKDefaults(old.PK, defs)
		c.byName[name] = &next
		// byName and byID hand out the same pointer per table; leaving
		// byID on the old one would fork the two views apart.
		if c.byID[next.ID] == old {
			c.byID[next.ID] = &next
		}
	}
}

// withPKDefaults returns a copy of cols with each PKDefault replaced by
// defs[Name] (zero when absent, matching a fresh build's own result).
// The remaining fields — and the caller's slice — are untouched.
func withPKDefaults(cols []Column, defs map[string]PKDefault) []Column {
	out := make([]Column, len(cols))
	copy(out, cols)
	for i := range out {
		out[i].PKDefault = defs[out[i].Name]
	}
	return out
}

// RebuildWithPKDefaults reloads the catalog from metadata and then
// repopulates PKDefault function pointers from app.db's
// pragma_table_xinfo. Used by both the producer (resolveLocalDDL) and
// the broker (after schema catchup applies) — the two callsites had
// identical wrappers before this was lifted.
func (c *Catalog) RebuildWithPKDefaults(app *sqlitebridge.Conn) error {
	if err := c.Reload(); err != nil {
		return fmt.Errorf("catalog.Reload: %w", err)
	}
	if err := c.RefreshPKDefaults(app); err != nil {
		return fmt.Errorf("catalog.RefreshPKDefaults: %w", err)
	}
	return nil
}

// RefreshPKDefaults re-derives PKDefault for every active replicated
// table by walking app.db's pragma_table_xinfo. Meta-only Reload
// loses this annotation; call RefreshPKDefaults after a Reload to
// repopulate it.
//
// Safe to call concurrently with table-info reads: it republishes
// replacement *Table values rather than editing the ones readers hold.
// A reader that captured a pointer first keeps a coherent view and
// simply misses this refresh, the same as any Reload that lands after
// it looked.
func (c *Catalog) RefreshPKDefaults(app *sqlitebridge.Conn) error {
	c.mu.RLock()
	names := make([]string, 0, len(c.byName))
	for name := range c.byName {
		names = append(names, name)
	}
	c.mu.RUnlock()

	built := make(map[string]*Table, len(names))
	for _, name := range names {
		t, err := buildTable(app, name)
		if err != nil {
			return fmt.Errorf("catalog: refresh PKDefaults for %q: %w", name, err)
		}
		built[name] = t
	}
	c.refreshPKDefaultsFromBuilt(built)
	return nil
}

// AllocTableID returns a fresh random TableID. Callers writing new
// CatalogOp entries (DDL apply) use this to allocate stable IDs.
func AllocTableID() crdt.TableID {
	return corecatalog.AllocTableID()
}

// AllocColumnID returns a fresh random ColumnID.
func AllocColumnID() crdt.ColumnID {
	return corecatalog.AllocColumnID()
}

// replicated reports whether name is eligible for replication. Mirrors
// the spec rules: skip sqlite_* and _*-prefixed tables.
func replicated(name string) bool {
	if strings.HasPrefix(name, "sqlite_") || strings.HasPrefix(name, "_") {
		return false
	}
	return true
}

// tableNames lists the user-visible tables in `main`.
func tableNames(conn *sqlitebridge.Conn) ([]string, error) {
	stmt, _, err := conn.Prepare(`
		SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()

	var out []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		out = append(out, stmt.ColumnText(0))
	}
}

// counterTypedColumn reports the first column of table name whose
// declared type carries the COUNTER token, if any. Schema seeding uses
// it to reject counter columns that bypassed replicated DDL.
func counterTypedColumn(conn *sqlitebridge.Conn, name string) (string, bool) {
	stmt, _, err := conn.Prepare(`SELECT name, type FROM pragma_table_xinfo(?)`)
	if err != nil {
		return "", false
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		return "", false
	}
	for {
		hasRow, err := stmt.Step()
		if err != nil || !hasRow {
			return "", false
		}
		if metadata.CounterType(stmt.ColumnText(1)) {
			return stmt.ColumnText(0), true
		}
	}
}

// buildTable reads pragma_table_xinfo and assembles a Table whose IDs
// are derived deterministically from names. Used by the seed path so
// two nodes that bootstrap from identical app.db schemas produce
// identical IDs without coordination. New tables introduced via
// replicated DDL (the SchemaLog path) allocate fresh random IDs;
// only the genesis bootstrap is name-derived.
func buildTable(conn *sqlitebridge.Conn, name string) (*Table, error) {
	stmt, _, err := conn.Prepare(`SELECT cid, name, pk, hidden, dflt_value FROM pragma_table_xinfo(?)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		return nil, err
	}

	t := &Table{Name: name, ID: deriveTableID(name)}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		ordinal := int(stmt.ColumnInt64(0))
		colName := stmt.ColumnText(1)
		pkPos := int(stmt.ColumnInt64(2))
		hidden := int(stmt.ColumnInt64(3))
		// table_xinfo's `hidden` is non-zero for generated, virtual, or
		// vtable-shadow columns. Skip them.
		if hidden != 0 {
			continue
		}
		var pkDefault PKDefault
		if !stmt.ColumnIsNull(4) {
			pkDefault = ClassifyPKDefault(stmt.ColumnText(4))
		}
		col := Column{
			Name:      colName,
			ID:        deriveColumnID(t.ID, colName),
			Ordinal:   ordinal,
			PKPos:     pkPos,
			PKDefault: pkDefault,
		}
		t.Columns = append(t.Columns, col)
		if pkPos > 0 {
			t.PK = append(t.PK, col)
		}
	}
	sortPKByPos(t.PK)
	return t, nil
}

func deriveTableID(name string) crdt.TableID {
	sum := sha256.Sum256([]byte("syzy/table:" + name))
	var id crdt.TableID
	copy(id[:], sum[:16])
	return id
}

func deriveColumnID(table crdt.TableID, colName string) crdt.ColumnID {
	h := sha256.New()
	h.Write([]byte("syzy/column:"))
	h.Write(table[:])
	h.Write([]byte(":"))
	h.Write([]byte(colName))
	sum := h.Sum(nil)
	var id crdt.ColumnID
	copy(id[:], sum[:16])
	return id
}

func sortColumnsByOrdinal(cols []Column) {
	for i := 1; i < len(cols); i++ {
		for j := i; j > 0 && cols[j-1].Ordinal > cols[j].Ordinal; j-- {
			cols[j-1], cols[j] = cols[j], cols[j-1]
		}
	}
}

func sortColumnsByOrdinalThenSeq(cols []Column) {
	less := func(a, b Column) bool {
		if a.Ordinal != b.Ordinal {
			return a.Ordinal < b.Ordinal
		}
		return a.CreateSeq < b.CreateSeq
	}
	for i := 1; i < len(cols); i++ {
		for j := i; j > 0 && less(cols[j], cols[j-1]); j-- {
			cols[j-1], cols[j] = cols[j], cols[j-1]
		}
	}
}

func hasCounterColumns(cols []Column) bool {
	for _, c := range cols {
		if c.Counter() {
			return true
		}
	}
	return false
}

func pkColumns(cols []Column) []Column {
	var pk []Column
	for _, c := range cols {
		if c.PKPos > 0 {
			pk = append(pk, c)
		}
	}
	sortPKByPos(pk)
	return pk
}

func sortPKByPos(pk []Column) {
	for i := 1; i < len(pk); i++ {
		for j := i; j > 0 && pk[j-1].PKPos > pk[j].PKPos; j-- {
			pk[j-1], pk[j] = pk[j], pk[j-1]
		}
	}
}

func sortKeyEntriesByOrdinal(entries []metadata.KeyEntry) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j-1].Ordinal > entries[j].Ordinal; j-- {
			entries[j-1], entries[j] = entries[j], entries[j-1]
		}
	}
}

func sortUniqueKeysByID(keys []UniqueKey) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keyIDLess(keys[j].KeyID, keys[j-1].KeyID); j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
}

func keyIDLess(a, b crdt.KeyID) bool {
	for i := 0; i < 16; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
