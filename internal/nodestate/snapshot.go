package nodestate

import (
	"maps"
	"slices"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
)

// RowEntry is one (table, pk) row_clock entry in a snapshot. Mirrors
// metadata.RowClockEntry but uses the cache's PKBlob form directly.
type RowEntry struct {
	Table crdt.TableID
	PK    crdt.PKBlob
	CL    uint64
	Base  crdt.Stamp
}

// CellEntry is one (table, pk, col) cell_clock change in a snapshot.
// Stamp present = UPSERT; Stamp.Origin == 0 && Stamp.Clock == zero with
// !Present = DELETE. Present is encoded explicitly so the deletion of a
// genuine zero-stamp entry round-trips correctly.
type CellEntry struct {
	Table   crdt.TableID
	PK      crdt.PKBlob
	Column  crdt.ColumnID
	Stamp   crdt.Stamp
	Present bool
}

// ClearedRow names a row whose cell_clock entries should be deleted
// before any UPSERTs in the same snapshot are applied. CL bumps and
// explicit ClearCellsForRow calls produce these. Epoch is the row's
// clear-counter at capture time; ClearSnapshotDirty uses it to detect
// concurrent re-clears that happened during metadata I/O.
type ClearedRow struct {
	Table crdt.TableID
	PK    crdt.PKBlob
	Epoch uint64
}

// Snapshot is the immutable view of cache state at a point in time. The
// snapshotter takes one of these under the cache mutex, then writes it
// to the metadata without further coordination with the hot path.
type Snapshot struct {
	// SenderNextSeq is the per-origin next-to-allocate sequence map.
	// In incremental mode this contains only entries dirtied since the
	// last snapshot.
	SenderNextSeq map[crdt.Origin]crdt.Seq
	HLCLast       crdt.Clock
	// MetaDirty is true when HLCLast, Frontier, AppliedGaps,
	// SenderNextSeq, or Markers need to be written. The maps below are
	// still captured as full copies so ClearSnapshotDirty can compare
	// them after metadata I/O.
	MetaDirty bool

	// Rows is the set of row_clock entries to write. In incremental mode
	// this contains only entries dirtied since the last snapshot; in
	// full-rebuild mode (e.g. the very first snapshot post-recovery)
	// it's the entire map.
	Rows []RowEntry

	// Frontier is the per-origin contiguously-applied head + hlc.
	Frontier map[crdt.Origin]FrontierEntry

	// AppliedGaps[origin] is the set of seqs above frontier[origin] that
	// have been applied non-contiguously. Stored so recovery can
	// reconstruct exactly the same cache.
	AppliedGaps map[crdt.Origin]crdt.SeqSet

	// Markers is the per-origin journal offset reflected by THIS
	// snapshot. Recovery replays each origin's journal from
	// Markers[origin] to head.
	Markers map[crdt.Origin]journal.Offset

	// Forgotten lists origins evicted by origin GC since the last
	// snapshot. The snapshotter DELETEs each one's frontier row (an
	// AdvanceFrontier upsert can't remove it); their applied-gaps and
	// marker entries drop out naturally because those are written as
	// full-map replaces.
	Forgotten []crdt.Origin

	// ClearedRows lists rows whose cell_clock entries must be DELETEd
	// before Cells UPSERTs in the same snapshot. Order: clear first,
	// then upsert.
	ClearedRows []ClearedRow

	// Cells is the set of cell_clock changes (UPSERT or DELETE)
	// dirtied since the last snapshot.
	Cells []CellEntry
}

// WriteSnapshot folds one Snapshot's contents into an open metadata
// transaction: row/cell clocks always, and — when MetaDirty — frontier,
// sender seqs, HLC, applied gaps, and markers. It is the shared write body of
// snapshot and recovery paths that must persist cache state atomically.
func WriteSnapshot(tx *metadata.Tx, snap Snapshot) error {
	for _, r := range snap.Rows {
		if err := tx.PutRowClock(r.Table, r.PK, metadata.RowClockEntry{CL: r.CL, Base: r.Base}); err != nil {
			return err
		}
	}
	// Cell-clock writes: clear-row first (CL bumps and explicit resets),
	// then UPSERT/DELETE per cell. Order matters: a row that has been
	// cleared and immediately re-populated must end the snapshot with the
	// new entries, not the cleared set.
	for _, cr := range snap.ClearedRows {
		if err := tx.DeleteCellClocksForRow(cr.Table, cr.PK); err != nil {
			return err
		}
	}
	for _, ce := range snap.Cells {
		if !ce.Present {
			if err := tx.DeleteCellClock(ce.Table, ce.PK, ce.Column); err != nil {
				return err
			}
			continue
		}
		if err := tx.PutCellClock(ce.Table, ce.PK, ce.Column, ce.Stamp); err != nil {
			return err
		}
	}
	if !snap.MetaDirty {
		return nil
	}
	for o, f := range snap.Frontier {
		if err := tx.AdvanceFrontier(o, f.LastSeq, f.LastHLC); err != nil {
			return err
		}
	}
	// Origin GC: delete evicted origins' frontier rows. Disjoint
	// from snap.Frontier (EvictOrigin/markApplied keep frontier and
	// forgotten mutually exclusive), so no advance+delete conflict.
	for _, o := range snap.Forgotten {
		if err := tx.DeleteFrontier(o); err != nil {
			return err
		}
	}
	for o, next := range snap.SenderNextSeq {
		if err := tx.PutSenderSeq(o, next); err != nil {
			return err
		}
	}
	if err := tx.SetHLCLast(snap.HLCLast); err != nil {
		return err
	}
	if err := tx.SetAppliedGaps(snap.AppliedGaps); err != nil {
		return err
	}
	markersU64 := make(map[crdt.Origin]uint64, len(snap.Markers))
	for o, off := range snap.Markers {
		markersU64[o] = uint64(off)
	}
	return tx.SetSnapshotMarkers(markersU64)
}

// SnapshotIncremental returns a Snapshot covering only the keys
// dirtied since the last call (caller-controlled by the snapshotter,
// which clears dirty after a successful write). Frontier + AppliedGaps
// + Markers are always full copies — they're small.
func (c *Cache) SnapshotIncremental() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := Snapshot{
		SenderNextSeq: make(map[crdt.Origin]crdt.Seq, len(c.dirtySenderSeqs)),
		HLCLast:       c.hlcLast,
		MetaDirty:     c.metaDirty,
		Frontier:      make(map[crdt.Origin]FrontierEntry, len(c.frontier)),
		AppliedGaps:   make(map[crdt.Origin]crdt.SeqSet, len(c.appliedGaps)),
		Markers:       make(map[crdt.Origin]journal.Offset, len(c.snapshotMarkers)),
	}
	for o := range c.dirtySenderSeqs {
		snap.SenderNextSeq[o] = c.senderNextSeq[o]
	}
	for k := range c.dirtyRows {
		st := c.rowClock[k]
		snap.Rows = append(snap.Rows, RowEntry{
			Table: k.t, PK: crdt.PKBlob(k.pk), CL: st.CL, Base: st.Base,
		})
	}
	for k, ep := range c.dirtyClearedRows {
		snap.ClearedRows = append(snap.ClearedRows, ClearedRow{Table: k.t, PK: crdt.PKBlob(k.pk), Epoch: ep})
	}
	for k, cols := range c.dirtyCells {
		st := c.rowClock[k]
		for col := range cols {
			ce := CellEntry{Table: k.t, PK: crdt.PKBlob(k.pk), Column: col}
			if s, ok := st.Cells[col]; ok {
				ce.Stamp = s
				ce.Present = true
			}
			snap.Cells = append(snap.Cells, ce)
		}
	}
	for o, f := range c.frontier {
		snap.Frontier[o] = f
	}
	for o, g := range c.appliedGaps {
		if g != nil {
			snap.AppliedGaps[o] = g.Clone()
		}
	}
	for o, off := range c.snapshotMarkers {
		snap.Markers[o] = off
	}
	if len(c.forgottenOrigins) > 0 {
		snap.Forgotten = make([]crdt.Origin, 0, len(c.forgottenOrigins))
		for o := range c.forgottenOrigins {
			snap.Forgotten = append(snap.Forgotten, o)
		}
	}
	return snap
}

// ClearSnapshotDirty drops only dirty trackers covered by snap; called by
// the snapshotter after a successful metadata write. Snapshot I/O happens
// without holding c.mu, so commits/applies may dirty cache entries while
// the metadata transaction is in flight. Those entries must remain dirty
// unless their current in-memory value still matches the value snap wrote.
func (c *Cache) ClearSnapshotDirty(snap Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range snap.Rows {
		k := rowKey{t: r.Table, pk: string(r.PK)}
		if st, ok := c.rowClock[k]; ok && rowClockCoveredBy(st, r) {
			delete(c.dirtyRows, k)
		}
	}
	for o, next := range snap.SenderNextSeq {
		if c.senderNextSeq[o] == next {
			delete(c.dirtySenderSeqs, o)
		}
	}
	for o, f := range snap.Frontier {
		if c.frontier[o] == f {
			delete(c.dirtyFrontiers, o)
		}
		// Advance the persisted-frontier ledger regardless of the dirty
		// re-check: f itself is durably written either way.
		if f.LastSeq > c.persistedFrontier[o] {
			c.prevPersistedFrontier[o] = c.persistedFrontier[o]
			c.persistedFrontier[o] = f.LastSeq
		}
	}
	// Clear a persisted eviction only if the origin wasn't re-admitted
	// (a straggler apply) during metadata I/O; a re-admit re-adds it to
	// the frontier (and markApplied already removed it from forgotten),
	// so the next snapshot re-advances the row.
	for _, o := range snap.Forgotten {
		if _, back := c.frontier[o]; !back {
			delete(c.forgottenOrigins, o)
		}
	}
	for _, cr := range snap.ClearedRows {
		k := rowKey{t: cr.Table, pk: string(cr.PK)}
		// Only drop the marker if no NEW ClearCellsForRow fired during
		// metadata I/O. The epoch counter increments on each clear, so a
		// match means we've covered the latest one.
		if c.dirtyClearedRows[k] == cr.Epoch {
			delete(c.dirtyClearedRows, k)
		}
	}
	for _, ce := range snap.Cells {
		k := rowKey{t: ce.Table, pk: string(ce.PK)}
		st := c.rowClock[k]
		var cur crdt.Stamp
		curPresent := false
		if st.Cells != nil {
			cur, curPresent = st.Cells[ce.Column]
		}
		// Clear the dirty bit only when the in-memory state still
		// matches what we just wrote. A concurrent re-mutation must
		// remain dirty for the next snapshot.
		if curPresent == ce.Present && cur == ce.Stamp {
			if cells, ok := c.dirtyCells[k]; ok {
				delete(cells, ce.Column)
				if len(cells) == 0 {
					delete(c.dirtyCells, k)
				}
			}
		}
	}
	if len(c.dirtyRows) == 0 &&
		len(c.dirtyFrontiers) == 0 &&
		len(c.dirtySenderSeqs) == 0 &&
		len(c.dirtyCells) == 0 &&
		len(c.dirtyClearedRows) == 0 &&
		len(c.forgottenOrigins) == 0 &&
		c.hlcLast == snap.HLCLast &&
		maps.Equal(c.frontier, snap.Frontier) &&
		appliedGapsCoveredBy(c.appliedGaps, snap.AppliedGaps) &&
		maps.Equal(c.snapshotMarkers, snap.Markers) {
		c.metaDirty = false
	}
}

func rowClockCoveredBy(st crdt.RowState, r RowEntry) bool {
	return st.CL == r.CL && st.Base == r.Base
}

func appliedGapsCoveredBy(cur map[crdt.Origin]*crdt.SeqSet, snap map[crdt.Origin]crdt.SeqSet) bool {
	if len(cur) != len(snap) {
		return false
	}
	for o, g := range cur {
		sg, ok := snap[o]
		if !ok {
			return false
		}
		var ranges []crdt.SeqRange
		if g != nil {
			ranges = g.Ranges()
		}
		if !slices.Equal(ranges, sg.Ranges()) {
			return false
		}
	}
	return true
}

// LoadFromMeta seeds the Cache from metadata storage (frontier,
// row_clock, sender_seq table, hlc_last meta). Recovery callers
// should invoke this once after construction; further changes go
// through the regular cache APIs.
func (c *Cache) LoadFromMeta(sc *metadata.Store) error {
	seqs, err := sc.SenderSeqs()
	if err != nil {
		return err
	}
	hlc, _, err := sc.GetHLCLast()
	if err != nil {
		return err
	}
	front, err := sc.Frontier()
	if err != nil {
		return err
	}
	rows, err := sc.AllRowClocks()
	if err != nil {
		return err
	}
	cells, err := sc.AllCellClocks()
	if err != nil {
		return err
	}
	gaps, err := sc.GetAppliedGaps()
	if err != nil {
		return err
	}
	markersU64, err := sc.GetSnapshotMarkers()
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for o, s := range seqs {
		c.senderNextSeq[o] = s
	}
	c.hlcLast = hlc
	for o, f := range front {
		c.frontier[o] = FrontierEntry{LastSeq: f.LastSeq, LastHLC: f.LastHLC}
	}
	for _, r := range rows {
		c.rowClock[rowKey{t: r.Table, pk: string(r.PK)}] = crdt.RowState{CL: r.CL, Base: r.Base}
	}
	for _, ce := range cells {
		k := rowKey{t: ce.Table, pk: string(ce.PK)}
		st := c.rowClock[k]
		if st.Cells == nil {
			st.Cells = map[crdt.ColumnID]crdt.Stamp{}
		}
		st.Cells[ce.Column] = ce.Stamp
		c.rowClock[k] = st
	}
	for o, gs := range gaps {
		g := gs.Clone()
		c.appliedGaps[o] = &g
	}
	for o, off := range markersU64 {
		c.snapshotMarkers[o] = journal.Offset(off)
	}
	clear(c.dirtyRows)
	clear(c.dirtyFrontiers)
	clear(c.dirtyCells)
	clear(c.dirtyClearedRows)
	c.metaDirty = false
	return nil
}
