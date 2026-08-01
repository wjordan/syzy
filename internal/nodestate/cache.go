// Package nodestate holds the in-memory CRDT state shared between the
// producer (local commits) and the broker (inbound applies). One Cache
// per node; all callers go through it for sender_next_seq, hlc_last,
// per-origin frontier + applied_gaps, and per-row row_clock state.
//
// The cache is the runtime source of truth. Persistence is handled by a
// separate Snapshotter that periodically writes dirty state to the
// metadata; recovery seeds the cache from the metadata snapshot and
// then replays journal records past the snapshot markers.
package nodestate

import (
	"sync"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// rowKey identifies one (table, pk) pair in the row_clock map. PK bytes
// are stringified so the map can use them as a key.
type rowKey struct {
	t  crdt.TableID
	pk string
}

// FrontierEntry is the contiguously-applied head per origin plus the
// HLC of the last record at that head.
type FrontierEntry struct {
	LastSeq crdt.Seq
	LastHLC crdt.Clock
}

// Cache holds the runtime CRDT state. Single mutex covers all fields:
// the spec calls for one locking domain so that frontier + row_clock +
// applied_gaps update atomically with each apply. Hot-path callers
// hold the lock briefly (a few microseconds of in-memory work).
type Cache struct {
	self crdt.Origin

	mu sync.Mutex

	// Per-origin: next seq to assign for a local commit on that origin,
	// and the HLC of our last commit on any origin. The map is sparse —
	// origins only get an entry once they've allocated. Daemons in the
	// loadable-extension model carry one entry per writer-process
	// origin they drain; in-process single-writer mode carries exactly
	// one entry (self).
	senderNextSeq map[crdt.Origin]crdt.Seq
	hlcLast       crdt.Clock

	// dirtySenderSeqs tracks which origin entries in senderNextSeq have
	// been touched since the last snapshot. Snapshotter writes only the
	// dirty origins.
	dirtySenderSeqs map[crdt.Origin]struct{}

	// Per-row LWW state. Lookup keyed by (table, pk). Live across all
	// origins — a write from any origin that wins LWW updates this map.
	rowClock map[rowKey]crdt.RowState

	// Per-origin contiguous frontier + non-contiguous gap tracker.
	// applied_gaps tracks seqs that we've applied above the contiguous
	// frontier; SeqSet.PromoteContiguous folds them in as the gaps fill.
	frontier    map[crdt.Origin]FrontierEntry
	appliedGaps map[crdt.Origin]*crdt.SeqSet

	// Per-origin journal-offset markers for the last persisted snapshot.
	// snapshotMarkers[o] = offset in origin o's journal that the metadata
	// snapshot reflects through. The snapshotter writes these alongside
	// each checkpoint; recovery uses them to bound journal replay.
	snapshotMarkers map[crdt.Origin]journal.Offset

	// dirty tracks which keys have been touched since the last snapshot.
	// The snapshotter consults these to write only changed entries.
	dirtyRows      map[rowKey]struct{}
	dirtyFrontiers map[crdt.Origin]struct{}

	// Per-cell dirty tracking. dirtyCells[k][col] means that cell
	// either appeared, changed, or disappeared since the last
	// snapshot; the snapshotter resolves UPSERT vs DELETE by looking
	// up the cell's current presence in rowClock[k].Cells.
	// dirtyClearedRows lists rows whose entire cell-clock set must be
	// dropped first (CL bump or explicit reset). Cells dirtied AFTER
	// the clear stay in dirtyCells and become UPSERTs on top. Each
	// clear bumps the row's clear-epoch so ClearSnapshotDirty can
	// distinguish "concurrent re-clear during metadata I/O" from the
	// no-op case.
	dirtyCells       map[rowKey]map[crdt.ColumnID]struct{}
	dirtyClearedRows map[rowKey]uint64

	// forgottenOrigins are origins evicted by origin GC (EvictOrigin)
	// since the last snapshot, whose frontier row the next snapshot must
	// DELETE from metadata (AdvanceFrontier only upserts, so an evicted
	// origin would otherwise reload on restart). Kept separate from the
	// dirty sets because the persist action is a delete, not an upsert.
	forgottenOrigins map[crdt.Origin]struct{}

	// persistedFrontier / prevPersistedFrontier track each origin's
	// contiguous frontier head as of the last and second-to-last
	// successfully persisted snapshot passes. PersistedFrontierBound
	// returns the older of the two — the conservative bound below which
	// a counter applied-marker (_syzy_applied, sqlite/docs/DDL.md#counter-columns)
	// may be pruned: any restorable (app.db, metadata) pairing is at
	// most one snapshot pass apart, so lagging one extra pass keeps a
	// marker alive for every seq an older paired frontier could miss.
	persistedFrontier     map[crdt.Origin]crdt.Seq
	prevPersistedFrontier map[crdt.Origin]crdt.Seq

	metaDirty bool // senderNextSeq / hlcLast / appliedGaps / markers / forgotten changed
}

// New returns an empty Cache for node self. Callers should immediately
// LoadFromMeta to seed it before use.
func New(self crdt.Origin) *Cache {
	return &Cache{
		self:             self,
		senderNextSeq:    map[crdt.Origin]crdt.Seq{},
		dirtySenderSeqs:  map[crdt.Origin]struct{}{},
		rowClock:         map[rowKey]crdt.RowState{},
		frontier:         map[crdt.Origin]FrontierEntry{},
		appliedGaps:      map[crdt.Origin]*crdt.SeqSet{},
		snapshotMarkers:  map[crdt.Origin]journal.Offset{},
		dirtyRows:        map[rowKey]struct{}{},
		dirtyFrontiers:   map[crdt.Origin]struct{}{},
		dirtyCells:       map[rowKey]map[crdt.ColumnID]struct{}{},
		dirtyClearedRows: map[rowKey]uint64{},
		forgottenOrigins: map[crdt.Origin]struct{}{},

		persistedFrontier:     map[crdt.Origin]crdt.Seq{},
		prevPersistedFrontier: map[crdt.Origin]crdt.Seq{},
	}
}

// PersistedFrontierBound returns origin's frontier head as of the
// second-to-last persisted snapshot pass (0 if fewer than two passes
// have covered it). Counter applied-markers at or below this seq are
// safe to prune — see the field comment on prevPersistedFrontier.
func (c *Cache) PersistedFrontierBound(origin crdt.Origin) crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prevPersistedFrontier[origin]
}

// MarkerPruneBound is the highest seq an apply may prune counter
// applied-markers up to: whatever PersistedFrontierBound already covers,
// but never the seq being certified by the same transaction.
//
// A forced retry of a quarantined changeset runs after the frontier has
// advanced past its seq, so an uncapped bound would delete the
// exactly-once certificate the transaction just wrote — and a later
// redelivery would contribute a second time. Shared by both engines'
// apply paths, which prune identically.
func MarkerPruneBound(persisted, seq crdt.Seq) crdt.Seq {
	if persisted >= seq {
		return seq - 1
	}
	return persisted
}

// Self returns this node's origin id. Set at construction.
func (c *Cache) Self() crdt.Origin { return c.self }

// StampHLC stamps wall using the standard HLC rule (advance to wall, or
// bump the logical counter if wall is not strictly greater than
// hlcLast). Returns the assigned Clock and updates hlcLast. Caller
// holds nothing; the Cache mutex is grabbed internally.
func (c *Cache) StampHLC(wall int64) crdt.Clock {
	c.mu.Lock()
	defer c.mu.Unlock()
	clk := crdt.Clock{WallTime: wall}
	if !c.hlcLast.Less(clk) {
		clk = crdt.Clock{WallTime: c.hlcLast.WallTime, Logical: c.hlcLast.Logical + 1}
	}
	c.hlcLast = clk
	c.metaDirty = true
	return clk
}

// HLCLast returns the most-recent HLC stamped by StampHLC or applied
// from a remote record (applies update hlc_last to MAX of incoming).
func (c *Cache) HLCLast() crdt.Clock {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hlcLast
}

// ObserveHLC advances hlcLast to at least clk (the same MAX-merge an apply
// performs), without allocating a new logical tick. Self-log recovery replay
// uses it to restore hlcLast to cover an exact stamp from a logged changeset,
// so a later local StampHLC stays monotonic above it. Idempotent and
// order-independent (a max).
func (c *Cache) ObserveHLC(clk crdt.Clock) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hlcLast.Less(clk) {
		c.hlcLast = clk
		c.metaDirty = true
	}
}

// AllocSelfSeq returns and consumes the next sender seq for origin.
// Monotonic per origin; caller is the drainer/sink for that origin.
// Daemons in the multi-origin model call this for each origin whose
// journal they drain.
func (c *Cache) AllocSelfSeq(origin crdt.Origin) crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	seq := c.senderNextSeq[origin]
	if seq == 0 {
		seq = 1
	}
	c.senderNextSeq[origin] = seq + 1
	c.dirtySenderSeqs[origin] = struct{}{}
	c.metaDirty = true
	return seq
}

// ObserveSelfSeq ensures the next self seq for origin is at least seq+1.
// Self-log recovery replay uses it to restore the seq counter from logged
// changesets' Dots, so re-derived later commits never reuse a shipped seq.
// Idempotent (a max).
func (c *Cache) ObserveSelfSeq(origin crdt.Origin, seq crdt.Seq) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if next := seq + 1; next > c.senderNextSeq[origin] {
		c.senderNextSeq[origin] = next
		c.dirtySenderSeqs[origin] = struct{}{}
		c.metaDirty = true
	}
}

// SenderNextSeq returns (without consuming) the next seq AllocSelfSeq
// would return for origin. Returns 1 if origin has never been
// allocated. Used by the snapshotter and the broker's gap planner.
func (c *Cache) SenderNextSeq(origin crdt.Origin) crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	seq := c.senderNextSeq[origin]
	if seq == 0 {
		return 1
	}
	return seq
}

// SenderNextSeqAll returns a copy of every (origin → next-seq) entry.
// Used by the snapshotter to write back all dirty origins in one tx.
func (c *Cache) SenderNextSeqAll() map[crdt.Origin]crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[crdt.Origin]crdt.Seq, len(c.senderNextSeq))
	for o, s := range c.senderNextSeq {
		out[o] = s
	}
	return out
}

// RowState returns the current per-row state, or zero if unseen.
func (c *Cache) RowState(table crdt.TableID, pk crdt.PKBlob) crdt.RowState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rowClock[rowKey{t: table, pk: string(pk)}]
}

// PutRowState records the new (CL, Base) for (table, pk). When st.CL
// strictly exceeds the row's current CL (resurrection or tombstone
// bump), every prior-generation fine-grained override is implicitly
// tombstoned per CRDT.md#causal-length-cl, so Cells/Ranges are cleared
// and a row-level cell_clock delete is queued for the next snapshot
// when a prior generation may have stored cell_clock rows. When CL is
// unchanged, existing Cells are preserved unless st.Cells is non-nil.
func (c *Cache) PutRowState(table crdt.TableID, pk crdt.PKBlob, st crdt.RowState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := rowKey{t: table, pk: string(pk)}
	prev := c.rowClock[k]
	// LWW monotonicity (invariant 7 plus stamp order at equal CL):
	// writers race here — the broker's apply path and the producer
	// drain both publish row states, and the drain's view can lag an
	// interleaved inbound apply. Never regress; the dominant state
	// wins regardless of arrival order. Returns whether st landed so
	// callers can gate side effects (cell clears) on the same outcome.
	if prev.CL > st.CL || (prev.CL == st.CL && !prev.DominatedBy(st.CL, st.Base)) {
		return false
	}
	switch {
	case st.CL > prev.CL:
		st.Cells = nil
		st.Ranges = nil
		if prev.CL != 0 || len(prev.Cells) > 0 || c.dirtyClearedRows[k] != 0 {
			c.dirtyClearedRows[k]++
		}
		delete(c.dirtyCells, k)
	case st.Cells == nil:
		st.Cells = prev.Cells
	}
	c.rowClock[k] = st
	c.dirtyRows[k] = struct{}{}
	return true
}

// PutCellStamp upserts one (table, pk, col) cell-clock override at
// stamp. Marks the cell dirty so the next snapshot writes it.
func (c *Cache) PutCellStamp(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID, stamp crdt.Stamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putCellStampLocked(table, pk, col, stamp)
}

// PutCellStamps merges multiple per-column stamps for one row in one
// pass. Equivalent to calling PutCellStamp per (col, stamp).
func (c *Cache) PutCellStamps(table crdt.TableID, pk crdt.PKBlob, stamps map[crdt.ColumnID]crdt.Stamp) {
	if len(stamps) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for col, s := range stamps {
		c.putCellStampLocked(table, pk, col, s)
	}
}

// DeleteCellStamp drops the (table, pk, col) override. Used by
// opportunistic collapse and bulk clear paths. Marks the cell dirty so
// the next snapshot DELETEs the metadata row.
func (c *Cache) DeleteCellStamp(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteCellStampLocked(table, pk, col)
}

// ClearCellsForRow drops every cell-clock override for one row. Called
// on CL bumps (resurrection / tombstone) per CRDT.md#causal-length-cl
// — bumping CL implicitly tombstones prior-generation overrides.
// Subsequent PutCellStamp calls before the next snapshot are honored:
// the snapshot first issues DeleteCellClocksForRow, then upserts each
// surviving entry.
func (c *Cache) ClearCellsForRow(table crdt.TableID, pk crdt.PKBlob) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := rowKey{t: table, pk: string(pk)}
	st := c.rowClock[k]
	st.Cells = nil
	c.rowClock[k] = st
	c.dirtyClearedRows[k]++
	delete(c.dirtyCells, k)
}

// CellStamp returns the cell-clock override for (table, pk, col), or
// (zero, false) if none. Read-only.
func (c *Cache) CellStamp(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID) (crdt.Stamp, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.rowClock[rowKey{t: table, pk: string(pk)}]
	if st.Cells == nil {
		return crdt.Stamp{}, false
	}
	s, ok := st.Cells[col]
	return s, ok
}

func (c *Cache) putCellStampLocked(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID, stamp crdt.Stamp) {
	k := rowKey{t: table, pk: string(pk)}
	st := c.rowClock[k]
	// Monotone: a cell override only advances the column's effective
	// stamp (Cells[col] if set, else Base). Replays and racing
	// publishes resolve to the dominant stamp in any order.
	if !stamp.Dominates(st.EffectiveStamp(col, crdt.ByteRange{})) {
		return
	}
	if st.Cells == nil {
		st.Cells = map[crdt.ColumnID]crdt.Stamp{}
	}
	st.Cells[col] = stamp
	c.rowClock[k] = st
	c.markCellDirty(k, col)
}

func (c *Cache) deleteCellStampLocked(table crdt.TableID, pk crdt.PKBlob, col crdt.ColumnID) {
	k := rowKey{t: table, pk: string(pk)}
	st := c.rowClock[k]
	if st.Cells != nil {
		delete(st.Cells, col)
		if len(st.Cells) == 0 {
			st.Cells = nil
		}
		c.rowClock[k] = st
	}
	c.markCellDirty(k, col)
}

func (c *Cache) markCellDirty(k rowKey, col crdt.ColumnID) {
	cells := c.dirtyCells[k]
	if cells == nil {
		cells = map[crdt.ColumnID]struct{}{}
		c.dirtyCells[k] = cells
	}
	cells[col] = struct{}{}
}

// IsAppliedRemote returns true iff (origin, seq) has been applied — i.e.
// it lies within the contiguous frontier or the applied_gaps set. Used
// by the apply path for idempotency. Origin=self is rejected since the
// producer never re-applies its own commits.
func (c *Cache) IsAppliedRemote(origin crdt.Origin, seq crdt.Seq) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if origin == c.self {
		return false
	}
	if f, ok := c.frontier[origin]; ok && seq <= f.LastSeq {
		return true
	}
	if gaps, ok := c.appliedGaps[origin]; ok && gaps.Contains(seq) {
		return true
	}
	return false
}

// MarkApplied records that (origin, seq, hlc) has been applied. Adds
// seq to applied_gaps and promotes the contiguous prefix into frontier.
// Updates hlcLast to MAX(self, incoming). Caller has already done the
// app.db DML and any rowClock updates. Returns the contiguous frontier
// head as it was *before* this call so callers can detect new gaps
// (seq > priorHead+1) without a second lock acquisition.
func (c *Cache) MarkApplied(origin crdt.Origin, seq crdt.Seq, hlc crdt.Clock) (priorHead crdt.Seq) {
	c.mu.Lock()
	defer c.mu.Unlock()
	priorHead = c.frontier[origin].LastSeq
	c.markAppliedLocked(origin, seq, hlc)
	return priorHead
}

func (c *Cache) markAppliedLocked(origin crdt.Origin, seq crdt.Seq, hlc crdt.Clock) {
	gaps := c.appliedGaps[origin]
	if gaps == nil {
		gaps = &crdt.SeqSet{}
		c.appliedGaps[origin] = gaps
	}
	gaps.Add(seq)
	front := c.frontier[origin]
	newHead := gaps.PromoteContiguous(front.LastSeq)
	if newHead > front.LastSeq {
		front.LastSeq = newHead
	}
	if front.LastHLC.Less(hlc) {
		front.LastHLC = hlc
	}
	c.frontier[origin] = front
	c.dirtyFrontiers[origin] = struct{}{}
	// A straggler from a previously-evicted origin re-admits it: cancel the
	// pending frontier-row delete so this snapshot doesn't both advance and
	// delete the same origin.
	delete(c.forgottenOrigins, origin)
	c.metaDirty = true
	if c.hlcLast.Less(hlc) {
		c.hlcLast = hlc
	}
}

// FrontierFor returns the contiguous-applied head + hlc for origin, or
// zero/false if no records have ever been seen from origin.
func (c *Cache) FrontierFor(origin crdt.Origin) (FrontierEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f, ok := c.frontier[origin]
	return f, ok
}

// FrontierMap returns a copy of the per-origin frontier map. Used by
// the snapshotter and the public Node.Frontier accessor; copy avoids
// aliasing under the caller's iteration.
func (c *Cache) FrontierMap() map[crdt.Origin]FrontierEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[crdt.Origin]FrontierEntry, len(c.frontier))
	for k, v := range c.frontier {
		out[k] = v
	}
	return out
}

// FrontierLen returns the number of remote origins tracked in the
// frontier. This is the origin-count signal: it grows with the number
// of distinct origins ever applied (fleet churn), not the live peer
// count, until origin GC prunes retired origins. Cheap — a len under
// the lock, no allocation.
func (c *Cache) FrontierLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frontier)
}

// EvictOrigin forgets a dead origin: it drops the origin's frontier,
// applied-gaps, and snapshot-marker state and records it for frontier-row
// deletion in the next snapshot. Returns false (no-op) for self or an
// origin not currently tracked. Callers (origin GC / the reaper) must only
// evict an origin that is provably dead and cluster-wide durable (its
// changesets sealed then swept from the object store), so that no live
// delivery or gap-fill can re-introduce it. If a straggler from the origin
// does arrive later, markApplied re-admits it (un-forgetting) and applies
// it normally — the origin simply reappears; this is the documented
// offline-deadline behavior (PRUNING.md), not a correctness break.
func (c *Cache) EvictOrigin(origin crdt.Origin) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if origin == c.self {
		return false
	}
	_, inFrontier := c.frontier[origin]
	_, inGaps := c.appliedGaps[origin]
	_, inMarkers := c.snapshotMarkers[origin]
	if !inFrontier && !inGaps && !inMarkers {
		return false
	}
	delete(c.frontier, origin)
	delete(c.appliedGaps, origin)
	delete(c.snapshotMarkers, origin)
	delete(c.dirtyFrontiers, origin)
	c.forgottenOrigins[origin] = struct{}{}
	c.metaDirty = true
	return true
}

// AppliedTip returns the highest seq the broker has accepted from
// origin — equal to the gap-set's last range end when gaps are
// non-empty, otherwise the contiguous frontier head. Useful for
// diagnostics: detects "applied but unable to promote due to a missed
// prefix seq" cases that FrontierMap alone hides.
func (c *Cache) AppliedTip(origin crdt.Origin) crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appliedTipLocked(origin)
}

func (c *Cache) appliedTipLocked(origin crdt.Origin) crdt.Seq {
	tip := c.frontier[origin].LastSeq
	if gaps, ok := c.appliedGaps[origin]; ok && gaps != nil {
		ranges := gaps.Ranges()
		if n := len(ranges); n > 0 && ranges[n-1].Hi > tip {
			tip = ranges[n-1].Hi
		}
	}
	return tip
}

// AppliedTipMap returns the per-origin applied-tip in one lock
// acquisition, with locally-drained origins (self + secondaries from
// senderNextSeq) overlaid as next-1 (the producer is contiguous, so
// tip == frontier for those). Used by the broker's gap-fill planner
// to bound its Fetch ranges against locally-observed tips.
func (c *Cache) AppliedTipMap() map[crdt.Origin]crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.frontier) + len(c.senderNextSeq)
	tips := make(map[crdt.Origin]crdt.Seq, n)
	for o := range c.frontier {
		tips[o] = c.appliedTipLocked(o)
	}
	for o, next := range c.senderNextSeq {
		if next > 0 {
			tips[o] = next - 1
		}
	}
	return tips
}

// RetentionFrontierMap returns the per-origin CONTIGUOUS frontier head
// (frontier[o].LastSeq) for REMOTE origins only — the safe reclaim floor for
// object-store retention. An epoch is durable in the metadata baseline+chain
// only up to the contiguous prefix, so reclaiming past it would strand a
// range the still-unfilled prefix needs.
//
// Locally-produced origins (self + drained secondaries, tracked in
// senderNextSeq) are DELIBERATELY EXCLUDED, so their own epochs are never
// swept by single-node/local retention. Keying them on senderNextSeq-1 (what
// this node produced) is unsafe: a peer may hold only a much lower contiguous
// prefix, and sweeping the owner's S3 epochs above that prefix strands the
// range for every such peer — the object-store side of the owner-origin
// wedge. Only member-scoped retention (min across current members' contiguous
// frontiers) can safely reclaim owner-origin epochs; until then this errs
// toward slow, bounded S3 growth over another permanent hole.
//
// Contrast AppliedTipMap: that advances to the gap-set tip (for fetch
// planning / discovery) and MUST NOT be used as a retention floor. Above
// the contiguous head an unfilled prefix seq still remains; the metadata
// baseline can neither prove it applied nor reconstruct it, so a tip-keyed
// sweep would delete the epochs that are its only remaining source.
func (c *Cache) RetentionFrontierMap() map[crdt.Origin]crdt.Seq {
	c.mu.Lock()
	defer c.mu.Unlock()
	fr := make(map[crdt.Origin]crdt.Seq, len(c.frontier))
	for o, f := range c.frontier {
		if _, produced := c.senderNextSeq[o]; produced {
			continue // owner-origin: not safe to sweep without member scope
		}
		fr[o] = f.LastSeq
	}
	return fr
}

// MissingRangesUpTo returns ranges in [frontier[o]+1, tip] that are
// neither in the contiguous frontier nor in applied_gaps for origin o.
// Returns nil when nothing is missing (frontier already at or above
// tip). Used by the broker's gap planner to translate cache state into
// transport.Range requests.
func (c *Cache) MissingRangesUpTo(o crdt.Origin, tip crdt.Seq) []crdt.SeqRange {
	c.mu.Lock()
	defer c.mu.Unlock()
	head := c.frontier[o].LastSeq
	if tip <= head {
		return nil
	}
	var out []crdt.SeqRange
	lo := head + 1
	if g, ok := c.appliedGaps[o]; ok && g != nil {
		for _, r := range g.Ranges() {
			if r.Lo > tip {
				break
			}
			if r.Lo > lo {
				out = append(out, crdt.SeqRange{Lo: lo, Hi: r.Lo - 1})
			}
			if r.Hi >= lo {
				lo = r.Hi + 1
			}
		}
	}
	if lo <= tip {
		out = append(out, crdt.SeqRange{Lo: lo, Hi: tip})
	}
	return out
}

// SetSnapshotMarker records the journal offset for origin reflected in
// the most recent snapshot. The snapshotter calls this after a
// successful metadata checkpoint; GC consults it before unlinking
// segments.
func (c *Cache) SetSnapshotMarker(origin crdt.Origin, off journal.Offset) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshotMarkers[origin] = off
	c.metaDirty = true
}

// SnapshotMarker returns the offset for origin's last persisted snapshot
// (zero if no snapshot yet covers origin).
func (c *Cache) SnapshotMarker(origin crdt.Origin) journal.Offset {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotMarkers[origin]
}
