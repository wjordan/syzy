package nodestate

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
)

// MirrorSource is the per-origin journal lookup the recovery walker
// needs. mirror.Manager satisfies it.
type MirrorSource interface {
	// Origins returns the set of origins this node has mirror journals
	// for.
	Origins() []crdt.Origin
	// Journal returns origin's journal handle. Recovery iterates from
	// the cache's snapshot marker to the journal head.
	Journal(origin crdt.Origin) (*journal.Journal, error)
}

// RecoverMirror walks every per-origin mirror journal past its
// snapshot marker, decodes each KindMirror payload, and advances the
// Cache (rowClock + frontier + applied_gaps) for any seq not already
// covered by IsAppliedRemote. App.db DML is NOT re-run — the apply
// path that wrote the mirror entry committed app.db before journal
// append, so post-snapshot mirror records imply post-snapshot app.db
// rows already.
//
// Replay order across origins doesn't affect the final state: LWW is
// a partial order over Stamps and converges regardless of replay
// interleaving.
//
// cat refines cell-group replay: for a table the catalog knows to be
// cell-group, a same-CL update replays as per-column cell stamps
// (mirroring the live apply path) instead of a row Base advance. nil
// cat — or a table missing from it — degrades to the row-level replay.
//
// Returns the per-origin journal head reached (informational; useful
// for setting drainer start points).
func RecoverMirror(cache *Cache, src MirrorSource, cat *catalog.Catalog) (map[crdt.Origin]journal.Offset, error) {
	heads := map[crdt.Origin]journal.Offset{}
	for _, origin := range src.Origins() {
		if origin == cache.Self() {
			// Self journal isn't a mirror — recovered separately by the
			// producer's drainer catch-up. Skip here even if the manager
			// happens to track us (defensive).
			continue
		}
		j, err := src.Journal(origin)
		if err != nil {
			return nil, err
		}
		// Align the persisted marker to this journal's actual record
		// geometry. A snapshot marker is a journal-physical byte offset
		// that can outlive the journal it was computed against: a node
		// that restored metadata from the bucket adopts the producer's
		// marker, but its own mirror journal is a physically distinct
		// artifact with different record boundaries — so the raw marker
		// can land mid-record and parse as garbage (a CRC mismatch that
		// crash-loops the node at boot). AlignResume snaps it down to the
		// nearest real boundary; replaying the few extra records is the
		// idempotent re-process-silently contract (IsAppliedRemote +
		// LWW below skip anything already covered). The drainer resume
		// path already aligns; mirror recovery must too.
		startOff := j.AlignResume(cache.SnapshotMarker(origin))
		head := j.Head()
		if head <= startOff {
			heads[origin] = head
			continue
		}
		it := j.Iterate(startOff)
		for {
			rec, _, err := it.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("nodestate: replay mirror origin=%d: %w", origin, err)
			}
			next := it.Offset()
			if next > head {
				break
			}
			if rec.Kind != journal.KindMirror {
				continue
			}
			if rec.Aborted() {
				continue
			}
			cs, err := crdt.Decode(rec.Payload)
			if err != nil {
				// A mirror entry is a cache of a peer's changeset, never
				// the only copy — an undecodable payload (corruption, or a
				// wire version this binary doesn't speak) must not be
				// fatal to node open. Skip it: the seq stays unapplied in
				// the cache, so the drainer re-fetches it from the cluster.
				slog.Warn("nodestate: skipping undecodable mirror payload",
					"origin", origin, "seq", rec.Seq, "err", err)
				continue
			}
			if cs.Dot.Origin != origin {
				return nil, fmt.Errorf("nodestate: mirror origin mismatch: journal=%d payload=%d", origin, cs.Dot.Origin)
			}
			if cache.IsAppliedRemote(origin, cs.Dot.Seq) {
				continue
			}
			// Advance rowClock for winners. We re-run the LWW dominance
			// check here because the snapshot's rowClock might have
			// later updates (from this same origin or others) that
			// dominate this record. Idempotent on app.db; cache reflects
			// final state.
			for _, r := range cs.Records {
				h := r.Header()
				rs := cache.RowState(h.Table, h.PK)
				// Cell-group refinement: a same-CL update (or UPSERT-
				// Insert) on a cell-group table is a per-column write —
				// replaying it as a row Base advance would inflate the
				// stamp over columns the record never carried, dropping
				// late-delivered legitimate winners after restart.
				// Handles counter contributions too (never stamped, CL-only
				// gate), including mixed counter+register updates, whose
				// register columns replay per column.
				if tab := cellGroupTable(cat, h.Table); tab != nil {
					if upd, isCell := tab.AsCellUpdate(r, rs); isCell {
						replayCellClocks(cache, tab, upd, rs, cs.Stamp)
						continue
					}
				} else if upd, ok := r.(crdt.Update); ok && updateHasDelta(upd) {
					// Degraded mode (table not in catalog): counter
					// contributions carry no stamp and must not inflate
					// the row base. A generation advance lands CL with a
					// zero Base; a same-CL contribution touches no clocks.
					if h.CL > rs.CL {
						if cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL}) {
							cache.ClearCellsForRow(h.Table, h.PK)
						}
					}
					continue
				}
				if !rs.DominatedBy(h.CL, cs.Stamp) {
					continue
				}
				cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL, Base: cs.Stamp})
			}
			cache.MarkApplied(origin, cs.Dot.Seq, cs.Stamp.Clock)
		}
		heads[origin] = head
	}
	return heads, nil
}

// SelfLogReplayer folds a recovered self changeset's blob_range_clock —
// the drain's forward blob effect (fold a BlobPatch's ranges, drop a
// dominant full-row DML's clock), sourced from the wire changeset. The
// syncer sink implements it (sharing foldBlobPatchLocal); pure-Cache tests
// pass nil. RecoverSelf calls it BEFORE the changeset's row-clock effects,
// so it reads the pre-effect RowState the forward path's baseline and
// dominance gate depend on.
type SelfLogReplayer interface {
	ReplayBlobClock(cs *crdt.Changeset) error
}

// RecoverSelf replays the self-log (the self origin's mirror journal,
// promoted to the durable capture boundary) into the Cache after a
// restart, restoring senderNextSeq / hlcLast / row_clock / cell_clock /
// blob_range_clock from the EXACT shipped changeset bytes — never
// re-deriving, which would diverge for incremental blob writes (§2). It
// then advances SnapshotMarker(self) — a self-JOURNAL offset — to the
// highest source endOffset carried in the replayed self-log record headers,
// so the drainer resumes the self-journal at the self-log tip and derives
// only the never-published tail fresh.
//
// It iterates from offset 0 and relies on idempotency: ObserveSelfSeq /
// ObserveHLC are maxes, PutRowState is DominatedBy-gated, and the blob fold
// is stamp-idempotent, so records already covered by the loaded snapshot
// (including sealed records truncated away but still reflected in it) fold
// to no-ops, and re-running recovery converges. This is a DEDICATED
// own-origin path: the inbound apply path rejects own-origin frames and
// tracks remote seqs via the frontier, not senderNextSeq.
//
// Pre-self-log origins can retain records with no source endOffset forever.
// A kept origin follows a clean shutdown whose final snapshot covers those
// records, so recovery skips them before decoding and leaves its marker alone.
func RecoverSelf(cache *Cache, j *journal.Journal, cat *catalog.Catalog, blobs SelfLogReplayer) error {
	self := cache.Self()
	head := j.Head()
	marker := cache.SnapshotMarker(self)
	// claimedNext is the persisted senderNextSeq before replay advances it:
	// the highest seq this node claims to have produced (+1). If the self-log
	// covers fewer seqs than that, seqs were allocated without capture —
	// see the promotion guard after the loop.
	claimedNext := cache.SenderNextSeq(self)
	var maxSeq crdt.Seq
	var legacy int
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			return fmt.Errorf("nodestate: replay self-log: %w", err)
		}
		if it.Offset() > head {
			break
		}
		if rec.Kind != journal.KindMirror || rec.Aborted() {
			continue
		}
		endOff := journal.Offset(rec.HLC)
		if endOff == 0 {
			legacy++
			continue
		}
		cs, err := crdt.Decode(rec.Payload)
		if err != nil {
			// The self-log is the ONLY durable copy of our origin's bytes;
			// an undecodable record is fatal (unlike a remote mirror record,
			// which is re-fetchable from the cluster).
			return fmt.Errorf("nodestate: decode self-log changeset at %d: %w", it.Offset(), err)
		}
		if cs.Dot.Origin != self {
			return fmt.Errorf("nodestate: self-log origin mismatch: cache=%d payload=%d", self, cs.Dot.Origin)
		}
		cache.ObserveSelfSeq(self, cs.Dot.Seq)
		cache.ObserveHLC(cs.Stamp.Clock)
		if cs.Dot.Seq > maxSeq {
			maxSeq = cs.Dot.Seq
		}
		// blob_range_clock first, so the fold reads the pre-effect RowState
		// (the forward path's baseline and drop-dominance gate).
		if blobs != nil {
			if err := blobs.ReplayBlobClock(cs); err != nil {
				return fmt.Errorf("nodestate: replay self-log blob clock at %d: %w", it.Offset(), err)
			}
		}
		// row/cell clock — mirror the remote apply (RecoverMirror), skipping
		// BlobPatch (handled above; it never advances row_clock).
		for _, r := range cs.Records {
			if _, ok := r.(crdt.BlobPatch); ok {
				continue
			}
			h := r.Header()
			rs := cache.RowState(h.Table, h.PK)
			if tab := cellGroupTable(cat, h.Table); tab != nil {
				if upd, isCell := tab.AsCellUpdate(r, rs); isCell {
					replayCellClocks(cache, tab, upd, rs, cs.Stamp)
					continue
				}
			} else if upd, ok := r.(crdt.Update); ok && updateHasDelta(upd) {
				if h.CL > rs.CL {
					if cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL}) {
						cache.ClearCellsForRow(h.Table, h.PK)
					}
				}
				continue
			}
			if !rs.DominatedBy(h.CL, cs.Stamp) {
				continue
			}
			cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL, Base: cs.Stamp})
		}
		if endOff > marker {
			marker = endOff
		}
	}
	if legacy > 0 {
		slog.Info("nodestate: self-log carries pre-self-log records; durable capture is active for new writes",
			"self", self, "legacyRecords", legacy)
	}
	// Promotion guard: the self-log must cover every seq this node claims to
	// have produced. A shortfall means seqs were allocated with no durable
	// capture — the single-node→replicated promotion footgun (a node first
	// opened without a transport/bucket runs the drainer and advances
	// senderNextSeq/marker but never calls AppendSelf). Those seqs are absent
	// from the self-log (peer-pull serves nothing) and from S3 (never
	// sealed), so a peer needing them wedges forever. Fail fast with a
	// remediation pointer instead of silently reintroducing §1's seq hole.
	// Legacy-bearing origins may have a holed self-log behind senderNextSeq.
	// Their clean-shutdown snapshot covers the skipped records; a true
	// single-node promotion has no legacy self-mirror records.
	if legacy == 0 && claimedNext > 1 && maxSeq < claimedNext-1 {
		return fmt.Errorf("nodestate: self-log covers up to seq %d but senderNextSeq=%d — %d locally-produced seq(s) were never captured (single-node run promoted to replicated?); re-provision this node from a peer or S3", maxSeq, claimedNext, claimedNext-1-maxSeq)
	}
	cache.SetSnapshotMarker(self, marker)
	return nil
}

// updateHasDelta reports whether upd carries a counter contribution
// (crdt.FormatDelta value). Catalog-free: the wire marker is
// authoritative for update records.
func updateHasDelta(upd crdt.Update) bool {
	for _, v := range upd.Changed {
		if v.Format == crdt.FormatDelta {
			return true
		}
	}
	return false
}

// cellGroupTable returns the catalog entry for id when replay should
// use per-column bookkeeping: known, not dropped, and cell-group.
// Returns nil otherwise (including cat == nil), which keeps the
// historical row-level replay.
func cellGroupTable(cat *catalog.Catalog, id crdt.TableID) *catalog.Table {
	if cat == nil {
		return nil
	}
	tab, ok := cat.TableByID(id)
	if !ok || tab.Dropped() || !tab.CellGroup() {
		return nil
	}
	return tab
}

// replayCellClocks re-derives the clock bookkeeping the live cell
// apply path (broker.applyCellUpdate, internal/broker/apply_cell.go)
// performs for one cell-group update, without re-running DML or
// arbitration side effects: per-column winner gating, opportunistic
// collapse when the update covers every non-PK column, and the
// zero-Base generation advance. Counter columns never carry cell
// stamps. Idempotent and order-independent — PutRowState and
// PutCellStamp are monotone, so replay converges to the live path's
// final state regardless of interleaving with snapshot contents.
func replayCellClocks(cache *Cache, tab *catalog.Table, upd crdt.Update, rs crdt.RowState, stamp crdt.Stamp) {
	// Same admission gate as the live path (applyRecordsLWW): a record
	// carrying counter contributions gates on CL alone — its Stamp never
	// arbitrates counter cells — and refines per column below.
	if upd.CL < rs.CL || (!updateHasDelta(upd) && !rs.DominatedBy(upd.CL, stamp)) {
		return
	}
	newGen := upd.CL > rs.CL
	winners := upd.Changed
	if !newGen {
		winners = make([]crdt.ColValue, 0, len(upd.Changed))
		for _, v := range upd.Changed {
			if v.Format == crdt.FormatDelta ||
				stamp.Dominates(rs.EffectiveStamp(v.Column, crdt.ByteRange{})) {
				winners = append(winners, v)
			}
		}
		if len(winners) == 0 {
			return
		}
	}
	if tab.CoversAllNonPK(winners) {
		// Opportunistic collapse: uniformly stamped, absorb into the
		// baseline and drop the overrides.
		if cache.PutRowState(upd.Table, upd.PK, crdt.RowState{CL: upd.CL, Base: stamp}) {
			cache.ClearCellsForRow(upd.Table, upd.PK)
		}
		return
	}
	if newGen {
		// Generation advance without the resurrecting Insert's full
		// image: Base stays zero so the (lower-stamped) Insert still
		// wins the columns this update didn't carry.
		if cache.PutRowState(upd.Table, upd.PK, crdt.RowState{CL: upd.CL}) {
			cache.ClearCellsForRow(upd.Table, upd.PK)
		}
	}
	for _, v := range winners {
		if col, ok := tab.ColumnByID(v.Column); ok && col.Counter() {
			// Counter cells carry no stamps — nothing to advance.
			continue
		}
		cache.PutCellStamp(upd.Table, upd.PK, v.Column, stamp)
	}
}
