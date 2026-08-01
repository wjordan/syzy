package postgres

import (
	"sync"

	"github.com/wjordan/syzy/crdt"
)

// Winner-repair stash (§9 Option A in docs/postgres.md). It stashes the
// post-arbitration full image of the latest peer-applied write that won LWW on
// each contended row. The local fold checks this on each per-record build:
// when its (CL, Stamp) loses to the stashed winner the row is repaired to the
// winner's image and the loser is dropped from the outbound changeset.
// Runtime-only — entries are not snapshotted; on restart subsequent applies
// repopulate as new contention is observed.
//
// This engine-local state used to live on the shared nodestate.Cache; it is
// Postgres-specific (the SQLite engine's synchronous hooks never fold a stale
// local write), so it lives here now.

// winnerEntry is the cluster's known LWW winner for one row, stashed at
// apply so a later losing local fold can self-correct. Image is the post-
// arbitration full row image (Insert: from the changeset; Update: the row
// read back from the local table inside the apply transaction, so it
// reflects any §5 cell-LWW stealing applied to it). A nil Image means the
// winner REMOVED the row (an applied Delete) — a losing local write there is
// dropped and the row deleted locally, rather than repaired to an image.
//
// An entry also serves as the marker that a peer physically wrote this row
// since the last local fold of it, which the fold needs in both directions:
// losing to it means repairing to its image, and beating it means re-asserting
// the local record (the apply may have overwritten a commit that had not been
// folded yet). That is why applied Deletes are stashed even though they have no
// image to repair to.
type winnerEntry struct {
	Dot   crdt.Dot
	CL    uint64
	Stamp crdt.Stamp
	Image []crdt.ColValue
	// Cols are the columns the winning record actually arbitrated for, set only
	// on a cell-group table. A losing local write there loses ONLY these columns
	// — the rest of it still wins on every peer — so the fold repairs and drops
	// exactly this set instead of the whole row. nil means the whole row (a
	// row-group table, or an image that defines every column).
	Cols map[crdt.ColumnID]struct{}
}

// splitCellLosers partitions a cell-group record's carried columns against the
// stashed winner's column set: kept still wins on every peer, lost is what the
// winner already owns.
//
// A counter contribution is never lost. Contributions do not arbitrate by stamp
// — every node sums every one of them — so dropping one here would erase it
// from the whole cluster, not just from this record.
func splitCellLosers(changed []crdt.ColValue, winnerCols map[crdt.ColumnID]struct{}) (kept, lost []crdt.ColValue) {
	for _, v := range changed {
		_, owned := winnerCols[v.Column]
		// len(winnerCols) == 0 is a whole-row winner: it owns every column.
		if v.Format != crdt.FormatDelta && (owned || len(winnerCols) == 0) {
			lost = append(lost, v)
			continue
		}
		kept = append(kept, v)
	}
	return kept, lost
}

// repairRow restricts a stashed winner's full row image to what a whole-record
// repair may write. Counter cells are excluded: the local cell is the running
// sum of every contribution applied so far, and a stamped absolute image is not
// entitled to roll that back. A losing DELETE is the exception — the row is
// gone, so the winner's image has to resurrect it whole (its NOT NULL counter
// included) exactly as every peer's apply of that winner did.
func repairRow(ti *tableInfo, image []crdt.ColValue, resurrect bool) []crdt.ColValue {
	if resurrect || !ti.hasCounters() {
		return image
	}
	out := make([]crdt.ColValue, 0, len(image))
	for _, v := range image {
		if c := ti.colByID(v.Column); c != nil && c.counter {
			continue
		}
		out = append(out, v)
	}
	return out
}

// repairImage restricts a stashed winner's full row image to the lost columns,
// keeping the PK columns so the repair UPSERT can address the row.
func repairImage(ti *tableInfo, image []crdt.ColValue, lost []crdt.ColValue) []crdt.ColValue {
	want := make(map[crdt.ColumnID]struct{}, len(lost))
	for _, v := range lost {
		want[v.Column] = struct{}{}
	}
	out := make([]crdt.ColValue, 0, len(lost)+len(ti.pk))
	for _, v := range image {
		c := ti.colByID(v.Column)
		if c == nil {
			continue
		}
		if _, ok := want[v.Column]; ok || c.isPK {
			out = append(out, v)
		}
	}
	return out
}

// reassertImage is the row content a WINNING local fold must re-write to the
// local table. Counter cells are excluded for the same reason repairRow
// excludes them: the local cell is the running sum of every contribution
// applied so far — including this commit's own — and re-writing an absolute
// image would erase the peer contributions that were summed onto it while
// every peer keeps them. resurrect (the row was deleted by the peer whose apply
// this write beat) is the exception: nothing is left to preserve, so the image
// has to define the row whole, NOT NULL counter included — which is why the
// fold only reaches here on a resurrect for a record that carries every column.
//
// A cell-group update carries only its changed columns, so the PK is prefixed
// to address the row. nil means there is nothing to re-assert — a counter-only
// write, whose value no overwrite can have lost.
func reassertImage(ti *tableInfo, pk crdt.PKBlob, rec crdt.Record, cellUpdate, resurrect bool) []crdt.ColValue {
	out := repairRow(ti, recordImage(rec), resurrect)
	if len(out) == 0 {
		return nil
	}
	if cellUpdate {
		return cellImage(ti, pk, out)
	}
	return out
}

// winnerStash maps contended rows to their stashed winner. Both the apply
// path and the fold path run on the orchestrator goroutine, but the mutex
// keeps it safe for the deterministic test paths too. All methods are
// nil-receiver-safe so Cache-only unit fixtures need no stash.
type winnerStash struct {
	mu sync.Mutex
	m  map[rowKey]winnerEntry
}

func newWinnerStash() *winnerStash { return &winnerStash{m: map[rowKey]winnerEntry{}} }

// stash records a peer-applied winner for (table, pk) so a later local fold
// can detect it lost LWW and self-correct. Overwrites any prior stash (the
// latest winner is always the relevant one).
func (w *winnerStash) stash(table crdt.TableID, pk crdt.PKBlob, e winnerEntry) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.m[rowKey{tid: table, pk: string(pk)}] = e
}

// winner returns the stashed winner for (table, pk), or zero, false.
func (w *winnerStash) winner(table crdt.TableID, pk crdt.PKBlob) (winnerEntry, bool) {
	if w == nil {
		return winnerEntry{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.m[rowKey{tid: table, pk: string(pk)}]
	return e, ok
}

// clear drops the stashed winner for (table, pk). The fold path calls this
// once its local write dominates the cluster's known winner — the stash is
// then stale (peers will adopt the local write).
func (w *winnerStash) clear(table crdt.TableID, pk crdt.PKBlob) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.m, rowKey{tid: table, pk: string(pk)})
}
