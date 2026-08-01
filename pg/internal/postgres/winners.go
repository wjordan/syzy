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
// reflects any §5 cell-LWW stealing applied to it).
type winnerEntry struct {
	Dot   crdt.Dot
	CL    uint64
	Stamp crdt.Stamp
	Image []crdt.ColValue
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
