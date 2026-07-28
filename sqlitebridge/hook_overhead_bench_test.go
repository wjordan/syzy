package sqlitebridge

import (
	"path/filepath"
	"testing"
)

// Hook-overhead ladder. Isolates each layer of the producer's hook
// fixture so the total cost of "hooks installed but doing nothing
// useful" can be read out of the COMMIT noise. Workload mirrors
// BenchmarkBaselineInsert (event(id BLOB PK, n TEXT), unique 8-byte PK
// per iter, autocommit per row).
//
// Layers, from bottom up:
//   - HookOverheadNone     : no hooks at all (= BenchmarkBaselineInsert)
//   - HookOverheadTouchOnly: preupdate trampoline installed, C-only
//                            touch-journal capture, no cgo per row
//   - HookOverheadGoNoop   : preupdate trampoline + cgo cross to a Go
//                            no-op on every row; no touch journal work
//   - HookOverheadCommit   : commit hook only (Go no-op); one cgo
//                            cross per commit
//   - HookOverheadWAL      : WAL hook only (Go no-op); one cgo cross
//                            per commit
//   - HookOverheadAllNoop  : preupdate (Go no-op) + commit + WAL,
//                            touch journal off — full hook fixture
//                            doing zero work
//   - HookOverheadAllNoopJournal: preupdate (Go no-op + touch journal)
//                            + commit + WAL — closest "hook fixture
//                            with realistic per-row C work" point
//                            without metadata SQL.
//
// Read the gaps:
//   gap(None → TouchOnly)        = C trampoline + sqlite preupdate
//                                  dispatch + C touch-journal append
//   gap(TouchOnly → GoNoop)      = cgo crossing per row, minus the
//                                  C-touch-journal append (which is
//                                  off in GoNoop)
//   gap(None → Commit)           = one cgo crossing per commit
//   gap(None → WAL)              = one cgo crossing per commit (WAL)
//   gap(AllNoop → AllNoopJournal)= cost of the C touch-journal append
//                                  alone, with the cgo cross already
//                                  paid

func openHookBenchDB(b *testing.B) *Conn {
	b.Helper()
	dir := b.TempDir()
	c, err := Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	if err := c.Exec(baselineSchema); err != nil {
		b.Fatalf("schema: %v", err)
	}
	return c
}

func benchHookInsert(b *testing.B, c *Conn) {
	stmt, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
		// touch journal accumulates across rows by design; clear per
		// iter so the journal-on benches don't leak buffer growth into
		// later iters (and to mirror what the producer does between
		// commits via rollback/clear).
		if c.TouchJournalEnabled() {
			c.ClearTouchJournal()
		}
	}
}

// BenchmarkHookOverheadNone: control. No hooks installed. Should
// approximately match BenchmarkBaselineInsert.
func BenchmarkHookOverheadNone(b *testing.B) {
	c := openHookBenchDB(b)
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadTouchOnly: C preupdate trampoline installed with
// touch-journal capture, no Go callback. Measures pure C-side hook
// work — no cgo crossing per row.
func BenchmarkHookOverheadTouchOnly(b *testing.B) {
	c := openHookBenchDB(b)
	c.EnableTouchJournal()
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadGoNoop: preupdate trampoline crosses cgo into a
// Go no-op for every row. Touch-journal off. Difference vs HookOverheadNone
// is the per-row cgo cost; difference vs HookOverheadTouchOnly is "cgo
// crossing minus C-touch-journal append."
func BenchmarkHookOverheadGoNoop(b *testing.B) {
	c := openHookBenchDB(b)
	c.SetPreupdateHook(func(*PreupdateEvent) {})
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadCommit: commit hook only, Go callback returns 0.
// One cgo crossing per commit.
func BenchmarkHookOverheadCommit(b *testing.B) {
	c := openHookBenchDB(b)
	c.SetCommitHook(func() int { return 0 })
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadWAL: WAL hook only, Go callback returns 0. One
// cgo crossing per commit.
func BenchmarkHookOverheadWAL(b *testing.B) {
	c := openHookBenchDB(b)
	c.SetWALHook(func(string, int) int { return 0 })
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadAllNoop: full producer hook fixture (preupdate +
// commit + WAL Go callbacks) but every callback is a no-op and the
// touch journal is OFF. Floor for "what does the syzy hook scaffolding
// cost before any metadata SQL runs."
func BenchmarkHookOverheadAllNoop(b *testing.B) {
	c := openHookBenchDB(b)
	c.SetPreupdateHook(func(*PreupdateEvent) {})
	c.SetCommitHook(func() int { return 0 })
	c.SetWALHook(func(string, int) int { return 0 })
	benchHookInsert(b, c)
}

// BenchmarkHookOverheadAllNoopJournal: full hook fixture plus C touch
// journal enabled. Closest to "producer overhead minus metadata SQL"
// realizable from this package without pulling in producer/.
func BenchmarkHookOverheadAllNoopJournal(b *testing.B) {
	c := openHookBenchDB(b)
	c.EnableTouchJournal()
	c.SetPreupdateHook(func(*PreupdateEvent) {})
	c.SetCommitHook(func() int { return 0 })
	c.SetWALHook(func(string, int) int { return 0 })
	benchHookInsert(b, c)
}
