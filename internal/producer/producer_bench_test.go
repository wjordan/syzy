package producer

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

// BenchmarkCommitInsertBatched sweeps the local commit-thread cost
// across batch sizes: each subtest issues b.N total INSERTs grouped
// into transactions of `batch` rows (one BEGIN IMMEDIATE / N x INSERT
// / one COMMIT per tx). Reports per-INSERT ns/op so the curve is
// directly comparable to BenchmarkCommitInsert (which is the batch=1
// autocommit case) and to the round-trip batched bench.
//
// What batches amortize on the local thread:
//   - one BEGIN + one COMMIT per tx (was: per row, autocommit)
//   - one walhook fire per tx (walhook is per-commit, not per-row)
//   - one journal.Append per tx
//   - one drainer-side encode + Cache advance per tx
//
// Per-row work that does NOT amortize: the preupdate hook (touch
// journal capture) fires per row regardless, and SQLite's vdbe still
// executes per row.
func BenchmarkCommitInsertBatched(b *testing.B) {
	for _, batch := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			runCommitInsertBatched(b, batch)
		})
	}
}

func runCommitInsertBatched(b *testing.B, batch int) {
	f := setupTBWithConfig(b, Config{
		JournalDir: filepath.Join(b.TempDir(), "jrn"),
	})
	insStmt, _, err := f.app.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare INSERT: %v", err)
	}
	defer insStmt.Finalize()
	begStmt, _, err := f.app.Prepare(`BEGIN IMMEDIATE`)
	if err != nil {
		b.Fatalf("Prepare BEGIN: %v", err)
	}
	defer begStmt.Finalize()
	cmtStmt, _, err := f.app.Prepare(`COMMIT`)
	if err != nil {
		b.Fatalf("Prepare COMMIT: %v", err)
	}
	defer cmtStmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	rowsLeft := b.N
	for rowsLeft > 0 {
		thisBatch := batch
		if thisBatch > rowsLeft {
			thisBatch = rowsLeft
		}
		if err := begStmt.Reset(); err != nil {
			b.Fatalf("BEGIN reset: %v", err)
		}
		if _, err := begStmt.Step(); err != nil {
			b.Fatalf("BEGIN step: %v", err)
		}
		for k := 0; k < thisBatch; k++ {
			i := b.N - rowsLeft + k
			var id [8]byte
			for j := 0; j < 8; j++ {
				id[j] = byte(i >> (8 * j))
			}
			if err := insStmt.Reset(); err != nil {
				b.Fatalf("INSERT reset: %v", err)
			}
			if err := insStmt.BindBlob(1, id[:]); err != nil {
				b.Fatalf("Bind: %v", err)
			}
			if _, err := insStmt.Step(); err != nil {
				b.Fatalf("INSERT step: %v", err)
			}
		}
		if err := cmtStmt.Reset(); err != nil {
			b.Fatalf("COMMIT reset: %v", err)
		}
		if _, err := cmtStmt.Step(); err != nil {
			b.Fatalf("COMMIT step: %v", err)
		}
		rowsLeft -= thisBatch
	}
}

// BenchmarkCommitInsert measures one INSERT per iteration through the
// full local capture+materialize+metadata-write path. No transport, no
// broker. Workload: event(id BLOB PK, n TEXT), unique 8-byte primary
// key per iteration.
func BenchmarkCommitInsert(b *testing.B) {
	f := setupTB(b)
	stmt, _, err := f.app.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
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
	}
}

// BenchmarkCommitUpdate measures the steady-state UPDATE path: one row
// pre-seeded, b.N updates against it. Captures the read-old-image cost
// the producer pays on UPDATE.
func BenchmarkCommitUpdate(b *testing.B) {
	f := setupTB(b)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'seed')`); err != nil {
		b.Fatalf("seed: %v", err)
	}
	stmt, _, err := f.app.Prepare(`UPDATE event SET n = ? WHERE id = x'01'`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		if err := stmt.BindText(1, fmt.Sprintf("v%d", i)); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
	}
}

// openHookFixtureNoOp opens a SQLite connection with the producer's
// hook fixture installed (touch journal enabled + wal_hook), but the
// wal_hook returns immediately without doing any of the producer's
// work (no HLC stamp, no journal append, no clear). Comparing
// BenchmarkCommitInsert/Update against the corresponding
// HookOverheadNoOp variant isolates the producer's own per-commit
// work cost on top of the SQLite-level hook infrastructure.
func openHookFixtureNoOp(b *testing.B) *sqlitebridge.Conn {
	b.Helper()
	dir := b.TempDir()
	c, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	if err := c.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		b.Fatalf("schema: %v", err)
	}
	c.EnableTouchJournal()
	c.SetWALHook(func(string, int) int {
		c.ClearTouchJournal()
		return 0
	})
	return c
}

// BenchmarkHookFixtureNoOpInsert is the matched no-work baseline for
// BenchmarkCommitInsert: same fixture (touch journal + wal_hook), but
// the wal_hook does no producer work — it just clears the touch buffer
// to mirror what the producer ultimately does. Subtract this from
// BenchmarkCommitInsert to see the producer's own per-commit work cost.
func BenchmarkHookFixtureNoOpInsert(b *testing.B) {
	c := openHookFixtureNoOp(b)
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
	}
}

// BenchmarkCommitInsertProducerNoopHook isolates the cost of "having
// the Producer fixture standing up at all" from the cost of the
// walHook content. After producer setup completes and the drainer
// goroutine is cancelled, the walHook is hot-swapped for a near-no-op
// that just clears the touch buffer (matching the fixture's wal_hook).
// Delta vs BenchmarkHookFixtureNoOpInsert is purely the cost of
// producer struct setup + idle mmap + dormant goroutine.
func BenchmarkCommitInsertProducerNoopHook(b *testing.B) {
	f := setupTB(b)
	f.p.drainCancel()
	<-f.p.drainDone
	// Swap to a near-no-op walHook (matches what the fixture does).
	f.app.SetWALHook(func(string, int) int {
		f.app.ClearTouchJournal()
		return 0
	})

	stmt, _, err := f.app.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
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
	}
}

// BenchmarkCommitInsertDrainerStopped isolates the producer's
// foreground hot path from background goroutine interference: the
// drainer is stopped before the timer starts, so only the wal_hook +
// journal.Append + sqlite path runs on the test thread. Compare
// against BenchmarkCommitInsert to attribute overhead to the drainer's
// concurrent work.
func BenchmarkCommitInsertDrainerStopped(b *testing.B) {
	f := setupTB(b)
	// Cancel the drainer goroutine. Subsequent walHooks still write to
	// the journal; nobody drains it. Bench measures only the producer's
	// foreground work.
	f.p.drainCancel()
	<-f.p.drainDone

	stmt, _, err := f.app.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
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
	}
}

// BenchmarkHookFixtureNoOpUpdate is the matched no-work baseline for
// BenchmarkCommitUpdate.
func BenchmarkHookFixtureNoOpUpdate(b *testing.B) {
	c := openHookFixtureNoOp(b)
	if err := c.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'seed')`); err != nil {
		b.Fatalf("seed: %v", err)
	}
	stmt, _, err := c.Prepare(`UPDATE event SET n = ? WHERE id = x'01'`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		if err := stmt.BindText(1, fmt.Sprintf("v%d", i)); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
	}
}
