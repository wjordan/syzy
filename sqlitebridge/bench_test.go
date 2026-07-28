package sqlitebridge

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Baseline benchmarks: stock SQLite via the cgo bridge with NO producer
// hooks, NO metadata, NO broker. The inner-loop benches in producer/,
// broker/, and testcluster/ can be read against these as the
// percentage-overhead floor.
//
// Workload mirrors the inner-loop benches: event(id BLOB PK, n TEXT)
// with a unique 8-byte PK per iteration, on a WAL-mode file-backed DB
// (not :memory:, so fsync costs match the syzy benches).

const baselineSchema = `PRAGMA journal_mode = WAL;
CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`

func openBaselineDB(b *testing.B) *Conn {
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

// BenchmarkBaselineInsert: stock SQLite INSERT, one row per iter, unique
// 8-byte PK. Floor for producer.BenchmarkCommitInsert and
// testcluster.BenchmarkRoundTripInsert.
func BenchmarkBaselineInsert(b *testing.B) {
	c := openBaselineDB(b)
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

// BenchmarkBaselineInsertBatched is the stock-SQLite counterpart to
// producer.BenchmarkCommitInsertBatched and
// testcluster.BenchmarkRoundTripInsertBatched: same workload, same
// batching strategy (b.N total INSERTs grouped into transactions of
// `batch` rows, one BEGIN IMMEDIATE / N x INSERT / one COMMIT per
// tx), reporting per-INSERT ns/op. Reading syzy's per-batch numbers
// against this isolates syzy's overhead at each amortization point.
func BenchmarkBaselineInsertBatched(b *testing.B) {
	for _, batch := range []int{1, 8, 64, 512} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			c := openBaselineDB(b)
			insStmt, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
			if err != nil {
				b.Fatalf("Prepare INSERT: %v", err)
			}
			defer insStmt.Finalize()
			begStmt, _, err := c.Prepare(`BEGIN IMMEDIATE`)
			if err != nil {
				b.Fatalf("Prepare BEGIN: %v", err)
			}
			defer begStmt.Finalize()
			cmtStmt, _, err := c.Prepare(`COMMIT`)
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
		})
	}
}

// BenchmarkBaselineUpdate: stock SQLite UPDATE against one pre-seeded
// row. Floor for producer.BenchmarkCommitUpdate.
func BenchmarkBaselineUpdate(b *testing.B) {
	c := openBaselineDB(b)
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
