package sqlitebridge

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Trigger-journal benchmarks: stock SQLite with an in-DB journal table
// populated by AFTER INSERT/UPDATE/DELETE triggers on the user table.
// Runs in the same WAL commit as the user mutation — no metadata, no
// hooks. Read these against the BaselineInsert/BaselineUpdate floor in
// bench_test.go to estimate what same-DB trigger journaling would cost
// the producer hot path on top of an unreplicated commit.
//
// The triggers write a single journal row per mutation. Payload mirrors
// the existing event(id BLOB PK, n TEXT) workload: op tag, PK, and the
// new value of n. UPDATE and DELETE share the journal so the comparison
// against BaselineUpdate is apples-to-apples.

const triggerJournalSchema = `
PRAGMA journal_mode = WAL;
CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT);
CREATE TABLE _journal (
  seq     INTEGER PRIMARY KEY,
  op      INTEGER NOT NULL,    -- 1=insert, 2=update, 3=delete
  pk      BLOB NOT NULL,
  payload BLOB
);
CREATE TRIGGER event_ai AFTER INSERT ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (1, NEW.id, NEW.n);
END;
CREATE TRIGGER event_au AFTER UPDATE ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (2, NEW.id, NEW.n);
END;
CREATE TRIGGER event_ad AFTER DELETE ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (3, OLD.id, NULL);
END;`

// triggerJournalOutboxSchema adds an outbox-style partial index that the
// broadcaster would scan. Trigger inserts a row with sent_at NULL, which
// enters the partial index — measures the cost of maintaining a "ready
// to broadcast" pointer in the same commit.
const triggerJournalOutboxSchema = `
PRAGMA journal_mode = WAL;
CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT);
CREATE TABLE _journal (
  seq     INTEGER PRIMARY KEY,
  op      INTEGER NOT NULL,
  pk      BLOB NOT NULL,
  payload BLOB,
  sent_at INTEGER                  -- NULL = unsent
);
CREATE INDEX _journal_outbox ON _journal(seq) WHERE sent_at IS NULL;
CREATE TRIGGER event_ai AFTER INSERT ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (1, NEW.id, NEW.n);
END;
CREATE TRIGGER event_au AFTER UPDATE ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (2, NEW.id, NEW.n);
END;
CREATE TRIGGER event_ad AFTER DELETE ON event BEGIN
  INSERT INTO _journal(op, pk, payload) VALUES (3, OLD.id, NULL);
END;`

func openTriggerJournalDB(b *testing.B, schema string) *Conn {
	b.Helper()
	dir := b.TempDir()
	c, err := Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	if err := c.Exec(schema); err != nil {
		b.Fatalf("schema: %v", err)
	}
	return c
}

// BenchmarkTriggerJournalInsert: INSERT into event, AFTER INSERT trigger
// writes one journal row. One commit per iter. Direct comparison vs
// BenchmarkBaselineInsert isolates the trigger fire + extra row write.
func BenchmarkTriggerJournalInsert(b *testing.B) {
	c := openTriggerJournalDB(b, triggerJournalSchema)
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

// BenchmarkTriggerJournalInsertOutbox: same as TriggerJournalInsert but
// the journal table also carries the outbox partial index a broker would
// need. Difference vs TriggerJournalInsert = index-maintenance cost on
// the commit path.
func BenchmarkTriggerJournalInsertOutbox(b *testing.B) {
	c := openTriggerJournalDB(b, triggerJournalOutboxSchema)
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

// BenchmarkTriggerJournalUpdate: steady-state UPDATE against one
// pre-seeded row, AFTER UPDATE trigger writes one journal row. Compare
// to BenchmarkBaselineUpdate.
func BenchmarkTriggerJournalUpdate(b *testing.B) {
	c := openTriggerJournalDB(b, triggerJournalSchema)
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

// BenchmarkTriggerJournalBatch100: 100 inserts per commit, b.N commits.
// Single-row benches above are dominated by COMMIT fsync; this batch
// view amortizes fsync over 100 rows so the per-row trigger cost shows
// up clearly. ns/op here is per-commit; divide by 100 for per-row.
func BenchmarkTriggerJournalBatch100(b *testing.B) {
	c := openTriggerJournalDB(b, triggerJournalSchema)
	begin, _, err := c.Prepare(`BEGIN`)
	if err != nil {
		b.Fatalf("Prepare BEGIN: %v", err)
	}
	defer begin.Finalize()
	commit, _, err := c.Prepare(`COMMIT`)
	if err != nil {
		b.Fatalf("Prepare COMMIT: %v", err)
	}
	defer commit.Finalize()
	ins, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare INSERT: %v", err)
	}
	defer ins.Finalize()

	const batch = 100
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		begin.Reset()
		if _, err := begin.Step(); err != nil {
			b.Fatalf("BEGIN: %v", err)
		}
		for k := 0; k < batch; k++ {
			ins.Reset()
			row := i*batch + k
			var id [8]byte
			for j := 0; j < 8; j++ {
				id[j] = byte(row >> (8 * j))
			}
			if err := ins.BindBlob(1, id[:]); err != nil {
				b.Fatalf("Bind: %v", err)
			}
			if _, err := ins.Step(); err != nil {
				b.Fatalf("INSERT: %v", err)
			}
		}
		commit.Reset()
		if _, err := commit.Step(); err != nil {
			b.Fatalf("COMMIT: %v", err)
		}
	}
}

// BenchmarkBaselineBatch100: matching no-trigger control for
// TriggerJournalBatch100. Use the difference to isolate per-row trigger
// overhead independent of fsync amortization.
func BenchmarkBaselineBatch100(b *testing.B) {
	c := openBaselineDB(b)
	begin, _, err := c.Prepare(`BEGIN`)
	if err != nil {
		b.Fatalf("Prepare BEGIN: %v", err)
	}
	defer begin.Finalize()
	commit, _, err := c.Prepare(`COMMIT`)
	if err != nil {
		b.Fatalf("Prepare COMMIT: %v", err)
	}
	defer commit.Finalize()
	ins, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare INSERT: %v", err)
	}
	defer ins.Finalize()

	const batch = 100
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		begin.Reset()
		if _, err := begin.Step(); err != nil {
			b.Fatalf("BEGIN: %v", err)
		}
		for k := 0; k < batch; k++ {
			ins.Reset()
			row := i*batch + k
			var id [8]byte
			for j := 0; j < 8; j++ {
				id[j] = byte(row >> (8 * j))
			}
			if err := ins.BindBlob(1, id[:]); err != nil {
				b.Fatalf("Bind: %v", err)
			}
			if _, err := ins.Step(); err != nil {
				b.Fatalf("INSERT: %v", err)
			}
		}
		commit.Reset()
		if _, err := commit.Step(); err != nil {
			b.Fatalf("COMMIT: %v", err)
		}
	}
}
