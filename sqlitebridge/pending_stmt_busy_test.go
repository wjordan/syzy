package sqlitebridge

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestPendingStmtWedgesBeginImmediate proves the prod wedge mechanism: a
// SELECT stmt stepped to SQLITE_ROW and abandoned (never stepped to done,
// never Reset) holds an open read snapshot on its connection. Once another
// connection advances the WAL, BEGIN IMMEDIATE on the pinned connection
// fails SQLITE_BUSY (BUSY_SNAPSHOT — busy_timeout does not apply) on every
// attempt until the stmt is Reset.
func TestPendingStmtWedgesBeginImmediate(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	w, err := Open(db, 0)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer w.Close()
	if err := w.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v INT); INSERT INTO t VALUES (1,1),(2,2)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	a, err := Open(db, 0)
	if err != nil {
		t.Fatalf("open apply: %v", err)
	}
	defer a.Close()
	if err := a.Exec(`PRAGMA busy_timeout = 100`); err != nil {
		t.Fatalf("busy_timeout: %v", err)
	}

	// The readKeyColumns pattern: LIMIT 1, one Step to ROW, abandon.
	stmt, _, err := a.Prepare(`SELECT v FROM t WHERE id = ? LIMIT 1`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, 1); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if has, err := stmt.Step(); err != nil || !has {
		t.Fatalf("step: has=%v err=%v", has, err)
	}
	// stmt is now pending at SQLITE_ROW — read txn open on conn a.

	// Writer advances the WAL past a's snapshot.
	if err := w.Exec(`INSERT INTO t VALUES (3,3)`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// Every BEGIN IMMEDIATE on a now fails "database is locked" instantly.
	for i := 0; i < 3; i++ {
		err := a.Exec(`BEGIN IMMEDIATE`)
		if err == nil {
			a.Exec(`ROLLBACK`)
			t.Fatalf("attempt %d: BEGIN IMMEDIATE unexpectedly succeeded — wedge not reproduced", i)
		}
		if !strings.Contains(err.Error(), "database is locked") {
			t.Fatalf("attempt %d: got %v, want 'database is locked'", i, err)
		}
	}

	// Reset clears the pinned snapshot; the connection heals.
	if err := stmt.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := a.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("post-reset BEGIN IMMEDIATE should succeed: %v", err)
	}
	if err := a.Exec(`ROLLBACK`); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
