package sqlitebridge

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommitHookFires(t *testing.T) {
	c := memDB(t)
	fired := 0
	c.SetCommitHook(func() int {
		fired++
		return 0
	})
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if fired != 2 {
		t.Errorf("commit hook fired %d times; want 2", fired)
	}
}

func TestCommitHookAbort(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.SetCommitHook(func() int { return 1 })
	err := c.Exec(`INSERT INTO t VALUES (1)`)
	if err == nil {
		t.Fatal("expected commit-hook abort error; got nil")
	}
	var sqErr Error
	if !errors.As(err, &sqErr) {
		t.Fatalf("expected sqlitebridge.Error; got %T", err)
	}
	if sqErr.Code&0xff != ResultConstraint {
		t.Errorf("expected primary code SQLITE_CONSTRAINT (%d); got %d (extended %d)",
			ResultConstraint, sqErr.Code&0xff, sqErr.Code)
	}
	c.SetCommitHook(nil)

	q, _, err := c.Prepare(`SELECT count(*) FROM t`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer q.Finalize()
	if _, err := q.Step(); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if got := q.ColumnInt64(0); got != 0 {
		t.Errorf("expected 0 rows after abort; got %d", got)
	}
}

func TestRollbackHookFires(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	fired := 0
	c.SetRollbackHook(func() { fired++ })
	if err := c.Exec(`BEGIN; INSERT INTO t VALUES (1); ROLLBACK;`); err != nil {
		t.Fatalf("BEGIN/ROLLBACK: %v", err)
	}
	if fired < 1 {
		t.Error("rollback hook did not fire")
	}
}

func TestPreupdateHookFires(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	type evt struct {
		op             PreupdateOp
		db, tbl        string
		oldID, newID   int64
		cols, depth    int
		blobWriteIndex int
	}
	var got []evt
	c.SetPreupdateHook(func(e *PreupdateEvent) {
		got = append(got, evt{
			op:             e.Op,
			db:             e.DBName,
			tbl:            e.TableName,
			oldID:          e.OldRowID,
			newID:          e.NewRowID,
			cols:           e.Count(),
			depth:          e.Depth(),
			blobWriteIndex: e.BlobWrite(),
		})
	})
	if err := c.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := c.Exec(`UPDATE t SET n='b' WHERE id=1`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if err := c.Exec(`DELETE FROM t WHERE id=1`); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("preupdate fires = %d; want 3 (got=%v)", len(got), got)
	}
	wantOps := []PreupdateOp{PreupdateInsert, PreupdateUpdate, PreupdateDelete}
	for i, want := range wantOps {
		if got[i].op != want {
			t.Errorf("fire %d op = %d; want %d", i, got[i].op, want)
		}
		if got[i].tbl != "t" {
			t.Errorf("fire %d table = %q; want t", i, got[i].tbl)
		}
		if got[i].db != "main" {
			t.Errorf("fire %d db = %q; want main", i, got[i].db)
		}
		if got[i].cols != 2 {
			t.Errorf("fire %d cols = %d; want 2", i, got[i].cols)
		}
		if got[i].depth != 0 {
			t.Errorf("fire %d depth = %d; want 0", i, got[i].depth)
		}
		if got[i].blobWriteIndex != -1 {
			t.Errorf("fire %d blobWrite = %d; want -1", i, got[i].blobWriteIndex)
		}
	}
}

func TestWALHookFires(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.db")
	c, err := Open(path, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	if err := c.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("PRAGMA wal: %v", err)
	}
	fired := 0
	var lastDB string
	var lastFrames int
	c.SetWALHook(func(db string, frames int) int {
		fired++
		lastDB = db
		lastFrames = frames
		return 0
	})
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if fired < 1 {
		t.Fatal("wal hook did not fire")
	}
	if lastDB != "main" {
		t.Errorf("wal hook db = %q; want main", lastDB)
	}
	if lastFrames < 1 {
		t.Errorf("wal hook frames = %d; want >= 1", lastFrames)
	}
}

func TestTraceHookStmt(t *testing.T) {
	c := memDB(t)
	var seen []string
	c.SetTraceHook(TraceStmt, func(evt TraceEvent, sql string) int {
		if evt == TraceStmt {
			seen = append(seen, sql)
		}
		return 0
	})
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(`SELECT 1`); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if len(seen) < 2 {
		t.Fatalf("trace fired %d times; want >= 2", len(seen))
	}
	if !strings.Contains(seen[0], "CREATE TABLE") {
		t.Errorf("first trace = %q; want substring CREATE TABLE", seen[0])
	}
}

func TestSetHookNilClears(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	fired := 0
	c.SetCommitHook(func() int {
		fired++
		return 0
	})
	if err := c.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	c.SetCommitHook(nil)
	if err := c.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	if fired != 1 {
		t.Errorf("commit fires after clear = %d; want 1", fired)
	}
}

func TestMultipleHooksOnOneConn(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	commits, preups := 0, 0
	c.SetCommitHook(func() int {
		commits++
		return 0
	})
	c.SetPreupdateHook(func(*PreupdateEvent) {
		preups++
	})
	if err := c.Exec(`INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if commits != 2 {
		t.Errorf("commit fires = %d; want 2", commits)
	}
	if preups != 2 {
		t.Errorf("preupdate fires = %d; want 2", preups)
	}
}

func TestHooksIsolationBetweenConns(t *testing.T) {
	a := memDB(t)
	b := memDB(t)
	aFires, bFires := 0, 0
	a.SetCommitHook(func() int { aFires++; return 0 })
	b.SetCommitHook(func() int { bFires++; return 0 })
	if err := a.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("a CREATE: %v", err)
	}
	if err := b.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("b CREATE: %v", err)
	}
	if aFires != 1 {
		t.Errorf("aFires = %d; want 1", aFires)
	}
	if bFires != 1 {
		t.Errorf("bFires = %d; want 1", bFires)
	}
}

func TestCloseClearsHooks(t *testing.T) {
	c, err := Open(":memory:", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c.SetCommitHook(func() int { return 0 })
	c.SetPreupdateHook(func(*PreupdateEvent) {})
	c.SetRollbackHook(func() {})
	c.SetTraceHook(TraceStmt, func(TraceEvent, string) int { return 0 })
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
