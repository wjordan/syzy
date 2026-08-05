package sqlite_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	syzy "github.com/wjordan/syzy/sqlite"
)

func openWithReadPool(t *testing.T, size int) (*syzy.Node, *syzy.DB) {
	t.Helper()
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:         filepath.Join(t.TempDir(), "app.db"),
		ReadPoolSize: size,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	db := syzy.NewDB(node)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	return node, db
}

func TestReadPoolOpenByDefault(t *testing.T) {
	t.Parallel()
	node, db := openWithReadPool(t, 0)
	if node.ReaderDB() == nil {
		t.Fatal("ReaderDB is nil with the default ReadPoolSize")
	}
	if node.ReaderDB() == node.WriterDB() {
		t.Fatal("reads share the pinned writer pool")
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// A committed write must be visible to the read pool immediately:
	// the reader takes a fresh WAL snapshot per statement.
	var v string
	if err := db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("read-after-write: %v", err)
	}
	if v != "a" {
		t.Fatalf("read-after-write = %q, want \"a\"", v)
	}
}

func TestReadPoolDisabled(t *testing.T) {
	t.Parallel()
	node, db := openWithReadPool(t, -1)
	if node.ReaderDB() != nil {
		t.Fatal("ReaderDB is non-nil with ReadPoolSize < 0")
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	var v string
	if err := db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("read via writer: %v", err)
	}
	if v != "a" {
		t.Fatalf("read via writer = %q, want \"a\"", v)
	}
}

// The point of the pool: concurrent reads must not queue behind an open
// transaction on the pinned writer connection. With reads routed through
// the writer they cannot even start until Commit.
func TestReadPoolDoesNotBlockOnOpenTx(t *testing.T) {
	t.Parallel()
	_, db := openWithReadPool(t, 0)
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'committed')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (2, 'uncommitted')`); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}

	done := make(chan int64, 1)
	go func() {
		var n int64
		if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
			n = -1
		}
		done <- n
	}()

	select {
	case n := <-done:
		// The reader sees the last committed snapshot, not the open
		// transaction's row.
		if n != 1 {
			t.Fatalf("count during open tx = %d, want 1 (committed snapshot)", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked on an open write transaction")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	var n int64
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count after commit: %v", err)
	}
	if n != 2 {
		t.Fatalf("count after commit = %d, want 2", n)
	}
}

// Reads must run concurrently with each other. Serialized on one
// connection, N readers each holding a row for d take N*d; pooled they
// overlap.
func TestReadPoolReadsRunConcurrently(t *testing.T) {
	t.Parallel()
	_, db := openWithReadPool(t, 8)
	for i := range 8 {
		if _, err := db.Exec(`INSERT INTO t VALUES (?, ?)`, int64(i), "v"); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	const readers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	inFlight := make(chan struct{}, readers)
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rows, err := db.Query(`SELECT id FROM t`)
			if err != nil {
				errs <- err
				return
			}
			defer rows.Close()
			rows.Next() // hold the statement (and its connection) open
			inFlight <- struct{}{}
		}()
	}
	close(start)

	// Every reader should reach an open statement without waiting for the
	// others to finish. A single shared connection deadlocks here.
	deadline := time.After(10 * time.Second)
	for range readers {
		select {
		case <-inFlight:
		case err := <-errs:
			t.Fatalf("concurrent read: %v", err)
		case <-deadline:
			t.Fatal("readers did not run concurrently")
		}
	}
	wg.Wait()
}
