package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// openWithDDL builds a syzy node with a local schema log so the
// producer's DDL admission path captures schema changes (INSERTs
// against the new table land in the journal).
func openWithDDL(t *testing.T, dbPath string) *syzy.Node {
	t.Helper()
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:      dbPath,
		SchemaLog: schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return node
}

func TestSubscribeLocalCommit(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node := openWithDDL(t, dbPath)
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	ch, cancel := node.Subscribe(syzy.SubscribeFilter{Tables: []string{"t"}})
	defer cancel()

	if _, err := db.Exec(`INSERT INTO t (id, name) VALUES (?, ?)`, int64(1), "alice"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	select {
	case n, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before notification arrived")
		}
		if n.Lossy {
			t.Fatalf("unexpected Lossy notification")
		}
		if len(n.Changes) != 1 {
			t.Fatalf("got %d changes, want 1", len(n.Changes))
		}
		c := n.Changes[0]
		if c.Table != "t" {
			t.Errorf("Table = %q; want %q", c.Table, "t")
		}
		if c.Op != syzy.OpInsert {
			t.Errorf("Op = %d; want OpInsert", c.Op)
		}
		if uint64(c.Origin) != node.Origin() {
			t.Errorf("Origin = %x; want %x", c.Origin, node.Origin())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notification received within 5s")
	}
}

func TestSubscribeFilterRejectsOtherTables(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node := openWithDDL(t, dbPath)
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE a (id INTEGER PRIMARY KEY) WITHOUT ROWID`); err != nil {
		t.Fatalf("CREATE a: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE b (id INTEGER PRIMARY KEY) WITHOUT ROWID`); err != nil {
		t.Fatalf("CREATE b: %v", err)
	}

	chA, cancelA := node.Subscribe(syzy.SubscribeFilter{Tables: []string{"a"}})
	defer cancelA()

	if _, err := db.Exec(`INSERT INTO b (id) VALUES (?)`, int64(1)); err != nil {
		t.Fatalf("INSERT b: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO a (id) VALUES (?)`, int64(2)); err != nil {
		t.Fatalf("INSERT a: %v", err)
	}

	select {
	case n, ok := <-chA:
		if !ok {
			t.Fatal("channel closed prematurely")
		}
		if len(n.Changes) != 1 || n.Changes[0].Table != "a" {
			t.Errorf("notification = %+v; want one change on table a", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notification on a within 5s")
	}

	// No second notification (the b INSERT was filtered out).
	select {
	case n := <-chA:
		t.Fatalf("got unexpected second notification: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSubscribeCancelClosesChannel(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node := openWithDDL(t, dbPath)
	defer node.Close()

	ch, cancel := node.Subscribe(syzy.SubscribeFilter{})
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel delivered a value after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close within 1s after cancel")
	}
}

func TestSubscribeNodeCloseDrainsSubscribers(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node := openWithDDL(t, dbPath)

	ch, _ := node.Subscribe(syzy.SubscribeFilter{})
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel delivered a value after Node.Close")
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close within 1s of Node.Close")
	}
}
