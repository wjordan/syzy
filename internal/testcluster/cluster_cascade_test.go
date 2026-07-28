package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport/memtransport"
)

// waitFor polls until cond returns true, failing the test if deadline elapses.
func waitFor(t *testing.T, deadline time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// rowCount returns the row count of `table` on n. Goes through the
// writer connection (idle during cascade-fire wait) to avoid racing
// with broker apply on AppApply.
func rowCount(t *testing.T, n *Node, table string) int {
	t.Helper()
	stmt, _, err := n.AppWrite.Prepare(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, table))
	if err != nil {
		t.Fatalf("prepare count: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step count: %v", err)
	}
	return int(stmt.ColumnInt64(0))
}

// TestCascade_DeleteCascadeReplicates verifies that ON DELETE CASCADE
// expressed via FK at CREATE TABLE time is rewritten into a BEFORE
// DELETE trigger on the parent, and that deleting the parent on node A
// also clears the children on node B via the synthesized trigger.
func TestCascade_DeleteCascadeReplicates(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE parent (id INT PRIMARY KEY NOT NULL, name TEXT)`); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if err := a.AppWrite.Exec(
		`CREATE TABLE child (id INT PRIMARY KEY NOT NULL, parent_id INTEGER, label TEXT,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)`); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	waitFor(t, 2*time.Second, "B sees child table", func() bool {
		_, ok := b.Catalog.Table("child")
		return ok
	})

	if err := a.AppWrite.Exec(`INSERT INTO parent VALUES (1, 'p1')`); err != nil {
		t.Fatalf("INSERT parent: %v", err)
	}
	if err := a.AppWrite.Exec(`INSERT INTO child VALUES (10, 1, 'c1')`); err != nil {
		t.Fatalf("INSERT child: %v", err)
	}
	waitFor(t, 2*time.Second, "B sees both rows", func() bool {
		return rowCount(t, b, "parent") == 1 && rowCount(t, b, "child") == 1
	})

	// Delete parent on A. The synthesized BEFORE DELETE trigger fires
	// first on A, removing child(10). The captured changeset carries
	// only the parent DELETE (cascade child writes elided by depth>0).
	if err := a.AppWrite.Exec(`DELETE FROM parent WHERE id = 1`); err != nil {
		t.Fatalf("DELETE parent: %v", err)
	}
	waitFor(t, 2*time.Second, "A's parent gone", func() bool {
		return rowCount(t, a.AppNode(), "parent") == 0
	})
	if got := rowCount(t, a.AppNode(), "child"); got != 0 {
		t.Errorf("A child rows = %d; want 0 (synth trigger should have fired)", got)
	}
	// On B, the parent DELETE arrives via inbound apply. The same synth
	// trigger fires on B's apply connection and removes child(10).
	waitFor(t, 2*time.Second, "B sees cascade", func() bool {
		return rowCount(t, b, "parent") == 0 && rowCount(t, b, "child") == 0
	})
}

// AppNode returns a *Node alias with a reader handle for symmetry with
// rowCount. Just exposes AppApply for the tests above.
func (n *Node) AppNode() *Node { return n }

// TestCascade_SetNullReplicates: ON DELETE SET NULL nulls FK columns
// on children when the parent is deleted; same effect must converge
// across replicas.
func TestCascade_SetNullReplicates(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE parent (id INT PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if err := a.AppWrite.Exec(
		`CREATE TABLE child (id INT PRIMARY KEY NOT NULL, parent_id INTEGER,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL)`); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	waitFor(t, 2*time.Second, "B sees child", func() bool {
		_, ok := b.Catalog.Table("child")
		return ok
	})

	if err := a.AppWrite.Exec(`INSERT INTO parent VALUES (1)`); err != nil {
		t.Fatalf("INSERT parent: %v", err)
	}
	if err := a.AppWrite.Exec(`INSERT INTO child VALUES (10, 1)`); err != nil {
		t.Fatalf("INSERT child: %v", err)
	}
	waitFor(t, 2*time.Second, "B has rows", func() bool {
		return rowCount(t, b, "parent") == 1 && rowCount(t, b, "child") == 1
	})
	if err := a.AppWrite.Exec(`DELETE FROM parent WHERE id = 1`); err != nil {
		t.Fatalf("DELETE parent: %v", err)
	}
	checkNull := func(n *Node) bool {
		stmt, _, err := n.AppWrite.Prepare(`SELECT parent_id IS NULL FROM child WHERE id = 10`)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		defer stmt.Finalize()
		hasRow, _ := stmt.Step()
		return hasRow && stmt.ColumnInt64(0) == 1
	}
	waitFor(t, 2*time.Second, "A child.parent_id IS NULL", func() bool { return checkNull(a) })
	waitFor(t, 2*time.Second, "B child.parent_id IS NULL", func() bool { return checkNull(b) })
}

// TestCascade_DropTableTearsDownSynthTriggers verifies that DROP TABLE
// child sends OpDropTrigger ops in the same bundle, so the parent's
// synth triggers are removed on every replica.
func TestCascade_DropTableTearsDownSynthTriggers(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE parent (id INT PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if err := a.AppWrite.Exec(
		`CREATE TABLE child (id INT PRIMARY KEY NOT NULL, parent_id INTEGER,
			FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)`); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	waitFor(t, 2*time.Second, "B sees child", func() bool {
		_, ok := b.Catalog.Table("child")
		return ok
	})

	hasTrigger := func(n *Node) bool {
		stmt, _, err := n.AppWrite.Prepare(
			`SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_syzy_fkcascade_child_%'`)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		defer stmt.Finalize()
		hasRow, _ := stmt.Step()
		return hasRow
	}
	if !hasTrigger(a) {
		t.Errorf("A: synth trigger missing after CREATE child")
	}
	waitFor(t, 2*time.Second, "B has synth trigger", func() bool { return hasTrigger(b) })

	if err := a.AppWrite.Exec(`DROP TABLE child`); err != nil {
		t.Fatalf("DROP child: %v", err)
	}
	waitFor(t, 2*time.Second, "A trigger removed", func() bool { return !hasTrigger(a) })
	waitFor(t, 2*time.Second, "B trigger removed", func() bool { return !hasTrigger(b) })
}

// TestTrigger_AfterInsertReplicates exercises the canonical "trigger
// maintains derived state from a source table" pattern using a plain
// table as the derived target (the same recipe fts5 external-content
// would use, minus fts5-module dependency). The AFTER INSERT trigger
// must fire on every replica's apply path so the derived table ends up
// populated everywhere from the source-row replication alone.
func TestTrigger_AfterInsertReplicates(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE posts (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE posts: %v", err)
	}
	// Local-only derived table (underscore prefix) so trigger writes
	// to it aren't replicated separately. Each replica re-derives.
	if err := a.AppWrite.Exec(`CREATE TABLE _post_index (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
		t.Fatalf("CREATE _post_index on A: %v", err)
	}
	if err := b.AppWrite.Exec(`CREATE TABLE _post_index (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
		t.Fatalf("CREATE _post_index on B: %v", err)
	}
	if err := a.AppWrite.Exec(
		`CREATE TRIGGER posts_ai AFTER INSERT ON posts BEGIN
			INSERT INTO _post_index(id, body) VALUES (new.id, new.body);
		END`); err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}
	waitFor(t, 2*time.Second, "B sees trigger", func() bool {
		stmt, _, err := b.Read.Prepare(
			`SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = 'posts_ai'`)
		if err != nil {
			return false
		}
		defer stmt.Finalize()
		hasRow, _ := stmt.Step()
		return hasRow
	})
	if err := a.AppWrite.Exec(`INSERT INTO posts(id, body) VALUES (1, 'hello'), (2, 'world')`); err != nil {
		t.Fatalf("INSERT posts: %v", err)
	}
	check := func(n *Node) bool { return rowCount(t, n, "_post_index") == 2 }
	waitFor(t, 2*time.Second, "A _post_index = 2", func() bool { return check(a) })
	waitFor(t, 2*time.Second, "B _post_index = 2", func() bool { return check(b) })
}
