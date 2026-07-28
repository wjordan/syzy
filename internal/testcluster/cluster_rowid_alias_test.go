package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport/memtransport"
)

// TestRowidAliasRewrite_MultiWriterNoCollisions exercises the canonical
// reason the rowid-alias rewrite exists: two writers on different
// origins INSERT into the same `INTEGER PRIMARY KEY` table without
// providing an id, and replicated rows converge on both nodes without
// PK collisions. Without the rewrite each writer would compute
// max(rowid)+1 locally; the first inbound apply from the other writer
// would collide on PK.
func TestRowidAliasRewrite_MultiWriterNoCollisions(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	b := NewWithDDL(t, hub, 2, log, 5*time.Millisecond)
	a.Start(t, ctx)
	b.Start(t, ctx)

	// User-style DDL: bare INTEGER PRIMARY KEY. Without the rewrite this
	// would land as a rowid alias on both nodes and the first cross-
	// origin INSERT to apply would crash on UNIQUE.
	if err := a.AppWrite.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	waitForTable(t, b, "posts", 2*time.Second)

	const perWriter = 50
	for i := 0; i < perWriter; i++ {
		if err := a.AppWrite.Exec(fmt.Sprintf(`INSERT INTO posts (title) VALUES ('a-%d')`, i)); err != nil {
			t.Fatalf("A insert %d: %v", i, err)
		}
		if err := b.AppWrite.Exec(fmt.Sprintf(`INSERT INTO posts (title) VALUES ('b-%d')`, i)); err != nil {
			t.Fatalf("B insert %d: %v", i, err)
		}
	}

	// Both nodes should converge on 2*perWriter distinct rows. The
	// catch-up loop runs continuously; poll until both sides agree.
	want := int64(2 * perWriter)
	waitForRowCount(t, a, "posts", want, 5*time.Second)
	waitForRowCount(t, b, "posts", want, 5*time.Second)

	// No two rows share an id.
	assertNoDuplicateIDs(t, a, "posts")
	assertNoDuplicateIDs(t, b, "posts")
}

// TestRowidAliasRewrite_ExplicitIDWins documents that the rewrite only
// fires when the user omits the id column. An INSERT with an explicit
// id stores that exact value — the gen_id default doesn't fire.
func TestRowidAliasRewrite_ExplicitIDWins(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	a.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := a.AppWrite.Exec(`INSERT INTO posts (id, title) VALUES (42, 'hand-picked')`); err != nil {
		t.Fatalf("INSERT explicit id: %v", err)
	}
	stmt, _, err := a.AppWrite.Prepare(`SELECT id FROM posts WHERE title='hand-picked'`)
	if err != nil {
		t.Fatalf("prep: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got := stmt.ColumnInt64(0); got != 42 {
		t.Errorf("id = %d; want 42 (explicit value should win over DEFAULT)", got)
	}
}

// TestRowidAliasRewrite_ReturningClauseSurfacesGenID confirms the
// recommended ORM idiom (INSERT ... RETURNING id) returns the assigned
// gen_id value — the modern replacement for sqlite3_last_insert_rowid
// that the rewrite no longer supports.
func TestRowidAliasRewrite_ReturningClauseSurfacesGenID(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := schemalog.NewLocal()
	a := NewWithDDL(t, hub, 1, log, 5*time.Millisecond)
	a.Start(t, ctx)

	if err := a.AppWrite.Exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	stmt, _, err := a.AppWrite.Prepare(`INSERT INTO posts (title) VALUES ('hi') RETURNING id`)
	if err != nil {
		t.Fatalf("prep RETURNING: %v", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("step: hasRow=%v err=%v", hasRow, err)
	}
	got := stmt.ColumnInt64(0)
	if got < (1 << 33) {
		t.Errorf("RETURNING id = %d; want gen_id value >= 2^33", got)
	}
}

func waitForTable(t *testing.T, n *Node, name string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, ok := n.Catalog.Table(name); ok {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("table %q never reached node origin=%v", name, n.Origin)
}

func waitForRowCount(t *testing.T, n *Node, table string, want int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last int64 = -1
	for time.Now().Before(deadline) {
		stmt, _, err := n.Read.Prepare(fmt.Sprintf(`SELECT count(*) FROM %s`, table))
		if err != nil {
			t.Fatalf("prep count: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			stmt.Finalize()
			t.Fatalf("step count: %v", err)
		}
		last = stmt.ColumnInt64(0)
		stmt.Finalize()
		if last == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("origin=%v: count(%s) = %d; want %d", n.Origin, table, last, want)
}

func assertNoDuplicateIDs(t *testing.T, n *Node, table string) {
	t.Helper()
	stmt, _, err := n.Read.Prepare(fmt.Sprintf(
		`SELECT id, count(*) FROM %s GROUP BY id HAVING count(*) > 1 LIMIT 5`, table))
	if err != nil {
		t.Fatalf("prep dup-check: %v", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step dup-check: %v", err)
	}
	if hasRow {
		t.Errorf("origin=%v: duplicate id in %s (first: id=%d count=%d)",
			n.Origin, table, stmt.ColumnInt64(0), stmt.ColumnInt64(1))
	}
}
