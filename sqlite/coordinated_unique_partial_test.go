package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// openPartialNode opens a single-node instance with an in-process Local
// reservation registry (no object backend), so coordinated reservations
// are immediately available and a release frees its value at once.
func openPartialNode(t *testing.T) *syzy.Node {
	t.Helper()
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:      t.TempDir() + "/app.db",
		SchemaLog: schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })
	return node
}

func mustExec(t *testing.T, node *syzy.Node, sql string) {
	t.Helper()
	if err := node.Exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func wantExecErr(t *testing.T, node *syzy.Node, sql string) {
	t.Helper()
	if err := node.Exec(sql); err == nil {
		t.Fatalf("exec %q: expected error, got nil", sql)
	}
}

// TestCoordinatedUnique_PartialSoftDelete drives the canonical soft-delete
// idiom end-to-end: UNIQUE(email) WHERE deleted_at IS NULL. It exercises
// the reservation participation gate — a soft-delete must *release* the
// value (else the reuse INSERT would conflict with a stale reservation even
// though the physical partial index allows it), and an undelete must
// re-acquire it (conflicting with a live holder).
func TestCoordinatedUnique_PartialSoftDelete(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, deleted_at INTEGER)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_email ON users(email) WHERE deleted_at IS NULL`)

	// A live row reserves its email; a second live row with the same email
	// is rejected.
	mustExec(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (1,'a@x',NULL)`)
	wantExecErr(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (2,'a@x',NULL)`)

	// Soft-delete row 1: the predicate flips false, releasing 'a@x'. A new
	// live row may then claim it — this only succeeds if the reservation was
	// actually released (the physical index alone can't prove the point,
	// but a stale reservation would make this INSERT fail).
	mustExec(t, node, `UPDATE users SET deleted_at=100 WHERE id=1`)
	mustExec(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (3,'a@x',NULL)`)

	// Row 3 now owns 'a@x'; another live duplicate is rejected, and
	// undeleting row 1 (which would create a second live 'a@x') fails.
	wantExecErr(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (4,'a@x',NULL)`)
	wantExecErr(t, node, `UPDATE users SET deleted_at=NULL WHERE id=1`)

	// Free the value by soft-deleting row 3; row 1 may then be undeleted,
	// re-acquiring 'a@x' (predicate-flip reserve).
	mustExec(t, node, `UPDATE users SET deleted_at=200 WHERE id=3`)
	mustExec(t, node, `UPDATE users SET deleted_at=NULL WHERE id=1`)

	// Exactly one live row holds 'a@x'.
	assertCount(t, node, `SELECT count(*) FROM users WHERE email='a@x' AND deleted_at IS NULL`, 1)
	assertCount(t, node, `SELECT count(*) FROM users WHERE email='a@x'`, 2)
}

// TestCoordinatedUnique_PartialNumericFlag covers a numeric-affinity
// predicate (WHERE archived = 0), validating the comparison path of the
// reserve-time evaluator against SQLite's own partial index.
func TestCoordinatedUnique_PartialNumericFlag(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE docs (id INTEGER PRIMARY KEY, slug TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_slug ON docs(slug) WHERE archived = 0`)

	mustExec(t, node, `INSERT INTO docs(id,slug,archived) VALUES (1,'intro',0)`)
	wantExecErr(t, node, `INSERT INTO docs(id,slug,archived) VALUES (2,'intro',0)`)
	// Archiving flips the predicate false, releasing 'intro'.
	mustExec(t, node, `UPDATE docs SET archived=1 WHERE id=1`)
	mustExec(t, node, `INSERT INTO docs(id,slug,archived) VALUES (3,'intro',0)`)
	assertCount(t, node, `SELECT count(*) FROM docs WHERE slug='intro' AND archived=0`, 1)
}

// TestCoordinatedUnique_PartialNocaseText drives a partial predicate with
// a case-insensitive text comparison (WHERE status = 'active' on a NOCASE
// column). The release on a predicate flip must evaluate the OLD image
// under NOCASE — if the Go-side collation disagreed with SQLite's index,
// the reuse INSERT would conflict with a stale reservation.
func TestCoordinatedUnique_PartialNocaseText(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE docs (id INTEGER PRIMARY KEY, slug TEXT NOT NULL, status TEXT COLLATE NOCASE)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_slug ON docs(slug) WHERE status = 'active'`)

	// 'ACTIVE' folds to 'active' (NOCASE), so id=1 participates and reserves.
	mustExec(t, node, `INSERT INTO docs(id,slug,status) VALUES (1,'intro','ACTIVE')`)
	wantExecErr(t, node, `INSERT INTO docs(id,slug,status) VALUES (2,'intro','active')`)
	// A non-active row may share the slug.
	mustExec(t, node, `INSERT INTO docs(id,slug,status) VALUES (3,'intro','draft')`)

	// Flip id=1 out of the predicate: the reserve path must NOCASE-match the
	// OLD 'ACTIVE' to release 'intro'.
	mustExec(t, node, `UPDATE docs SET status='draft' WHERE id=1`)
	mustExec(t, node, `INSERT INTO docs(id,slug,status) VALUES (4,'intro','Active')`)
	assertCount(t, node, `SELECT count(*) FROM docs WHERE slug='intro' AND status='active'`, 1)
}

// TestCoordinatedUnique_CollationRules checks the collation admission
// boundaries: a non-BINARY unique-key member is rejected (the value
// encoding can't fold), while a non-BINARY *predicate* column is fine; a
// custom collation is rejected as non-replicable.
func TestCoordinatedUnique_CollationRules(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)

	// NOCASE on a coordinated key member is rejected.
	if err := node.Exec(`CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT COLLATE NOCASE NOT NULL UNIQUE)`); err == nil {
		t.Fatal("NOCASE coordinated key member accepted; want rejection")
	}
	// Custom collation is rejected at table creation.
	if err := node.Exec(`CREATE TABLE c (id INTEGER PRIMARY KEY, x TEXT COLLATE FOOBAR)`); err == nil {
		t.Fatal("custom collation accepted; want rejection")
	}
	// BINARY key member + NOCASE predicate column is accepted.
	mustExec(t, node, `CREATE TABLE ok (id INTEGER PRIMARY KEY, slug TEXT NOT NULL, status TEXT COLLATE NOCASE)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq ON ok(slug) WHERE status = 'active'`)
}

// TestCoordinatedUnique_PartialAdmission checks the admission boundaries:
// eventual partial keys and unsupported predicate grammar are rejected
// with clear errors, while the supported grammar is accepted.
func TestCoordinatedUnique_PartialAdmission(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL, nick TEXT, status TEXT, deleted_at INTEGER)`)

	// Eventual (nullable member) partial index is rejected.
	if err := node.Exec(`CREATE UNIQUE INDEX uq_nick ON t(nick) WHERE deleted_at IS NULL`); err == nil {
		t.Fatal("eventual partial index accepted; want rejection")
	} else if !strings.Contains(err.Error(), "partial") && !strings.Contains(err.Error(), "NOT NULL") {
		t.Logf("eventual-partial rejection error: %v", err)
	}

	// A text comparison against a non-text column is rejected (affinity
	// mismatch would force a coercion the evaluator doesn't model).
	if err := node.Exec(`CREATE UNIQUE INDEX uq_email_n ON t(email) WHERE deleted_at = 'soon'`); err == nil {
		t.Fatal("text literal vs numeric column accepted; want rejection")
	}

	// Unsupported operator (LIKE) is rejected.
	if err := node.Exec(`CREATE UNIQUE INDEX uq_email_l ON t(email) WHERE email LIKE 'a%'`); err == nil {
		t.Fatal("LIKE predicate accepted; want rejection")
	}

	// Supported grammar is accepted, including a text comparison.
	mustExec(t, node, `CREATE UNIQUE INDEX uq_email ON t(email) WHERE deleted_at IS NULL`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_email_s ON t(email) WHERE status = 'active'`)
}

// TestCoordinatedUnique_SameTxnTransfer covers freeing and re-claiming a
// coordinated value within ONE transaction — the reserve runs pre-commit
// and the release post-commit, so these must be netted into a transfer or
// the reserve spuriously conflicts with the still-held releaser. A
// transaction is one atomic changeset, so the transfer is replica-safe.
func TestCoordinatedUnique_SameTxnTransfer(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, deleted_at INTEGER)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq ON users(email) WHERE deleted_at IS NULL`)
	mustExec(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (1,'a@x',NULL)`)

	// Soft-delete row 1 and insert row 2 with the same email, atomically.
	mustTxn(t, node,
		`UPDATE users SET deleted_at=1 WHERE id=1`,
		`INSERT INTO users(id,email,deleted_at) VALUES (2,'a@x',NULL)`)
	assertCount(t, node, `SELECT count(*) FROM users WHERE email='a@x' AND deleted_at IS NULL`, 1)

	// A second live 'a@x' is still rejected (the value is held by row 2).
	wantExecErr(t, node, `INSERT INTO users(id,email,deleted_at) VALUES (3,'a@x',NULL)`)
}

// TestCoordinatedUnique_SameTxnSwap covers two live rows swapping their
// coordinated values in one transaction — each reserve must transfer from
// the other's release.
func TestCoordinatedUnique_SameTxnSwap(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)`)
	mustExec(t, node, `INSERT INTO u(id,email) VALUES (1,'a@x')`)
	mustExec(t, node, `INSERT INTO u(id,email) VALUES (2,'b@x')`)
	// Swap via a temporary value to avoid a transient same-value collision
	// inside the statement order, then assert the final swap committed.
	mustTxn(t, node,
		`UPDATE u SET email='tmp' WHERE id=1`,
		`UPDATE u SET email='a@x' WHERE id=2`,
		`UPDATE u SET email='b@x' WHERE id=1`)
	assertCount(t, node, `SELECT count(*) FROM u WHERE id=1 AND email='b@x'`, 1)
	assertCount(t, node, `SELECT count(*) FROM u WHERE id=2 AND email='a@x'`, 1)
}

func mustTxn(t *testing.T, node *syzy.Node, stmts ...string) {
	t.Helper()
	tx, err := node.WriterDB().Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s); err != nil {
			tx.Rollback()
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func assertCount(t *testing.T, node *syzy.Node, query string, want int) {
	t.Helper()
	var got int
	if err := node.WriterDB().QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q: count = %d, want %d", query, got, want)
	}
}

// TestCoordinatedUnique_DropIndexPicksKeyByPredicate: DROP INDEX maps to
// key removal by column tuple AND predicate. Two partial keys over one
// column are legal, so a column-only match would drop whichever the
// catalog happened to list first — silent loss of a constraint the
// operator did not name.
func TestCoordinatedUnique_DropIndexPicksKeyByPredicate(t *testing.T) {
	t.Parallel()
	node := openPartialNode(t)
	mustExec(t, node, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT NOT NULL, status TEXT, deleted_at INTEGER)`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_live ON t(email) WHERE deleted_at IS NULL`)
	mustExec(t, node, `CREATE UNIQUE INDEX uq_active ON t(email) WHERE status = 'active'`)

	mustExec(t, node, `DROP INDEX uq_active`)
	// uq_live's key must survive: recreating an equivalent key is
	// rejected only while the original is still active.
	if err := node.Exec(`CREATE UNIQUE INDEX uq_live2 ON t(email) WHERE deleted_at IS NULL`); err == nil {
		t.Fatal("uq_live's key was removed instead of uq_active's")
	}
	// uq_active's key really is gone: an equivalent key is admissible.
	mustExec(t, node, `CREATE UNIQUE INDEX uq_active2 ON t(email) WHERE status = 'active'`)
}
