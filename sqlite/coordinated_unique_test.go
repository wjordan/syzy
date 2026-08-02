package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
	"github.com/wjordan/syzy/transport"
)

// retryInsert retries a coordinated INSERT until it succeeds or the
// deadline passes. A freshly-opened node's leaseholder needs a moment to
// acquire the lease; until then coordinated writes return a retryable
// "unavailable" error (the CAP cost, surfaced cleanly).
func retryInsert(t *testing.T, node *syzy.Node, sql string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := node.Exec(sql)
		if err == nil {
			return
		}
		if !syzy.IsCoordinatedUnavailable(err) {
			t.Fatalf("INSERT failed permanently: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("INSERT never succeeded: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCoordinatedUnique_SingleNode confirms NOT NULL UNIQUE is admitted and
// enforced by the in-process reservation registry on a single node.
func TestCoordinatedUnique_SingleNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{Path: t.TempDir() + "/app.db", SchemaLog: schemalog.NewLocal()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("CREATE TABLE with NOT NULL UNIQUE: %v", err)
	}
	if err := node.Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// Duplicate email is rejected as a final conflict.
	if err := node.Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err == nil {
		t.Fatal("duplicate email accepted; want UNIQUE rejection")
	} else if !syzy.IsCoordinatedConflict(err) {
		t.Fatalf("duplicate classification = %v", err)
	}
	// A distinct email succeeds.
	if err := node.Exec(`INSERT INTO users (email) VALUES ('b@x.com')`); err != nil {
		t.Fatalf("second distinct insert: %v", err)
	}
}

// noUniqueTransport is a Transport that cannot carry uniqueness RPCs
// (no unique.TransportProvider).
type noUniqueTransport struct{}

func (noUniqueTransport) Broadcast(context.Context, []byte) error { return nil }
func (noUniqueTransport) Subscribe(context.Context, transport.ApplyFunc) error {
	return transport.ErrClosed
}

// TestCoordinatedUnique_FailsClosedWithoutRPCTransport: a clustered
// config (bucket + transport) whose transport cannot carry uniqueness
// RPCs must fail Open — the old loopback fallback published an
// undialable leaseholder address and silently broke cross-node
// uniqueness. Config.LoopbackUnique explicitly opts single-process
// setups back in.
func TestCoordinatedUnique_FailsClosedWithoutRPCTransport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := syzy.Config{
		Path:          t.TempDir() + "/app.db",
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: testBackend(t),
		Transport:     noUniqueTransport{},
	}
	if _, err := syzy.Open(ctx, cfg); err == nil || !strings.Contains(err.Error(), "LoopbackUnique") {
		t.Fatalf("Open err = %v, want uniqueness-transport failure naming LoopbackUnique", err)
	}
	cfg.Path = t.TempDir() + "/app.db"
	cfg.LoopbackUnique = true
	node, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open with LoopbackUnique: %v", err)
	}
	_ = node.Close()
}

// TestCoordinatedUnique_LeaseholderBacked confirms the full leaseholder
// path: with object storage, the node elects a leaseholder, reserves
// coordinated values through it, and rejects duplicates.
func TestCoordinatedUnique_LeaseholderBacked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          t.TempDir() + "/app.db",
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: testBackend(t),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// First coordinated insert (retried until the leaseholder is serving).
	retryInsert(t, node, `INSERT INTO users (email) VALUES ('a@x.com')`)
	// Duplicate rejected.
	if err := node.Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err == nil {
		t.Fatal("duplicate accepted via leaseholder; want rejection")
	} else if !syzy.IsCoordinatedConflict(err) || syzy.IsCoordinatedUnavailable(err) {
		t.Fatalf("duplicate classification = %v", err)
	} else if !sqlitebridge.IsCode(err, sqlitebridge.ResultConstraintCommitHook) {
		t.Fatalf("duplicate lost commit-hook code: %v", err)
	}
	// Distinct succeeds; changing its value reserves the new and releases
	// the old (reclaim-after-quarantine is covered by the reservation-table
	// unit tests, which use a fake clock — the default window is too long
	// to wait on here).
	if err := node.Exec(`INSERT INTO users (email) VALUES ('b@x.com')`); err != nil {
		t.Fatalf("distinct insert: %v", err)
	}
	if err := node.Exec(`UPDATE users SET email='c@x.com' WHERE email='b@x.com'`); err != nil {
		t.Fatalf("update email: %v", err)
	}
}

// TestServeClones_FailsClosedWithoutBundleSource: ServeClones promises
// peers can clone this node; a Transport that cannot accept clone
// requests makes that promise silently unkeepable, so Open must fail.
func TestServeClones_FailsClosedWithoutBundleSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := syzy.Config{
		Path:           t.TempDir() + "/app.db",
		SchemaLog:      schemalog.NewLocal(),
		ObjectBackend:  testBackend(t),
		Transport:      noUniqueTransport{},
		LoopbackUnique: true,
		ServeClones:    true,
	}
	if _, err := syzy.Open(ctx, cfg); err == nil || !strings.Contains(err.Error(), "ServeClones") {
		t.Fatalf("Open err = %v, want ServeClones bundle-source failure", err)
	}
}

// TestCoordinatedUnique_TransferAppliesOnOriginator reproduces the
// originator apply boundary: a same-transaction transfer committed on
// another writer may order the claimant INSERT before the releasing DELETE.
// That is legal under reservation netting, so index normalization must leave
// the originator free of physical UNIQUE enforcement when it applies.
func TestCoordinatedUnique_TransferAppliesOnOriginator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dbA := filepath.Join(t.TempDir(), "app.db")
	dbB := filepath.Join(t.TempDir(), "app.db")
	log := schemalog.NewLocal()

	txA, err := syzy.NewTestTx(tcpmesh.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport A: %v", err)
	}
	defer txA.Close()
	txB, err := syzy.NewTestTx(tcpmesh.Config{Seeds: []string{txA.Addr()}, DialRetry: 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("transport B: %v", err)
	}
	defer txB.Close()

	nodeA, err := syzy.Open(ctx, syzy.Config{
		Path: dbA, Transport: txA, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	defer nodeA.Close()
	if err := syzy.JoinCluster(dbB, nodeA.ClusterID()); err != nil {
		t.Fatalf("JoinCluster B: %v", err)
	}
	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path: dbB, Transport: txB, ObjectBackend: testBackend(t),
		SchemaLog: log, SchemaCatchupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open B: %v", err)
	}
	defer nodeB.Close()

	// A originates the coordinated key and seeds the value holder.
	if err := nodeA.Exec(`CREATE TABLE accounts (id INTEGER PRIMARY KEY NOT NULL, handle TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("CREATE TABLE on A: %v", err)
	}
	retryInsert(t, nodeA, `INSERT INTO accounts(id,handle) VALUES (1,'x')`)

	readB, err := sqlitebridge.Open(dbB, 0)
	if err != nil {
		t.Fatalf("open B reader: %v", err)
	}
	defer readB.Close()
	waitForCount(t, readB, `SELECT count(*) FROM accounts WHERE id=1 AND handle='x'`,
		1, 5*time.Second, "seed row never replicated to B")

	// Same-txn transfer on the NON-originator, claimant INSERT first.
	// Retried: B's reserve path may be transiently unavailable while the
	// leaseholder RPC route settles.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := func() error {
			tx, err := nodeB.WriterDB().Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`INSERT INTO accounts(id,handle) VALUES (2,'x')`); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM accounts WHERE id=1`); err != nil {
				return err
			}
			return tx.Commit()
		}()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("transfer txn never committed on B: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The originator must apply the transfer changeset cleanly: new
	// holder present, old holder gone. A wedge times this out.
	readA, err := sqlitebridge.Open(dbA, 0)
	if err != nil {
		t.Fatalf("open A reader: %v", err)
	}
	defer readA.Close()
	waitForCount(t, readA,
		`SELECT (SELECT count(*) FROM accounts WHERE id=2 AND handle='x') = 1
		    AND (SELECT count(*) FROM accounts WHERE id=1) = 0`,
		1, 5*time.Second, "transfer never applied on originator A (apply wedge)")
}

// coordNode opens a single-node database whose reservation backend is
// the in-process registry (no ObjectBackend, no Transport).
func coordNode(t *testing.T, path string, log schemalog.Log) *syzy.Node {
	t.Helper()
	n, err := syzy.Open(context.Background(), syzy.Config{Path: path, SchemaLog: log})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return n
}

// TestCoordinatedUnique_LocalRegistryDerivesFromRows: the in-process
// registry is the ONLY enforcement in single-node mode, and it is
// process state — it starts empty while the rows do not. Both cases
// below committed duplicate coordinated values before the registry
// derived each key's taken-set from the rows on first use.
func TestCoordinatedUnique_LocalRegistryDerivesFromRows(t *testing.T) {
	t.Parallel()
	t.Run("across restart", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "app.db")
		log := schemalog.NewLocal()
		n := coordNode(t, path, log)
		if err := n.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`); err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
		if err := n.Exec(`INSERT INTO t VALUES (1,'x')`); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		n.Close()

		n = coordNode(t, path, log)
		defer n.Close()
		if err := n.Exec(`INSERT INTO t VALUES (2,'x')`); err == nil {
			t.Error("duplicate accepted after restart; the registry forgot the row's value")
		}
	})

	t.Run("no schema log", func(t *testing.T) {
		// A producer opened without a SchemaLog installs no DDL admission,
		// and used to skip commit_hook with it — taking the reservation
		// gate along, though the rows still needed gating.
		path := filepath.Join(t.TempDir(), "app.db")
		n := coordNode(t, path, schemalog.NewLocal())
		for _, s := range []string{
			`CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`,
			`INSERT INTO t VALUES (1,'x')`,
		} {
			if err := n.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		n.Close()

		n2, err := syzy.Open(context.Background(), syzy.Config{Path: path})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer n2.Close()
		if err := n2.Exec(`INSERT INTO t VALUES (2,'x')`); err == nil {
			t.Error("duplicate accepted with no SchemaLog; the gate was not installed")
		}
	})

	t.Run("key on populated table", func(t *testing.T) {
		n := coordNode(t, filepath.Join(t.TempDir(), "app.db"), schemalog.NewLocal())
		defer n.Close()
		for _, s := range []string{
			`CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, email TEXT NOT NULL)`,
			`INSERT INTO t VALUES (1,'x')`,
			`CREATE UNIQUE INDEX ux ON t(email)`,
		} {
			if err := n.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		if err := n.Exec(`INSERT INTO t VALUES (2,'x')`); err == nil {
			t.Error("duplicate accepted; the pre-key row's value was never derived")
		}
	})
}

// TestCoordinatedUnique_RejectsSavepointRollback: SQLite reports a row
// change through the preupdate hook and does not un-report it when
// ROLLBACK TO a savepoint undoes it, so the change capture's last image
// can be a value the commit never lands. The gate would then reserve the
// phantom and leave the committed value free for a duplicate. Refused at
// commit instead.
func TestCoordinatedUnique_RejectsSavepointRollback(t *testing.T) {
	t.Parallel()
	n := coordNode(t, filepath.Join(t.TempDir(), "app.db"), schemalog.NewLocal())
	defer n.Close()
	for _, s := range []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`,
		`INSERT INTO t VALUES (1,'a')`,
	} {
		if err := n.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	tx, err := n.WriterDB().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	for _, s := range []string{
		`UPDATE t SET email='c' WHERE id=1`,
		`SAVEPOINT s`,
		`UPDATE t SET email='b' WHERE id=1`,
		`ROLLBACK TO s`,
		`RELEASE s`,
	} {
		if _, err := tx.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("commit accepted; the gate reserved the rolled-back value, not the committed one")
	}
	// A savepoint-free transaction over the same table still commits.
	if err := n.Exec(`UPDATE t SET email='c' WHERE id=1`); err != nil {
		t.Fatalf("plain update rejected: %v", err)
	}
	if err := n.Exec(`INSERT INTO t VALUES (2,'c')`); err == nil {
		t.Error("duplicate of the committed value accepted")
	}
}
