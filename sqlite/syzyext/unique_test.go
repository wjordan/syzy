package syzyext

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
)

// attachFixture opens a fresh app DB with seeded metadata and attaches a
// producer, with every SYZY_* env cleared except those in env.
func attachFixture(t *testing.T, env map[string]string) (*sqlitebridge.Conn, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// The producer's wal_hook (journal append + DDL intent resolution)
	// only fires in WAL mode, which the shared DB always runs under.
	if err := conn.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.SetClusterID(crdt.ClusterID{1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatal(err)
	}
	if err := meta.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"SYZY_SCHEMA_LOG",
		"SYZY_SCHEMA_LOG_DIAL",
		"SYZY_SCHEMA_LOG_S3",
		"SYZY_CLUSTER",
		"SYZY_OBJECT_BACKEND",
		"SYZY_WAKE_VSOCK",
		"SYZY_UNIQUE_DIAL",
	} {
		t.Setenv(key, "")
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
	attached, err := Attach(conn, Config{
		DBPath: dbPath,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Cleanup(func() { _ = attached.Close() })
	return conn, dbPath
}

const coordinatedDDL = `CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`

// TestAttachUniqueDialConflictRoundTrips is the end-to-end guest path:
// SYZY_UNIQUE_DIAL enables coordinated DDL admission, the first insert
// claims its value through the proxied registry, a duplicate loses with
// ErrConflict (never an outage), and a distinct value still lands.
func TestAttachUniqueDialConflictRoundTrips(t *testing.T) {
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "unique.sock"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := unique.ServeProxy(ln, unique.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	conn, _ := attachFixture(t, map[string]string{
		"SYZY_UNIQUE_DIAL": "unix:" + ln.Addr().String(),
	})
	if err := conn.Exec(coordinatedDDL); err != nil {
		t.Fatalf("coordinated DDL: %v", err)
	}
	if err := conn.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err = conn.Exec(`INSERT INTO u (id, email) VALUES (x'02', 'a@x.com')`)
	if !errors.Is(err, unique.ErrConflict) {
		t.Fatalf("duplicate insert err = %v, want ErrConflict", err)
	}
	if err := conn.Exec(`INSERT INTO u (id, email) VALUES (x'02', 'b@x.com')`); err != nil {
		t.Fatalf("distinct insert: %v", err)
	}
}

func TestAttachUniqueDialAbsentRejectsCoordinatedDDL(t *testing.T) {
	conn, _ := attachFixture(t, nil)
	if err := conn.Exec(coordinatedDDL); err == nil {
		t.Fatal("coordinated DDL admitted with no reservation registry")
	}
}

func TestAttachUniqueDialUnreachableRejectsCoordinatedDDL(t *testing.T) {
	conn, _ := attachFixture(t, map[string]string{
		"SYZY_UNIQUE_DIAL": "unix:" + filepath.Join(t.TempDir(), "absent.sock"),
	})
	if err := conn.Exec(coordinatedDDL); err == nil {
		t.Fatal("coordinated DDL admitted with unreachable reservation endpoint")
	}
}
