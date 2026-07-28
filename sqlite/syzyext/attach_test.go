package syzyext

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

func TestAttachDoesNotDialConfiguredStreamSchemaLog(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
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

	// The socket deliberately has no listener. Attach must only construct
	// the lazy client; the first DDL admission or catch-up operation owns
	// connecting to the schema authority.
	for _, key := range []string{
		"SYZY_SCHEMA_LOG",
		"SYZY_SCHEMA_LOG_S3",
		"SYZY_CLUSTER",
		"SYZY_OBJECT_BACKEND",
		"SYZY_WAKE_VSOCK",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("SYZY_SCHEMA_LOG_DIAL", "unix:"+filepath.Join(t.TempDir(), "absent.sock"))

	attached, err := Attach(conn, Config{
		DBPath: dbPath,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Attach with unavailable schema-log endpoint: %v", err)
	}
	t.Cleanup(func() { _ = attached.Close() })
}
