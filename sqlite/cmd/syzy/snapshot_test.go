package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/sqlitebridge"
)

// snapshotFixture creates a stopped node's app.db + metadata.db with a
// minted cluster_id, as snapshotCmd expects to find them.
func snapshotFixture(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("open app.db: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t VALUES (1, 'offline')`,
	} {
		if err := conn.Exec(sql); err != nil {
			t.Fatalf("Exec %q: %v", sql, err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close app.db: %v", err)
	}
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		t.Fatalf("create meta dir: %v", err)
	}
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatalf("open metadata.db: %v", err)
	}
	if err := sc.SetClusterID(crdt.ClusterID{0xCA, 0xFE}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("close metadata.db: %v", err)
	}
	return dbPath
}

func TestSnapshotCmdPublishesAndReleasesReservation(t *testing.T) {
	dbPath := snapshotFixture(t)
	bucketDir := t.TempDir()

	if err := snapshotCmd([]string{"--db", dbPath, "--bucket", "file://" + bucketDir}); err != nil {
		t.Fatalf("snapshotCmd: %v", err)
	}

	be, err := objectstore.OpenFS(bucketDir)
	if err != nil {
		t.Fatal(err)
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Baseline == nil || head.Baseline.TXID != 1 || head.MetaBaseline == nil || head.MetaBaseline.TXID != 1 {
		t.Fatalf("coupled baselines not promoted: %+v", head)
	}
	if head.Publisher == nil || !strings.HasPrefix(head.Publisher.NodeID, "maintenance:") {
		t.Fatalf("HEAD publisher = %+v, want maintenance identity", head.Publisher)
	}
	if head.Publisher.ExpiresAtUS != 0 {
		t.Fatalf("maintenance reservation not released: expires_at_us=%d", head.Publisher.ExpiresAtUS)
	}
}

func TestSnapshotCmdRefusesActivePublisher(t *testing.T) {
	dbPath := snapshotFixture(t)
	bucketDir := t.TempDir()
	be, err := objectstore.OpenFS(bucketDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objstore.CASHead(context.Background(), be, &objstore.HEAD{
		Version: objstore.HEADVersion, ClusterID: "cafe0000000000000000000000000000",
		Publisher: &objstore.Publisher{
			NodeID: "node-live", Generation: 3,
			ExpiresAtUS: time.Now().Add(time.Minute).UnixMicro(),
		},
	}, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed live publisher: %v", err)
	}

	err = snapshotCmd([]string{"--db", dbPath, "--bucket", "file://" + bucketDir})
	if !errors.Is(err, objstore.ErrPublisherLeaseActive) {
		t.Fatalf("snapshotCmd error = %v, want ErrPublisherLeaseActive", err)
	}
	for _, prefix := range []string{objstore.DBPrefix, objstore.MetadataPrefix} {
		files, err := objstore.ListLTX(context.Background(), be, prefix, objstore.BaselineLevel)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 0 {
			t.Fatalf("refused snapshot uploaded %d %s baseline objects", len(files), prefix)
		}
	}
	head, _, err := objstore.LoadHEAD(context.Background(), be)
	if err != nil {
		t.Fatal(err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "node-live" || head.Publisher.Generation != 3 {
		t.Fatalf("refused snapshot disturbed live publisher: %+v", head.Publisher)
	}
}
