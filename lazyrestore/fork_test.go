//go:build linux

package lazyrestore_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/lazyrestore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
)

func TestFork_NoPublishedSource_ReturnsZero(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	apps, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}

	dstSlot := filepath.Join(t.TempDir(), "dst")
	manifest, err := lazyrestore.Fork(context.Background(), lazyrestore.ForkConfig{
		SourcePrefix:   "src-uuid/",
		Bucket:         apps,
		DestinationDir: dstSlot,
		DatabaseName:   "shared.db",
	})
	if err != nil {
		t.Fatalf("Fork on empty source: %v", err)
	}
	if manifest != nil {
		t.Fatalf("Fork on empty source returned %v, want nil", manifest)
	}
	if _, err := os.Stat(dstSlot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Fork on empty source left dst behind: %v", err)
	}
}

func TestFork_RoundTrip_ColdSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	apps, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const srcPrefix = "src-uuid/"
	srcView := objectstore.Prefixed(apps, srcPrefix)
	srcDB := filepath.Join(t.TempDir(), "src.db")
	publishSourceBucket(t, srcView, srcDB)

	dstSlot := filepath.Join(t.TempDir(), "dst")
	manifest, err := lazyrestore.Fork(context.Background(), lazyrestore.ForkConfig{
		SourcePrefix:   srcPrefix,
		Bucket:         apps,
		DestinationDir: dstSlot,
		DatabaseName:   "shared.db",
	})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if manifest == nil {
		t.Fatalf("Fork: nil manifest from populated source")
	}
	parentTXID := manifest.PinnedTXID
	if parentTXID == 0 {
		t.Fatalf("Fork: zero parentTXID from populated source")
	}

	// Manifest keys must be qualified with SourcePrefix so a
	// host-wide Bucket FetchPage can resolve them.
	if len(manifest.Pages) == 0 {
		t.Fatalf("manifest has no pages")
	}
	for pgno, loc := range manifest.Pages {
		if !strings.HasPrefix(loc.Key, srcPrefix) {
			t.Fatalf("page %d key %q missing %q prefix", pgno, loc.Key, srcPrefix)
		}
	}

	// Destination slot must have shared.db (sparse) + metadata.db
	// + sidecar.
	dstShared := filepath.Join(dstSlot, "shared.db")
	st, err := os.Stat(dstShared)
	if err != nil {
		t.Fatalf("stat dst shared.db: %v", err)
	}
	if want := int64(manifest.CommitPages) * int64(manifest.PageSize); st.Size() != want {
		t.Fatalf("dst size=%d want=%d", st.Size(), want)
	}
	if _, err := os.Stat(filepath.Join(dstSlot, "shared.db-syzy", "metadata.db")); err != nil {
		t.Fatalf("dst metadata.db: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dstSlot, ".lazy-manifest.bin")); err != nil {
		t.Fatalf("dst sidecar: %v", err)
	}

	// FetchPage against the host-wide apps bucket must succeed.
	page1, err := manifest.FetchPage(context.Background(), apps, 1)
	if err != nil {
		t.Fatalf("FetchPage 1 via apps bucket: %v", err)
	}
	if string(page1[0:16]) != "SQLite format 3\x00" {
		t.Fatalf("page 1 magic: got %q", page1[0:16])
	}

	// Reopen the staged metadata.db and confirm AdoptFork's effects:
	// new cluster_id, new node_id, schema_seq=0, parent_app_txid
	// preserved.
	metaDBPath := filepath.Join(dstSlot, "shared.db-syzy", "metadata.db")
	verifyAdoptedFork(t, metaDBPath, parentTXID)
}

func TestFork_LiveSource_PublishesAndForks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	apps, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const srcPrefix = "src-uuid/"
	srcView := objectstore.Prefixed(apps, srcPrefix)
	ctx := context.Background()
	srcDB := filepath.Join(t.TempDir(), "src.db")
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          srcDB,
		ObjectBackend: srcView,
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	defer node.Close()
	if err := node.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 1; i <= 4; i++ {
		if err := node.Exec(fmt.Sprintf(`INSERT INTO notes VALUES (%d, 'r%d')`, i, i)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	dstSlot := filepath.Join(t.TempDir(), "dst")
	manifest, err := lazyrestore.Fork(ctx, lazyrestore.ForkConfig{
		SourcePrefix:   srcPrefix,
		Bucket:         apps,
		SourceNode:     node,
		DestinationDir: dstSlot,
		DatabaseName:   "shared.db",
	})
	if err != nil {
		t.Fatalf("Fork (live): %v", err)
	}
	if manifest == nil || manifest.PinnedTXID == 0 {
		t.Fatalf("Fork (live): manifest=%v", manifest)
	}
	// PublishSnapshot was called as part of Fork — the source must
	// now have a HEAD with a baseline.
	head, _, err := objstore.LoadHEAD(ctx, srcView)
	if err != nil {
		t.Fatalf("LoadHEAD after live fork: %v", err)
	}
	if head.Baseline == nil {
		t.Fatalf("source has no baseline after Fork(live)")
	}
}

func TestFork_RefusesExistingDst(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	apps, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	const srcPrefix = "src-uuid/"
	srcView := objectstore.Prefixed(apps, srcPrefix)
	srcDB := filepath.Join(t.TempDir(), "src.db")
	publishSourceBucket(t, srcView, srcDB)

	dstSlot := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(dstSlot, 0o700); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}

	_, err = lazyrestore.Fork(context.Background(), lazyrestore.ForkConfig{
		SourcePrefix:   srcPrefix,
		Bucket:         apps,
		DestinationDir: dstSlot,
		DatabaseName:   "shared.db",
	})
	if err == nil {
		t.Fatalf("Fork against existing dst: nil error, want refusal")
	}
}

func TestFork_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	apps, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "dst")

	cases := []struct {
		name string
		mut  func(c *lazyrestore.ForkConfig)
	}{
		{"nil-bucket", func(c *lazyrestore.ForkConfig) { c.Bucket = nil }},
		{"empty-prefix", func(c *lazyrestore.ForkConfig) { c.SourcePrefix = "" }},
		{"empty-dst", func(c *lazyrestore.ForkConfig) { c.DestinationDir = "" }},
		{"empty-name", func(c *lazyrestore.ForkConfig) { c.DatabaseName = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := lazyrestore.ForkConfig{
				SourcePrefix:   "src/",
				Bucket:         apps,
				DestinationDir: dst,
				DatabaseName:   "shared.db",
			}
			tc.mut(&cfg)
			if _, err := lazyrestore.Fork(context.Background(), cfg); err == nil {
				t.Fatalf("Fork: nil error, want validation failure")
			}
		})
	}
}

// verifyAdoptedFork reopens the forked metadata.db read-only and
// checks the AdoptFork-mutated meta values.
func verifyAdoptedFork(t *testing.T, metaDBPath string, wantParentTXID uint64) {
	t.Helper()
	v := readMetaBlob(t, metaDBPath, "node_id")
	if v == nil {
		t.Fatalf("node_id missing")
	}
	if got := crdt.Origin(binary.BigEndian.Uint64(v)); got == 0 {
		t.Fatal("node_id is zero")
	}
	cid := readMetaBlob(t, metaDBPath, "cluster_id")
	if cid == nil {
		t.Fatalf("cluster_id missing")
	}
	var gotCluster crdt.ClusterID
	copy(gotCluster[:], cid)
	if gotCluster == (crdt.ClusterID{}) {
		t.Fatal("cluster_id is zero")
	}
	ss := readMetaBlob(t, metaDBPath, "schema_seq")
	// Allow absent (== 0) or explicit zero — both indicate fresh log namespace.
	if ss != nil && binary.BigEndian.Uint64(ss) != 0 {
		t.Fatalf("schema_seq=%d want=0", binary.BigEndian.Uint64(ss))
	}
	pat := readMetaBlob(t, metaDBPath, "parent_app_txid")
	if pat == nil {
		t.Fatalf("parent_app_txid missing after AdoptFork")
	}
	if got := binary.BigEndian.Uint64(pat); got != wantParentTXID {
		t.Fatalf("parent_app_txid=%d want=%d", got, wantParentTXID)
	}
}

// readMetaBlob opens metaDB via sqlitebridge and returns the value
// blob for the given meta key, or nil if absent.
func readMetaBlob(t *testing.T, metaDB string, key string) []byte {
	t.Helper()
	conn, err := sqlitebridge.Open(metaDB, 0)
	if err != nil {
		t.Fatalf("open %s: %v", metaDB, err)
	}
	defer conn.Close()
	stmt, _, err := conn.Prepare(`SELECT value FROM meta WHERE key = ?`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, key); err != nil {
		t.Fatalf("bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !hasRow {
		return nil
	}
	v := stmt.ColumnBlob(0)
	if v == nil {
		return []byte{}
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}
