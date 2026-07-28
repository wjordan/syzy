//go:build linux

package lazyrestore_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/lazyrestore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestPrepare_StaleBaselineGrownChain pins the commit-pages bug: a HEAD
// whose baseline predates an L0 chain that grew the database (the publisher
// had not rebaselined — e.g. the publishing node crashed between L0 pushes
// and its next baseline). The sparse file and manifest must be sized to the
// merged state's page count, not the baseline header's; otherwise the merged
// page 1 declares more pages than the file holds and SQLite reports
// "database disk image is malformed" on first open.
func TestPrepare_StaleBaselineGrownChain(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()

	// Publish twice: a small early baseline, then substantial growth and a
	// second baseline. Both baseline objects and the L0 chain between them
	// remain in the bucket.
	srcDB := filepath.Join(t.TempDir(), "src.db")
	publishGrownBucket(t, be, srcDB)

	baselines, err := objstore.ListLTX(ctx, be, objstore.DBPrefix, objstore.BaselineLevel)
	if err != nil {
		t.Fatalf("ListLTX baselines: %v", err)
	}
	if len(baselines) < 2 {
		t.Fatalf("want >=2 baselines (early + rebaselined), got %d", len(baselines))
	}
	oldest := baselines[0]
	for _, b := range baselines[1:] {
		if b.MaxTXID < oldest.MaxTXID {
			oldest = b
		}
	}

	// Doctor HEAD back to the stale baseline, keeping the metadata tip (and
	// therefore the pinned TXID) where it is — the exact shape a crashed
	// publisher leaves when L0s outran the last rebaseline.
	head, etag, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	info, err := be.Stat(ctx, oldest.Key)
	if err != nil {
		t.Fatalf("stat stale baseline: %v", err)
	}
	head.Baseline = &objstore.Baseline{
		TXID:   oldest.MaxTXID,
		LTXRef: objstore.FileRef{Key: oldest.Key, Size: info.Size},
	}
	if _, err := objstore.CASHead(ctx, be, head, &etag); err != nil {
		t.Fatalf("CASHead: %v", err)
	}

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "shared.db")
	manifest, err := lazyrestore.Prepare(ctx, dst, be, "")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if manifest == nil {
		t.Fatal("Prepare returned nil manifest")
	}

	// The merged page 1 is the authority on the database size; the manifest
	// and the sparse file must agree with it.
	page1, err := manifest.FetchPage(ctx, be, 1)
	if err != nil {
		t.Fatalf("FetchPage 1: %v", err)
	}
	headerPages := binary.BigEndian.Uint32(page1[28:32])
	if headerPages == 0 {
		t.Fatal("page 1 header has zero page count (pre-3.7 header?); test setup broken")
	}
	if manifest.CommitPages != headerPages {
		t.Errorf("manifest.CommitPages = %d, want %d (page-1 header)", manifest.CommitPages, headerPages)
	}
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	if want := int64(headerPages) * int64(manifest.PageSize); st.Size() != want {
		t.Errorf("shared.db size = %d, want %d (header pages × page size)", st.Size(), want)
	}
	// The grown state must be strictly larger than the stale baseline said.
	if manifest.CommitPages <= staleBaselineCommit(t, be, oldest.Key) {
		t.Errorf("CommitPages = %d did not grow past the stale baseline; test lost its premise", manifest.CommitPages)
	}
	// Every in-range page must be fetchable (coverage held across the chain).
	for pgno := uint32(1); pgno <= manifest.CommitPages; pgno++ {
		if _, err := manifest.FetchPage(ctx, be, pgno); err != nil {
			t.Fatalf("FetchPage %d: %v", pgno, err)
		}
	}
}

// publishGrownBucket publishes a small baseline, then grows the database by
// several pages and publishes again, leaving both baselines plus the L0
// chain between them in the bucket.
func publishGrownBucket(t *testing.T, bucket objectstore.Bucket, dbPath string) {
	t.Helper()
	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: bucket,
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	defer func() {
		if node != nil {
			_ = node.Close()
		}
	}()
	if err := node.Exec(`CREATE TABLE notes (id INT PRIMARY KEY NOT NULL, body TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := node.Exec(`INSERT INTO notes VALUES (1, 'seed')`); err != nil {
		t.Fatalf("INSERT seed: %v", err)
	}
	waitForPublisher(t, bucket)
	if err := node.PublishSnapshot(ctx); err != nil {
		t.Fatalf("PublishSnapshot 1: %v", err)
	}
	// Grow by multiple pages: ~4KB of body per row forces a page each.
	big := strings.Repeat("x", 4000)
	for i := 2; i <= 12; i++ {
		if err := node.Exec(fmt.Sprintf(`INSERT INTO notes VALUES (%d, '%s')`, i, big)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		t.Fatalf("PublishSnapshot 2: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close source: %v", err)
	}
	node = nil
}

func waitForPublisher(t *testing.T, bucket objectstore.Bucket) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(context.Background(), bucket)
		if err == nil && h.Publisher != nil && h.Baseline != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// staleBaselineCommit decodes the stale baseline LTX's header Commit field.
func staleBaselineCommit(t *testing.T, be objectstore.Bucket, key string) uint32 {
	t.Helper()
	rc, _, err := be.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get stale baseline: %v", err)
	}
	defer rc.Close()
	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode stale baseline header: %v", err)
	}
	return dec.Header().Commit
}
