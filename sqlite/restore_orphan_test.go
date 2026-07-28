package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
)

// These tests pin down the row_clock <-> app.db "orphan" failure class:
// when the db/ stream is reconstructed to a different logical point than
// the metadata/ stream, the restored node carries live CRDT clocks for
// rows its app.db lacks. restoreFromBucket materializes the two streams
// independently (no parent_app_txid cross-check — the coupling described
// in internal/objstore/layout.go is aspirational, not enforced), so the
// mismatch is accepted silently. This is the prod mechanism behind a row
// present on most replicas but missing on a few: the meta baseline keeps
// advancing (taken on every publisher resume) while the db baseline
// freezes (only re-taken on init/takeover), and a db delta carrying the
// row gets dropped from the chain below the meta tip.

// buildKVBucket creates a bucket holding a one-table DB taken through two
// coupled baselines: B1 with {row 1} only, then B2 with {row 1, row 2}.
// It returns the bucket and B1's HEAD (whose db baseline holds only row
// 1). The bucket's live HEAD is B2 (both baselines carry rows 1+2).
func buildKVBucket(t *testing.T) (objectstore.Bucket, *objstore.HEAD) {
	t.Helper()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "src.db")
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
		SchemaLog:     schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE kv (k INT PRIMARY KEY NOT NULL, v TEXT)`,
		`INSERT INTO kv VALUES (1, 'one')`,
	} {
		if err := node.Exec(sql); err != nil {
			_ = node.Close()
			t.Fatalf("Exec %q: %v", sql, err)
		}
	}
	waitForBaseline(t, ctx, be)

	// B1: coupled baseline carrying only row 1.
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot B1: %v", err)
	}
	head1, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		_ = node.Close()
		t.Fatalf("LoadHEAD B1: %v", err)
	}

	// B2: coupled baseline carrying rows 1+2 on both streams.
	if err := node.Exec(`INSERT INTO kv VALUES (2, 'two')`); err != nil {
		_ = node.Close()
		t.Fatalf("INSERT row 2: %v", err)
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		_ = node.Close()
		t.Fatalf("PublishSnapshot B2: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	head2, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD B2: %v", err)
	}
	if head1.Baseline == nil || head2.Baseline == nil || head2.MetaBaseline == nil {
		t.Fatalf("baselines not all set: B1=%+v B2=%+v", head1, head2)
	}
	if head2.Baseline.TXID <= head1.Baseline.TXID {
		t.Fatalf("db baseline did not advance across PublishSnapshot: B1=%d B2=%d",
			head1.Baseline.TXID, head2.Baseline.TXID)
	}
	return be, head1
}

// TestRestoreConsistency_ConsistentBucketHasNoOrphans is the control: an
// untouched bucket restores both rows and reports zero orphans. It
// validates the harness — the orphan in the sibling test comes from the
// manipulation, not from CheckRestoreConsistency mis-counting.
func TestRestoreConsistency_ConsistentBucketHasNoOrphans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, _ := buildKVBucket(t)

	dst := filepath.Join(t.TempDir(), "dst.db")
	if err := syzy.RestoreFromBucket(ctx, dst, be); err != nil {
		t.Fatalf("RestoreFromBucket: %v", err)
	}
	if got := readKVCount(t, dst); got != 2 {
		t.Errorf("kv rows after consistent restore = %d, want 2", got)
	}
	res, err := syzy.CheckRestoreConsistency(dst, syzy.MetadataPathFor(dst))
	if err != nil {
		t.Fatalf("CheckRestoreConsistency: %v", err)
	}
	if res.Orphans != 0 {
		t.Errorf("consistent restore reports orphans: %+v", res)
	}
}

// TestRestoreConsistency_DecoupledBaselineProducesOrphan reproduces the
// prod failure deterministically: freeze the db baseline at B1 (row 1
// only) while the live HEAD's meta baseline stays at B2 (clocks for rows
// 1+2), and hole the db delta chain so app.db cannot reconstruct row 2.
// The restore succeeds with no error, yet app.db is missing row 2 while
// metadata.db still carries row 2's live clock — an orphan.
func TestRestoreConsistency_DecoupledBaselineProducesOrphan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, head1 := buildKVBucket(t)

	// Point HEAD.Baseline back at B1 (frozen db baseline) while keeping
	// the B2 meta baseline. CAS off the current (B2) HEAD.
	cur, etag, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	mixed := *cur
	mixed.Baseline = head1.Baseline
	if _, err := objstore.CASHead(ctx, be, &mixed, &etag); err != nil {
		t.Fatalf("CASHead decoupled: %v", err)
	}

	// Hole the db chain: drop every L0/L1 frame above B1 so row 2's delta
	// is unreachable and app.db reconstructs to the frozen baseline only.
	deleted := 0
	for _, level := range []int{objstore.L0Level, objstore.L1Level} {
		files, err := objstore.ListLTX(ctx, be, objstore.DBPrefix, level)
		if err != nil {
			t.Fatalf("ListLTX db L%d: %v", level, err)
		}
		for _, f := range files {
			if f.MaxTXID > head1.Baseline.TXID {
				if err := be.Delete(ctx, f.Key); err != nil {
					t.Fatalf("delete %s: %v", f.Key, err)
				}
				deleted++
			}
		}
	}
	t.Logf("froze db baseline at txid=%d, holed %d L0/L1 db frames above it", head1.Baseline.TXID, deleted)

	dst := filepath.Join(t.TempDir(), "dst.db")
	if err := syzy.RestoreFromBucket(ctx, dst, be); err != nil {
		t.Fatalf("RestoreFromBucket: %v", err)
	}

	// app.db lost row 2; metadata.db still has its clock -> orphan.
	if got := readKVCount(t, dst); got != 1 {
		t.Errorf("kv rows after decoupled restore = %d, want 1 (row 2 unreachable)", got)
	}
	res, err := syzy.CheckRestoreConsistency(dst, syzy.MetadataPathFor(dst))
	if err != nil {
		t.Fatalf("CheckRestoreConsistency: %v", err)
	}
	if res.Orphans == 0 {
		t.Fatalf("expected an orphan (live clock with no app row); got none — restore was consistent")
	}
	if res.PerTable["kv"] != 1 {
		t.Errorf("kv orphan count = %d, want 1 (res=%+v)", res.PerTable["kv"], res)
	}
}

// TestRestoreConsistency_AppAheadOfMetaPinIsCapped proves the parent_app_txid
// coupling. The app stream advances PAST the metadata's pin with a later
// deletion, so app.db at its own tip lacks a row the frozen metadata still holds
// a live clock for — the mirror of the decoupled-baseline orphan. restoreFromBucket
// caps the app chain at metadata's parent_app_txid, restoring the row and
// reporting zero orphans, instead of the false orphan a tip-to-tip restore yields.
func TestRestoreConsistency_AppAheadOfMetaPinIsCapped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "src.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath, ObjectBackend: be, SchemaLog: schemalog.NewLocal()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec := func(sql string) {
		t.Helper()
		if err := node.Exec(sql); err != nil {
			_ = node.Close()
			t.Fatalf("Exec %q: %v", sql, err)
		}
	}
	mustSnap := func(label string) *objstore.HEAD {
		t.Helper()
		if err := node.PublishSnapshot(ctx); err != nil {
			_ = node.Close()
			t.Fatalf("PublishSnapshot %s: %v", label, err)
		}
		h, _, err := objstore.LoadHEAD(ctx, be)
		if err != nil {
			_ = node.Close()
			t.Fatalf("LoadHEAD %s: %v", label, err)
		}
		return h
	}

	mustExec(`CREATE TABLE kv (k INT PRIMARY KEY NOT NULL, v TEXT)`)
	mustExec(`INSERT INTO kv VALUES (1, 'one')`)
	waitForBaseline(t, ctx, be)
	head0 := mustSnap("B0") // db baseline carries {1}
	mustExec(`INSERT INTO kv VALUES (2, 'two')`)
	head1 := mustSnap("B1") // meta baseline holds row 2's LIVE clock; parent_app_txid pins here
	mustExec(`DELETE FROM kv WHERE k = 2`)
	_ = mustSnap("B2") // app stream advances past the pin, deleting row 2
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if head0.Baseline == nil || head1.MetaBaseline == nil {
		t.Fatalf("baselines not set: head0=%+v head1=%+v", head0, head1)
	}

	// Pin HEAD to the decoupled point: db baseline at B0 (below the pin), meta
	// baseline at B1. Then hole the META chain above B1 so meta restores to B1
	// (row 2 still live, parent_app_txid=T1), while the APP chain stays intact
	// through the row-2 deletion.
	cur, etag, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	mixed := *cur
	mixed.Baseline = head0.Baseline
	mixed.MetaBaseline = head1.MetaBaseline
	if _, err := objstore.CASHead(ctx, be, &mixed, &etag); err != nil {
		t.Fatalf("CASHead: %v", err)
	}
	holed := 0
	for _, level := range []int{objstore.L0Level, objstore.L1Level} {
		files, err := objstore.ListLTX(ctx, be, objstore.MetadataPrefix, level)
		if err != nil {
			t.Fatalf("ListLTX meta L%d: %v", level, err)
		}
		for _, f := range files {
			if f.MaxTXID > head1.MetaBaseline.TXID {
				if err := be.Delete(ctx, f.Key); err != nil {
					t.Fatalf("delete %s: %v", f.Key, err)
				}
				holed++
			}
		}
	}
	t.Logf("froze meta baseline at txid=%d, holed %d meta frames above it", head1.MetaBaseline.TXID, holed)

	dst := filepath.Join(t.TempDir(), "dst.db")
	if err := syzy.RestoreFromBucket(ctx, dst, be); err != nil {
		t.Fatalf("RestoreFromBucket: %v", err)
	}
	// Capped at parent_app_txid: the app stream is materialized through the pin
	// (row 2 present), not to its own tip (row 2 deleted), so the streams agree.
	if got := readKVCount(t, dst); got != 2 {
		t.Errorf("kv rows = %d, want 2 (app capped at parent_app_txid restores row 2)", got)
	}
	res, err := syzy.CheckRestoreConsistency(dst, syzy.MetadataPathFor(dst))
	if err != nil {
		t.Fatalf("CheckRestoreConsistency: %v", err)
	}
	if res.Orphans != 0 {
		t.Errorf("expected zero orphans after coupling cap, got %+v", res)
	}
}

// waitForBaseline blocks until the bucket has a publisher lease and an
// initial baseline, so PublishSnapshot has a seeded TXID counter.
func waitForBaseline(t *testing.T, ctx context.Context, be objectstore.Bucket) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && h.Publisher != nil && h.Baseline != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("publisher lease + baseline did not appear within 5s")
}

// readKVCount returns the row count of table kv in the app.db at dbPath.
func readKVCount(t *testing.T, dbPath string) int {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer c.Close()
	stmt, _, err := c.Prepare(`SELECT count(*) FROM kv`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	return int(stmt.ColumnInt64(0))
}
