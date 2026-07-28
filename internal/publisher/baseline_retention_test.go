package publisher_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/publisher"
)

func exists(t *testing.T, be objectstore.Bucket, key string) bool {
	t.Helper()
	_, err := be.Stat(context.Background(), key)
	if err == nil {
		return true
	}
	if errors.Is(err, objectstore.ErrNotFound) || strings.Contains(err.Error(), "not found") {
		return false
	}
	t.Fatalf("stat %s: %v", key, err)
	return false
}

// putBaseline writes a placeholder baseline object at <stream>0009/<txid>-<txid>.
func putBaseline(t *testing.T, be objectstore.Bucket, stream string, txid uint64) string {
	t.Helper()
	key := objstore.LTXKey(stream, objstore.BaselineLevel, txid, txid)
	if _, err := be.Put(context.Background(), key, strings.NewReader("x"), 1, nil); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return key
}

// writeHEAD stamps a HEAD whose db/meta baselines sit at the given TXIDs.
func writeHEAD(t *testing.T, be objectstore.Bucket, dbTXID, metaTXID uint64) {
	t.Helper()
	head := &objstore.HEAD{
		Version:   objstore.HEADVersion,
		ClusterID: "test-cluster",
		Baseline: &objstore.Baseline{
			TXID:   dbTXID,
			LTXRef: objstore.FileRef{Key: objstore.LTXKey(objstore.DBPrefix, objstore.BaselineLevel, dbTXID, dbTXID)},
		},
		MetaBaseline: &objstore.Baseline{
			TXID:   metaTXID,
			LTXRef: objstore.FileRef{Key: objstore.LTXKey(objstore.MetadataPrefix, objstore.BaselineLevel, metaTXID, metaTXID)},
		},
	}
	if _, err := objstore.CASHead(context.Background(), be, head, nil); err != nil {
		t.Fatalf("CASHead: %v", err)
	}
}

// TestBaselineSweep: superseded baselines (TXID below the active HEAD baseline,
// aged past grace) are deleted on both streams; the active baseline and any
// newer one are kept. Regression for the leak where every rebaseline orphaned
// the prior baseline forever (retention had no baseline-level rule).
func TestBaselineSweep(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// db stream: 100, 200 superseded; 300 active (HEAD); 400 newer-than-HEAD.
	dbOld1 := putBaseline(t, be, objstore.DBPrefix, 100)
	dbOld2 := putBaseline(t, be, objstore.DBPrefix, 200)
	dbCur := putBaseline(t, be, objstore.DBPrefix, 300)
	dbNewer := putBaseline(t, be, objstore.DBPrefix, 400)
	// meta stream: 250 superseded; 300 active.
	metaOld := putBaseline(t, be, objstore.MetadataPrefix, 250)
	metaCur := putBaseline(t, be, objstore.MetadataPrefix, 300)

	writeHEAD(t, be, 300, 300)

	r := &publisher.Retention{Backend: be, Grace: time.Nanosecond} // all aged
	res, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.BaselineDeleted != 3 { // dbOld1, dbOld2, metaOld
		t.Errorf("BaselineDeleted = %d, want 3", res.BaselineDeleted)
	}
	for _, k := range []string{dbOld1, dbOld2, metaOld} {
		if exists(t, be, k) {
			t.Errorf("superseded baseline %s should be deleted", k)
		}
	}
	for _, k := range []string{dbCur, dbNewer, metaCur} {
		if !exists(t, be, k) {
			t.Errorf("active/newer baseline %s must be kept", k)
		}
	}
}

// TestBaselineSweepGrace: a superseded baseline within the grace window is kept.
func TestBaselineSweepGrace(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old := putBaseline(t, be, objstore.DBPrefix, 100)
	putBaseline(t, be, objstore.DBPrefix, 300)
	writeHEAD(t, be, 300, 300)

	r := &publisher.Retention{Backend: be, Grace: time.Hour} // freshly written -> within grace
	res, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.BaselineDeleted != 0 {
		t.Errorf("BaselineDeleted = %d, want 0 (within grace)", res.BaselineDeleted)
	}
	if !exists(t, be, old) {
		t.Error("superseded baseline within grace must be kept")
	}
}

// TestBaselineSweepDryRun: counts but does not delete.
func TestBaselineSweepDryRun(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	old := putBaseline(t, be, objstore.DBPrefix, 100)
	putBaseline(t, be, objstore.DBPrefix, 300)
	writeHEAD(t, be, 300, 300)

	r := &publisher.Retention{Backend: be, Grace: time.Nanosecond, DryRun: true}
	res, err := r.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.BaselineDeleted != 1 {
		t.Errorf("BaselineDeleted = %d, want 1", res.BaselineDeleted)
	}
	if !exists(t, be, old) {
		t.Error("dry-run must not delete")
	}
}
