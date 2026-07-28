package metadata_test

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

func TestClusterRoot_RoundTrip(t *testing.T) {
	sc, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sc.Close()

	if _, ok, err := sc.GetClusterRoot(); err != nil || ok {
		t.Fatalf("fresh metadata should have no cluster_root; ok=%v err=%v", ok, err)
	}
	const want = "file:///srv/myapp"
	if err := sc.SetClusterRoot(want); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := sc.GetClusterRoot()
	if err != nil || !ok || got != want {
		t.Fatalf("got=%q ok=%v err=%v; want %q true nil", got, ok, err, want)
	}
}

func TestClusterRoot_SurvivesAdoptClone(t *testing.T) {
	sc, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sc.Close()
	const root = "s3://my-bucket/myapp"
	if err := sc.SetClusterRoot(root); err != nil {
		t.Fatalf("set: %v", err)
	}
	// AdoptClone needs cluster_id seeded for typical use, but it
	// only rewrites identity-bearing fields — cluster_root must
	// pass through untouched, matching cluster_id's behavior.
	if err := sc.SetClusterID(crdt.ClusterID{1, 2, 3}); err != nil {
		t.Fatalf("seed cluster_id: %v", err)
	}
	if err := sc.AdoptClone(crdt.Origin(0xABC), crdt.Clock{}); err != nil {
		t.Fatalf("AdoptClone: %v", err)
	}
	got, ok, err := sc.GetClusterRoot()
	if err != nil || !ok || got != root {
		t.Fatalf("after AdoptClone got=%q ok=%v err=%v; want %q true nil", got, ok, err, root)
	}
}
