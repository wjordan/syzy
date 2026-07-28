package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestObjectBackend_ClusterIDRendezvous verifies that two nodes opened
// against the same FileBackend converge on the same cluster_id. The
// first Open CAS-stubs HEAD with a fresh id; the second Open reads that
// id back. Without the rendezvous, both would mint local random ids
// and split-brain.
func TestObjectBackend_ClusterIDRendezvous(t *testing.T) {
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}

	ctx := context.Background()

	openOnce := func(name string) [16]byte {
		t.Helper()
		dbPath := filepath.Join(t.TempDir(), name+"-app.db")
		node, err := syzy.Open(ctx, syzy.Config{
			Path:          dbPath,
			ObjectBackend: be,
		})
		if err != nil {
			t.Fatalf("Open %s: %v", name, err)
		}
		defer node.Close()
		return node.ClusterID()
	}

	cidA := openOnce("a")
	cidB := openOnce("b")

	if cidA != cidB {
		t.Fatalf("cluster_id rendezvous failed: A=%x B=%x", cidA, cidB)
	}

	// HEAD should now exist with that cluster_id and Baseline:nil
	// (beacon-only state).
	head, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Baseline != nil {
		t.Fatalf("expected beacon-only HEAD, got baseline %+v", head.Baseline)
	}
}

// TestObjectBackend_NoBackend_LocalMint verifies the legacy path: when
// ObjectBackend is nil, two opens against unrelated paths mint distinct
// random cluster_ids (no rendezvous mechanism).
func TestObjectBackend_NoBackend_LocalMint(t *testing.T) {
	ctx := context.Background()
	openOnce := func(name string) [16]byte {
		t.Helper()
		dbPath := filepath.Join(t.TempDir(), name+"-app.db")
		node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
		if err != nil {
			t.Fatalf("Open %s: %v", name, err)
		}
		defer node.Close()
		return node.ClusterID()
	}
	a, b := openOnce("a"), openOnce("b")
	if a == b {
		t.Fatalf("expected distinct local-mint cluster_ids; got %x == %x", a, b)
	}
}
