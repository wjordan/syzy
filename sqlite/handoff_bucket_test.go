package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	syzy "github.com/wjordan/syzy/sqlite"
)

// waitLeaseHead polls HEAD until a publisher lease + baseline exist.
func waitLeaseHead(t *testing.T, ctx context.Context, be objectstore.Bucket) *objstore.HEAD {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && h.Publisher != nil && h.Baseline != nil {
			return h
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("publisher did not claim lease + baseline within deadline")
	return nil
}

// TestHandoff_BucketLeaseRetained is the multi-node-relevant proof: a
// bucket-backed node (which runs the publisher and holds a lease in HEAD) hands
// off, and the lease is RETAINED — not released — so the same-NodeID successor
// resumes it. Without retention the lease would expire (ExpiresAtUS=0) during
// the handoff window and any standby peer would force-take-over with a full
// rebaseline. The contrast test below shows that a normal Close DOES release it.
func TestHandoff_BucketLeaseRetained(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	cfg := syzy.Config{Path: dbPath, ObjectBackend: be, InProcessOnly: true, Log: newTestLogger()}

	a, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	if err := a.Exec(`CREATE TABLE vms (id TEXT PRIMARY KEY NOT NULL, status TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := a.Exec(`INSERT INTO vms VALUES ('vm-1','running')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	head := waitLeaseHead(t, ctx, be)
	originA := a.OriginHex()
	if head.Publisher.NodeID != originA {
		t.Fatalf("publisher NodeID=%s, want origin %s", head.Publisher.NodeID, originA)
	}
	gen0 := head.Publisher.Generation
	t.Logf("A holds lease: node=%s gen=%d", originA, gen0)

	// Handoff: Detach should RETAIN the lease.
	h, err := a.Detach()
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}
	hd, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD after Detach: %v", err)
	}
	if hd.Publisher == nil || hd.Publisher.NodeID != originA {
		t.Fatalf("lease lost on handoff: %+v", hd.Publisher)
	}
	if hd.Publisher.ExpiresAtUS <= time.Now().UnixMicro() {
		t.Fatalf("lease EXPIRED on handoff (a peer could take over + rebaseline): expires=%d now=%d",
			hd.Publisher.ExpiresAtUS, time.Now().UnixMicro())
	}
	t.Logf("lease RETAINED across Detach: node=%s gen=%d still valid (expires in %s)",
		hd.Publisher.NodeID, hd.Publisher.Generation,
		time.Duration(hd.Publisher.ExpiresAtUS-time.Now().UnixMicro())*time.Microsecond)

	// Successor resumes the held lease: same NodeID, generation bumps.
	b, err := syzy.Attach(ctx, cfg, h)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if b.OriginHex() != originA {
		t.Fatalf("origin rotated across handoff: A=%s B=%s", originA, b.OriginHex())
	}
	if err := h.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Wait for the successor's publisher to re-claim (generation > gen0).
	deadline := time.Now().Add(5 * time.Second)
	var resumed *objstore.HEAD
	for time.Now().Before(deadline) {
		hh, _, err := objstore.LoadHEAD(ctx, be)
		if err == nil && hh.Publisher != nil && hh.Publisher.Generation > gen0 {
			resumed = hh
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resumed == nil {
		t.Fatal("successor did not re-claim the lease within deadline")
	}
	if resumed.Publisher.NodeID != originA {
		t.Fatalf("successor resumed under different node %s (a takeover, not a resume)", resumed.Publisher.NodeID)
	}
	t.Logf("successor resumed: node=%s gen=%d (was %d)", resumed.Publisher.NodeID, resumed.Publisher.Generation, gen0)

	var n int
	if err := syzy.NewDB(b).QueryRow(`SELECT count(*) FROM vms`).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows after handoff = %d, want 1 (data continuity)", n)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}
}

// TestHandoff_NormalCloseReleasesLease is the contrast: a normal Close releases
// the lease (ExpiresAtUS=0). That is exactly the window a standby peer would
// exploit to force a rebaseline — which is why the handoff path (Detach) retains
// it instead.
func TestHandoff_NormalCloseReleasesLease(t *testing.T) {
	t.Parallel()
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	cfg := syzy.Config{Path: dbPath, ObjectBackend: be, InProcessOnly: true, Log: newTestLogger()}

	a, err := syzy.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := a.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = waitLeaseHead(t, ctx, be)

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	hd, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if hd.Publisher != nil && hd.Publisher.ExpiresAtUS > time.Now().UnixMicro() {
		t.Fatalf("normal Close left the lease VALID (expires=%d now=%d); expected release (=0)",
			hd.Publisher.ExpiresAtUS, time.Now().UnixMicro())
	}
	t.Logf("normal Close released the lease (ExpiresAtUS=%d) — the window Detach avoids", hd.Publisher.ExpiresAtUS)
}
