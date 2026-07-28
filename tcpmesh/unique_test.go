package tcpmesh

import (
	"context"
	"net"
	"net/rpc"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/unique"
)

// claim builds a minimal unique.Claim for the reservation tests.
func uniqueClaim(owner string, table, key byte, value string) unique.Claim {
	c := unique.Claim{Value: []byte(value), Owner: []byte(owner)}
	c.Table[0] = table
	c.Key[0] = key
	return c
}

// reserveUntilLeader polls the client's Reserve until the leaseholder
// finishes acquiring + draining + rebuilding and serves (the maintenance
// loop runs on its own goroutine). It returns the first non-ErrUnavailable
// outcome, or fails on timeout.
func reserveUntilLeader(t *testing.T, client *unique.LeaseClient, claims []unique.Claim, timeout time.Duration) (bool, *unique.Claim) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ok, conflict, err := client.Reserve(ctx, claims)
		cancel()
		if err == nil {
			return ok, conflict
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("reservation never reached a serving leaseholder within %s", timeout)
	return false, nil
}

// TestUniqueRPC_FollowerReservesThroughLeaderOverMesh is the multi-node
// regression test for the localhost-leaseholder bug. The leaseholder runs
// on the LEADER mesh's listener and publishes that channel's endpoint URL
// into the lease. A FOLLOWER LeaseClient — wired to a SEPARATE mesh's dial
// transport — must reach the leader by routing the published URL over the
// mesh.
//
// What makes this catch the bug: the leader listens on a Unix socket, so
// the published address is a "unix://…?topic=…" endpoint URL, not the
// "127.0.0.1:<port>" net/rpc address the old leaseholder published. A
// follower on a different host (modeled here by a different mesh with no
// listener at that address) could never reach a localhost address; it
// reaches the leader only because the dial transport understands the
// endpoint URL. We assert both: (1) the published address is the leader's
// endpoint URL, not a bare tcp host:port, and (2) the old client's exact
// call — rpc.Dial("tcp", addr) — cannot reach it.
func TestUniqueRPC_FollowerReservesThroughLeaderOverMesh(t *testing.T) {
	dir := t.TempDir()
	leaderGossip := "unix:" + filepath.Join(dir, "leader.gossip")
	followerGossip := "unix:" + filepath.Join(dir, "follower.gossip")

	leaderMux, err := New(Config{
		Listen: leaderGossip,
		Seeds:  []string{followerGossip}, DialRetry: 25 * time.Millisecond, NodeID: 1,
	})
	if err != nil {
		t.Fatalf("New leader: %v", err)
	}
	t.Cleanup(func() { _ = leaderMux.Close() })

	followerMux, err := New(Config{
		Listen: followerGossip,
		Seeds:  []string{leaderGossip}, DialRetry: 25 * time.Millisecond, NodeID: 2,
	})
	if err != nil {
		t.Fatalf("New follower: %v", err)
	}
	t.Cleanup(func() { _ = followerMux.Close() })

	if !waitForReady(leaderMux, 1, 2*time.Second) || !waitForReady(followerMux, 1, 2*time.Second) {
		t.Fatalf("meshes did not peer")
	}

	const topic = "unique"
	leaderCh, err := leaderMux.Channel(topic)
	if err != nil {
		t.Fatalf("leader Channel: %v", err)
	}
	followerCh, err := followerMux.Channel(topic)
	if err != nil {
		t.Fatalf("follower Channel: %v", err)
	}

	// One shared lease object store (models the cluster object store both
	// nodes read/write).
	bucket, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	store := unique.OpenLease(bucket, "unique/lease")

	// Leaseholder on the LEADER mux: publishes the leader's bundle URL.
	lh := unique.NewLeaseholder(unique.LeaseholderConfig{
		Store:     store,
		Owner:     "leader",
		Transport: leaderCh.UniqueServeTransport(),
		Enumerate: func(context.Context) (unique.Snapshot, error) {
			return unique.Snapshot{Keys: []unique.KeyRef{{Table: [16]byte{1}, Key: [16]byte{1}}}}, nil
		},
		TTLUS:        2_000_000,
		DrainUS:      1,
		QuarantineUS: 1000,
		GraceUS:      1000,
	})
	if err := lh.Start(); err != nil {
		t.Fatalf("leaseholder Start: %v", err)
	}
	defer lh.Close()

	// Assert the published address is the LEADER's mesh bundle URL — the
	// crux of the fix. The old code published a "127.0.0.1:<port>" net/rpc
	// address bound on the leaseholder's own host, unreachable from a peer.
	addr := lh.Addr()
	if !strings.HasPrefix(addr, "unix://") || !strings.Contains(addr, "topic="+topic) {
		t.Fatalf("published addr %q is not a mesh bundle URL; the leaseholder is not mesh-routed", addr)
	}
	wantEndpoint := leaderCh.Endpoint()
	if addr != wantEndpoint {
		t.Fatalf("published addr %q != leader bundle URL %q", addr, wantEndpoint)
	}

	lhCtx, lhCancel := context.WithCancel(context.Background())
	defer lhCancel()
	go lh.RunMaintenance(lhCtx)

	// FOLLOWER client: dials via the FOLLOWER mux's transport. It can only
	// reach the leader by routing the published URL over the mesh.
	client := unique.NewLeaseClientTransport(store, followerCh.UniqueDialTransport())
	defer client.Close()

	// First reservation succeeds through the leader.
	ok, conflict := reserveUntilLeader(t, client, []unique.Claim{uniqueClaim("rowA", 1, 1, "app-name-x")}, 4*time.Second)
	if !ok {
		t.Fatalf("first reserve through leader: ok=false conflict=%+v", conflict)
	}

	// A different owner claiming the same value is rejected — proves the
	// follower's reserve actually mutated the leader's reservation table,
	// not a local short-circuit.
	ctx := context.Background()
	ok, conflict, err = client.Reserve(ctx, []unique.Claim{uniqueClaim("rowB", 1, 1, "app-name-x")})
	if err != nil {
		t.Fatalf("conflicting reserve err: %v", err)
	}
	if ok || conflict == nil {
		t.Fatalf("want conflict from leader, got ok=%v", ok)
	}

	// Release is an advisory no-op on the lease client (the leaseholder
	// observes vacated values in the rows); it must not error or dial.
	if err := client.Release(ctx, []unique.Claim{uniqueClaim("rowA", 1, 1, "app-name-x")}); err != nil {
		t.Fatalf("release: %v", err)
	}

	// The old client's exact call — rpc.Dial("tcp", rec.Addr) — could not
	// have reached this address (it's a unix bundle URL, not host:port), so
	// this topology is one the pre-fix code provably fails on.
	if _, derr := rpc.Dial("tcp", addr); derr == nil {
		t.Fatalf("published addr %q was unexpectedly dialable as plain tcp; test does not exercise the mesh-only path", addr)
	}
}

// TestUniqueRPC_NoListenerRefuses asserts a clustered node without a
// listener cannot publish a reachable address (fail loud, not bind
// localhost): Serve must error rather than silently degrade.
func TestUniqueRPC_NoListenerRefuses(t *testing.T) {
	m, err := New(Config{NodeID: 1}) // no Listen
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	ch, err := m.Channel("unique")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if _, err := ch.UniqueServeTransport().Serve(func(net.Conn) {}); err == nil {
		t.Fatal("Serve without a listener should error, not publish an unreachable addr")
	}
}
