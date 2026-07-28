package tcpmesh_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestMesh_OpenTwoTopics_OneHost wires two syzy.Nodes through a
// single tcpmesh, each on its own topic. The mesh has exactly one
// gossip listener + one bundle listener; channels share them.
func TestMesh_OpenTwoTopics_OneHost(t *testing.T) {
	dir := t.TempDir()
	be, err := objectstore.OpenFS(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("objectstore: %v", err)
	}
	mesh, err := tcpmesh.New(tcpmesh.Config{
		Listen:    "unix:" + filepath.Join(dir, "gossip.sock"),
		DialRetry: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("tcpmesh.New: %v", err)
	}
	t.Cleanup(func() { _ = mesh.Close() })

	ctx := context.Background()

	openNode := func(name, topic string) *syzy.Node {
		t.Helper()
		tx, err := mesh.Channel(topic)
		if err != nil {
			t.Fatalf("%s mesh.Channel: %v", name, err)
		}
		// One schemalog per topic — each logical cluster has
		// independent DDL state.
		node, err := syzy.Open(ctx, syzy.Config{
			Path:          filepath.Join(dir, name+".db"),
			Transport:     tx,
			ObjectBackend: objectstore.Prefixed(be, topic+"/"),
			SchemaLog:     schemalog.NewLocal(),
			ServeClones:   true,
		})
		if err != nil {
			t.Fatalf("%s syzy.Open: %v", name, err)
		}
		t.Cleanup(func() { _ = node.Close() })
		return node
	}

	appNode := openNode("app", "app")
	cdnNode := openNode("cdn", "cdn")

	if err := appNode.Exec(`CREATE TABLE app (id INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("app CREATE: %v", err)
	}
	if err := cdnNode.Exec(`CREATE TABLE cdn_t (id INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("cdn CREATE: %v", err)
	}
	if err := appNode.Exec(`INSERT INTO app VALUES (1, 100)`); err != nil {
		t.Fatalf("app INSERT: %v", err)
	}
	if err := cdnNode.Exec(`INSERT INTO cdn_t VALUES (2, 200)`); err != nil {
		t.Fatalf("cdn INSERT: %v", err)
	}

	if a := mesh.Addr(); a == "" {
		t.Errorf("mesh.Addr() empty")
	}
}

// TestMesh_ChannelIdempotent: repeated Channel(topic) returns the
// same Transport instance.
func TestMesh_ChannelIdempotent(t *testing.T) {
	dir := t.TempDir()
	mesh, err := tcpmesh.New(tcpmesh.Config{
		Listen: "unix:" + filepath.Join(dir, "gossip.sock"),
	})
	if err != nil {
		t.Fatalf("tcpmesh.New: %v", err)
	}
	t.Cleanup(func() { _ = mesh.Close() })

	a, err := mesh.Channel("topic-a")
	if err != nil {
		t.Fatalf("first Channel: %v", err)
	}
	b, err := mesh.Channel("topic-a")
	if err != nil {
		t.Fatalf("second Channel: %v", err)
	}
	if a != b {
		t.Errorf("Channel(\"topic-a\") returned different instances on repeat call")
	}
}

// TestMesh_CloseShutsDownAllChannels: Mesh.Close terminates the
// underlying mux; Channel calls after Close error out.
func TestMesh_CloseShutsDownAllChannels(t *testing.T) {
	dir := t.TempDir()
	mesh, err := tcpmesh.New(tcpmesh.Config{
		Listen: "unix:" + filepath.Join(dir, "gossip.sock"),
	})
	if err != nil {
		t.Fatalf("tcpmesh.New: %v", err)
	}
	if _, err := mesh.Channel("x"); err != nil {
		t.Fatalf("Channel x: %v", err)
	}
	if _, err := mesh.Channel("y"); err != nil {
		t.Fatalf("Channel y: %v", err)
	}
	if err := mesh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second Close is a no-op.
	if err := mesh.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := mesh.Channel("z"); err == nil {
		t.Errorf("Channel after Close should error")
	}
}

// TestMesh_SetSeeds_DynamicUpdate: the overlay-refresh shape — a
// single SetSeeds call covers every channel on the mesh.
func TestMesh_SetSeeds_DynamicUpdate(t *testing.T) {
	dir := t.TempDir()
	aGossip := "unix:" + filepath.Join(dir, "a.gossip.sock")
	bGossip := "unix:" + filepath.Join(dir, "b.gossip.sock")

	a, err := tcpmesh.New(tcpmesh.Config{
		Listen:    aGossip,
		DialRetry: 25 * time.Millisecond,
		NodeID:    1,
	})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := tcpmesh.New(tcpmesh.Config{
		Listen:    bGossip,
		DialRetry: 25 * time.Millisecond,
		NodeID:    2,
	})
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if _, err := a.Channel("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Channel("cdn"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel("app"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Channel("cdn"); err != nil {
		t.Fatal(err)
	}

	// Single mux-wide SetSeeds covers both topics.
	a.SetSeeds([]string{bGossip})
	b.SetSeeds([]string{aGossip})

	// Verify peers connect via Mesh.PeerAddrs.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.PeerAddrs()) >= 1 && len(b.PeerAddrs()) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("peers did not connect after mux-wide SetSeeds: a=%v b=%v",
		a.PeerAddrs(), b.PeerAddrs())
}
