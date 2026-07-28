package tcpmesh_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
)

// waitPair builds two meshed nodes over one shared schema log and
// object backend, seeded b → a, and returns them with their db paths.
func waitPair(t *testing.T) (a, b *syzy.Node, aPath, bPath string) {
	t.Helper()
	dir := t.TempDir()
	be, err := objectstore.OpenFS(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("objectstore: %v", err)
	}
	log := schemalog.NewLocal()
	ctx := context.Background()

	open := func(name string, seeds []string, clusterID *[16]byte) (*syzy.Node, *tcpmesh.Mesh, string) {
		gossip := "unix:" + filepath.Join(dir, name+".sock")
		mesh, err := tcpmesh.New(tcpmesh.Config{
			Listen:    gossip,
			Seeds:     seeds,
			DialRetry: 25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("%s mesh: %v", name, err)
		}
		t.Cleanup(func() { _ = mesh.Close() })
		ch, err := mesh.Channel("app")
		if err != nil {
			t.Fatalf("%s channel: %v", name, err)
		}
		dbPath := filepath.Join(dir, name+".db")
		if clusterID != nil {
			if err := syzy.JoinCluster(dbPath, *clusterID); err != nil {
				t.Fatalf("%s JoinCluster: %v", name, err)
			}
		}
		node, err := syzy.Open(ctx, syzy.Config{
			Path:                  dbPath,
			Transport:             ch,
			ObjectBackend:         be,
			SchemaLog:             log,
			SchemaCatchupInterval: 25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("%s Open: %v", name, err)
		}
		t.Cleanup(func() { _ = node.Close() })
		return node, mesh, dbPath
	}

	var aMesh *tcpmesh.Mesh
	a, aMesh, aPath = open("a", nil, nil)
	cid := a.ClusterID()
	b, _, bPath = open("b", []string{aMesh.Addr()}, &cid)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(aMesh.PeerAddrs()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(aMesh.PeerAddrs()) == 0 {
		t.Fatal("mesh never converged")
	}
	return a, b, aPath, bPath
}

func exec(t *testing.T, node *syzy.Node, sql string) {
	t.Helper()
	if err := node.Exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func count(t *testing.T, dbPath string) int {
	t.Helper()
	return len(readColumn(t, dbPath, "SELECT id FROM notes"))
}

// The guarantee behind `syzy wait`: once it returns on the writer, the
// write is readable on every peer. A premature success is worse than no
// command at all — it turns a race into a silent wrong answer.
func TestWaitReplicated_WriteIsReadableOnThePeer(t *testing.T) {
	a, b, aPath, bPath := waitPair(t)

	exec(t, a, `CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, text TEXT NOT NULL)`)
	exec(t, a, `INSERT INTO notes VALUES ('a', 'from a')`)

	// Setup, not the property under test: b needs the schema before it
	// can write. (Waiting for DDL is what `syzy wait` would be used for
	// too, but here it must not mask the thing being measured.)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !hasTable(t, bPath, "notes") {
		time.Sleep(10 * time.Millisecond)
	}

	// b's write is committed locally before the wait begins, so a
	// correct wait cannot return until a has it.
	exec(t, b, `INSERT INTO notes VALUES ('b', 'from b')`)

	// Waiting on b, the writer: b's own write is what must land on a.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.WaitReplicated(ctx); err != nil {
		t.Fatalf("WaitReplicated: %v", err)
	}

	if n := count(t, aPath); n != 2 {
		t.Fatalf("after WaitReplicated, the peer has %d rows, want 2 — wait returned before the write landed", n)
	}
}

func TestWaitReplicated_NoTransport(t *testing.T) {
	dir := t.TempDir()
	node, err := syzy.Open(context.Background(), syzy.Config{Path: filepath.Join(dir, "solo.db")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	err = node.WaitReplicated(context.Background())
	if !errors.Is(err, syzy.ErrNoPeerTransport) {
		t.Fatalf("WaitReplicated on a transportless node = %v, want ErrNoPeerTransport", err)
	}
}
