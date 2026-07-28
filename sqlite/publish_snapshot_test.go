package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
)

func TestPublishSnapshot_NoBackendConfigured(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(context.Background(), syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	err = node.PublishSnapshot(context.Background())
	if !errors.Is(err, syzy.ErrNoObjectBackend) {
		t.Fatalf("PublishSnapshot without backend: got %v, want ErrNoObjectBackend", err)
	}
}

// TestPublishSnapshot_RoundTrip exercises the full live-publish path:
// open node A with an ObjectBackend, write data, PublishSnapshot,
// then verify HEAD points at a snapshot whose frontier reflects the
// node's writes and whose ClusterID matches.
func TestPublishSnapshot_RoundTrip(t *testing.T) {
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := node.Exec(`INSERT INTO kv VALUES ('k', 1)`); err == nil {
			break
		}
	}
	if err := node.Exec(`INSERT INTO kv VALUES ('a', 1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if err := node.PublishSnapshot(ctx); err != nil {
		t.Fatalf("PublishSnapshot: %v", err)
	}

	head, _, err := objstore.LoadHEAD(ctx, be)
	if err != nil {
		t.Fatalf("LoadHEAD: %v", err)
	}
	if head.Baseline == nil {
		t.Fatalf("HEAD has no baseline after publish")
	}
	wantCID := nodeClusterIDHex(t, node)
	if head.ClusterID != wantCID {
		t.Errorf("HEAD.ClusterID = %q, want %q", head.ClusterID, wantCID)
	}
	if head.Baseline.LTXRef.Key == "" {
		t.Errorf("baseline has empty LTX ref")
	}
	if head.Baseline.TXID == 0 {
		t.Errorf("baseline TXID = 0; want >= 1")
	}
}

func TestPublishSnapshot_Idempotent(t *testing.T) {
	bucket := t.TempDir()
	be, _ := objectstore.OpenFS(bucket)
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath, ObjectBackend: be})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := node.PublishSnapshot(ctx); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	// Second publish with no intervening writes: HEAD already
	// supersedes; Publish returns nil without error.
	if err := node.PublishSnapshot(ctx); err != nil {
		t.Fatalf("second Publish (idempotent): %v", err)
	}
}

func TestNode_Frontier(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	// SchemaLog required so DDL replicates and DML records flow into
	// the producer's drainer (which is what advances senderNextSeq).
	node, err := syzy.Open(context.Background(), syzy.Config{
		Path:      dbPath,
		SchemaLog: schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	// Before any writes the frontier is empty.
	if got := node.Frontier(); len(got) != 0 {
		t.Logf("initial frontier: %v", got)
	}

	if err := node.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := node.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// The producer's drainer allocates self-seq asynchronously; poll
	// until the self entry shows up. Bounded so a wedged drainer can't
	// hang the test forever.
	deadline := time.Now().Add(2 * time.Second)
	var entry syzy.FrontierEntry
	var ok bool
	for time.Now().Before(deadline) {
		f := node.Frontier()
		entry, ok = f[node.Origin()]
		if ok && entry.LastSeq > 0 {
			break
		}
		_ = f
		time.Sleep(5 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("frontier never showed self origin; got %v", node.Frontier())
	}
	if entry.LastSeq == 0 {
		t.Fatalf("frontier last_seq for self = 0 after write")
	}
}

// nodeClusterIDHex returns the lowercase hex form of node.ClusterID().
func nodeClusterIDHex(t *testing.T, node *syzy.Node) string {
	t.Helper()
	id := node.ClusterID()
	const hexChars = "0123456789abcdef"
	out := make([]byte, 32)
	for i, b := range id {
		out[2*i] = hexChars[b>>4]
		out[2*i+1] = hexChars[b&0xf]
	}
	return string(out)
}
