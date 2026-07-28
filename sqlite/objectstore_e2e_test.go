package sqlite_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/transport"
)

// TestObjectStore_SealerEndToEnd exercises the full live pipeline:
// open a syzy node with an ObjectBackend, do INSERTs, observe that
// the sealer uploads epoch objects to the bucket, then verify the
// uploaded epoch is consumable via s3fetch.Source.Fetch.
func TestObjectStore_SealerEndToEnd(t *testing.T) {
	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("FileBackend: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "app.db")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		SchemaLog:     schemalog.NewLocal(),
		ObjectBackend: be,
		SealerConfig: syzy.SealerConfig{
			MaxBytes:   1024, // tiny, force frequent flush
			MaxAge:     50 * time.Millisecond,
			QueueDepth: 64,
			Logf:       t.Logf,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := node.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for i := 0; i < 30; i++ {
		s := strconv.Itoa(i)
		if err := node.Exec(`INSERT INTO kv VALUES ('k` + s + `', ` + s + `)`); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	originHex := node.OriginHex()

	// Wait for the sealer to upload at least one epoch.
	deadline := time.Now().Add(3 * time.Second)
	var found []objectstore.ObjectInfo
	for time.Now().Before(deadline) {
		objs, err := be.List(context.Background(), "origins/"+originHex+"/", "")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(objs) > 0 {
			found = objs
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(found) == 0 {
		t.Fatalf("expected epoch objects under origins/%s/", originHex)
	}

	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After Close, the sealer's final flush should have published
	// any remaining buffered records. List again and verify the
	// upload ranges cover seqs ≥ 1.
	objs, err := be.List(context.Background(), "origins/"+originHex+"/", "")
	if err != nil {
		t.Fatalf("List post-close: %v", err)
	}

	var coveredSeqs []uint64
	src := s3fetch.NewSource(be)
	src.SetCacheTTL(0)
	apply := func(_ context.Context, payload []byte) error {
		coveredSeqs = append(coveredSeqs, binary.BigEndian.Uint64(payload[9:17]))
		return nil
	}
	origin := mustParseOriginHex(t, originHex)
	if err := src.Fetch(context.Background(), []transport.Range{
		{Origin: origin, Lo: 1, Hi: 0}, // open-ended: everything
	}, apply); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(coveredSeqs) == 0 {
		t.Fatalf("Fetch returned no records; got %d epoch objects", len(objs))
	}
	t.Logf("recovered %d Changesets across %d epoch objects", len(coveredSeqs), len(objs))
}

func mustParseOriginHex(t *testing.T, h string) crdt.Origin {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 8 {
		t.Fatalf("origin hex %q: %v", h, err)
	}
	return crdt.Origin(binary.BigEndian.Uint64(b))
}
