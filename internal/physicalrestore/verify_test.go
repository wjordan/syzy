package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// Restore must verify baseline objects against HEAD's recorded
// sha256: under a concurrent double-claim the HEAD pointer and the
// object bytes can come from different publishers, and decoding the
// mix silently yields a corrupt database.
func TestFetchVerifiedRef(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	ctx := context.Background()
	body := []byte("baseline-bytes")
	if _, err := be.Put(ctx, "db/0009/x", bytes.NewReader(body), int64(len(body)), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	sum := sha256.Sum256(body)
	dst := func(name string) string { return filepath.Join(t.TempDir(), name) }

	good := objstore.FileRef{Key: "db/0009/x", Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	goodPath := dst("good")
	if err := FetchVerifiedRef(ctx, be, good, goodPath); err != nil {
		t.Fatalf("matching ref rejected: %v", err)
	}
	if got, _ := os.ReadFile(goodPath); !bytes.Equal(got, body) {
		t.Fatalf("downloaded bytes mismatch: %q", got)
	}
	foreign := good
	foreign.Sha256 = strings.Repeat("00", 32)
	if err := FetchVerifiedRef(ctx, be, foreign, dst("foreign")); err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Fatalf("foreign-bytes ref accepted: %v", err)
	}
	legacy := objstore.FileRef{Key: "db/0009/x"}
	if err := FetchVerifiedRef(ctx, be, legacy, dst("legacy")); err != nil {
		t.Fatalf("legacy ref (no hash) rejected: %v", err)
	}
}
