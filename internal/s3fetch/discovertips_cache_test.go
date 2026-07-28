package s3fetch_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/transport"
)

// TestDiscoverTipsSeedsFetchCache is the cost fix: a DiscoverTips prefix
// walk seeds the per-origin epoch cache, so a subsequent gap-fetch reuses
// it and issues NO per-origin List. In production every node gap-fills
// every inherited origin each round, so the per-origin List fan-out was
// the dominant origins/ request cost; seeding collapses it into the single
// prefix List DiscoverTips already does.
func TestDiscoverTipsSeedsFetchCache(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	const origin = uint64(0xABCD)
	stageEpoch(t, be, origin, 1, 50)
	stageEpoch(t, be, origin, 51, 100)

	m := objectstore.NewMetered(be, nil) // counts List calls
	src := s3fetch.NewSource(m)

	// One round: DiscoverTips (one prefix List) then a gap-fetch.
	if _, err := src.DiscoverTips(context.Background()); err != nil {
		t.Fatal(err)
	}
	listsAfterDiscover := m.Stats().TotalList.ListCount
	if listsAfterDiscover != 1 {
		t.Fatalf("DiscoverTips did %d lists, want 1", listsAfterDiscover)
	}

	var got int
	apply := func(ctx context.Context, payload []byte) error { got++; return nil }
	if err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(origin), Lo: 10, Hi: 90},
	}, apply); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != 81 { // seqs 10..90 inclusive
		t.Fatalf("Fetch applied %d records, want 81 (seeded epochs unusable)", got)
	}
	// The Fetch must not have issued a per-origin List: the seed covered it.
	if extra := m.Stats().TotalList.ListCount - listsAfterDiscover; extra != 0 {
		t.Fatalf("Fetch issued %d per-origin List(s) after seeding, want 0", extra)
	}
}

// TestDiscoverTipsAbsenceSkipsList verifies the absence short-circuit: once a
// DiscoverTips prefix walk has run, a Fetch for an origin with no bucket epochs
// (e.g. one the broker tracks via senderNextSeq but that never sealed) issues NO
// per-origin List — the complete walk already proved the origin absent.
func TestDiscoverTipsAbsenceSkipsList(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	stageEpoch(t, be, 7, 1, 50) // one real origin present in the bucket

	m := objectstore.NewMetered(be, nil)
	src := s3fetch.NewSource(m)

	if _, err := src.DiscoverTips(context.Background()); err != nil {
		t.Fatal(err)
	}
	listsAfterDiscover := m.Stats().TotalList.ListCount

	// Fetch a DIFFERENT origin that has no epochs in the bucket.
	var applied int
	apply := func(ctx context.Context, payload []byte) error { applied++; return nil }
	if err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(0xDEAD), Lo: 1, Hi: 100},
	}, apply); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied %d records for an absent origin, want 0", applied)
	}
	if extra := m.Stats().TotalList.ListCount - listsAfterDiscover; extra != 0 {
		t.Fatalf("absent-origin Fetch issued %d per-origin List(s), want 0", extra)
	}
}

// TestDiscoverTipsCaches verifies the TTL cache that keeps the broker's
// every-round DiscoverTips from re-LISTing origins/ each time.
func TestDiscoverTipsCaches(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	stageEpoch(t, be, 7, 1, 100)
	stageEpoch(t, be, 7, 101, 200)

	m := objectstore.NewMetered(be, nil) // counts List calls
	src := s3fetch.NewSource(m)
	src.SetTipsTTL(time.Minute)

	tips, err := src.DiscoverTips(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tips[crdt.Origin(7)] != crdt.Seq(200) {
		t.Errorf("tip = %d, want 200", tips[crdt.Origin(7)])
	}

	// Second call within TTL is served from cache: no new LIST.
	if _, err := src.DiscoverTips(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := m.Stats().TotalList.ListCount; got != 1 {
		t.Errorf("ListCount = %d, want 1 (second call cached)", got)
	}

	// Returned map is a copy: caller mutation must not corrupt the cache.
	tips[crdt.Origin(7)] = 0
	again, _ := src.DiscoverTips(context.Background())
	if again[crdt.Origin(7)] != crdt.Seq(200) {
		t.Errorf("cache corrupted by caller mutation: %d", again[crdt.Origin(7)])
	}

	// TTL=0 disables caching: each call lists.
	src.SetTipsTTL(0)
	before := m.Stats().TotalList.ListCount
	_, _ = src.DiscoverTips(context.Background())
	_, _ = src.DiscoverTips(context.Background())
	if got := m.Stats().TotalList.ListCount - before; got != 2 {
		t.Errorf("with TTL=0 expected 2 lists, got %d", got)
	}
}

// TestDiscoverTipsPaginates verifies DiscoverTips walks the full origins/
// prefix, not just the first ~1000-key page. A single unpaginated LIST would
// silently drop origins past the page boundary, which origin GC would then
// misread as swept and evict (re-fetch thrash), and which would also starve
// the fetcher of catch-up targets.
func TestDiscoverTipsPaginates(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	ctx := context.Background()
	const n = 1001 // one past the fs backend's 1000-key page cap
	for i := 1; i <= n; i++ {
		key := objstore.EpochKey(fmt.Sprintf("%016x", i), 1, uint64(i))
		if _, err := be.Put(ctx, key, strings.NewReader("x"), 1, nil); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	src := s3fetch.NewSource(be) // TTL=0: always lists
	tips, err := src.DiscoverTips(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tips) != n {
		t.Fatalf("DiscoverTips returned %d origins, want %d (pagination truncated)", len(tips), n)
	}
	// An origin that can only live on the second page must be present.
	if tips[crdt.Origin(n)] != crdt.Seq(n) {
		t.Errorf("second-page origin tip = %d, want %d", tips[crdt.Origin(n)], n)
	}
}
