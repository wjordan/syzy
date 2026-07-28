package s3fetch_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/epoch"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/transport"
)

// stageEpoch encodes records [lo..hi] into one epoch object under key
// for origin. Each record's payload starts with the canonical header
// so consumers can verify origin/seq via parseHeader-style inspection.
func stageEpoch(t *testing.T, be objectstore.Bucket, origin uint64, lo, hi uint64) {
	t.Helper()
	var buf bytes.Buffer
	enc, err := epoch.NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	enc.FrameTargetBytes = 64 // many tiny frames for testing
	for seq := lo; seq <= hi; seq++ {
		var body bytes.Buffer
		body.WriteByte(0x01)
		var b8 [8]byte
		binary.BigEndian.PutUint64(b8[:], origin)
		body.Write(b8[:])
		binary.BigEndian.PutUint64(b8[:], seq)
		body.Write(b8[:])
		binary.BigEndian.PutUint64(b8[:], 1000+seq)
		body.Write(b8[:])
		body.Write(make([]byte, 16)) // cluster_id
		body.WriteByte(0)            // deps_count
		body.WriteString("payload")
		if err := enc.Append(epoch.Record{Seq: seq, Bytes: body.Bytes()}); err != nil {
			t.Fatalf("Append %d: %v", seq, err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	key := objstore.EpochKey(layout.OriginHex(crdt.Origin(origin)), lo, hi)
	if _, err := be.Put(context.Background(), key, bytes.NewReader(buf.Bytes()), int64(buf.Len()), objectstore.IfAbsent()); err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
}

func TestSource_FetchRangeSpansOneEpoch(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	const origin = uint64(0xABCD)
	stageEpoch(t, be, origin, 1, 50)

	src := s3fetch.NewSource(be)
	src.SetCacheTTL(0) // no caching for the test

	var got []uint64
	apply := func(ctx context.Context, payload []byte) error {
		// origin starts at byte 1, seq at byte 9.
		seq := binary.BigEndian.Uint64(payload[9:17])
		got = append(got, seq)
		return nil
	}
	err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(origin), Lo: 10, Hi: 20},
	}, apply)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(got) != 11 {
		t.Fatalf("got %d records, want 11; got=%v", len(got), got)
	}
	for i, s := range got {
		if s != uint64(10+i) {
			t.Fatalf("got[%d] = %d, want %d", i, s, 10+i)
		}
	}
}

func TestSource_FetchRangeSpansMultipleEpochs(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	const origin = uint64(0x42)
	stageEpoch(t, be, origin, 1, 30)
	stageEpoch(t, be, origin, 31, 60)
	stageEpoch(t, be, origin, 61, 90)

	src := s3fetch.NewSource(be)
	var got []uint64
	apply := func(ctx context.Context, payload []byte) error {
		got = append(got, binary.BigEndian.Uint64(payload[9:17]))
		return nil
	}
	err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(origin), Lo: 25, Hi: 70},
	}, apply)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want := 70 - 25 + 1
	if len(got) != want {
		t.Fatalf("got %d records, want %d", len(got), want)
	}
}

func TestSource_FetchOpenEnded(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	const origin = uint64(0x99)
	stageEpoch(t, be, origin, 1, 20)
	stageEpoch(t, be, origin, 21, 40)

	src := s3fetch.NewSource(be)
	count := atomic.Int32{}
	apply := func(ctx context.Context, payload []byte) error {
		count.Add(1)
		return nil
	}
	err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(origin), Lo: 15, Hi: 0},
	}, apply)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want := int32(40 - 15 + 1)
	if count.Load() != want {
		t.Fatalf("count = %d, want %d", count.Load(), want)
	}
}

func TestSource_FetchUnknownOriginEmpty(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	src := s3fetch.NewSource(be)
	var calls int
	apply := func(ctx context.Context, payload []byte) error {
		calls++
		return nil
	}
	err := src.Fetch(context.Background(), []transport.Range{
		{Origin: crdt.Origin(0xDEADBEEF), Lo: 1, Hi: 100},
	}, apply)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected 0 calls, got %d", calls)
	}
}

// TestCoverageFromSurvivingEpochs: Coverage reports each origin's merged
// surviving-epoch intervals (snapshot-consistent with DiscoverTips) —
// adjacent epochs coalesce, gaps between epochs stay visible, and an
// origin with no epochs is absent (authoritative: the walk is complete).
func TestCoverageFromSurvivingEpochs(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	stageEpoch(t, be, 0xA1, 10, 50)
	stageEpoch(t, be, 0xA1, 51, 90) // adjacent → coalesces with [10,50]
	stageEpoch(t, be, 0xB2, 1, 20)
	stageEpoch(t, be, 0xB2, 40, 60) // gap [21,39] survives the merge

	src := s3fetch.NewSource(be)
	cov, err := src.Coverage(context.Background())
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	want := map[crdt.Origin][]transport.Range{
		0xA1: {{Origin: 0xA1, Lo: 10, Hi: 90}},
		0xB2: {{Origin: 0xB2, Lo: 1, Hi: 20}, {Origin: 0xB2, Lo: 40, Hi: 60}},
	}
	if len(cov) != len(want) {
		t.Fatalf("coverage = %v; want %v", cov, want)
	}
	for o, iv := range want {
		if len(cov[o]) != len(iv) {
			t.Fatalf("coverage[%x] = %v; want %v", o, cov[o], iv)
		}
		for i := range iv {
			if cov[o][i] != iv[i] {
				t.Errorf("coverage[%x][%d] = %v; want %v", o, i, cov[o][i], iv[i])
			}
		}
	}

	tips, err := src.DiscoverTips(context.Background())
	if err != nil {
		t.Fatalf("DiscoverTips: %v", err)
	}
	if tips[0xA1] != 90 || tips[0xB2] != 60 {
		t.Errorf("tips = %v; want A1:90 B2:60", tips)
	}
}
