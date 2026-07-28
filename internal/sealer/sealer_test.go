package sealer_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/epoch"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/sealer"
)

// fakeChangeset constructs a payload with the canonical Changeset
// wire-header so parseHeader recovers (origin, seq, hlc).
func fakeChangeset(origin, seq, hlc uint64, payload string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(0x01) // version
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], origin)
	buf.Write(b[:])
	binary.BigEndian.PutUint64(b[:], seq)
	buf.Write(b[:])
	binary.BigEndian.PutUint64(b[:], hlc)
	buf.Write(b[:])
	// 16 bytes cluster_id (zeros) + 1 byte deps_count = 0
	buf.Write(make([]byte, 16))
	buf.WriteByte(0)
	// trailing payload, opaque to the sealer
	buf.WriteString(payload)
	return buf.Bytes()
}

func TestSealer_FlushBySize(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	s := sealer.New(be, sealer.Config{
		MaxBytes:   2 * 1024,
		MaxAge:     1 * time.Hour,
		QueueDepth: 64,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	const origin = uint64(0xABCD)
	for seq := uint64(1); seq <= 30; seq++ {
		s.OnEncoded(fakeChangeset(origin, seq, 1000+seq, strings.Repeat("a", 200)))
	}
	// Wait for the size threshold to push at least one epoch.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.UploadedSeq(origin) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.UploadedSeq(origin) == 0 {
		t.Fatalf("expected at least one upload")
	}
	cancel()
	s.Stop()

	// All uploaded seqs should be covered between the uploaded epochs.
	covered := coveredSeqs(t, be, origin)
	if len(covered) == 0 {
		t.Fatalf("no epoch objects; expected ≥1")
	}
	for i := uint64(1); i <= s.UploadedSeq(origin); i++ {
		if !covered[i] {
			t.Errorf("seq %d not covered by any uploaded epoch", i)
		}
	}
}

func TestSealer_FlushByAge(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	s := sealer.New(be, sealer.Config{
		MaxBytes:   1 << 30, // never hit
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 32,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	const origin = uint64(0x42)
	for seq := uint64(1); seq <= 5; seq++ {
		s.OnEncoded(fakeChangeset(origin, seq, 100+seq, "tiny"))
	}
	// Wait more than the age threshold + tick interval.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.UploadedSeq(origin) >= 5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if s.UploadedSeq(origin) < 5 {
		t.Fatalf("expected age-based flush to upload all 5 records, got %d", s.UploadedSeq(origin))
	}
	cancel()
	s.Stop()
}

func TestSealer_FlushOnStop(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	s := sealer.New(be, sealer.Config{
		MaxBytes:   1 << 30,
		MaxAge:     1 * time.Hour,
		QueueDepth: 16,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	const origin = uint64(0x99)
	s.OnEncoded(fakeChangeset(origin, 1, 1000, "alpha"))
	s.OnEncoded(fakeChangeset(origin, 2, 1001, "beta"))
	s.OnEncoded(fakeChangeset(origin, 3, 1002, "gamma"))
	// Give the sealer a tick to drain the queue into its buffers.
	time.Sleep(50 * time.Millisecond)
	s.Stop()
	<-done

	if s.UploadedSeq(origin) != 3 {
		t.Fatalf("after Stop: uploaded=%d, want 3", s.UploadedSeq(origin))
	}
}

func TestSealer_IdempotentDuplicateUpload(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	// Pre-populate the bucket so the sealer's PUT collides.
	const origin = uint64(0x77)
	const lo, hi = uint64(1), uint64(2)
	hex := layout.OriginHex(crdt.Origin(origin))
	key := objstore.EpochKey(hex, lo, hi)
	if _, err := be.Put(context.Background(), key, bytes.NewReader([]byte("preexisting")), 11, objectstore.IfAbsent()); err != nil {
		t.Fatalf("seed: %v", err)
	}

	s := sealer.New(be, sealer.Config{
		MaxBytes:   100,
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 16,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	s.OnEncoded(fakeChangeset(origin, 1, 1000, "x"))
	s.OnEncoded(fakeChangeset(origin, 2, 1001, "y"))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.UploadedSeq(origin) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.UploadedSeq(origin) != 2 {
		t.Fatalf("idempotent upload: uploaded=%d, want 2", s.UploadedSeq(origin))
	}
	cancel()
	s.Stop()
}

func TestSealer_GapResetsBuffer(t *testing.T) {
	be, _ := objectstore.OpenFS(t.TempDir())
	s := sealer.New(be, sealer.Config{
		MaxBytes:   1 << 30,
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 16,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	const origin = uint64(0x55)
	s.OnEncoded(fakeChangeset(origin, 1, 100, "a"))
	s.OnEncoded(fakeChangeset(origin, 2, 101, "b"))
	// Skip 3, jump to 4 — gap. Sealer should flush [1,2] and start a new buffer at [4,4].
	s.OnEncoded(fakeChangeset(origin, 4, 103, "d"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.UploadedSeq(origin) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	s.Stop()

	hex := layout.OriginHex(crdt.Origin(origin))
	objs, _ := be.List(context.Background(), objstore.OriginPrefixOf(hex), "")
	// Expect at least the [1,2] epoch to be present.
	found12 := false
	want := objstore.EpochKey(hex, 1, 2)
	for _, o := range objs {
		if o.Key == want {
			found12 = true
		}
	}
	if !found12 {
		t.Fatalf("expected epoch [1,2]; got %v", listKeys(objs))
	}
}

// flakyBucket wraps a Bucket and fails Put while failing is set,
// recording attempted and successful keys.
type flakyBucket struct {
	objectstore.Bucket
	mu        sync.Mutex
	failing   bool
	failPuts  int // fail this many Puts (in addition to failing)
	attempts  []string
	succeeded []string
}

func (f *flakyBucket) setFailing(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = v
}

func (f *flakyBucket) putAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.attempts)
}

func (f *flakyBucket) successKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.succeeded...)
}

func (f *flakyBucket) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	f.mu.Lock()
	f.attempts = append(f.attempts, key)
	fail := f.failing || f.failPuts > 0
	if f.failPuts > 0 {
		f.failPuts--
	}
	f.mu.Unlock()
	if fail {
		return "", errors.New("injected put failure")
	}
	etag, err := f.Bucket.Put(ctx, key, body, length, ifMatch)
	if err == nil {
		f.mu.Lock()
		f.succeeded = append(f.succeeded, key)
		f.mu.Unlock()
	}
	return etag, err
}

// TestSealer_RetainsOnFailedUpload injects a single Put failure on the
// size flush and asserts nothing is lost: after the backend recovers,
// every seq is covered by an uploaded epoch and UploadedSeq reaches
// the tail.
func TestSealer_RetainsOnFailedUpload(t *testing.T) {
	fsbe, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	be := &flakyBucket{Bucket: fsbe, failPuts: 1}

	const origin = uint64(0xF00D)
	recLen := len(fakeChangeset(origin, 1, 100, "payload"))
	s := sealer.New(be, sealer.Config{
		MaxBytes:   9 * recLen, // size flush trips exactly at seq 9
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 16,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	for seq := uint64(1); seq <= 10; seq++ {
		s.OnEncoded(fakeChangeset(origin, seq, 100+seq, "payload"))
	}

	// The [1,9] size flush fails once, is retained, retries on a tick,
	// then [10,10] ships by age.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.UploadedSeq(origin) < 10 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.UploadedSeq(origin); got != 10 {
		t.Fatalf("UploadedSeq=%d, want 10 (failed upload dropped instead of retained?)", got)
	}
	cancel()
	s.Stop()

	covered := coveredSeqs(t, fsbe, origin)
	for i := uint64(1); i <= 10; i++ {
		if !covered[i] {
			t.Errorf("seq %d lost: not covered by any uploaded epoch", i)
		}
	}
}

// TestSealer_WatermarkHoldsWhileFailing keeps the backend down across
// multiple flush attempts and asserts UploadedSeq stays 0 the whole
// time; on recovery, uploads land strictly in order (the first
// successful epoch starts at seq 1).
func TestSealer_WatermarkHoldsWhileFailing(t *testing.T) {
	fsbe, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	be := &flakyBucket{Bucket: fsbe, failing: true}

	const origin = uint64(0xBEEF)
	recLen := len(fakeChangeset(origin, 1, 100, "payload"))
	s := sealer.New(be, sealer.Config{
		MaxBytes:   3 * recLen, // first epoch seals at [1,3]
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 16,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	for seq := uint64(1); seq <= 9; seq++ {
		s.OnEncoded(fakeChangeset(origin, seq, 100+seq, "payload"))
	}

	// Wait for at least two attempts (initial size flush + a tick
	// retry) while the backend is down.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && be.putAttempts() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if be.putAttempts() < 2 {
		t.Fatalf("expected ≥2 put attempts while failing, got %d", be.putAttempts())
	}
	if got := s.UploadedSeq(origin); got != 0 {
		t.Fatalf("UploadedSeq advanced to %d while every upload failed; watermark vouches for lost records", got)
	}

	be.setFailing(false)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.UploadedSeq(origin) < 9 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.UploadedSeq(origin); got != 9 {
		t.Fatalf("after recovery UploadedSeq=%d, want 9", got)
	}
	cancel()
	s.Stop()

	// Order: the first successful upload must be the oldest epoch [1,3].
	hex := layout.OriginHex(crdt.Origin(origin))
	succ := be.successKeys()
	if len(succ) == 0 || succ[0] != objstore.EpochKey(hex, 1, 3) {
		t.Fatalf("first successful upload = %v, want %s first", succ, objstore.EpochKey(hex, 1, 3))
	}
	covered := coveredSeqs(t, fsbe, origin)
	for i := uint64(1); i <= 9; i++ {
		if !covered[i] {
			t.Errorf("seq %d lost", i)
		}
	}
}

// TestSealer_StopWithFailingBackendKeepsGateClosed: if the final stop
// flush fails, records are dropped loudly but UploadedSeq must NOT
// advance — the journal GC gate stays closed.
func TestSealer_StopWithFailingBackendKeepsGateClosed(t *testing.T) {
	fsbe, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	be := &flakyBucket{Bucket: fsbe, failing: true}

	var logMu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		logMu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		logMu.Unlock()
	}

	s := sealer.New(be, sealer.Config{
		MaxBytes:   1 << 30,
		MaxAge:     25 * time.Millisecond, // observable (failing) age flush before Stop
		QueueDepth: 16,
		Logf:       logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	const origin = uint64(0xDEAD)
	for seq := uint64(1); seq <= 3; seq++ {
		s.OnEncoded(fakeChangeset(origin, seq, 100+seq, "x"))
	}
	// Wait until the records are buffered and a flush attempt has
	// failed, so Stop's final flush is what drops them.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && be.putAttempts() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if be.putAttempts() == 0 {
		t.Fatalf("no flush attempt before Stop")
	}
	s.Stop()
	<-done

	if got := s.UploadedSeq(origin); got != 0 {
		t.Fatalf("UploadedSeq=%d after failed final flush, want 0 (GC gate must stay closed)", got)
	}
	if be.putAttempts() < 2 {
		t.Fatalf("expected the final Stop flush to retry the upload, attempts=%d", be.putAttempts())
	}
	objs, err := fsbe.List(context.Background(), "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) != 0 {
		t.Fatalf("bucket should be empty, got %v", listKeys(objs))
	}
	logMu.Lock()
	defer logMu.Unlock()
	found := false
	for _, l := range logs {
		if strings.Contains(l, "ERROR") && strings.Contains(l, "dropping") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a loud ERROR drop log on failed final flush; logs: %v", logs)
	}
}

// ---- helpers ---------------------------------------------------------------

// coveredSeqs decodes every uploaded epoch for origin and returns the
// set of record seqs they cover.
func coveredSeqs(t *testing.T, be objectstore.Bucket, origin uint64) map[uint64]bool {
	t.Helper()
	hex := layout.OriginHex(crdt.Origin(origin))
	objs, err := be.List(context.Background(), objstore.OriginPrefixOf(hex), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	covered := map[uint64]bool{}
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	for _, o := range objs {
		body, err := readAll(be, o.Key)
		if err != nil {
			t.Fatalf("read %s: %v", o.Key, err)
		}
		footer, err := epoch.ReadFooter(int64(len(body)), readAtFromBytes(body))
		if err != nil {
			t.Fatalf("ReadFooter %s: %v", o.Key, err)
		}
		for _, fr := range footer.Frames {
			compressed := body[fr.Offset : fr.Offset+fr.CompressedSize]
			recs, err := epoch.DecodeFrame(compressed, dec)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			for _, r := range recs {
				covered[r.Seq] = true
			}
		}
	}
	return covered
}

func readAll(b objectstore.Bucket, key string) ([]byte, error) {
	rc, _, err := b.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(rc)
	return buf.Bytes(), err
}

func readAtFromBytes(body []byte) func(off, length int64) ([]byte, error) {
	return func(off, length int64) ([]byte, error) {
		end := off + length
		if end > int64(len(body)) {
			end = int64(len(body))
		}
		return body[off:end], nil
	}
}

func listKeys(objs []objectstore.ObjectInfo) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Key)
	}
	return out
}

// TestSealer_ContiguousStopsAtInputHole feeds seqs 1-5, skips 6, then
// feeds 7-10. The sealer uploads epochs on both sides of the hole, so
// UploadedSeq (a max) reaches 10 — but ContiguousSealedSeq must park at
// 5, the last seq before the never-fed hole. This is the watermark the
// journal-GC gate relies on to avoid unlinking source records behind a
// hole (the drain-before-listeners incident's evidence-loss vector).
func TestSealer_ContiguousStopsAtInputHole(t *testing.T) {
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	s := sealer.New(be, sealer.Config{
		MaxBytes:   1 << 30, // never trip on size
		MaxAge:     50 * time.Millisecond,
		QueueDepth: 32,
		Logf:       t.Logf,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	const origin = uint64(0xBEEF)
	for _, seq := range []uint64{1, 2, 3, 4, 5, 7, 8, 9, 10} { // 6 skipped
		s.OnEncoded(fakeChangeset(origin, seq, 100+seq, "payload"))
	}

	// Both sides flush: [1,5] on the gap when 7 arrives, [7,10] by age.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.UploadedSeq(origin) < 10 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	s.Stop()

	if got := s.UploadedSeq(origin); got != 10 {
		t.Fatalf("UploadedSeq=%d, want 10 (max should sail past the hole)", got)
	}
	if got := s.ContiguousSealedSeq(origin); got != 5 {
		t.Fatalf("ContiguousSealedSeq=%d, want 5 (must stop at the hole before seq 6)", got)
	}
}
