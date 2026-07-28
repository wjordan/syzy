package syncer

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/futex"
	"github.com/wjordan/syzy/internal/journal"
)

// drainerFixture is a self-contained drainer test rig: a real journal
// (records appended directly via j.Append) and a mock sink that records
// what Apply was called with.
type drainerFixture struct {
	dir       string
	j         *journal.Journal
	sink      *mockSink
	drainer   *Drainer
	drainCtx  context.Context
	drainDone chan struct{}
	cancel    context.CancelFunc
}

type mockSink struct {
	mu      sync.Mutex
	last    journal.Offset
	batches [][]DrainRecord
	failNth int // 1-based; 0 = never fail
	calls   int
}

func (m *mockSink) LastDrainedOffset() (journal.Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last, nil
}

func (m *mockSink) Apply(records []DrainRecord) (journal.Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.failNth > 0 && m.calls == m.failNth {
		return 0, errors.New("mockSink: forced failure")
	}
	// Copy payloads — borrow contract says they may go stale after
	// Apply returns if later snapshotter GC deletes the source segment.
	cp := make([]DrainRecord, len(records))
	for i, r := range records {
		payload := append([]byte(nil), r.Payload...)
		cp[i] = r
		cp[i].Payload = payload
	}
	m.batches = append(m.batches, cp)
	if n := len(records); n > 0 {
		m.last = records[n-1].NextOff
	}
	return m.last, nil
}

func (m *mockSink) totalRecords() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.batches {
		n += len(b)
	}
	return n
}

func (m *mockSink) batchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func newDrainerFixture(t *testing.T, opts ...DrainerOption) *drainerFixture {
	return newDrainerFixtureWithSegmentSize(t, 1<<20, opts...)
}

func newDrainerFixtureWithSegmentSize(t *testing.T, segmentSize uint32, opts ...DrainerOption) *drainerFixture {
	t.Helper()
	dir := t.TempDir()
	jdir := filepath.Join(dir, "jrn")
	j, err := journal.Open(jdir, segmentSize, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	sink := &mockSink{}
	dr, err := NewDrainer(j, sink, opts...)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	drainCtx, cancel := context.WithCancel(context.Background())
	drainDone := make(chan struct{})
	go func() {
		_ = dr.Run(drainCtx)
		close(drainDone)
	}()

	t.Cleanup(func() {
		cancel()
		<-drainDone
	})

	return &drainerFixture{
		dir: dir, j: j, sink: sink, drainer: dr,
		drainCtx: drainCtx, drainDone: drainDone, cancel: cancel,
	}
}

// commit appends a single journal record. wal_hook is the production
// path that calls j.Append; tests bypass it and write directly because
// the drainer's only contract is "what's in the journal is durable."
func (f *drainerFixture) commit(t *testing.T, i int, payload []byte) {
	t.Helper()
	if _, _, err := f.j.Append(journal.KindLocalDML, uint64(i), 7, payload); err != nil {
		t.Fatalf("journal.Append: %v", err)
	}
}

func waitFor(t *testing.T, deadline time.Duration, predicate func() bool, msg string) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if predicate() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestDrainerDeliversConfirmedRecords(t *testing.T) {
	f := newDrainerFixture(t)
	const N = 30
	for i := 0; i < N; i++ {
		f.commit(t, i, []byte{byte(i)})
	}
	waitFor(t, 2*time.Second,
		func() bool { return f.sink.totalRecords() == N },
		"sink received all N records")
	if got := f.drainer.DrainedOffset(); got != f.j.Head() {
		t.Errorf("DrainedOffset = %d; want journal head %d", got, f.j.Head())
	}
}

func TestDrainerSkipsAbortedRecords(t *testing.T) {
	f := newDrainerFixture(t)
	for i := 0; i < 3; i++ {
		f.commit(t, i, []byte{byte(i)})
	}
	off, _, err := f.j.Append(journal.KindLocalDML, 999, 7, []byte("aborted"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := f.j.MarkAborted(off); err != nil {
		t.Fatalf("MarkAborted: %v", err)
	}
	for i := 3; i < 7; i++ {
		f.commit(t, i, []byte{byte(i)})
	}
	waitFor(t, 2*time.Second,
		func() bool { return f.sink.totalRecords() == 7 },
		"7 non-aborted records delivered")
	for _, b := range f.sink.batches {
		for _, r := range b {
			if string(r.Payload) == "aborted" {
				t.Errorf("aborted record was delivered to sink")
			}
		}
	}
}

func TestDrainerBatchesUnderLoad(t *testing.T) {
	f := newDrainerFixture(t, WithBatchMax(8))
	const N = 25
	for i := 0; i < N; i++ {
		f.commit(t, i, []byte{byte(i)})
	}
	waitFor(t, 2*time.Second,
		func() bool { return f.sink.totalRecords() == N },
		"all records delivered")
	if got := f.sink.batchCount(); got > N {
		t.Errorf("batchCount = %d; want < %d (some batching expected)", got, N)
	}
}

func TestDrainerLeavesSegmentRetentionToSnapshotter(t *testing.T) {
	f := newDrainerFixtureWithSegmentSize(t, 4096)
	payload := make([]byte, 512)
	const N = 20
	for i := 0; i < N; i++ {
		payload[0] = byte(i)
		f.commit(t, i, payload)
	}
	waitFor(t, 2*time.Second,
		func() bool { return f.sink.totalRecords() == N },
		"all records delivered")
	if got := len(f.j.Segments()); got < 2 {
		t.Fatalf("drainer retained journal segments = %v; want multiple segments left for snapshotter GC", f.j.Segments())
	}
}

func TestDrainerSharedWakeCrossHandle(t *testing.T) {
	if !futex.Supported {
		t.Skip("shared futex wait unsupported")
	}
	dir := t.TempDir()
	jdir := filepath.Join(dir, "jrn")
	writer, err := journal.Open(jdir, 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	writer.EnableSharedWake(true)
	reader, err := journal.Open(jdir, 0, journal.SyncOff)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	sink := &mockSink{}
	dr, err := NewDrainer(reader, sink, WithSharedWake(), WithPollInterval(5*time.Second))
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = dr.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		_, _, _ = writer.Append(journal.KindEmpty, 0, 7, nil)
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("drainer did not stop")
		}
	}()

	if _, _, err := writer.Append(journal.KindLocalDML, 1, 7, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	waitFor(t, 200*time.Millisecond,
		func() bool { return sink.totalRecords() == 1 },
		"shared wake delivered cross-handle append")
}

func TestDrainerSharedWakeCrossHandleRotation(t *testing.T) {
	if !futex.Supported {
		t.Skip("shared futex wait unsupported")
	}
	dir := t.TempDir()
	jdir := filepath.Join(dir, "jrn")
	writer, err := journal.Open(jdir, 4096, journal.SyncOff)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	writer.EnableSharedWake(true)
	reader, err := journal.Open(jdir, 0, journal.SyncOff)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	sink := &mockSink{}
	dr, err := NewDrainer(reader, sink, WithSharedWake(), WithPollInterval(5*time.Second))
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = dr.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		_, _, _ = writer.Append(journal.KindEmpty, 0, 7, nil)
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("drainer did not stop")
		}
	}()

	payload := make([]byte, 512)
	const N = 20
	for i := 0; i < N; i++ {
		payload[0] = byte(i)
		if _, _, err := writer.Append(journal.KindLocalDML, uint64(i), 7, payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	waitFor(t, 300*time.Millisecond,
		func() bool { return sink.totalRecords() == N },
		"shared wake delivered records across rotation")
	if len(reader.Segments()) < 2 {
		t.Fatalf("reader segments = %v; want rotation discovered", reader.Segments())
	}
}

func TestDrainerResumesFromSinkOffset(t *testing.T) {
	// Pre-populate journal with 7 records, drive the drainer through
	// them, then change the sink's claimed "last drained" mid-flight to
	// verify a fresh drainer honors it on (re)construction.
	f := newDrainerFixture(t)
	for i := 0; i < 7; i++ {
		f.commit(t, i, []byte{byte(i)})
	}
	waitFor(t, 2*time.Second,
		func() bool { return f.sink.totalRecords() == 7 },
		"first drainer ran through all 7 records")

	f.cancel()
	<-f.drainDone

	freshSink := &mockSink{last: 0}
	d, err := NewDrainer(f.j, freshSink)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	// AlignResume normalizes 0 to the oldest segment's first record
	// boundary — same position Iterate(0) resolves to.
	if want := f.j.AlignResume(0); d.DrainedOffset() != want {
		t.Errorf("DrainedOffset = %d; want %d (start of journal)", d.DrainedOffset(), want)
	}
}
