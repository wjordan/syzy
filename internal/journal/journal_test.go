package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/futex"
)

const testSegmentSize uint32 = 64 * 1024

func openTest(t testing.TB) *Journal {
	t.Helper()
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func TestOpenCreatesAndReopensCleanly(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := j.Head(); got != fileHeaderSize {
		t.Errorf("Head() on fresh journal = %d; want %d", got, fileHeaderSize)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen empty journal.
	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	if got := j2.Head(); got != fileHeaderSize {
		t.Errorf("Head() after reopen of empty journal = %d; want %d", got, fileHeaderSize)
	}
}

func TestAppendIterate(t *testing.T) {
	j := openTest(t)
	payload := []byte("hello world")
	off, seq, err := j.Append(KindLocalDML, 0xabcd, 42, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != fileHeaderSize {
		t.Errorf("first record offset = %d; want %d", off, fileHeaderSize)
	}
	if seq != 1 {
		t.Errorf("first record seq = %d; want 1", seq)
	}

	it := j.Iterate(0)
	rec, gotOff, err := it.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if gotOff != off {
		t.Errorf("Next offset = %d; want %d", gotOff, off)
	}
	if rec.Kind != KindLocalDML {
		t.Errorf("Kind = %d; want %d", rec.Kind, KindLocalDML)
	}
	if rec.Seq != 1 || rec.HLC != 0xabcd || rec.Origin != 42 {
		t.Errorf("rec metadata = %+v; want seq=1 hlc=0xabcd origin=42", rec)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("Payload = %q; want %q", rec.Payload, payload)
	}
	if rec.Aborted() {
		t.Error("fresh record reports Aborted() = true")
	}

	// Second Next reaches the pending publish word.
	if _, _, err := it.Next(); !errors.Is(err, ErrPending) {
		t.Errorf("second Next err = %v; want ErrPending", err)
	}
}

func TestAppendNotifyIsCoalesced(t *testing.T) {
	j := openTest(t)
	select {
	case <-j.Notify():
		t.Fatal("fresh journal unexpectedly notified")
	default:
	}

	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("one")); err != nil {
		t.Fatalf("Append one: %v", err)
	}
	if _, _, err := j.Append(KindLocalDML, 2, 1, []byte("two")); err != nil {
		t.Fatalf("Append two: %v", err)
	}

	select {
	case <-j.Notify():
	default:
		t.Fatal("Append did not notify")
	}
	select {
	case <-j.Notify():
		t.Fatal("notifications were not coalesced")
	default:
	}

	if _, _, err := j.Append(KindLocalDML, 3, 1, []byte("three")); err != nil {
		t.Fatalf("Append three: %v", err)
	}
	select {
	case <-j.Notify():
	default:
		t.Fatal("Append after draining notification did not notify")
	}
}

func TestWaitAtSeesCrossHandleAppend(t *testing.T) {
	if !futex.Supported {
		t.Skip("shared futex wait unsupported")
	}
	dir := t.TempDir()
	writer, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	writer.EnableSharedWake(true)
	reader, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- reader.WaitAt(ctx, makeOffset(0, fileHeaderSize), time.Second)
	}()

	if _, _, err := writer.Append(KindLocalDML, 7, 9, []byte("wake")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitAt: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	rec, _, err := reader.Iterate(0).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.HLC != 7 || rec.Origin != 9 || string(rec.Payload) != "wake" {
		t.Fatalf("record = %+v payload=%q; want hlc=7 origin=9 payload=wake", rec, rec.Payload)
	}
}

func TestSetWakeFuncCalledOnAppend(t *testing.T) {
	// SetWakeFunc replaces the futex.WakeAll default. Verify it fires
	// once per Append and receives the publish-word address.
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	var wakeCalls atomic.Int32
	var lastAddr atomic.Pointer[uint32]
	j.SetWakeFunc(func(addr *uint32) {
		wakeCalls.Add(1)
		lastAddr.Store(addr)
	})
	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := wakeCalls.Load(); got != 1 {
		t.Fatalf("wakeFn calls = %d; want 1", got)
	}
	if addr := lastAddr.Load(); addr == nil || atomic.LoadUint32(addr) == 0 {
		t.Fatalf("wakeFn addr was not the published kind word")
	}
}

func TestSetWaitFuncReplacesFutexWait(t *testing.T) {
	// SetWaitFunc replaces futex.Wait inside WaitAt. Park on an
	// offset that's never published; verify the custom waitFn is
	// called and its ErrTimeout return propagates through WaitAt's
	// loop.
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	var waitCalls atomic.Int32
	j.SetWaitFunc(func(ctx context.Context, _ *uint32, _ uint32, timeout time.Duration) error {
		waitCalls.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
			return futex.ErrTimeout
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = j.WaitAt(ctx, makeOffset(0, fileHeaderSize), 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAt err = %v; want context.DeadlineExceeded", err)
	}
	if got := waitCalls.Load(); got < 2 {
		t.Fatalf("waitFn calls = %d; want >= 2 (ctx deadline 50ms / timeout 10ms)", got)
	}
}

func TestSetWakeFuncNilDisablesWake(t *testing.T) {
	// SetWakeFunc(nil) restores the EnableSharedWake-controlled
	// default. Verify Append still works without an external wake.
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	called := false
	j.SetWakeFunc(func(*uint32) { called = true })
	j.SetWakeFunc(nil)

	if _, _, err := j.Append(KindLocalDML, 1, 1, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if called {
		t.Fatal("wakeFn invoked after SetWakeFunc(nil)")
	}
}

func TestAppendManyIterateInOrder(t *testing.T) {
	j := openTest(t)
	const N = 100
	offsets := make([]Offset, N)
	for i := 0; i < N; i++ {
		payload := make([]byte, 16+i%32)
		binary.LittleEndian.PutUint64(payload, uint64(i))
		off, _, err := j.Append(KindLocalDML, uint64(i), 7, payload)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offsets[i] = off
	}
	it := j.Iterate(0)
	for i := 0; i < N; i++ {
		rec, off, err := it.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if off != offsets[i] {
			t.Errorf("rec %d offset = %d; want %d", i, off, offsets[i])
		}
		if rec.Seq != uint64(i+1) {
			t.Errorf("rec %d seq = %d; want %d", i, rec.Seq, i+1)
		}
		if rec.HLC != uint64(i) {
			t.Errorf("rec %d hlc = %d; want %d", i, rec.HLC, i)
		}
		if got := binary.LittleEndian.Uint64(rec.Payload); got != uint64(i) {
			t.Errorf("rec %d payload prefix = %d; want %d", i, got, i)
		}
	}
	if _, _, err := it.Next(); !errors.Is(err, ErrPending) {
		t.Errorf("Next past end err = %v; want ErrPending", err)
	}
}

func TestSegmentRotationOnFull(t *testing.T) {
	dir := t.TempDir()
	// Smallest legal segment: holds the header + ~1KiB.
	j, err := Open(dir, fileHeaderSize+1024, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	payload := bytes.Repeat([]byte("x"), 200)
	// Append enough records to force at least two rotations. With 200B
	// payloads in a ~1KB segment, ~3-4 records fit per segment.
	const N = 30
	var firstSeg, lastSeg uint32
	for i := 0; i < N; i++ {
		off, _, err := j.Append(KindLocalDML, 0, 0, payload)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if i == 0 {
			firstSeg = off.seg()
		}
		lastSeg = off.seg()
	}
	if lastSeg <= firstSeg {
		t.Errorf("segment did not rotate: firstSeg=%d lastSeg=%d", firstSeg, lastSeg)
	}
}

func TestAppendOversizedPayloadGetsDedicatedSegment(t *testing.T) {
	dir := t.TempDir()
	targetSize := uint32(fileHeaderSize + 1024)
	j, err := Open(dir, targetSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()

	payload := bytes.Repeat([]byte("x"), int(targetSize))
	off, seq, err := j.Append(KindLocalDML, 0xabc, 7, payload)
	if err != nil {
		t.Fatalf("Append oversized: %v", err)
	}
	if off.seg() == 0 {
		t.Fatalf("oversized record stayed in initial target segment at off=%d", off)
	}
	if seq != 1 {
		t.Fatalf("seq=%d, want 1", seq)
	}

	info, err := os.Stat(segmentPath(dir, off.seg()))
	if err != nil {
		t.Fatalf("stat oversized segment: %v", err)
	}
	need, err := requiredSegmentSize(recordTotalLen(uint32(len(payload))))
	if err != nil {
		t.Fatalf("requiredSegmentSize: %v", err)
	}
	if info.Size() < int64(need) {
		t.Fatalf("oversized segment size=%d, want at least %d", info.Size(), need)
	}

	it := j.Iterate(0)
	rec, gotOff, err := it.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if gotOff != off {
		t.Fatalf("record offset=%d, want %d", gotOff, off)
	}
	if rec.Seq != seq || rec.HLC != 0xabc || rec.Origin != 7 {
		t.Fatalf("record metadata=%+v, want seq=%d hlc=0xabc origin=7", rec, seq)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("payload mismatch len=%d want=%d", len(rec.Payload), len(payload))
	}
}

func TestOversizedPayloadRecovery(t *testing.T) {
	dir := t.TempDir()
	targetSize := uint32(fileHeaderSize + 1024)
	payload := bytes.Repeat([]byte("y"), int(targetSize))
	j, err := Open(dir, targetSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	off, _, err := j.Append(KindLocalDML, 1, 2, payload)
	if err != nil {
		t.Fatalf("Append oversized: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(dir, targetSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	rec, gotOff, err := j2.Iterate(0).Next()
	if err != nil {
		t.Fatalf("Next after reopen: %v", err)
	}
	if gotOff != off {
		t.Fatalf("offset after reopen=%d, want %d", gotOff, off)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("payload after reopen len=%d want=%d", len(rec.Payload), len(payload))
	}
}

func TestReaderOpensExistingOversizedSuccessor(t *testing.T) {
	dir := t.TempDir()
	targetSize := uint32(fileHeaderSize + 1024)
	writer, err := Open(dir, targetSize, SyncOff)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()
	reader, err := Open(dir, targetSize, SyncOff)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	payload := bytes.Repeat([]byte("z"), int(targetSize))
	off, _, err := writer.Append(KindLocalDML, 3, 4, payload)
	if err != nil {
		t.Fatalf("Append oversized: %v", err)
	}

	reader.Refresh()
	rec, gotOff, err := reader.Iterate(0).Next()
	if err != nil {
		t.Fatalf("reader Next: %v", err)
	}
	if gotOff != off {
		t.Fatalf("reader offset=%d, want %d", gotOff, off)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Fatalf("reader payload len=%d want=%d", len(rec.Payload), len(payload))
	}
}

func TestRecoveryAfterCleanClose(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		payload := []byte{byte(i)}
		if _, _, err := j.Append(KindLocalDML, uint64(i), 0, payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	headBefore := j.Head()
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	if j2.Head() != headBefore {
		t.Errorf("recovered head = %d; want %d", j2.Head(), headBefore)
	}

	it := j2.Iterate(0)
	for i := 0; i < 5; i++ {
		rec, _, err := it.Next()
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		if rec.HLC != uint64(i) {
			t.Errorf("recovered rec %d hlc = %d; want %d", i, rec.HLC, i)
		}
	}
	if _, _, err := it.Next(); !errors.Is(err, ErrPending) {
		t.Errorf("Next past recovered end err = %v; want ErrPending", err)
	}

	// Append after recovery — seq picks up from last+1.
	_, seq, err := j2.Append(KindLocalDML, 999, 0, []byte("post"))
	if err != nil {
		t.Fatalf("post-recovery Append: %v", err)
	}
	if seq != 6 {
		t.Errorf("post-recovery seq = %d; want 6", seq)
	}
}

// TestRecoveryFromTornTail simulates a process crash mid-record-write.
// We produce a clean journal, then corrupt the last record's CRC, then
// reopen and verify recovery stops at the last fully valid record.
func TestRecoveryFromTornTail(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var lastOff Offset
	const lastPayloadLen = 1 // single-byte payloads
	for i := 0; i < 5; i++ {
		off, _, err := j.Append(KindLocalDML, uint64(i), 0, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastOff = off
	}
	expectedHeadAfterTear := lastOff
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the last record's CRC. With the [header][payload][crc][pad]
	// layout, the CRC is at lastOff + headerLen + payloadLen.
	f, err := os.OpenFile(segmentPath(dir, lastOff.seg()), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	crcOff := int64(lastOff.byteOff()) + int64(recordHeaderLen) + int64(lastPayloadLen)
	bad := []byte{0xff, 0xff, 0xff, 0xff}
	if _, err := f.WriteAt(bad, crcOff); err != nil {
		t.Fatalf("corrupt CRC: %v", err)
	}
	f.Close()

	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen torn: %v", err)
	}
	defer j2.Close()
	if j2.Head() != expectedHeadAfterTear {
		t.Errorf("torn-recovery head = %d; want %d (last good record only)", j2.Head(), expectedHeadAfterTear)
	}
}

// TestTornTailIteratesCleanly is the regression for the OOM crash-loop:
// after an unclean exit leaves a torn trailing record (publish word set,
// CRC bad), recovery must truncate the torn tail so iteration reads the
// valid prefix and stops at clean EOF/Pending instead of surfacing a
// fatal CRC mismatch.
func TestTornTailIteratesCleanly(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const good = 5
	var lastOff Offset
	for i := 0; i < good; i++ {
		off, _, err := j.Append(KindLocalDML, uint64(i), 0, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		lastOff = off
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the last record's CRC: publish word stays set so the
	// record still "looks" published, but verification fails — exactly
	// the torn trailing append an OOM/power-loss mid-write produces.
	corruptRecordCRC(t, segmentPath(dir, lastOff.seg()), lastOff, 1)

	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen torn: %v", err)
	}
	defer j2.Close()
	if j2.Head() != lastOff {
		t.Errorf("head after torn recovery = %d; want %d", j2.Head(), lastOff)
	}

	it := j2.Iterate(0)
	count := 0
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, ErrPending) {
			break
		}
		if err != nil {
			t.Fatalf("Next returned %v; torn tail must not surface as an error", err)
		}
		if rec.HLC != uint64(count) {
			t.Errorf("rec %d hlc = %d; want %d", count, rec.HLC, count)
		}
		count++
	}
	if count != good-1 {
		t.Errorf("iterated %d records; want %d (valid prefix, torn tail dropped)", count, good-1)
	}

	// The torn tail is gone on disk too: a fresh appender continues from
	// the recovered head with no phantom record in between.
	_, seq, err := j2.Append(KindLocalDML, 99, 0, []byte("post"))
	if err != nil {
		t.Fatalf("post-recovery Append: %v", err)
	}
	if seq != good { // good-1 survived + this new one reuses the torn slot's seq space
		t.Errorf("post-recovery seq = %d; want %d", seq, good)
	}
}

// TestMidJournalCorruptionStillErrors guards the other half: a bad
// record with a VALID record after it is real corruption, not a torn
// append, and recovery must refuse to silently drop it.
func TestMidJournalCorruptionStillErrors(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const N = 5
	offs := make([]Offset, N)
	for i := 0; i < N; i++ {
		off, _, err := j.Append(KindLocalDML, uint64(i), 0, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		offs[i] = off
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt a MIDDLE record's CRC; records after it remain valid.
	corruptRecordCRC(t, segmentPath(dir, offs[2].seg()), offs[2], 1)

	if _, err := Open(dir, testSegmentSize, SyncOff); err == nil {
		t.Fatal("Open accepted mid-journal corruption; want error")
	} else if !strings.Contains(err.Error(), "mid-journal corruption") {
		t.Fatalf("Open err = %v; want mid-journal corruption", err)
	}
}

// corruptRecordCRC overwrites the 4-byte CRC trailer of the record at
// off with 0xff bytes. payloadLen is the record's payload length (the
// trailer sits at off + recordHeaderLen + payloadLen).
func corruptRecordCRC(t *testing.T, path string, off Offset, payloadLen int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer f.Close()
	crcOff := int64(off.byteOff()) + int64(recordHeaderLen) + int64(payloadLen)
	if _, err := f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, crcOff); err != nil {
		t.Fatalf("corrupt CRC: %v", err)
	}
}

func TestMarkAbortedSetsFlagAndPreservesCRC(t *testing.T) {
	j := openTest(t)
	off, _, err := j.Append(KindLocalDML, 1, 1, []byte("abort me"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.MarkAborted(off); err != nil {
		t.Fatalf("MarkAborted: %v", err)
	}

	rec, _, err := j.Iterate(0).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !rec.Aborted() {
		t.Errorf("Aborted() = false after MarkAborted")
	}
	if !bytes.Equal(rec.Payload, []byte("abort me")) {
		t.Errorf("payload after abort = %q; want %q", rec.Payload, "abort me")
	}

	// Idempotent.
	if err := j.MarkAborted(off); err != nil {
		t.Errorf("second MarkAborted: %v", err)
	}
	rec2, _, _ := j.Iterate(0).Next()
	if !rec2.Aborted() {
		t.Errorf("Aborted() lost after second MarkAborted call")
	}
}

func TestMarkAbortedSurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	off, _, err := j.Append(KindLocalDML, 0, 0, []byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := j.MarkAborted(off); err != nil {
		t.Fatalf("MarkAborted: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(dir, testSegmentSize, SyncOff)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	rec, _, err := j2.Iterate(0).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !rec.Aborted() {
		t.Errorf("Aborted flag lost across recovery")
	}
}

// TestConcurrentAppendIterate exercises the producer-iterator memory
// ordering: the iterator goroutine must observe records up to the
// publisher's atomic head store, with payloads intact.
func TestConcurrentAppendIterate(t *testing.T) {
	dir := t.TempDir()
	// 5000 records × ~56 bytes each = ~280 KiB; size the segment to fit.
	j, err := Open(dir, 4*1024*1024, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })
	const N = 5000

	var done atomic.Bool
	var seen atomic.Int64

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		it := j.Iterate(0)
		for !done.Load() || seen.Load() < N {
			rec, _, err := it.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, ErrPending) {
				continue
			}
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			expectSeq := uint64(seen.Load() + 1)
			if rec.Seq != expectSeq {
				t.Errorf("got seq %d; want %d", rec.Seq, expectSeq)
				return
			}
			// payload encodes the seq for redundancy.
			if got := binary.LittleEndian.Uint64(rec.Payload); got != rec.Seq {
				t.Errorf("payload mismatch at seq %d: got %d", rec.Seq, got)
				return
			}
			seen.Add(1)
		}
	}()

	for i := 0; i < N; i++ {
		var p [8]byte
		binary.LittleEndian.PutUint64(p[:], uint64(i+1))
		if _, _, err := j.Append(KindLocalDML, uint64(i), 1, p[:]); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	done.Store(true)
	wg.Wait()
	if seen.Load() != N {
		t.Errorf("iterator saw %d records; want %d", seen.Load(), N)
	}
}

// TestIterateResume confirms an iterator started past the file header
// at a previous Offset() returns only later records.
func TestIterateResume(t *testing.T) {
	j := openTest(t)
	for i := 0; i < 10; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 0, []byte{byte(i)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	it1 := j.Iterate(0)
	for i := 0; i < 5; i++ {
		if _, _, err := it1.Next(); err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
	}
	resumeAt := it1.Offset()

	it2 := j.Iterate(resumeAt)
	for i := 5; i < 10; i++ {
		rec, _, err := it2.Next()
		if err != nil {
			t.Fatalf("resumed Next %d: %v", i, err)
		}
		if rec.HLC != uint64(i) {
			t.Errorf("resumed rec %d hlc = %d; want %d", i, rec.HLC, i)
		}
	}
}

func TestRejectsBadHeader(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, 0)
	if err := os.WriteFile(path, bytes.Repeat([]byte{0xff}, int(testSegmentSize)), 0o600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	if _, err := Open(dir, testSegmentSize, SyncOff); err == nil {
		t.Errorf("Open accepted a file with a corrupt header")
	}
}

// TestCrossSegmentIteration verifies the iterator advances across
// segment boundaries seamlessly.
func TestCrossSegmentIteration(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, fileHeaderSize+1024, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	payload := bytes.Repeat([]byte("x"), 200)
	const N = 30
	for i := 0; i < N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 0, payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if got := len(j.Segments()); got < 2 {
		t.Fatalf("expected at least 2 segments, got %d", got)
	}
	it := j.Iterate(0)
	count := 0
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, ErrPending) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if rec.HLC != uint64(count) {
			t.Errorf("rec %d HLC = %d; want %d", count, rec.HLC, count)
		}
		count++
	}
	if count != N {
		t.Errorf("iterated %d records; want %d", count, N)
	}
}

// TestRecoveryAcrossSegments confirms reopening a multi-segment
// journal restores the head correctly and resumes appending into the
// last segment.
func TestRecoveryAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, fileHeaderSize+1024, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	payload := bytes.Repeat([]byte("x"), 200)
	const N = 20
	for i := 0; i < N; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 0, payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	headBefore := j.Head()
	segsBefore := j.Segments()
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := Open(dir, 0, SyncOff) // size ignored on reopen
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	if !equalSegs(j2.Segments(), segsBefore) {
		t.Errorf("segments after reopen = %v; want %v", j2.Segments(), segsBefore)
	}
	if j2.Head() != headBefore {
		t.Errorf("head after reopen = %d; want %d", j2.Head(), headBefore)
	}
	// Append continues into the last segment without creating a new
	// one (the last segment had room).
	_, seq, err := j2.Append(KindLocalDML, 999, 0, []byte("post"))
	if err != nil {
		t.Fatalf("post-recovery Append: %v", err)
	}
	if seq != uint64(N+1) {
		t.Errorf("post-recovery seq = %d; want %d", seq, N+1)
	}
}

// TestRetainAfterDeletesOldSegments verifies RetainAfter physically
// removes drained segments and the iterator skips forward correctly
// when its start offset points at a removed segment.
func TestRetainAfterDeletesOldSegments(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, fileHeaderSize+1024, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	payload := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < 30; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 0, payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	segs := j.Segments()
	if len(segs) < 3 {
		t.Fatalf("need at least 3 segments to exercise retention; got %d", len(segs))
	}
	// Retain only segments [last-1, last]; drop everything before.
	cutoffSeg := segs[len(segs)-2]
	cutoffOff := makeOffset(cutoffSeg, fileHeaderSize)
	if err := j.RetainAfter(cutoffOff); err != nil {
		t.Fatalf("RetainAfter: %v", err)
	}
	if got := j.Segments(); len(got) != 2 {
		t.Errorf("Segments after retain = %v; want 2 segments", got)
	}
	// The deleted segment files are gone from disk.
	for _, n := range segs[:len(segs)-2] {
		if _, err := os.Stat(segmentPath(dir, n)); !os.IsNotExist(err) {
			t.Errorf("segment %d not deleted", n)
		}
	}
	// An iterator opened at the (now removed) earliest segment should
	// auto-advance to the oldest still-resident segment.
	it := j.Iterate(makeOffset(segs[0], fileHeaderSize))
	if _, _, err := it.Next(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, ErrPending) {
		t.Errorf("Next after retain: %v", err)
	}
}

func equalSegs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
