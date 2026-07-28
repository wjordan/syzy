package notify

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/futex"
)

func newFeed(t *testing.T, numSlots uint32) (*Writer, *Reader) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "notify.feed")
	w, err := NewWriter(WriterConfig{Path: path, NumSlots: numSlots})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	r, err := NewReader(ReaderConfig{Path: path})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return w, r
}

func TestFeedPath(t *testing.T) {
	if got, want := FeedPath("/var/lib/app.db"), "/var/lib/app.db-syzy/notify.feed"; got != want {
		t.Fatalf("FeedPath = %q, want %q", got, want)
	}
}

func TestRoundTripSingle(t *testing.T) {
	w, r := newFeed(t, 64)

	want := []Change{{
		Origin: 0xAA,
		Seq:    1,
		Table:  "fs_inode",
		Op:     OpInsert,
		PK:     []byte{0x01, 0x02, 0x03},
	}}
	if err := w.Append(want); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notifs, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(notifs) != 1 {
		t.Fatalf("got %d notifications, want 1", len(notifs))
	}
	n := notifs[0]
	if n.Lossy {
		t.Fatalf("unexpected Lossy=true")
	}
	if n.Origin != 0xAA || n.Seq != 1 {
		t.Errorf("dot = (%x, %d); want (AA, 1)", n.Origin, n.Seq)
	}
	if len(n.Changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(n.Changes))
	}
	c := n.Changes[0]
	if c.Table != "fs_inode" || c.Op != OpInsert {
		t.Errorf("change = (table=%q op=%d); want (fs_inode, %d)", c.Table, c.Op, OpInsert)
	}
	if string(c.PK) != string([]byte{0x01, 0x02, 0x03}) {
		t.Errorf("PK = %x; want 010203", c.PK)
	}
	if c.PKTruncated || c.TableTruncated {
		t.Errorf("unexpected truncation: pk=%v table=%v", c.PKTruncated, c.TableTruncated)
	}
}

func TestGroupingByDot(t *testing.T) {
	w, r := newFeed(t, 64)

	// One Append = one Notification with two changes.
	if err := w.Append([]Change{
		{Origin: 1, Seq: 5, Table: "t1", Op: OpInsert, PK: []byte{1}},
		{Origin: 1, Seq: 5, Table: "t1", Op: OpUpdate, PK: []byte{1}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Second Append = second Notification.
	if err := w.Append([]Change{
		{Origin: 2, Seq: 9, Table: "t2", Op: OpDelete, PK: []byte{2}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := drainAll(t, r, ctx, 2)
	if len(got) != 2 {
		t.Fatalf("got %d notifications, want 2", len(got))
	}
	if got[0].Origin != 1 || got[0].Seq != 5 || len(got[0].Changes) != 2 {
		t.Errorf("notif[0] = %+v; want (origin=1 seq=5 #changes=2)", got[0])
	}
	if got[1].Origin != 2 || got[1].Seq != 9 || len(got[1].Changes) != 1 {
		t.Errorf("notif[1] = %+v; want (origin=2 seq=9 #changes=1)", got[1])
	}
}

func TestTableNameTruncation(t *testing.T) {
	w, r := newFeed(t, 16)

	long := "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd" // 40 chars
	if err := w.Append([]Change{{
		Origin: 1, Seq: 1, Table: long, Op: OpInsert, PK: []byte{1},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notifs, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	c := notifs[0].Changes[0]
	if !c.TableTruncated {
		t.Errorf("TableTruncated = false; want true")
	}
	if len(c.Table) != TableNameMaxBytes {
		t.Errorf("len(Table) = %d; want %d", len(c.Table), TableNameMaxBytes)
	}
	if c.Table != long[:TableNameMaxBytes] {
		t.Errorf("Table = %q; want %q", c.Table, long[:TableNameMaxBytes])
	}
}

func TestPKTruncation(t *testing.T) {
	w, r := newFeed(t, 16)

	long := make([]byte, PKMaxBytes+10)
	for i := range long {
		long[i] = byte(i)
	}
	if err := w.Append([]Change{{
		Origin: 1, Seq: 1, Table: "t1", Op: OpInsert, PK: long,
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notifs, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	c := notifs[0].Changes[0]
	if !c.PKTruncated {
		t.Errorf("PKTruncated = false; want true")
	}
	if len(c.PK) != PKMaxBytes {
		t.Errorf("len(PK) = %d; want %d", len(c.PK), PKMaxBytes)
	}
}

func TestLossyOnRingOverrun(t *testing.T) {
	w, r := newFeed(t, 4) // tiny ring

	// Don't read between appends. Push 10 changes, way past 4 slots.
	for i := 0; i < 10; i++ {
		if err := w.Append([]Change{{
			Origin: 1, Seq: uint64(i + 1), Table: "t1", Op: OpInsert, PK: []byte{byte(i)},
		}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notifs, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(notifs) != 1 || !notifs[0].Lossy {
		t.Fatalf("got %+v; want one Lossy notification", notifs)
	}

	// After Lossy, subsequent Appends are visible normally.
	if err := w.Append([]Change{{
		Origin: 1, Seq: 100, Table: "t1", Op: OpInsert, PK: []byte{0xFF},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	notifs, err = r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(notifs) != 1 || notifs[0].Lossy || notifs[0].Seq != 100 {
		t.Errorf("post-lossy notif = %+v; want non-lossy seq=100", notifs)
	}
}

func TestWriterRestartLossy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "notify.feed")

	w1, err := NewWriter(WriterConfig{Path: path, NumSlots: 16})
	if err != nil {
		t.Fatalf("NewWriter 1: %v", err)
	}
	r, err := NewReader(ReaderConfig{Path: path})
	if err != nil {
		w1.Close()
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { r.Close() })

	if err := w1.Append([]Change{{Origin: 1, Seq: 1, Table: "t1", Op: OpInsert, PK: []byte{1}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notifs, err := r.Read(ctx)
	if err != nil {
		t.Fatalf("Read pre-restart: %v", err)
	}
	if len(notifs) != 1 || notifs[0].Lossy {
		t.Fatalf("pre-restart notif = %+v; want one non-lossy", notifs)
	}
	w1.Close()

	// New writer reuses the file; generation bumps; reader detects.
	w2, err := NewWriter(WriterConfig{Path: path, NumSlots: 16})
	if err != nil {
		t.Fatalf("NewWriter 2: %v", err)
	}
	t.Cleanup(func() { w2.Close() })

	notifs, err = r.Read(ctx)
	if err != nil {
		t.Fatalf("Read post-restart: %v", err)
	}
	if len(notifs) != 1 || !notifs[0].Lossy {
		t.Fatalf("post-restart notif = %+v; want one Lossy", notifs)
	}

	// Subsequent normal events from w2 deliver as usual.
	if err := w2.Append([]Change{{Origin: 1, Seq: 99, Table: "t1", Op: OpInsert, PK: []byte{99}}}); err != nil {
		t.Fatalf("Append after restart: %v", err)
	}
	notifs, err = r.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(notifs) != 1 || notifs[0].Seq != 99 {
		t.Errorf("post-restart event = %+v; want seq=99", notifs)
	}
}

func TestCrossGoroutineWake(t *testing.T) {
	if !futex.Supported {
		t.Skip("futex not supported on this platform")
	}
	w, r := newFeed(t, 64)

	var wg sync.WaitGroup
	wg.Add(1)
	got := make(chan Notification, 1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		notifs, err := r.Read(ctx)
		if err != nil {
			t.Errorf("Read: %v", err)
			return
		}
		got <- notifs[0]
	}()

	// Give the reader a moment to enter futex_wait.
	time.Sleep(50 * time.Millisecond)

	if err := w.Append([]Change{{Origin: 7, Seq: 1, Table: "wake", Op: OpInsert, PK: []byte{7}}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	select {
	case n := <-got:
		if n.Origin != 7 {
			t.Errorf("got origin=%d; want 7", n.Origin)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not wake within 2s after Append")
	}
	wg.Wait()
}

func TestTryReadEmpty(t *testing.T) {
	_, r := newFeed(t, 16)

	notifs, err := r.TryRead()
	if err != nil {
		t.Fatalf("TryRead: %v", err)
	}
	if notifs != nil {
		t.Errorf("got %+v; want nil on empty feed", notifs)
	}
}

func TestTryReadDrains(t *testing.T) {
	w, r := newFeed(t, 16)

	if err := w.Append([]Change{{
		Origin: 3, Seq: 7, Table: "tt", Op: OpInsert, PK: []byte{9},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	notifs, err := r.TryRead()
	if err != nil {
		t.Fatalf("TryRead: %v", err)
	}
	if len(notifs) != 1 || notifs[0].Origin != 3 || notifs[0].Seq != 7 {
		t.Fatalf("got %+v; want one notif (origin=3 seq=7)", notifs)
	}

	// Second TryRead with no new events: nil, nil.
	notifs, err = r.TryRead()
	if err != nil {
		t.Fatalf("TryRead 2: %v", err)
	}
	if notifs != nil {
		t.Errorf("got %+v; want nil after drain", notifs)
	}
}

// drainAll calls Read in a loop until want notifications have been
// collected, ctx expires, or an error occurs.
func drainAll(t *testing.T, r *Reader, ctx context.Context, want int) []Notification {
	t.Helper()
	var got []Notification
	for len(got) < want {
		notifs, err := r.Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, notifs...)
	}
	return got
}

// Interrupt must kick a Read parked in futex_wait so it observes ctx
// cancellation well before readerWakeInterval elapses. Guards the
// join path used when the feed's backing mount is about to go away.
func TestInterruptWakesParkedRead(t *testing.T) {
	_, r := newFeed(t, 64)

	ctx, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() {
		_, err := r.Read(ctx)
		ret <- err
	}()

	// Let Read park in the futex (no events pending).
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	cancel()
	r.Interrupt()

	select {
	case err := <-ret:
		if err != context.Canceled {
			t.Fatalf("Read returned %v; want context.Canceled", err)
		}
	case <-time.After(readerWakeInterval - 100*time.Millisecond):
		t.Fatalf("Read still parked %v after Interrupt", time.Since(start))
	}
}
