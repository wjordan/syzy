package tcpmesh

import (
	"net"
	"testing"
	"time"
)

// TestOutboundQueue_DropAndCount: with the writer wedged (a pipe
// nobody reads) and a 2-frame queue, further enqueues drop and
// count instead of blocking the caller.
func TestOutboundQueue_DropAndCount(t *testing.T) {
	m, err := New(Config{NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	p := &peer{
		conn:           local,
		nodeID:         2,
		writeTimeout:   50 * time.Millisecond,
		dataQ:          make(chan []byte, 2),
		ctrlQ:          make(chan []byte, ctrlQueueLen),
		maxQueuedBytes: 1 << 20,
		closed:         make(chan struct{}),
	}
	// No writeLoop: the queue can only fill.
	frame, err := encodeFrame(msgData, "t", []byte("payload"))
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	if !p.enqueueData(frame) || !p.enqueueData(frame) {
		t.Fatalf("first two enqueues should fit")
	}
	start := time.Now()
	if p.enqueueData(frame) {
		t.Fatalf("third enqueue should drop (queue full)")
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Fatalf("enqueueData blocked on a full queue")
	}
	if got := p.drops.Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
	if got := p.queuedBytes.Load(); got != 2*int64(len(frame)) {
		t.Fatalf("queuedBytes = %d, want %d", got, 2*len(frame))
	}

	// Byte bound trips independently of the frame bound.
	pb := &peer{
		conn:           local,
		nodeID:         3,
		writeTimeout:   50 * time.Millisecond,
		dataQ:          make(chan []byte, 64),
		ctrlQ:          make(chan []byte, ctrlQueueLen),
		maxQueuedBytes: int64(len(frame)) + 1,
		closed:         make(chan struct{}),
	}
	if !pb.enqueueData(frame) {
		t.Fatalf("first enqueue should fit the byte budget")
	}
	if pb.enqueueData(frame) {
		t.Fatalf("second enqueue should exceed the byte budget and drop")
	}
	if got := pb.dropBytes.Load(); got != uint64(len(frame)) {
		t.Fatalf("dropBytes = %d, want %d", got, len(frame))
	}
}

// TestWriteLoop_ControlPriorityAndRetireOnError: the writer drains
// control frames ahead of queued data, and a write failure retires
// the peer (write deadline trips on a pipe nobody reads).
func TestWriteLoop_ControlPriorityAndRetireOnError(t *testing.T) {
	m, err := New(Config{NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	local, remote := net.Pipe()
	defer remote.Close()
	p := &peer{
		conn:           local,
		nodeID:         2,
		writeTimeout:   500 * time.Millisecond,
		dataQ:          make(chan []byte, 8),
		ctrlQ:          make(chan []byte, ctrlQueueLen),
		maxQueuedBytes: 1 << 20,
		closed:         make(chan struct{}),
	}
	dataFrame, _ := encodeFrame(msgData, "t", []byte("data"))
	ctrlFrame, _ := encodeFrame(msgPing, "", nil)
	// Queue data first, then control; the writer must emit control first.
	if !p.enqueueData(dataFrame) || !p.enqueueControl(ctrlFrame) {
		t.Fatalf("enqueues failed")
	}
	go m.writeLoop(p)

	first := make([]byte, len(ctrlFrame))
	_ = remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readFull(remote, first); err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	if first[4] != msgPing {
		t.Fatalf("first frame msgType = 0x%02x, want PING (control priority)", first[4])
	}

	// Stop reading; the queued data frame write must trip the
	// deadline and retire the peer.
	select {
	case <-p.closed:
	case <-time.After(3 * time.Second):
		t.Fatalf("peer not retired after write deadline")
	}
	if m.Stats().PeerRetirements == 0 {
		t.Fatalf("PeerRetirements = 0, want >= 1")
	}
}

func readFull(c net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		k, err := c.Read(buf[n:])
		n += k
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
