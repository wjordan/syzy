package tcpmesh

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestLoopback_DeliverDropsCounted overflows a channel's deliver buffer (no
// Subscribe consumer drains it) and checks that excess frames are counted as
// drops at both the transport and channel level, with byte volume accompanying
// each drop. Per-frame logging of these drops was removed in favour of
// dropReportLoop's coalesced summary; the counters remain the source of truth.
func TestLoopback_DeliverDropsCounted(t *testing.T) {
	dir := t.TempDir()
	aSock := "unix:" + filepath.Join(dir, "a.sock")

	// A.NodeID=9999 > fake.NodeID=1 so A keeps the inbound connection.
	a, err := New(Config{Listen: aSock, NodeID: 9999})
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	// Open the channel but never Subscribe: the deliver buffer fills and
	// overflow frames are dropped instead of drained.
	ch, err := a.Channel("drop-topic")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}

	conn, err := net.Dial("unix", filepath.Join(dir, "a.sock"))
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := writeHello(conn, Hello{NodeID: 1, Topics: nil}); err != nil {
		t.Fatalf("writeHello: %v", err)
	}
	if _, err := readHello(conn); err != nil {
		t.Fatalf("readHello: %v", err)
	}

	// Send twice the buffer's worth so at least channelDeliverSize frames
	// overflow once the (consumer-less) buffer is full.
	const n = channelDeliverSize * 2
	for i := 0; i < n; i++ {
		if err := writeFrame(conn, msgData, "drop-topic", []byte("payload..")); err != nil {
			t.Fatalf("writeFrame %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := a.Stats()
		if s.DeliverDrops > 0 {
			if s.DeliverDropBytes < s.DeliverDrops {
				t.Fatalf("DeliverDropBytes (%d) < DeliverDrops (%d): each dropped frame must add wire bytes",
					s.DeliverDropBytes, s.DeliverDrops)
			}
			if cs := ch.Stats(); cs.DeliverDrops == 0 || cs.DeliverDropBytes == 0 {
				t.Fatalf("per-channel drop stats not recorded: %+v", cs)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("no deliver drops recorded after %d frames: %+v", n, a.Stats())
}
