package tcpmesh

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// restartNode models one host on a shared control-plane topic: a mesh, its
// topic channel, and a delivery sink. recv survives across a restart (it is
// owned by the struct, not the mesh) so the test can assert post-restart
// delivery on the same sink that received the pre-restart baseline. A restart
// reuses the same NodeID and listen address, matching a host's persisted
// node id (loadOrCreateNodeID) and fixed cluster listener.
type restartNode struct {
	id    uint64
	sock  string
	topic string
	seeds []string
	recv  chan string

	mu     sync.Mutex
	mux    *Mesh
	cancel context.CancelFunc
}

func (n *restartNode) start(t *testing.T) {
	t.Helper()
	m, err := New(Config{
		Listen:    n.sock,
		Seeds:     n.seeds,
		DialRetry: 25 * time.Millisecond,
		NodeID:    n.id,
	})
	if err != nil {
		t.Fatalf("node %d New: %v", n.id, err)
	}
	ch, err := m.Channel(n.topic)
	if err != nil {
		t.Fatalf("node %d Channel: %v", n.id, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = ch.Subscribe(ctx, func(_ context.Context, p []byte) error {
			select {
			case n.recv <- string(p):
			default: // sink full: the test only needs to observe SOME delivery
			}
			return nil
		})
	}()
	n.mu.Lock()
	n.mux, n.cancel = m, cancel
	n.mu.Unlock()
}

func (n *restartNode) stop() {
	n.mu.Lock()
	cancel, m := n.cancel, n.mux
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if m != nil {
		_ = m.Close()
	}
}

func (n *restartNode) transport() *Mesh {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mux
}

func drainStrings(ch chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func mustRecv(t *testing.T, ch chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case got := <-ch:
			if got == want {
				return
			}
		case <-time.After(25 * time.Millisecond):
		}
	}
	t.Fatalf("did not receive %q within %v", want, timeout)
}

// TestRestartResumesLiveDelivery reproduces the prod control-plane wedge:
// nodes restart (same persisted NodeID + listen addr) during a rolling deploy
// and must resume receiving LIVE topic broadcasts from a stable peer.
// host_capacity-style rows propagate ONLY over the live mux — they are
// source-gated out of the bucket, so neither s3 nor peer-pull catch-up can
// re-deliver them. A membership desync after restart therefore freezes inbound
// silently ("host_capacity froze at restart time"). The restart collides with
// the node's own stale (dead) peer entry on every peer; the NodeID tie-break
// must not strand the live reconnect behind that stale entry.
//
// This is a pure live-delivery test (no broker/S3/peer-pull), so a pass means
// the mesh membership layer self-heals under continuous broadcast and a fail
// localizes the wedge to this layer.
func TestRestartResumesLiveDelivery(t *testing.T) {
	const topic = "host-app"
	dir := t.TempDir()
	sock := func(name string) string { return "unix:" + filepath.Join(dir, name+".sock") }

	// Stable node S plus two nodes X, Y that restart concurrently.
	s := &restartNode{id: 100, sock: sock("s"), topic: topic, recv: make(chan string, 256)}
	x := &restartNode{id: 200, sock: sock("x"), topic: topic, recv: make(chan string, 256)}
	y := &restartNode{id: 300, sock: sock("y"), topic: topic, recv: make(chan string, 256)}
	s.seeds = []string{x.sock, y.sock}
	x.seeds = []string{s.sock, y.sock}
	y.seeds = []string{s.sock, x.sock}

	for _, n := range []*restartNode{s, x, y} {
		n.start(t)
		defer n.stop()
	}

	// Converge: S observes both peers interested in the topic.
	if !waitForReady(s.transport(), 2, 3*time.Second) {
		t.Fatalf("S did not connect to both peers: have %d", peerCount(s.transport()))
	}
	waitMembership(t, s.transport(), x.id, topic, true, 2*time.Second)
	waitMembership(t, s.transport(), y.id, topic, true, 2*time.Second)

	// Baseline: a live broadcast from S reaches X and Y.
	sCh, err := s.transport().Channel(topic)
	if err != nil {
		t.Fatalf("S Channel: %v", err)
	}
	if err := sCh.Broadcast(context.Background(), []byte("baseline")); err != nil {
		t.Fatalf("baseline broadcast: %v", err)
	}
	mustRecv(t, x.recv, "baseline", 2*time.Second)
	mustRecv(t, y.recv, "baseline", 2*time.Second)

	// Rolling restart: X and Y go down and come back with the same identity
	// while S keeps running and holds the now-stale X/Y peer entries.
	var wg sync.WaitGroup
	for _, n := range []*restartNode{x, y} {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.stop()
			n.start(t)
		}()
	}
	wg.Wait()

	// Only accept payloads produced AFTER the restart.
	drainStrings(x.recv)
	drainStrings(y.recv)

	// Heartbeat: S broadcasts continuously (like host_capacity). Each restarted
	// node must receive a post-restart payload well before TCP keepalive (~90s)
	// would reap the stale entry. If membership never re-converges, inbound
	// stays frozen and this fails.
	got := map[*restartNode]bool{x: false, y: false}
	deadline := time.Now().Add(10 * time.Second)
	for seq := 0; time.Now().Before(deadline) && !(got[x] && got[y]); seq++ {
		_ = sCh.Broadcast(context.Background(), []byte(fmt.Sprintf("post-restart-%d", seq)))
		for _, n := range []*restartNode{x, y} {
			select {
			case msg := <-n.recv:
				if strings.HasPrefix(msg, "post-restart-") {
					got[n] = true
				}
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !got[x] {
		t.Errorf("X did not resume live delivery after restart (membership desync); S peers=%d", peerCount(s.transport()))
	}
	if !got[y] {
		t.Errorf("Y did not resume live delivery after restart (membership desync); S peers=%d", peerCount(s.transport()))
	}
}
