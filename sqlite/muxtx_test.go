package sqlite

import (
	"github.com/wjordan/syzy/tcpmesh"
)

// TestTx is a single-topic transport for tests: one tcpmesh.Mesh
// carrying one DefaultTopic channel — the shape production daemons
// run. Addr, PeerAddrs, and Close operate on the whole transport;
// everything else promotes from the channel.
type TestTx struct {
	*tcpmesh.Channel
	m *tcpmesh.Mesh
}

func (tt *TestTx) Addr() string        { return tt.m.Addr() }
func (tt *TestTx) PeerAddrs() []string { return tt.m.PeerAddrs() }
func (tt *TestTx) Close() error        { return tt.m.Close() }

// NewTestTx builds a TestTx from a mesh config. A config with no
// listener gets a random loopback port, so capabilities that need a
// reachable endpoint (unique RPC, clone bundles, peer catchup) work
// out of the box even for seed-only consumers.
func NewTestTx(cfg tcpmesh.Config) (*TestTx, error) {
	if cfg.Listen == "" && cfg.Listener == nil {
		cfg.Listen = "127.0.0.1:0"
	}
	m, err := tcpmesh.New(cfg)
	if err != nil {
		return nil, err
	}
	ch, err := m.Channel(DefaultTopic)
	if err != nil {
		m.Close()
		return nil, err
	}
	return &TestTx{Channel: ch, m: m}, nil
}
