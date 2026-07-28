package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/transport"
)

// ErrNoPeerTransport means the node has no way to reach peers, so there
// is nothing it could wait for.
var ErrNoPeerTransport = errors.New("no peer transport configured")

// waitPoll is how often WaitReplicated re-queries peers. Each round is
// one frontier RPC per peer, so this trades a little chatter for a
// tight bound on how long the caller blocks past convergence.
const waitPoll = 25 * time.Millisecond

// WaitReplicated blocks until every write produced on this host has
// been applied by every connected peer.
//
// This is the deterministic answer to "is my write on the other
// replicas yet?" — for a deploy gate, an integration test, or reading a
// write back from a peer without a sleep.
//
// It is anchored on the writer, which is the only side that knows the
// write exists. Waiting on the reader instead ("catch up with what
// peers advertise") looks equivalent and is not: a peer's advertised
// frontier only covers what its daemon has already drained from its
// producers' journals, so a write committed moments ago is durable on
// disk but absent from the frontier, and the reader concludes it is
// caught up when it is not. Hence the order here: adopt every producer
// journal on this host, drain them, and only then compare against
// peers.
//
// A peer that cannot be reached is an error, never a pass — "replicated"
// must not be able to mean "could not ask anyone". Returns
// ErrNoPeerTransport when the node has no peer transport at all; when no
// peer is connected yet, it keeps waiting (peers dial asynchronously)
// until ctx expires.
func (n *Node) WaitReplicated(ctx context.Context) error {
	if n.peerFrontier == nil {
		return ErrNoPeerTransport
	}

	// Pick up producer journals whose origin appeared since the last
	// periodic rescan. A `.load syzy` client that wrote and exited
	// seconds ago is exactly this case, and skipping it would silently
	// narrow what "replicated" covers.
	if err := n.scanSecondaries(ctx, n.appPath, n.log); err != nil {
		return fmt.Errorf("scan producer journals: %w", err)
	}
	if err := n.waitAllDrained(ctx); err != nil {
		return err
	}

	target := n.localProduced()
	if len(target) == 0 {
		return nil
	}

	t := time.NewTicker(waitPoll)
	defer t.Stop()
	for {
		behind, err := n.peersBehind(ctx, target)
		if err != nil {
			return err
		}
		if len(behind) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for peers to apply local writes; %s: %w",
				strings.Join(behind, "; "), ctx.Err())
		case <-t.C:
		}
	}
}

// localProduced returns the highest seq produced on this host per
// locally-owned origin: this node's own, plus every producer journal it
// drains for a co-located extension process.
func (n *Node) localProduced() map[crdt.Origin]crdt.Seq {
	out := make(map[crdt.Origin]crdt.Seq)
	for origin, next := range n.cache.SenderNextSeqAll() {
		if next > 1 {
			out[origin] = next - 1
		}
	}
	return out
}

// peersBehind re-queries every connected peer and reports which ones
// have not yet applied target, empty when all have. An unreachable peer
// is an error rather than an omission: its state is unknown, and
// treating unknown as caught up is how a fetch failure becomes a false
// success.
func (n *Node) peersBehind(ctx context.Context, target map[crdt.Origin]crdt.Seq) ([]string, error) {
	n.peerFrontier.Refresh(ctx)
	obs := n.peerFrontier.Observations()
	if len(obs) == 0 {
		return []string{"no peers connected"}, nil
	}

	var behind []string
	for _, o := range obs {
		if o.State != transport.FrontierOK {
			return nil, fmt.Errorf("cannot confirm local writes replicated: %s", describeUnreachable(o))
		}
		var lag []string
		for origin, want := range target {
			if have := o.Frontier[origin]; have < want {
				lag = append(lag, fmt.Sprintf("%s at %d, want %d",
					layout.OriginHex(origin), have, want))
			}
		}
		if len(lag) > 0 {
			sort.Strings(lag)
			behind = append(behind, fmt.Sprintf("peer %s behind on %s", o.Addr, strings.Join(lag, ", ")))
		}
	}
	sort.Strings(behind)
	return behind, nil
}

func describeUnreachable(o transport.FrontierObservation) string {
	if o.State == transport.FrontierUnknown {
		return fmt.Sprintf("peer %s has not answered a frontier query", o.Addr)
	}
	return fmt.Sprintf("peer %s: %s", o.Addr, o.Err)
}
