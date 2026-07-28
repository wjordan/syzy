package unique

import (
	"context"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// LeaseClient is a Registry that routes Reserve to whichever node
// currently holds the lease. It discovers the leaseholder from the lease
// object (address + generation), caches the connection, and on a
// handover (NotLeader) or dead connection re-resolves and retries once.
// A request that cannot reach a live leaseholder returns ErrUnavailable —
// the CAP cost surfaced as a retryable error, never a silent conflict.
type LeaseClient struct {
	store *LeaseStore
	dial  DialTransport
	nowUS func() int64

	// local, when set, is the leaseholder co-located in this process. When the
	// live lease names it as Owner, Reserve/Release run in-process instead of
	// dialing the published address (which is advertised for remote peers and
	// may be self-unreachable under 1:1 NAT). Set via UseLocalLeaseholder.
	local *Leaseholder

	mu       sync.Mutex
	conn     *rpc.Client
	useLocal bool // cached decision: serve via local (this node holds the lease)
	gen      uint64
	addr     string // leaseholder address the cached conn dials (for diagnostics)
}

// NewLeaseClient returns a Registry backed by the lease at store. It dials
// the leaseholder's published address with a plain TCP dial — correct only
// when the leaseholder publishes a peer-reachable address (it must run with
// a matching ServeTransport). For mesh routing, use NewLeaseClientTransport
// so the dial form matches the leaseholder's published address.
func NewLeaseClient(store *LeaseStore) *LeaseClient {
	return NewLeaseClientTransport(store, loopbackDial{})
}

// NewLeaseClientTransport returns a Registry that dials the current
// leaseholder via dial. dial MUST understand the address form the matching
// ServeTransport publishes into the lease (the mux bundle transport pairs
// its Serve and Dial so a follower reaches the leader over the mesh).
func NewLeaseClientTransport(store *LeaseStore, dial DialTransport) *LeaseClient {
	return &LeaseClient{
		store: store,
		dial:  dial,
		nowUS: func() int64 { return time.Now().UnixMicro() },
	}
}

// UseLocalLeaseholder registers the leaseholder co-located with this client.
// When the live lease names lh as Owner, Reserve serves in-process
// (lh.ReserveLocal) rather than dialing the published address —
// which, being advertised for remote peers, need not be reachable from the
// leader's own host under 1:1 NAT. Call once before first use.
func (c *LeaseClient) UseLocalLeaseholder(lh *Leaseholder) *LeaseClient {
	c.local = lh
	return c
}

// Reserve implements Registry.
func (c *LeaseClient) Reserve(ctx context.Context, claims []Claim) (bool, *Claim, error) {
	if len(claims) == 0 {
		return true, nil, nil
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		local, conn, gen, addr, err := c.connect(ctx)
		if err != nil {
			return false, nil, err // already wraps ErrUnavailable with the cause
		}
		if local != nil {
			ok, conflict, notLeader := local.ReserveLocal(gen, claims)
			if notLeader {
				c.reset()
				lastErr = fmt.Errorf("local leaseholder not serving gen=%d (draining/fenced/handover)", gen)
				continue
			}
			return ok, conflict, nil
		}
		var reply ReserveReply
		if err := call(ctx, conn, "Reserve", ReserveArgs{Gen: gen, Claims: claims}, &reply); err != nil {
			c.reset()
			lastErr = fmt.Errorf("reserve rpc to %q (gen=%d): %v", addr, gen, err)
			continue
		}
		if reply.NotLeader {
			c.reset()
			lastErr = fmt.Errorf("leaseholder %q not serving gen=%d (draining/fenced/handover)", addr, gen)
			continue
		}
		return reply.OK, reply.Conflict, nil
	}
	return false, nil, fmt.Errorf("%w: %v", ErrUnavailable, lastErr)
}

// Release implements Registry as a no-op. The leaseholder derives every
// release by observing the replicated rows: a vacated value leaves its
// derived taken-set at the next maintenance tick and enters the release
// hold from that observation. Signalling it early would only rebase the
// hold on a weaker event (RPC receipt rather than replicated visibility),
// and a lost signal would have to be retried forever — observation needs
// neither. The method exists for in-process backends (Local), where
// immediate freeing is correct because every change is trivially stable.
func (c *LeaseClient) Release(context.Context, []Claim) error { return nil }

// connect returns a cached or freshly-dialed connection to the current
// leaseholder, with its generation and address. Every failure return wraps
// ErrUnavailable (callers treat it as retryable) but carries the specific
// cause — lease read error, no live leaseholder, missing address, or a dial
// failure — and the lease owner/generation, so a producer logging the error
// can tell a transient handover from a routing fault without guessing.
// connect resolves the current leaseholder and caches the decision as
// either "serve in-process" (local != nil, this node holds the lease) or a
// dialed connection to a remote leaseholder. reset clears the cache on a
// handover or transport error so the next call re-resolves. Exactly one of
// (local, conn) is non-nil on a nil-error return.
func (c *LeaseClient) connect(ctx context.Context) (local *Leaseholder, conn *rpc.Client, gen uint64, addr string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.useLocal {
		return c.local, nil, c.gen, "local", nil
	}
	if c.conn != nil {
		return nil, c.conn, c.gen, c.addr, nil
	}
	rec, _, err := c.store.Read(ctx)
	if err != nil {
		return nil, nil, 0, "", fmt.Errorf("%w: lease read: %v", ErrUnavailable, err)
	}
	now := c.nowUS()
	if !rec.held(now) {
		return nil, nil, 0, "", fmt.Errorf("%w: no live leaseholder (owner=%q gen=%d expired_by=%dus)",
			ErrUnavailable, rec.Owner, rec.Generation, now-rec.ExpiresAtUS)
	}
	// Co-located fast path: when this process holds the lease, serve in-process.
	// The published address is advertised for remote peers and may not be
	// self-reachable under 1:1 NAT (no hairpin), so never dial it from the
	// owner's own host.
	if c.local != nil && rec.Owner == c.local.Owner() {
		c.useLocal, c.gen = true, rec.Generation
		return c.local, nil, rec.Generation, "local", nil
	}
	if rec.Addr == "" {
		return nil, nil, 0, "", fmt.Errorf("%w: leaseholder gen=%d published no address", ErrUnavailable, rec.Generation)
	}
	netConn, err := c.dial.Dial(ctx, rec.Addr)
	if err != nil {
		return nil, nil, 0, "", fmt.Errorf("%w: dial leaseholder %q (gen=%d): %v", ErrUnavailable, rec.Addr, rec.Generation, err)
	}
	rc := rpc.NewClient(netConn)
	c.conn, c.gen, c.addr = rc, rec.Generation, rec.Addr
	return nil, rc, rec.Generation, rec.Addr, nil
}

// reset drops the cached connection so the next call re-resolves the
// lease (used after a handover or transport error).
func (c *LeaseClient) reset() {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.addr = ""
	}
	c.useLocal = false
	c.mu.Unlock()
}

// Close drops any cached connection.
func (c *LeaseClient) Close() error {
	c.reset()
	return nil
}

// call invokes a net/rpc method, honoring ctx cancellation (net/rpc's
// synchronous Call does not).
func call(ctx context.Context, conn *rpc.Client, method string, args, reply any) error {
	done := conn.Go(rpcServiceName+"."+method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-done.Done:
		return res.Error
	}
}
