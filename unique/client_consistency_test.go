package unique

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
)

// lagBucket models a Tigris global bucket: a strongly-consistent Get
// (WithConsistentRead) sees the leader's current object, an ordinary Get sees a
// stale regional replica. Only Get is exercised by the lease read path.
type lagBucket struct {
	objectstore.Bucket // nil; only Get is called
	fresh, stale       []byte
	etag               string
}

func (b *lagBucket) Get(ctx context.Context, _ string) (io.ReadCloser, string, error) {
	data := b.stale
	if objectstore.IsConsistentRead(ctx) {
		data = b.fresh
	}
	if data == nil {
		return nil, "", objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), b.etag, nil
}

func mustLease(t *testing.T, r LeaseRecord) []byte {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	return raw
}

// TestLeaseStore_ReadIsConsistent pins the production fix: the lease read must
// reflect the leader, not a lagging replica. The replica here shows the lease
// expired while the leader shows it freshly renewed; a non-consistent read
// would (wrongly) report the lease as dead.
func TestLeaseStore_ReadIsConsistent(t *testing.T) {
	const now = int64(1_000_000)
	b := &lagBucket{
		fresh: mustLease(t, LeaseRecord{Owner: "a", Generation: 7, ExpiresAtUS: now + leaseTTL, Addr: "leader:1"}),
		stale: mustLease(t, LeaseRecord{Owner: "a", Generation: 7, ExpiresAtUS: now - 1, Addr: "leader:1"}),
		etag:  "e1",
	}
	rec, _, err := OpenLease(b, "unique/lease").Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !rec.held(now) {
		t.Fatalf("Read returned a stale, expired lease %+v; LeaseStore.Read must read from the leader (consistent read)", rec)
	}
}

type failDial struct{}

func (failDial) Dial(context.Context, string) (net.Conn, error) {
	return nil, errors.New("dial should not be reached when there is no live lease")
}

// TestLeaseClient_UnavailableCarriesCause pins observability: a reservation that
// cannot be served still returns ErrUnavailable (retryable), but now names the
// cause and the lease generation so an operator can tell a transient handover
// from a routing fault. Without this the producer logged a bare
// "reservation backend unavailable" with no way to diagnose it.
func TestLeaseClient_UnavailableCarriesCause(t *testing.T) {
	const now = int64(1_000_000)
	expired := mustLease(t, LeaseRecord{Owner: "a", Generation: 9, ExpiresAtUS: now - 1, Addr: "leader:1"})
	c := NewLeaseClientTransport(OpenLease(&lagBucket{fresh: expired, stale: expired, etag: "e"}, "unique/lease"), failDial{})
	c.nowUS = func() int64 { return now }

	_, _, err := c.Reserve(context.Background(), []Claim{{}})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Reserve err = %v; want errors.Is ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "gen=9") || !strings.Contains(err.Error(), "no live leaseholder") {
		t.Fatalf("Reserve err = %q; want the cause + lease generation surfaced", err)
	}
}
