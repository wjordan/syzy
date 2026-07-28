package unique

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wjordan/objectstore"
)

// ErrLeaseHeld is returned by Acquire when a different owner holds an
// unexpired lease. The caller is not the leaseholder and should route
// reservations to LeaseRecord.Addr instead.
var ErrLeaseHeld = errors.New("unique: lease held by another owner")

// ErrFenced is returned by Renew/Release when the lease has moved to a
// newer generation — this owner has been fenced and must stop acting as
// leaseholder. A generation change is the durable fencing signal.
var ErrFenced = errors.New("unique: lease fenced (generation moved)")

// LeaseRecord is the single-writer lease for the coordinated-uniqueness
// leaseholder, stored as one JSON object mutated by ETag-CAS. Generation is
// the monotonic fencing token, bumped on every acquisition; a successor may
// take over once ExpiresAtUS lies in the past. ExpiresAtUS == 0 means the
// lease was cleanly relinquished.
type LeaseRecord struct {
	Owner       string `json:"owner"`       // leaseholder node identity
	Generation  uint64 `json:"generation"`  // monotonic fencing token
	ExpiresAtUS int64  `json:"expiresAtUS"` // wall-clock expiry (UTC µs); 0 = relinquished
	Addr        string `json:"addr"`        // leaseholder reservation-RPC address
}

// held reports whether the record names a live lease at nowUS.
func (r LeaseRecord) held(nowUS int64) bool {
	return r.Owner != "" && r.ExpiresAtUS > nowUS
}

// LeaseStore reads and CAS-mutates the lease object in a bucket.
type LeaseStore struct {
	bucket objectstore.Bucket
	key    string
}

// OpenLease binds a LeaseStore to the lease object at key in bucket.
func OpenLease(bucket objectstore.Bucket, key string) *LeaseStore {
	return &LeaseStore{bucket: bucket, key: key}
}

// Read returns the current lease and its ETag. A missing object reads as
// the zero LeaseRecord with an empty ETag (callers CAS with IfAbsent).
//
// The read is strongly consistent. The lease is a coordination object: every
// caller acts on the leader's true state — the leaseholder's acquire/renew/
// release CAS, and the client's routing decision (held? where?). An ordinary
// Get on a Tigris global bucket is served from a REGIONAL replica that can lag
// the global leader by many seconds and show a live, freshly-renewed lease as
// expired; the client then returns ErrUnavailable for a perfectly healthy
// lease — the spurious reservation failures seen in production. Pairing the
// read with the consistent CAS write makes the whole read-modify-write
// linearizable; mirrors the objstore HEAD coordination read.
func (s *LeaseStore) Read(ctx context.Context) (LeaseRecord, string, error) {
	body, etag, err := s.bucket.Get(objectstore.WithConsistentRead(ctx), s.key)
	if errors.Is(err, objectstore.ErrNotFound) {
		return LeaseRecord{}, "", nil
	}
	if err != nil {
		return LeaseRecord{}, "", fmt.Errorf("unique: read lease: %w", err)
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return LeaseRecord{}, "", fmt.Errorf("unique: read lease body: %w", err)
	}
	var rec LeaseRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return LeaseRecord{}, "", fmt.Errorf("unique: decode lease: %w", err)
	}
	return rec, etag, nil
}

// Acquire takes the lease for owner at RPC address addr, valid for ttlUS
// microseconds from nowUS. It succeeds only when the lease is empty,
// relinquished, expired, or already held by owner; the new record bumps
// Generation (the fence). Returns ErrLeaseHeld when a different owner
// holds a live lease, or objectstore.ErrPreconditionFailed when a
// concurrent writer won the CAS (the caller re-reads and retries).
func (s *LeaseStore) Acquire(ctx context.Context, owner, addr string, nowUS, ttlUS int64) (LeaseRecord, string, error) {
	cur, etag, err := s.Read(ctx)
	if err != nil {
		return LeaseRecord{}, "", err
	}
	if cur.held(nowUS) && cur.Owner != owner {
		return cur, etag, ErrLeaseHeld
	}
	next := LeaseRecord{
		Owner:       owner,
		Generation:  cur.Generation + 1,
		ExpiresAtUS: nowUS + ttlUS,
		Addr:        addr,
	}
	newEtag, err := s.cas(ctx, next, etag)
	if err != nil {
		return LeaseRecord{}, "", err
	}
	return next, newEtag, nil
}

// Renew extends rec's expiry, holding the same Generation. etag is the
// holder's last-observed ETag. Returns ErrFenced if the lease has moved
// to another owner or a newer generation.
func (s *LeaseStore) Renew(ctx context.Context, rec LeaseRecord, etag string, nowUS, ttlUS int64) (LeaseRecord, string, error) {
	cur, curEtag, err := s.Read(ctx)
	if err != nil {
		return LeaseRecord{}, "", err
	}
	if cur.Owner != rec.Owner || cur.Generation != rec.Generation {
		return LeaseRecord{}, "", ErrFenced
	}
	next := rec
	next.ExpiresAtUS = nowUS + ttlUS
	// Prefer the holder's tracked etag; fall back to the freshly-read one
	// (a renew that raced its own prior write still matches by content).
	useEtag := etag
	if useEtag == "" {
		useEtag = curEtag
	}
	newEtag, err := s.cas(ctx, next, useEtag)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return LeaseRecord{}, "", ErrFenced
	}
	if err != nil {
		return LeaseRecord{}, "", err
	}
	return next, newEtag, nil
}

// Release relinquishes the lease (sets ExpiresAtUS = 0) so a successor
// can take over immediately. A fenced holder's release is a no-op
// success — the lease already moved on.
func (s *LeaseStore) Release(ctx context.Context, rec LeaseRecord, etag string) error {
	cur, curEtag, err := s.Read(ctx)
	if err != nil {
		return err
	}
	if cur.Owner != rec.Owner || cur.Generation != rec.Generation {
		return nil // already fenced; nothing to relinquish
	}
	next := rec
	next.ExpiresAtUS = 0
	useEtag := etag
	if useEtag == "" {
		useEtag = curEtag
	}
	if _, err := s.cas(ctx, next, useEtag); err != nil && !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return err
	}
	return nil
}

// cas writes rec conditional on etag (IfAbsent when etag is empty).
func (s *LeaseStore) cas(ctx context.Context, rec LeaseRecord, etag string) (string, error) {
	raw, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("unique: encode lease: %w", err)
	}
	ifMatch := &etag
	if etag == "" {
		ifMatch = objectstore.IfAbsent()
	}
	newEtag, err := s.bucket.Put(ctx, s.key, bytes.NewReader(raw), int64(len(raw)), ifMatch)
	if err != nil {
		return "", err
	}
	return newEtag, nil
}
