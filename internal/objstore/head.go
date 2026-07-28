package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wjordan/objectstore"
)

// HEADVersion is the schema version of the HEAD object. Bump only for an
// incompatible change; optional JSON fields remain rolling-compatible.
const HEADVersion = 3

// HEAD is the bucket-wide mutable manifest. It carries:
//   - cluster_id beacon (set on first claim, never changes)
//   - publisher lease
//   - app.db baseline pointer (db/0009/...)
//   - metadata.db baseline pointer (metadata/0009/...)
//
// Everything else in the bucket (L0/L1 LTX files, origin epochs,
// schema events) is immutable and discovered
// via LIST. HEAD is therefore the only object that needs CAS
// coordination, and the two baseline pointers are written under one
// CAS so a half-rotated physical baseline can never appear.
type HEAD struct {
	Version      int        `json:"version"`
	ClusterID    string     `json:"cluster_id"`
	Publisher    *Publisher `json:"publisher,omitempty"`
	Baseline     *Baseline  `json:"baseline,omitempty"`      // app.db (db/0009/)
	MetaBaseline *Baseline  `json:"meta_baseline,omitempty"` // metadata.db (metadata/0009/)
}

// Publisher describes the lease held by the current physical-stream
// publisher. ExpiresAtUS is renewed via CAS heartbeat. Non-publisher
// nodes attempt CAS-takeover when ExpiresAtUS lies in the past.
//
// Phase 2 leaves this nil; the implicit publisher is whichever node
// is configured to PublishSnapshot. Phase 3 introduces real lease
// semantics.
type Publisher struct {
	NodeID      string `json:"node_id"`
	Generation  uint64 `json:"generation"`
	ExpiresAtUS int64  `json:"expires_at_us"`
}

// PublisherIdentity is the immutable part of one publisher claim. Lease-scoped
// HEAD updates fence on both fields; NodeID alone does not distinguish a stale
// process from a newer claim by the same node.
type PublisherIdentity struct {
	NodeID     string
	Generation uint64
}

// Matches reports whether p is the exact claim represented by id. Lease expiry
// is deliberately excluded: heartbeats may extend it without changing owner.
func (id PublisherIdentity) Matches(p *Publisher) bool {
	return p != nil && p.NodeID == id.NodeID && p.Generation == id.Generation
}

// ActiveAt reports whether p is the exact claim represented by id and its
// lease remains valid beyond nowUS. Expiry is exclusive: a lease expiring at
// nowUS no longer authorizes HEAD mutations.
func (id PublisherIdentity) ActiveAt(p *Publisher, nowUS int64) bool {
	return id.Matches(p) && p.ExpiresAtUS > nowUS
}

// Baseline describes the LTX snapshot that anchors one stream's
// L0 chain. Restore = download this LTX → apply L0/L1 chain on top.
// Both HEAD.Baseline (app.db) and HEAD.MetaBaseline (metadata.db)
// share this shape, written under one CAS at takeover.
type Baseline struct {
	TXID      uint64  `json:"txid"`
	LTXRef    FileRef `json:"ltx_ref"`
	BuiltAtUS int64   `json:"built_at_us"`
}

// FileRef is an immutable reference to a content-addressed object.
// Sha256 is hex-encoded.
type FileRef struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	Sha256 string `json:"sha256"`
}

// LoadHEAD fetches and decodes HEAD. Returns the parsed HEAD and
// current ETag (for subsequent CAS). ErrNoHEAD if HEAD does not exist.
func LoadHEAD(ctx context.Context, b objectstore.Bucket) (*HEAD, string, error) {
	// HEAD is the lease/publish coordination object: every read feeds a
	// CAS decision, so it must be linearizable. A regional-replica read
	// can lag the leader and drive a doomed CAS (publisher claim-loop
	// churn observed in prod from a trailing region). Pairs the read with
	// the consistent CAS write; non-Tigris backends ignore the hint.
	rc, etag, err := b.Get(objectstore.WithConsistentRead(ctx), HEADKey)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, "", ErrNoHEAD
		}
		return nil, "", fmt.Errorf("objstore: load HEAD: %w", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", fmt.Errorf("objstore: read HEAD: %w", err)
	}
	var h HEAD
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, "", fmt.Errorf("objstore: decode HEAD: %w", err)
	}
	if h.Version != HEADVersion {
		return nil, "", fmt.Errorf("objstore: unsupported HEAD version %d (want %d)", h.Version, HEADVersion)
	}
	return &h, etag, nil
}

// ErrNoHEAD is returned by LoadHEAD when HEAD doesn't exist yet.
var ErrNoHEAD = errors.New("objstore: HEAD does not exist")

// ErrClusterIDMismatch is returned when an existing HEAD's ClusterID
// differs from the publisher's. Catches the misconfiguration of two
// unrelated clusters targeting the same bucket.
var ErrClusterIDMismatch = errors.New("objstore: HEAD cluster_id mismatch")

// ErrPublisherOwnershipLost is returned when a lease-scoped HEAD mutation no
// longer finds the exact, unexpired publisher claim that initiated it.
var ErrPublisherOwnershipLost = errors.New("objstore: publisher ownership lost")

// CASHead writes head with CAS. ifMatch=nil overwrites
// unconditionally; ifMatch=&"" requires HEAD not exist; ifMatch=&etag
// requires the current ETag matches. Returns the new ETag.
func CASHead(ctx context.Context, b objectstore.Bucket, head *HEAD, ifMatch *string) (string, error) {
	if head == nil {
		return "", errors.New("objstore: nil HEAD")
	}
	if head.Version == 0 {
		head.Version = HEADVersion
	}
	body, err := json.MarshalIndent(head, "", "  ")
	if err != nil {
		return "", fmt.Errorf("objstore: encode HEAD: %w", err)
	}
	etag, err := b.Put(ctx, HEADKey, bytes.NewReader(body), int64(len(body)), ifMatch)
	if err != nil {
		return "", fmt.Errorf("objstore: Put HEAD: %w", err)
	}
	return etag, nil
}
