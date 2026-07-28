// S3 backend: every node speaks directly to an S3-compatible bucket
// using conditional PutObject (`If-None-Match: *`) as the CAS primitive.
// No leader, no failover, no discovery — the bucket is the consensus.
//
// Layout: one object per event under <prefix>/events/<seq:016x>.bin.
// Object body is the same wire format the TCP server uses for one event
// (encodeEvent / decodeEvent). Append at parentSeq=N PUTs key N+1 with
// If-None-Match:*; ErrPreconditionFailed → ErrHeadMoved. Read fetches
// exact sequence keys from N+1 and stops at the first missing key. Head
// LISTs the highest key.
//
// Provider requirement: conditional PutObject support. AWS S3, R2,
// MinIO ≥ recent, Tigris all qualify. Document the requirement at the
// daemon flag.

package schemalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wjordan/objectstore"
)

// Catch-up read resilience. A node that joins (or restarts) reads the durable
// event log to advance its schema to head. A single transient GET failure there
// — a slow, high-latency link returning a 408, a dropped connection, a 5xx — must
// NOT abort catch-up, or the node's schema stays frozen behind head while its
// data broker keeps replicating rows that reference the un-applied schema, which
// it then drops forever (a silent, unrecoverable divergence). So getEvent retries
// transient errors with a bounded, backed-off budget and a fresh per-attempt
// deadline, stopping only on a missing object (normal end-of-log) or a cancelled
// parent context (genuine shutdown).
const (
	defaultGetAttempts = 6
	defaultGetTimeout  = 30 * time.Second
	defaultGetBackoff  = 200 * time.Millisecond
)

// S3Config configures the S3 backend. Endpoint is the bucket URL
// (e.g. https://my-bucket.s3.us-east-1.amazonaws.com or
// http://localhost:9000/my-bucket). Credentials come from the runtime
// (AWSConfig fields, or the AWS SDK default credential chain).
type S3Config struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string

	// Prefix isolates one cluster's events within a bucket. Must be
	// non-empty; trailing "/" is normalized.
	Prefix string
}

// S3 implements Log backed by an S3-compatible bucket via the
// shared objectstore.Bucket abstraction.
type S3 struct {
	backend objectstore.Bucket

	// Catch-up read retry budget (zero => the package defaults). Overridable
	// for tests; see getEvent.
	getAttempts int
	getTimeout  time.Duration
	getBackoff  time.Duration
}

func (s *S3) attempts() int {
	if s.getAttempts > 0 {
		return s.getAttempts
	}
	return defaultGetAttempts
}

func (s *S3) readTimeout() time.Duration {
	if s.getTimeout > 0 {
		return s.getTimeout
	}
	return defaultGetTimeout
}

func (s *S3) readBackoff() time.Duration {
	if s.getBackoff > 0 {
		return s.getBackoff
	}
	return defaultGetBackoff
}

// OpenS3 validates cfg and returns an S3 schema log backed by an
// AWSBackend. Parses the bucket name out of cfg.Endpoint to satisfy
// AWSConfig. Does not touch the network.
func OpenS3(cfg S3Config) (*S3, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("schemalog/s3: Endpoint required")
	}
	if cfg.Region == "" {
		return nil, errors.New("schemalog/s3: Region required")
	}
	if cfg.Prefix == "" {
		return nil, errors.New("schemalog/s3: Prefix required (use one prefix per cluster)")
	}
	bucket, baseEndpoint, pathStyle, err := parseBucketEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("schemalog/s3: parse endpoint: %w", err)
	}

	awsCfg := objectstore.S3Config{
		Bucket:       bucket,
		Prefix:       strings.Trim(cfg.Prefix, "/"),
		Region:       cfg.Region,
		AccessKey:    cfg.AccessKey,
		SecretKey:    cfg.SecretKey,
		EndpointURL:  baseEndpoint,
		UsePathStyle: pathStyle,
	}
	be, err := objectstore.OpenS3(context.Background(), awsCfg)
	if err != nil {
		return nil, fmt.Errorf("schemalog/s3: backend: %w", err)
	}
	return &S3{backend: be}, nil
}

// NewS3WithBackend constructs an S3 schema log over an arbitrary
// Backend. Useful for tests (FileBackend) and for sharing one
// backend across the schema log + sealer + snapshot pipeline.
func NewS3WithBackend(backend objectstore.Bucket) *S3 {
	return &S3{backend: backend}
}

// Close is a no-op; included to satisfy the close-on-shutdown idiom
// used by the daemon's openSchemaLog helper.
func (s *S3) Close() error { return nil }

// keyFor returns the object key (relative to the backend prefix) for
// a given schema_seq.
func (s *S3) keyFor(seq uint64) string {
	return fmt.Sprintf("events/%016x.bin", seq)
}

func (s *S3) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	next := parentSeq + 1
	ev := Event{SchemaSeq: next, ParentSeq: parentSeq, CatalogOp: op, RawSQL: raw}
	body := make([]byte, eventEncodedSize(ev))
	encodeEvent(body, ev)

	_, err := s.backend.Put(ctx, s.keyFor(next), bytes.NewReader(body), int64(len(body)), objectstore.IfAbsent())
	if err != nil {
		if errors.Is(err, objectstore.ErrPreconditionFailed) {
			return 0, ErrHeadMoved
		}
		return 0, fmt.Errorf("schemalog/s3: Append: %w", err)
	}
	return next, nil
}

func (s *S3) Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error) {
	if limit <= 0 {
		return nil, nil
	}
	out := make([]Event, 0, min(limit, 64))
	// seq != 0 stops on uint64 wrap: when fromSeq is ^uint64(0) the
	// loop never enters, and once we process seq == ^uint64(0) the
	// post-increment exits cleanly instead of restarting at key 0.
	for seq := fromSeq + 1; seq != 0 && len(out) < limit; seq++ {
		key := s.keyFor(seq)
		ev, err := s.getEvent(ctx, key)
		if err != nil {
			if errors.Is(err, objectstore.ErrNotFound) {
				return out, nil
			}
			return nil, err
		}
		if ev.SchemaSeq != seq {
			return nil, fmt.Errorf("schemalog/s3: %s has schema_seq=%d", key, ev.SchemaSeq)
		}
		out = append(out, ev)
	}
	return out, nil
}

func (s *S3) Head(ctx context.Context) (uint64, error) {
	// LIST in lexicographic order; the largest key is the head. For
	// realistic DDL volumes (tens to hundreds of events) one LIST page
	// covers everything; we still walk pagination correctly so it stays
	// correct under unexpected growth.
	var (
		head       uint64
		startAfter string
	)
	for {
		page, err := s.backend.List(ctx, "events/", startAfter)
		if err != nil {
			return 0, fmt.Errorf("schemalog/s3: list: %w", err)
		}
		if len(page) == 0 {
			return head, nil
		}
		for _, o := range page {
			seq, ok := seqFromKey(o.Key)
			if !ok {
				continue
			}
			if seq > head {
				head = seq
			}
		}
		// If the page returned fewer than the typical max, we're done.
		// Otherwise paginate from the last key.
		if len(page) < 1000 {
			return head, nil
		}
		startAfter = page[len(page)-1].Key
	}
}

// getEvent fetches and decodes one event, retrying transient backend failures
// (see the catch-up-resilience note above). It stops immediately on ErrNotFound
// (the normal end-of-log signal Read relies on) or a cancelled parent context,
// and after the attempt budget surfaces the last error.
func (s *S3) getEvent(ctx context.Context, key string) (Event, error) {
	attempts := s.attempts()
	backoff := s.readBackoff()
	var lastErr error
	for attempt := 1; ; attempt++ {
		ev, err := s.getEventOnce(ctx, key)
		if err == nil {
			return ev, nil
		}
		// Missing object: the end of the log, not a failure — surface as-is.
		if errors.Is(err, objectstore.ErrNotFound) {
			return Event{}, err
		}
		// A done parent context is a real shutdown/cancel, not a backend
		// hiccup: stop instead of burning the remaining attempts.
		if ctx.Err() != nil {
			return Event{}, err
		}
		lastErr = err
		if attempt >= attempts {
			return Event{}, fmt.Errorf("schemalog/s3: get %s: after %d attempts: %w", key, attempts, lastErr)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (s *S3) getEventOnce(ctx context.Context, key string) (Event, error) {
	// A fresh per-attempt deadline bounds a single slow GET so it can't stall
	// catch-up indefinitely, and lets a transient server timeout clear on retry.
	getCtx, cancel := context.WithTimeout(ctx, s.readTimeout())
	defer cancel()
	rc, _, err := s.backend.Get(getCtx, key)
	if err != nil {
		return Event{}, fmt.Errorf("schemalog/s3: get %s: %w", key, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, maxFrameSize))
	if err != nil {
		return Event{}, fmt.Errorf("schemalog/s3: read event: %w", err)
	}
	ev, _, err := decodeEvent(body, 0)
	return ev, err
}

// seqFromKey parses "events/<016x>.bin" → seq. Returns false on any
// malformed key (defensive against unrelated objects).
func seqFromKey(key string) (uint64, bool) {
	const want = "events/"
	if !strings.HasPrefix(key, want) {
		return 0, false
	}
	rest := key[len(want):]
	if !strings.HasSuffix(rest, ".bin") {
		return 0, false
	}
	rest = rest[:len(rest)-len(".bin")]
	if len(rest) != 16 {
		return 0, false
	}
	v, err := strconv.ParseUint(rest, 16, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// parseBucketEndpoint extracts (bucket, baseEndpoint, pathStyle) from
// a full URL. Recognized shapes:
//
//   - virtual-hosted: https://<bucket>.s3.<region>.amazonaws.com
//     → bucket=<bucket>, baseEndpoint="" (SDK default), pathStyle=false
//   - virtual-hosted bucket-only: https://<bucket>.s3.amazonaws.com
//     → bucket=<bucket>, baseEndpoint="", pathStyle=false
//   - path-style: http://host:port/<bucket>
//     → bucket=<bucket>, baseEndpoint="http://host:port", pathStyle=true
func parseBucketEndpoint(raw string) (bucket, baseEndpoint string, pathStyle bool, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", false, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", false, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	host := u.Host
	// AWS virtual-hosted style: <bucket>.s3.<region>.amazonaws.com or
	// <bucket>.s3.amazonaws.com.
	if strings.Contains(host, ".s3.") || strings.HasSuffix(host, ".s3.amazonaws.com") {
		dot := strings.Index(host, ".s3.")
		if dot > 0 {
			return host[:dot], "", false, nil
		}
	}
	if strings.HasSuffix(host, ".amazonaws.com") {
		// Bare s3.amazonaws.com or s3.<region>.amazonaws.com with bucket
		// in the path.
		bucket, _ = splitFirst(strings.TrimPrefix(u.Path, "/"))
		if bucket == "" {
			return "", "", false, errors.New("expected bucket in path for AWS endpoint")
		}
		return bucket, u.Scheme + "://" + host, true, nil
	}
	// Path-style on a custom endpoint.
	bucket, _ = splitFirst(strings.TrimPrefix(u.Path, "/"))
	if bucket == "" {
		return "", "", false, errors.New("expected bucket in URL path (path-style)")
	}
	return bucket, u.Scheme + "://" + host, true, nil
}

func splitFirst(s string) (head, tail string) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}
