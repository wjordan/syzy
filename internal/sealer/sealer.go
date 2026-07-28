// Package sealer uploads per-origin Changeset epochs to object
// storage. It registers as an OnEncoded listener on the producer's
// drainer; for each new Changeset it extracts (origin, seq, hlc) from
// the payload header and buffers the bytes by origin. When a buffer
// reaches the size or age threshold, the sealer encodes a single
// epoch object via internal/epoch and PUTs it via objectstore.Bucket
// using a content-addressed key.
//
// PUTs are idempotent: the key is epoch-<lo_seq>-<hi_seq>.zst, and
// Put with IfAbsent returns ErrPreconditionFailed on duplicate, which
// the sealer treats as success (same content, same key).
//
// One Sealer per node. The sealer owns its own goroutine; OnEncoded
// enqueues into a bounded channel and returns. On full channel
// OnEncoded BLOCKS until the queue accepts (or the sealer goroutine
// has exited). The drainer→sealer chain is back-pressuring: a slow
// object backend slows local commits rather than letting unsealed
// records pile up in the producer journal indefinitely.
//
// Durability contract: the journal-GC gate and peer-mirror reaping
// watermark must never vouch for a seq whose bytes are not durably in
// the bucket. A failed epoch upload RETAINS the buffered records and
// retries on a later tick. Uploads within a per-origin queue happen
// strictly front-to-back, so UploadedSeq (a max) never runs ahead of a
// FAILED upload — but it CAN run ahead of an input-stream hole: if the
// sealer is never fed a seq (e.g. a record consumed before OnEncoded
// was wired), the gap-handling opens a fresh epoch past the hole and
// uploads it, advancing UploadedSeq over a seq that was never sealed.
// ContiguousSealedSeq is the hole-safe watermark and is what the GC
// gate uses; UploadedSeq is only for dedup-skip and status. The final
// stop/ctx-done flush may drop records, and does so loudly and WITHOUT
// advancing either watermark (the GC gate stays closed). Every upload
// runs under its own bounded background context so shutdown
// cancellation cannot abort an in-flight PUT.
package sealer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/epoch"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/objstore"
)

// Config tunes a Sealer.
type Config struct {
	// MaxBytes is the soft upper bound on the buffered uncompressed
	// changeset bytes per origin before the sealer flushes. Default
	// 64 MiB.
	MaxBytes int

	// MaxAge is the maximum buffer age before the sealer flushes,
	// regardless of size. Default 5 minutes.
	MaxAge time.Duration

	// QueueDepth bounds the in-memory queue between OnEncoded and the
	// sealer goroutine. Default 1024.
	QueueDepth int

	// Logf is an optional log sink. Defaults to no-op.
	Logf func(format string, args ...any)
}

func (c Config) withDefaults() Config {
	if c.MaxBytes <= 0 {
		c.MaxBytes = 64 << 20
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 5 * time.Minute
	}
	if c.QueueDepth <= 0 {
		// 16 not 1024: the queue is bounded by item count, not bytes.
		// Each item is one full encoded changeset, which can be many MB
		// during catch-up replay. 1024 × MB-scale items risked OOM long
		// before sealer threw backpressure. 16 caps worst-case in-flight
		// at ~16 × max-changeset and still absorbs steady-state bursts.
		c.QueueDepth = 16
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

// Sealer is the goroutine-driven epoch uploader.
type Sealer struct {
	cfg     Config
	backend objectstore.Bucket

	queue chan changeset
	stop  chan struct{}
	done  chan struct{}

	mu       sync.Mutex
	uploaded map[uint64]uint64 // origin → highest uploaded seq
	// contiguous[origin] is the highest seq S such that every seq in
	// [firstObserved..S] has been uploaded with no gap. Unlike uploaded
	// (a max that the gap-handling below can advance PAST a hole in the
	// input stream), this watermark stops at the first unsealed hole, so
	// the journal-GC gate can gate on "everything contiguously durable"
	// and never unlink source records behind a hole.
	contiguous map[uint64]uint64
}

// changeset is one OnEncoded payload, copied so the sealer can hold
// it past the listener call.
type changeset struct {
	origin uint64
	seq    uint64
	bytes  []byte
}

// New returns a Sealer ready to be started. The caller must call Run
// to start the goroutine.
func New(backend objectstore.Bucket, cfg Config) *Sealer {
	cfg = cfg.withDefaults()
	return &Sealer{
		cfg:        cfg,
		backend:    backend,
		queue:      make(chan changeset, cfg.QueueDepth),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		uploaded:   map[uint64]uint64{},
		contiguous: map[uint64]uint64{},
	}
}

// OnEncoded is the callback to register on the drainer. It blocks
// until the queue accepts (back-pressuring the drainer when the
// sealer is slow) or until the sealer goroutine has exited (in which
// case the payload is dropped — only reachable during shutdown).
func (s *Sealer) OnEncoded(payload []byte) {
	hdr, ok := parseHeader(payload)
	if !ok {
		s.cfg.Logf("sealer: malformed payload (len=%d), dropped", len(payload))
		return
	}
	cp := append([]byte(nil), payload...)
	select {
	case s.queue <- changeset{origin: hdr.origin, seq: hdr.seq, bytes: cp}:
	case <-s.done:
		s.cfg.Logf("sealer: stopped, dropping changeset (origin=%016x seq=%d)", hdr.origin, hdr.seq)
	}
}

// Run drives the sealer until ctx is cancelled or Stop is called. It
// returns nil after a clean drain.
func (s *Sealer) Run(ctx context.Context) error {
	defer close(s.done)

	// pending is one epoch's worth of buffered records. Once a flush is
	// attempted the pending is sealed: its [minSeq,maxSeq] range is
	// frozen so a retry PUTs the identical key (idempotent even against
	// a PUT that landed but reported an error), and later records open
	// a new pending behind it.
	type pending struct {
		records  []epoch.Record
		bytes    int // sum of record body lengths
		minSeq   uint64
		maxSeq   uint64
		openedAt time.Time
		sealed   bool
	}
	// Per-origin FIFO of epochs awaiting upload. Only the tail may be
	// unsealed/appendable. Uploads go strictly front-to-back, stopping
	// at the first failure, so the max-only UploadedSeq watermark can
	// never vouch for a hole.
	queues := map[uint64][]*pending{}
	// Origins whose last upload attempt failed. Retried on the next
	// tick rather than on every arriving record (backoff: size-trips
	// don't hammer a down backend).
	failed := map[uint64]bool{}

	// The tick drives age flushes and failure retries. Follow MaxAge
	// down below 1s so short-MaxAge configurations (tests) aren't
	// quantized to a full second.
	tickEvery := time.Second
	if s.cfg.MaxAge < tickEvery {
		tickEvery = s.cfg.MaxAge
	}
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()

	// flushOrigin uploads queued epochs front-to-back, stopping at the
	// first failure; the failed epoch and everything behind it are
	// retained for retry. includeOpen seals and flushes the open tail
	// too; retries leave an open tail to its own size/age trigger.
	flushOrigin := func(origin uint64, reason string, includeOpen bool) {
		q := queues[origin]
		if includeOpen && len(q) > 0 {
			q[len(q)-1].sealed = true
		}
		delete(failed, origin)
		for len(q) > 0 {
			p := q[0]
			if !p.sealed {
				break // open tail; not part of this flush
			}
			s.cfg.Logf("sealer: flushing origin=%016x lo=%d hi=%d records=%d bytes=%d reason=%s",
				origin, p.minSeq, p.maxSeq, len(p.records), p.bytes, reason)
			if err := s.uploadEpoch(origin, p.records, p.minSeq, p.maxSeq); err != nil {
				failed[origin] = true
				if reason == "stop" || reason == "ctx-done" {
					dropped := 0
					for _, r := range q {
						dropped += len(r.records)
					}
					s.cfg.Logf("sealer: ERROR: final upload origin=%016x lo=%d hi=%d failed: %v; dropping %d records (seqs %d-%d) WITHOUT advancing UploadedSeq — journal GC stays gated",
						origin, p.minSeq, p.maxSeq, err, dropped, p.minSeq, q[len(q)-1].maxSeq)
				} else {
					s.cfg.Logf("sealer: upload origin=%016x lo=%d hi=%d failed, retained for retry: %v",
						origin, p.minSeq, p.maxSeq, err)
				}
				break
			}
			s.recordUpload(origin, p.minSeq, p.maxSeq)
			q = q[1:]
		}
		if len(q) == 0 {
			delete(queues, origin)
			delete(failed, origin)
		} else {
			queues[origin] = q
		}
	}

	flushAll := func(reason string) {
		for origin := range queues {
			flushOrigin(origin, reason, true)
		}
	}

	for {
		select {
		case <-ctx.Done():
			flushAll("ctx-done")
			return ctx.Err()
		case <-s.stop:
			flushAll("stop")
			return nil
		case cs := <-s.queue:
			if cs.seq <= s.UploadedSeq(cs.origin) {
				// Already uploaded; skip.
				continue
			}
			q := queues[cs.origin]
			var tail *pending
			if len(q) > 0 {
				tail = q[len(q)-1]
			}
			if tail != nil && !tail.sealed && cs.seq != tail.maxSeq+1 {
				// Gap or out-of-order: the new record must not share an
				// epoch with a hole. Ship (or seal for retry) what we have.
				flushOrigin(cs.origin, "gap", true)
				if q = queues[cs.origin]; len(q) > 0 {
					tail = q[len(q)-1]
				} else {
					tail = nil
				}
			}
			if tail == nil || tail.sealed || cs.seq != tail.maxSeq+1 {
				tail = &pending{openedAt: time.Now()}
				queues[cs.origin] = append(queues[cs.origin], tail)
			}
			tail.records = append(tail.records, epoch.Record{Seq: cs.seq, Bytes: cs.bytes})
			tail.bytes += len(cs.bytes)
			if len(tail.records) == 1 {
				tail.minSeq = cs.seq
			}
			tail.maxSeq = cs.seq
			if tail.bytes >= s.cfg.MaxBytes && !failed[cs.origin] {
				flushOrigin(cs.origin, "size", true)
			}
		case <-tick.C:
			now := time.Now()
			for origin, q := range queues {
				tail := q[len(q)-1]
				if !tail.sealed && now.Sub(tail.openedAt) >= s.cfg.MaxAge {
					flushOrigin(origin, "age", true)
				} else if failed[origin] {
					flushOrigin(origin, "retry", false)
				}
			}
		}
	}
}

// Stop signals the goroutine to exit cleanly. Returns when Run has
// returned.
func (s *Sealer) Stop() {
	select {
	case <-s.stop:
		return
	default:
		close(s.stop)
	}
	<-s.done
}

// UploadedSeq returns the highest seq known to be uploaded for the
// given origin. This is a max, not a contiguous prefix: the gap-handling
// in Run can upload epochs on both sides of a hole in the input stream,
// so a non-zero UploadedSeq does NOT imply every lower seq is durable.
// Used for the OnEncoded dedup-skip and external status reporting; the
// journal-GC gate must use ContiguousSealedSeq instead.
func (s *Sealer) UploadedSeq(origin uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploaded[origin]
}

// ContiguousSealedSeq returns the highest seq S such that every seq up
// to S (from the first the sealer observed) is durably uploaded with no
// gap. This is the safe watermark for the journal-GC gate: it never
// vouches for a seq beyond an unsealed hole, so source records behind a
// hole are retained rather than truncated.
func (s *Sealer) ContiguousSealedSeq(origin uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contiguous[origin]
}

func (s *Sealer) recordUpload(origin, minSeq, maxSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if maxSeq > s.uploaded[origin] {
		s.uploaded[origin] = maxSeq
	}
	// Advance the contiguous watermark only when this epoch abuts the
	// current prefix (minSeq == watermark+1) or is the first epoch seen
	// for the origin (anchor at its range). An epoch opened after an
	// input gap has minSeq > watermark+1 and leaves the watermark parked
	// at the hole. Epoch bodies are always internally contiguous (Run
	// only appends seq==maxSeq+1), so [minSeq,maxSeq] fills no holes.
	if cur, ok := s.contiguous[origin]; !ok {
		s.contiguous[origin] = maxSeq
	} else if minSeq <= cur+1 && maxSeq > cur {
		s.contiguous[origin] = maxSeq
	}
}

// uploadTimeout bounds a single epoch PUT. Uploads run under a fresh
// background context so a cancelled run ctx (shutdown) cannot abort an
// in-flight upload.
const uploadTimeout = 30 * time.Second

func (s *Sealer) uploadEpoch(origin uint64, records []epoch.Record, lo, hi uint64) error {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc, err := epoch.NewEncoder(&buf)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := enc.Append(r); err != nil {
			return err
		}
	}
	if err := enc.Close(); err != nil {
		return err
	}
	key := objstore.EpochKey(layout.OriginHex(crdt.Origin(origin)), lo, hi)
	body := buf.Bytes()
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()
	_, err = s.backend.Put(ctx, key, bytes.NewReader(body), int64(len(body)), objectstore.IfAbsent())
	if err == nil {
		return nil
	}
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		// Same key already present; treat as idempotent success.
		s.cfg.Logf("sealer: epoch %s already exists; treating as uploaded", key)
		return nil
	}
	return fmt.Errorf("sealer: PUT %s: %w", key, err)
}

// header is the parsed prefix of a Changeset payload.
type header struct {
	origin uint64
	seq    uint64
}

// parseHeader extracts (origin, seq) from the canonical Changeset
// wire format defined in crdt/codec.go: 1 byte version, then
// 8-byte big-endian origin/seq/hlc. The 25-byte minimum spans through
// the hlc field even though the sealer doesn't consume it — the wire
// payload still carries it and downstream replays need it intact.
func parseHeader(payload []byte) (header, bool) {
	if len(payload) < 25 {
		return header{}, false
	}
	return header{
		origin: binary.BigEndian.Uint64(payload[1:9]),
		seq:    binary.BigEndian.Uint64(payload[9:17]),
	}, true
}
