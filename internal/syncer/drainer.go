// Package syncer moves producer writes from the origin journal into
// replicated metadata. Drainer tails a journal.Journal and hands
// batches, in journal order, to a DrainSink; MetaSink (the production
// sink) decodes each record into a changeset + row_clock updates and
// commits the batch as one metadata transaction. materialize.go builds
// that evidence from raw preupdate touch observations; coordinated.go
// extracts unique-key reserve/release claims from the same buffer.
// SecondaryDrainer runs the same pipeline for origins written by a
// different process (loadable extension writers).
package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/internal/journal"
)

// DrainSink is the integration point between the deferred-drain
// machinery (journal) and the metadata. Implementations are responsible
// for: decoding journal record payloads into changesets + row_clock
// updates, assigning Dot.Seq from sender_next_seq, writing the batched
// metadata transaction (log rows, row_clocks, frontier, meta), and
// persisting the new drained offset.
//
// The Drainer guarantees:
//   - Records are delivered in journal order.
//   - Aborted records (FlagAborted set) are skipped before delivery.
//   - Apply is called exactly once per batch; the returned offset is
//     the drainer's resume point on restart.
//
// The Sink owns ordering of its writes and is the only source of truth
// on what "drained" means — the drainer treats LastDrainedOffset as
// opaque persisted state that survives restarts.
type DrainSink interface {
	// LastDrainedOffset returns the most recently persisted drained
	// offset. Called once during NewDrainer to position the iterator.
	LastDrainedOffset() (journal.Offset, error)

	// Apply persists records to the metadata in one transaction. The
	// returned offset is the new drained offset (typically the
	// iterator's offset just past the last record in records).
	Apply(records []DrainRecord) (journal.Offset, error)
}

// DrainRecord is one record handed to the sink. The Payload borrows
// from the journal's mmap; the sink must copy if it needs to retain
// past Apply.
type DrainRecord struct {
	Offset  journal.Offset // record start offset
	NextOff journal.Offset // offset past this record (= start of next)
	Kind    journal.Kind
	HLC     uint64
	Origin  uint64
	JSeq    uint64 // journal sequence (NOT cluster Dot.Seq)
	// SchemaSeq is the writer's schema stamp at capture time —
	// schema_seq+1 by the producer's epoch convention, 0 for pre-stamp
	// records. The sink decodes the positional payload under the
	// capture-time column layout (MetaSink.captureTable), not the
	// current one.
	SchemaSeq uint32
	Payload   []byte
}

// Drainer reads journal records and feeds them to a sink in batched
// form. One Drainer per producer; runs in its own goroutine.
//
// wal_hook fires after WAL fsync, so any record visible at j.Head() is
// already durable; the drainer treats j.Head() directly as the work
// limit without a separate confirmation step.
type Drainer struct {
	j    *journal.Journal
	sink DrainSink

	drained      atomic.Uint64
	batchMax     int
	pollInterval time.Duration // polling fallback or futex safety timeout
	sharedWake   bool
	waitOff      journal.Offset

	// batch is a scratch slice reused across collectBatch calls. The
	// drainer is single-threaded so no concurrency concern.
	batch []DrainRecord
}

// DrainerOption configures a Drainer.
type DrainerOption func(*Drainer)

// WithBatchMax caps the number of records per Apply call. Default 64.
func WithBatchMax(n int) DrainerOption {
	return func(d *Drainer) {
		if n > 0 {
			d.batchMax = n
		}
	}
}

// WithPollInterval enables the legacy polling fallback on top of
// journal.Notify. When WithSharedWake is set, the same duration is the
// futex safety timeout used to notice missed wakes and context
// cancellation. Default 0 means Notify-only for in-process drainers and
// a conservative journal.WaitAt default for shared-wake drainers.
func WithPollInterval(d time.Duration) DrainerOption {
	return func(dr *Drainer) {
		if d > 0 {
			dr.pollInterval = d
		}
	}
}

// WithSharedWake makes the drainer wait on the journal record publish
// word at its current tail. Used for cross-process extension journals.
func WithSharedWake() DrainerOption {
	return func(dr *Drainer) {
		dr.sharedWake = true
	}
}

// NewDrainer wires up a Drainer. Reads the sink's LastDrainedOffset to
// position the iterator, aligned to the journal's actual record
// geometry — a marker persisted against a previous journal generation
// otherwise wedges or kills the drain loop (see Journal.AlignResume).
func NewDrainer(j *journal.Journal, sink DrainSink, opts ...DrainerOption) (*Drainer, error) {
	off, err := sink.LastDrainedOffset()
	if err != nil {
		return nil, fmt.Errorf("drainer: read last-drained: %w", err)
	}
	off = j.AlignResume(off)
	d := &Drainer{
		j:        j,
		sink:     sink,
		batchMax: 64,
	}
	d.storeDrained(off)
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// DrainedOffset returns the last offset the sink durably committed
// past. Increases monotonically.
func (d *Drainer) DrainedOffset() journal.Offset {
	return journal.Offset(d.drained.Load())
}

func (d *Drainer) storeDrained(off journal.Offset) {
	d.drained.Store(uint64(off))
}

// Run drives the drainer loop until ctx is cancelled. Returns nil on
// clean shutdown.
func (d *Drainer) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		batch, err := d.collectBatch()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if len(batch) == 0 {
			if err := d.waitForAppend(ctx); err != nil {
				return nil
			}
			continue
		}
		newOff, err := d.sink.Apply(batch)
		if err != nil {
			return fmt.Errorf("drainer: apply: %w", err)
		}
		current := d.DrainedOffset()
		if newOff < current {
			return fmt.Errorf("drainer: sink returned regressing offset %d (was %d)", newOff, current)
		}
		d.storeDrained(newOff)
	}
}

// collectBatch reads up to batchMax non-aborted records past d.drained.
// Returns an empty batch if the iterator reaches a zero publish word.
// Records are published only after their bytes and CRC are complete.
//
// The returned slice aliases d.batch and is valid only until the next
// collectBatch call.
//
// If iteration advances through structural records while producing no
// sink batch, drained is advanced to the iterator's current offset so
// consumers waiting on "drained reached observed tail" don't deadlock.
func (d *Drainer) collectBatch() ([]DrainRecord, error) {
	drained := d.DrainedOffset()
	it := d.j.Iterate(drained)
	batch := d.batch[:0]
	progressed := false
	for len(batch) < d.batchMax {
		rec, off, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			d.waitOff = it.Offset()
			if len(batch) == 0 && it.Offset() > drained {
				d.storeDrained(it.Offset())
			}
			break
		}
		if err != nil {
			return nil, err
		}
		next := it.Offset()
		if rec.Kind == journal.KindSeal || rec.Aborted() {
			// Structural and aborted records advance d.drained without
			// being applied. KindSeal is normally hidden by the iterator;
			// keep this branch defensive for manually-constructed records.
			// Bring drained up to next so the next iteration skips it
			// without reading it again — but only do that if the batch
			// is empty; otherwise fold it into the next batch's
			// post-Apply update.
			if len(batch) == 0 {
				drained = next
				d.storeDrained(next)
				progressed = true
				continue
			}
			break
		}
		batch = append(batch, DrainRecord{
			Offset:    off,
			NextOff:   next,
			Kind:      rec.Kind,
			HLC:       rec.HLC,
			Origin:    rec.Origin,
			JSeq:      rec.Seq,
			SchemaSeq: rec.SchemaSeq,
			Payload:   rec.Payload,
		})
	}
	if len(batch) == 0 && progressed {
		d.waitOff = it.Offset()
	}
	d.batch = batch
	return batch, nil
}

func (d *Drainer) waitForAppend(ctx context.Context) error {
	if d.sharedWake && d.waitOff != 0 {
		return d.j.WaitAt(ctx, d.waitOff, d.pollInterval)
	}
	if d.pollInterval == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.j.Notify():
			return nil
		}
	}
	t := time.NewTimer(d.pollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.j.Notify():
		return nil
	case <-t.C:
		// Legacy polling fallback for callers that do not enable
		// shared record-publish wakes.
		d.j.Refresh()
		return nil
	}
}
