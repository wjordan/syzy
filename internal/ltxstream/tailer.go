// Package ltxstream tails a SQLite WAL and emits LTX files describing
// committed transactions' dirty pages. LTX is encoded with
// HeaderFlagNoChecksum (matching Litestream); per-LTX integrity comes
// from the trailer's FileChecksum, and per-frame integrity from the
// SQLite WAL checksum chain that WALReader validates as it reads.
//
// Concept of operations:
//   - Tailer is a passive consumer of the WAL file; it does not write
//     to app.db, does not hold any SQLite connection, and does not
//     coordinate with the writer.
//   - Each Sync() pass reads all available frames past the last
//     persisted position and emits ONE LTX whose TXID range
//     [MinTXID..MaxTXID] covers every committed transaction the pass
//     observed. Pages are deduped across the batch (latest write per
//     pgno wins), pages above the final commit's db_size are dropped,
//     and frames belonging to an in-progress (uncommitted) transaction
//     at end-of-WAL are excluded — they get picked up by the next pass.
//   - SyncInterval governs cadence strictly: Run only invokes Sync on
//     tick. Callers that need an immediate drain (snapshotter, lease
//     handoff) call Sync directly via the Sync API surface (e.g.
//     publisher.SyncAppStream).
//   - Position state (salt, offset, running checksum, TXID) is held in
//     memory by the Tailer; callers persist it externally
//     (metadata.db.meta.ltx_position) and pass it back on restart.
package ltxstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"

	"github.com/wjordan/syzy/internal/syzylog"
)

// Position is the durable state needed to resume tailing. Persist
// after each successful OnLTX call.
type Position struct {
	Salt1, Salt2     uint32
	FrameN           int    // count of frames consumed past the WAL header
	Offset           int64  // file offset just past the last consumed frame
	Chksum1, Chksum2 uint32 // running WAL checksum at Offset
	TXID             uint64 // bucket-relative TXID of the last emitted LTX (0 = none yet)
}

// IsZero reports whether p is the empty initial position (no frames
// consumed). A zero position triggers a fresh-WAL-header read on the
// next Sync.
func (p Position) IsZero() bool { return p.Offset == 0 && p.Salt1 == 0 && p.Salt2 == 0 }

// OnLTXFunc receives one fully-encoded LTX file. The slice aliases an
// internal buffer that may be reused after the call returns; copy if
// retained. header is the LTX header that was just encoded (TXID
// range, page size, commit).
type OnLTXFunc func(ctx context.Context, header ltx.Header, body []byte) error

// OnRecycleFunc is invoked when Sync detects the WAL was recycled past
// the saved position — i.e. SQLite auto-checkpointed and the next
// writer reset the WAL with fresh salt, leaving our (offset, salt,
// checksum) tuple unrecoverable. The callback's job is to publish a
// fresh baseline LTX for this stream so the chain has a new anchor,
// and return the Position the tailer should resume from (typically a
// zero-Offset Position that primes a fresh-WAL-header read on next
// Sync). Returning an error logs and re-tries on the next tick with
// the existing (broken) position.
type OnRecycleFunc func(ctx context.Context) (Position, error)

// Config tunes a Tailer.
type Config struct {
	WALPath  string
	NextTXID func() uint64 // mints the bucket TXID for the next emitted LTX
	OnLTX    OnLTXFunc
	Logger   *slog.Logger

	// OnRecycle, if non-nil, is invoked when Sync returns
	// PrevFrameMismatchError. If nil, the recycle is unrecoverable and
	// each tick logs the same error.
	OnRecycle OnRecycleFunc

	// SyncInterval is the strict period at which Run invokes Sync;
	// callers that need an immediate drain call Sync directly.
	// Default 1s (matches Litestream's default replication tick).
	SyncInterval time.Duration
}

// Tailer is the WAL→LTX consumer. One per app.db.
type Tailer struct {
	cfg Config

	// mu serializes the position state machine. A Sync includes the WAL
	// read, OnLTX upload callback, and final position advance; letting
	// two callers overlap those phases can duplicate LTX output or race
	// a coordinated checkpoint's SetPosition.
	mu  sync.Mutex
	pos Position

	// successfulSyncs is a lock-independent liveness signal for callers that
	// supervise the tailer. Every public Sync pass that returns nil advances it,
	// including an idle pass with no WAL or no new commits. Coordinated
	// checkpoint drains use syncLocked directly and deliberately do not count.
	successfulSyncs atomic.Uint64

	done chan struct{}
}

// New returns a Tailer in the given starting position. Pass a zero
// Position to start from the beginning of the current WAL.
func New(cfg Config, start Position) *Tailer {
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = syzylog.Default()
	}
	return &Tailer{
		cfg:  cfg,
		pos:  start,
		done: make(chan struct{}),
	}
}

// Position returns the current persisted position. Callers persist
// this externally after each OnLTX completion; the in-memory copy
// here is the cache, not the durability primitive.
func (t *Tailer) Position() Position {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pos
}

// SetPosition replaces the in-memory position. Used by the publisher
// after it issues a coordinated WAL recycle: pass Position{} so the
// next Sync reads the new WAL header from offset 0 with the new salt.
func (t *Tailer) SetPosition(pos Position) {
	t.mu.Lock()
	t.pos = pos
	t.mu.Unlock()
}

// SuccessfulSyncs returns the number of public Sync passes that completed
// successfully. The counter advances for idle passes as well as passes that
// emit LTX, so it reports executor liveness rather than write activity.
func (t *Tailer) SuccessfulSyncs() uint64 { return t.successfulSyncs.Load() }

// Run loops on Sync until ctx is canceled. Sync runs on every tick.
// Returns ctx.Err() on exit.
func (t *Tailer) Run(ctx context.Context) error {
	defer close(t.done)
	tick := time.NewTicker(t.cfg.SyncInterval)
	defer tick.Stop()
	for {
		err := t.Sync(ctx)
		// Skip recovery once the context is canceled. A recycle observed during
		// shutdown can't be rebaselined — OnRecycle's writes would run on the
		// canceled context and fail — and the next open rebaselines anyway, so
		// attempting it here only logs a misleading failure.
		if err != nil && ctx.Err() == nil {
			var pre *PrevFrameMismatchError
			if errors.As(err, &pre) && t.cfg.OnRecycle != nil {
				if pos, rerr := t.cfg.OnRecycle(ctx); rerr != nil {
					t.cfg.Logger.Warn("ltxstream recycle: rebaseline failed", "err", rerr)
				} else {
					t.mu.Lock()
					t.pos = pos
					t.mu.Unlock()
					t.cfg.Logger.Info("ltxstream recycle: rebaselined", "txid", pos.TXID)
				}
			} else {
				t.cfg.Logger.Warn("ltxstream sync", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// Sync runs one tail pass: opens the WAL, reads all available frames
// past the current position, and emits a single LTX whose TXID range
// covers every committed transaction observed in the pass. Pages are
// deduped (latest write per pgno wins), pages above the final commit's
// db_size are dropped, and frames belonging to an in-progress
// transaction at end-of-WAL are excluded — they get picked up next
// pass. A pass that observes zero commits emits no LTX and leaves the
// position unchanged.
func (t *Tailer) Sync(ctx context.Context) error {
	err := func() error {
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.syncLocked(ctx)
	}()
	if err == nil {
		t.successfulSyncs.Add(1)
	}
	return err
}

// Drain performs the same tail pass as Sync without recording a supervisor
// liveness proof. Coordinated checkpointing uses it for its bulk pre-drain;
// checkpoint work must not satisfy the publisher's independent renewal proof.
func (t *Tailer) Drain(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.syncLocked(ctx)
}

// CheckpointUnderLock performs a coordinated WAL recycle atomically with
// respect to a concurrent Sync. The caller must already hold the database's
// writer fence; this method then holds the position mutex across the last-mile
// drain, checkpoint, and position reset so the next Sync reads the fresh WAL
// header. The writer-fence-before-tailer order is required by baseline
// snapshots, which drain this tailer while holding the same fence. Holding t.mu
// across checkpoint and reset prevents the Run-loop Sync from observing the
// recycled WAL against the stale position and rebaselining on every checkpoint.
func (t *Tailer) CheckpointUnderLock(ctx context.Context, checkpoint func() error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// The writer fence is held, so this pass catches every last frame.
	if err := t.syncLocked(ctx); err != nil {
		return err
	}
	if err := checkpoint(); err != nil {
		return err
	}
	t.pos = Position{}
	return nil
}

// syncLocked runs one tail pass; the caller must hold t.mu. Split out of Sync so
// CheckpointUnderLock can drain without releasing the lock between the drain,
// the WAL recycle, and the position reset.
func (t *Tailer) syncLocked(ctx context.Context) error {
	walFile, err := os.Open(t.cfg.WALPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// WAL doesn't exist yet — the database hasn't been written
			// to, or has been checkpoint-truncated to nothing.
			return nil
		}
		return fmt.Errorf("open wal: %w", err)
	}
	defer walFile.Close()

	pos := t.pos

	var r *WALReader
	if pos.IsZero() {
		r, err = NewWALReader(walFile, t.cfg.Logger)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Empty/incomplete WAL header. Wait for next tick.
				return nil
			}
			return fmt.Errorf("new wal reader: %w", err)
		}
		// Adopt the live header's salt; we're starting fresh in this WAL.
		pos.Salt1 = r.Salt1()
		pos.Salt2 = r.Salt2()
	} else {
		r, err = NewWALReaderWithOffset(ctx, walFile, pos.Offset, pos.Salt1, pos.Salt2, t.cfg.Logger)
		if err != nil {
			var pre *PrevFrameMismatchError
			if errors.As(err, &pre) {
				// Salt or checksum drift: WAL was recycled (TRUNCATE
				// checkpoint) past our position. Caller's responsibility
				// to take a fresh baseline; we cannot tail across the
				// gap without losing transactions.
				return fmt.Errorf("ltxstream: wal recycled past saved position; rebaseline required: %w", err)
			}
			return fmt.Errorf("resume wal reader: %w", err)
		}
	}

	pageSize := r.PageSize()
	if pageSize == 0 {
		return nil
	}

	// One staged entry per WAL frame read. Frames between the last
	// commit boundary and EOF belong to an in-progress tx and are
	// truncated off `staged` before encoding.
	type pending struct {
		pgno   uint32
		offset int64
	}
	var staged []pending

	// Snapshot of reader state captured at the most recent commit
	// boundary. We use this — not the post-EOF reader state — to
	// advance pos so we don't include uncommitted frames.
	var (
		commits         int    // number of inner commits observed
		committedFrames int    // len(staged) up through the last commit
		lastCommit      uint32 // db_size in pages from the last commit frame
		lastNextOffset  int64  // r.NextOffset() at the last commit
		lastChksum1     uint32 // r.Checksums() at the last commit
		lastChksum2     uint32
	)

	pageBuf := make([]byte, pageSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pgno, commit, err := r.ReadFrame(ctx, pageBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read frame: %w", err)
		}
		staged = append(staged, pending{pgno: pgno, offset: r.Offset()})
		if commit != 0 {
			commits++
			committedFrames = len(staged)
			lastCommit = commit
			lastNextOffset = r.NextOffset()
			lastChksum1, lastChksum2 = r.Checksums()
		}
	}

	if commits == 0 {
		return nil
	}

	// Build deduped page map across every committed frame in the
	// batch; latest staged offset wins per pgno. Pages above the
	// final commit's db_size are dropped (DB shrunk and they're no
	// longer addressable).
	pageMap := make(map[uint32]int64, committedFrames)
	for _, e := range staged[:committedFrames] {
		pageMap[e.pgno] = e.offset
	}
	for p := range pageMap {
		if p > lastCommit {
			delete(pageMap, p)
		}
	}
	if len(pageMap) == 0 {
		// All committed frames were for pages truncated by the same
		// or a later commit in the batch. Treat as a no-op LTX:
		// advance pos so we don't re-read these frames, but emit
		// nothing.
		pos.FrameN += committedFrames
		pos.Offset = lastNextOffset
		pos.Chksum1, pos.Chksum2 = lastChksum1, lastChksum2
		t.pos = pos
		return nil
	}

	// Allocate a TXID per inner commit so the LTX range
	// [MinTXID..MaxTXID] is contiguous with the bucket counter and
	// identifies how many transactions were folded in.
	firstTXID := t.cfg.NextTXID()
	lastTXID := firstTXID
	for i := 1; i < commits; i++ {
		lastTXID = t.cfg.NextTXID()
	}

	var body bytes.Buffer
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    lastCommit,
		MinTXID:   ltx.TXID(firstTXID),
		MaxTXID:   ltx.TXID(lastTXID),
		Timestamp: time.Now().UnixMilli(),
	}
	if err := EncodeIncremental(ctx, &body, walFile, pageMap, pageSize, lastCommit, firstTXID, lastTXID); err != nil {
		return fmt.Errorf("encode ltx [%d..%d]: %w", firstTXID, lastTXID, err)
	}
	if err := t.cfg.OnLTX(ctx, hdr, body.Bytes()); err != nil {
		return fmt.Errorf("onLTX [%d..%d]: %w", firstTXID, lastTXID, err)
	}

	// Advance position only after OnLTX accepted; if it failed, the
	// next Sync re-reads the same WAL range and ships a fresh LTX
	// under new TXIDs (the original TXID range is leaked from the
	// counter; orphan-on-S3 tolerated since restore walks LTX in
	// order and ignores ranges outside the active baseline).
	pos.FrameN += committedFrames
	pos.Offset = lastNextOffset
	pos.Chksum1, pos.Chksum2 = lastChksum1, lastChksum2
	pos.TXID = lastTXID

	t.pos = pos
	return nil
}
