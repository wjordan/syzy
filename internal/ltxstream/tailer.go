// Package ltxstream tails a SQLite WAL and emits LTX files describing
// committed transactions' dirty pages. Per-LTX integrity comes from
// the trailer's FileChecksum, and per-frame integrity from the SQLite
// WAL checksum chain that WALReader validates as it reads. When a
// ChecksumState is installed (seeded by EncodeBaseline), emitted LTX
// additionally carries pre/post-apply database checksums so a restorer
// can verify the materialized database against the chain; without one
// the header carries HeaderFlagNoChecksum.
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
// Sync) plus the checksum state seeded from that baseline (nil keeps
// the current state). Position and state are adopted together under
// the tailer lock: a baseline-seeded state is only sound if no frame
// is consumed between the states it covers and the position it pairs
// with. Returning an error logs and re-tries on the next tick with
// the existing (broken) position.
type OnRecycleFunc func(ctx context.Context) (Position, *ChecksumState, error)

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
	// ck, when set, supplies pre/post-apply checksums for emitted LTX.
	// Seeded from the latest baseline encode; nil until installed (or
	// after a page-size mismatch drops it), in which case LTX is
	// emitted with HeaderFlagNoChecksum.
	ck *ChecksumState

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

// SetChecksumState installs the checksum state emitted LTX chains
// from. Only sound BEFORE the tailer's first Sync: a baseline-seeded
// state covers everything up to its pin, so it must not replace a
// live state after the tailer has consumed frames — commits emitted
// between the pin and the install would vanish from the tracked
// state and every later attestation would be wrong. Mid-life state
// swaps ride the OnRecycle return instead, atomically with the
// position reset.
func (t *Tailer) SetChecksumState(s *ChecksumState) {
	t.mu.Lock()
	t.ck = s
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
				if pos, ck, rerr := t.cfg.OnRecycle(ctx); rerr != nil {
					t.cfg.Logger.Warn("ltxstream recycle: rebaseline failed", "err", rerr)
				} else {
					t.mu.Lock()
					t.pos = pos
					if ck != nil {
						t.ck = ck
					}
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

// CheckpointResult is one PRAGMA wal_checkpoint row. PASSIVE mode never
// resets the wal-index header, so its NLog and NCkpt are the real frame
// counts; a recycling mode (RESTART/TRUNCATE) zeroes mxFrame before the
// pragma reads it back and reports 0/0 unconditionally.
type CheckpointResult struct {
	Busy  bool  // checkpoint could not run or could not finish its backfill
	NLog  int64 // committed frames in the WAL
	NCkpt int64 // frames backfilled into the database
}

// CheckpointHooks are the database operations CheckpointUnderLock composes
// into a coordinated WAL recycle. The embedder supplies them because only it
// owns connections to the tailed database.
type CheckpointHooks struct {
	// Checkpoint runs PRAGMA wal_checkpoint(PASSIVE) and reports SQLite's
	// (busy, log, checkpointed) row. PASSIVE backfills frames without
	// touching the WAL header, so it can never strand unread frames.
	Checkpoint func() (CheckpointResult, error)
	// Recycle brackets the recycle write in one write transaction on the
	// embedder's writer connection: BEGIN IMMEDIATE, validate() (rolling
	// back and returning its error on failure), one minimal committing
	// write, COMMIT. It returns the WAL's total frame count recorded by
	// the connection's wal_hook for that commit (0 when no capture is
	// wired; nothing is adopted then). See CheckpointUnderLock for why
	// the validation must ride inside the commit's own locked
	// transaction.
	Recycle func(validate func() error) (walFrames int64, err error)
}

// ErrCheckpointBusy reports a coordinated checkpoint pass that could not
// recycle the WAL: a reader held off the backfill, or new commits kept
// landing between drain and checkpoint. Nothing was recycled and the tailer
// position is still valid; retry on the next cycle.
var ErrCheckpointBusy = errors.New("ltxstream: checkpoint busy; WAL not recycled")

// CheckpointUnderLock performs a coordinated WAL recycle atomically with
// respect to a concurrent Sync, holding the position mutex across the final
// drain, verification, recycle write, and position reset. Per attempt:
//
//  1. Drain: read and emit every committed frame.
//  2. hooks.Checkpoint (PASSIVE): backfill the WAL, reporting real frame
//     counts.
//  3. Verify NLog == NCkpt == frames drained; otherwise the WAL is not
//     quiesced — retry, then give up until the next cycle. Nothing has
//     been recycled, so nothing can have been lost.
//  4. hooks.Recycle: inside one write transaction — whose write lock
//     freezes appends, restarts, and resets — revalidate that the WAL
//     still holds exactly the drained generation (same salts, no
//     committed frame beyond the drained offset), then commit a minimal
//     write. SQLite restarts a fully-backfilled WAL inside such a commit
//     (walRestartLog: fresh salts, frames from offset 32). The restart
//     rewinds the write position without shrinking the file — the
//     embedder must leave journal_size_limit unset, because commit-tail
//     truncation runs while readers are live and turns any stale
//     wal-index view into zero-filled hole reads; only a
//     wal_checkpoint(TRUNCATE), which holds every read slot exclusively,
//     may shrink the file. Frames beyond the restart carry dead salts
//     and are never re-read. No out-of-band checkpoint can pin the WAL
//     from validation through restart the way this lock does. Stale
//     validation rolls back, then retries (a commit landed behind the
//     drain) or defers (the generation was replaced).
//  5. Prove the transition before replacing the position. The restart is
//     best-effort — a reader holding a non-zero read mark suppresses it
//     and the commit appends — and the commit's frame count settles which
//     happened: reset to the commit's own frames means it restarted the
//     validated generation; grown by them means suppressed. The header
//     must also show exactly one transition (walRestartHdr sets salt1 to
//     exactly old+1), revalidated on the same reader that drains the new
//     generation. A suppressed restart with salts unchanged drains the
//     appended write in place and defers. Anything else — further
//     restarts, or a foreign restart supplying the salt increment a
//     suppressed commit lacked — keeps the position and defers, and the
//     next Sync's salt check escalates to a loud rebaseline. An
//     uncoordinated checkpointer can therefore cost a rebaseline, never
//     a silent gap.
func (t *Tailer) CheckpointUnderLock(ctx context.Context, hooks CheckpointHooks) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	const attempts = 3
	for range attempts {
		if err := t.syncLocked(ctx); err != nil {
			return err
		}
		res, err := hooks.Checkpoint()
		if err != nil {
			return err
		}
		if res.Busy || res.NCkpt != res.NLog {
			return ErrCheckpointBusy
		}
		if res.NLog <= 0 {
			// Empty (or absent) WAL: nothing to recycle.
			return nil
		}
		if int64(t.pos.FrameN) != res.NLog {
			// Commits landed between the drain and the checkpoint's header
			// read. They are backfilled but not yet emitted, so a restart
			// now would strand them: drain and re-verify.
			continue
		}
		frames, err := hooks.Recycle(func() error { return t.validateDrained(ctx) })
		if err != nil {
			if errors.Is(err, ErrCheckpointBusy) {
				continue
			}
			return fmt.Errorf("recycle write: %w", err)
		}
		if frames <= 0 {
			// Embedder wiring bug, not a runtime race: without the commit's
			// frame count no transition can be proven, so never adopt.
			return fmt.Errorf("ltxstream: recycle write reported no wal frame count; wal-frame capture not wired")
		}
		// Sound only because validation rode inside the commit's write
		// lock: the WAL provably held exactly res.NLog drained frames when
		// the commit ran, so fewer frames now means it restarted and more
		// means the restart was suppressed (contract step 5).
		restarted := frames <= res.NLog
		// walRestartHdr sets salt1 to exactly old+1 through the shared
		// wal-index header, so restarts are countable across connections
		// (unlike the header's checkpoint-sequence field — see Salt1).
		salt1, salt2, err := readWALHeader(t.cfg.WALPath, t.cfg.Logger)
		if err != nil {
			return fmt.Errorf("recycle post-check: %w", err)
		}
		switch {
		case salt1 == t.pos.Salt1 && salt2 == t.pos.Salt2:
			// Restart prevented by a reader: the recycle write appended to
			// the tailed generation. Drain it in place and retry on a later
			// cycle; zeroing the position here would re-emit the whole
			// generation and bypass the resume salt check.
			if err := t.syncLocked(ctx); err != nil {
				return err
			}
			return ErrCheckpointBusy
		case restarted && salt1 == t.pos.Salt1+1:
			// The recycle commit restarted the validated generation and no
			// further restart preceded the sample. Adopt through a drain
			// that revalidates the sampled salts on the file it reads, so
			// a later restart cannot slip a generation past the zero
			// position.
			return t.syncFrom(ctx, Position{}, &walGen{salt1: salt1, salt2: salt2})
		default:
			// Unprovable transition (contract step 5). Keep the position;
			// the next Sync surfaces the salt mismatch and demands a
			// rebaseline.
			return fmt.Errorf("ltxstream: unprovable wal transition during recycle (salt1 %08x -> %08x, recycle commit restarted=%t); rebaseline will follow", t.pos.Salt1, salt1, restarted)
		}
	}
	return ErrCheckpointBusy
}

// validateDrained is the revalidation CheckpointUnderLock passes to
// hooks.Recycle (contract step 4). It runs inside the recycle write
// transaction, whose write lock keeps the observation true through the
// commit: the header must still carry the drained generation's salts, and
// no committed frame may follow the drained offset. Checksum-valid frames
// with no commit flag are cache-spill remnants of a rolled-back
// transaction (ROLLBACK rewinds mxFrame, not the file); under the write
// lock no transaction is in flight, so they cannot be a commit's prefix,
// and the recycle write overwrites or truncates them.
func (t *Tailer) validateDrained(ctx context.Context) error {
	f, err := os.Open(t.cfg.WALPath)
	if err != nil {
		return fmt.Errorf("recycle revalidation: %w", err)
	}
	defer f.Close()
	r, err := NewWALReaderWithOffset(ctx, f, t.pos.Offset, t.pos.Salt1, t.pos.Salt2, t.cfg.Logger)
	if err != nil {
		// Salt or checksum drift: the drained generation was replaced
		// after the verify; defer, rebaseline follows.
		return fmt.Errorf("recycle revalidation: %w", err)
	}
	buf := make([]byte, r.PageSize())
	for {
		_, commit, err := r.ReadFrame(ctx, buf)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recycle revalidation: %w", err)
		}
		if commit != 0 {
			// A commit landed behind the drain after the verify.
			// Retryable: drain it and re-verify.
			return fmt.Errorf("recycle revalidation: commit appended behind the drain: %w", ErrCheckpointBusy)
		}
	}
}

// readWALHeader opens the WAL at path and parses its 32-byte header,
// returning the generation salts. Fails on a missing, torn, or
// checksum-invalid header.
func readWALHeader(path string, logger *slog.Logger) (salt1, salt2 uint32, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	r, err := NewWALReader(f, logger)
	if err != nil {
		return 0, 0, err
	}
	return r.Salt1(), r.Salt2(), nil
}

// syncLocked runs one tail pass from the current position; the caller must
// hold t.mu. Split out of Sync so CheckpointUnderLock can drain without
// releasing the lock between the drain, the WAL recycle, and the position
// reset.
func (t *Tailer) syncLocked(ctx context.Context) error {
	return t.syncFrom(ctx, t.pos, nil)
}

// walGen identifies one WAL generation by its header salts.
type walGen struct {
	salt1, salt2 uint32
}

// syncFrom runs one tail pass starting from pos; the caller must hold t.mu.
// expect, when non-nil, requires the live WAL header to match that exact
// generation before any frames are consumed: the caller proved that
// generation fully accounted for, and a different header appearing between
// the caller's sample and this open means further restarts occurred whose
// frames this tailer cannot account for. Validation happens on the same
// opened file the pass drains from, and t.pos is replaced only on success —
// on mismatch (or any error) it is left untouched for the caller's resume
// salt check to escalate.
func (t *Tailer) syncFrom(ctx context.Context, pos Position, expect *walGen) error {
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

	if expect != nil && (r.Salt1() != expect.salt1 || r.Salt2() != expect.salt2) {
		return fmt.Errorf(
			"ltxstream: wal generation %08x-%08x differs from expected %08x-%08x: further restarts occurred; rebaseline will follow",
			r.Salt1(), r.Salt2(), expect.salt1, expect.salt2)
	}

	pageSize := r.PageSize()
	if pageSize == 0 {
		return nil
	}

	// One staged entry per WAL frame read. Frames between the last
	// commit boundary and EOF belong to an in-progress tx and are
	// truncated off `staged` before encoding.
	type pending struct {
		pgno uint32
		data []byte
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
		// Keep the bytes from the checksum-verified read. Rereading this WAL
		// offset after the scan is unsafe: another SQLite connection can
		// recycle the WAL between scan and encode, making the same offset
		// refer to a different generation.
		staged = append(staged, pending{pgno: pgno, data: append([]byte(nil), pageBuf...)})
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

	// Build a deduped page map across every committed frame in the
	// batch; latest verified bytes win per pgno. Pages above the
	// final commit's db_size are dropped (DB shrunk and they're no
	// longer addressable).
	pageMap := make(map[uint32][]byte, committedFrames)
	for _, e := range staged[:committedFrames] {
		pageMap[e.pgno] = e.data
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
		// nothing. The checksum state must still absorb the shrink.
		if t.ck != nil {
			t.ck.Stage(nil, lastCommit).Commit()
		}
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

	if t.ck != nil && t.ck.PageSize() != pageSize {
		// A page-size change means the state was seeded from a
		// different database geometry; its checksums would be wrong.
		// Drop it — emissions degrade to NoChecksum until the next
		// baseline installs a fresh state.
		t.cfg.Logger.Warn("ltxstream: checksum state page size mismatch; dropping state",
			"state_page_size", t.ck.PageSize(), "wal_page_size", pageSize)
		t.ck = nil
	}
	var body bytes.Buffer
	var att StagedChecksums
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    lastCommit,
		MinTXID:   ltx.TXID(firstTXID),
		MaxTXID:   ltx.TXID(lastTXID),
		Timestamp: time.Now().UnixMilli(),
	}
	if t.ck != nil {
		att = t.ck.Stage(pageMap, lastCommit)
		hdr.PreApplyChecksum = att.Pre
	} else {
		hdr.Flags = ltx.HeaderFlagNoChecksum
	}
	if err := EncodeIncremental(ctx, &body, pageMap, hdr, att.Post); err != nil {
		return fmt.Errorf("encode ltx [%d..%d]: %w", firstTXID, lastTXID, err)
	}
	if err := t.cfg.OnLTX(ctx, hdr, body.Bytes()); err != nil {
		return fmt.Errorf("onLTX [%d..%d]: %w", firstTXID, lastTXID, err)
	}
	if t.ck != nil {
		att.Commit()
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
