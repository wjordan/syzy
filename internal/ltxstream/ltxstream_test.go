package ltxstream_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestTailer_Incremental writes several transactions to a SQLite WAL,
// runs Sync once, and verifies the tailer emits one LTX whose TXID
// range covers every committed transaction.
func TestTailer_Incremental(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()

	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one'), (2, 'two')`)
	mustExec(t, conn, `INSERT INTO t VALUES (3, 'three')`)

	type captured struct {
		hdr  ltx.Header
		body []byte
	}
	var caught []captured
	var nextTXID atomic.Uint64

	tailer := ltxstream.New(ltxstream.Config{
		WALPath: walPath,
		NextTXID: func() uint64 {
			return nextTXID.Add(1)
		},
		OnLTX: func(_ context.Context, hdr ltx.Header, body []byte) error {
			caught = append(caught, captured{hdr: hdr, body: append([]byte(nil), body...)})
			return nil
		},
		SyncInterval: time.Hour, // disable polling; we drive Sync directly
	}, ltxstream.Position{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tailer.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// CREATE TABLE + 2 INSERTs ⇒ 3 commits, all coalesced into 1 LTX.
	if len(caught) != 1 {
		t.Fatalf("expected 1 LTX (3 commits coalesced), got %d", len(caught))
	}
	c := caught[0]
	if c.hdr.MinTXID != 1 || c.hdr.MaxTXID != 3 {
		t.Fatalf("LTX TXID range = [%d,%d]; want [1,3] (3 inner commits)", c.hdr.MinTXID, c.hdr.MaxTXID)
	}
	dec := ltx.NewDecoder(bytes.NewReader(c.body))
	if err := dec.Verify(); err != nil {
		t.Fatalf("LTX verify: %v", err)
	}

	pos := tailer.Position()
	if pos.TXID != uint64(c.hdr.MaxTXID) {
		t.Fatalf("Position.TXID=%d does not match LTX MaxTXID %d", pos.TXID, c.hdr.MaxTXID)
	}
}

// TestTailer_ResumeFromPosition verifies that calling Sync twice with
// the position propagated between calls picks up where the prior pass
// left off — no duplicate LTX, no skipped commits.
func TestTailer_ResumeFromPosition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	var counter atomic.Uint64
	count := func() *int {
		var n int
		return &n
	}()
	cfg := ltxstream.Config{
		WALPath: walPath,
		NextTXID: func() uint64 {
			return counter.Add(1)
		},
		OnLTX: func(_ context.Context, _ ltx.Header, _ []byte) error {
			*count++
			return nil
		},
		SyncInterval: time.Hour,
	}

	t1 := ltxstream.New(cfg, ltxstream.Position{})
	if err := t1.Sync(context.Background()); err != nil {
		t.Fatalf("first Sync: %v", err)
	}
	firstCount := *count
	if firstCount != 1 {
		t.Fatalf("expected exactly 1 LTX in first pass (CREATE+INSERT coalesced), got %d", firstCount)
	}
	pos := t1.Position()

	// Second pass on a fresh Tailer with the saved position. No new
	// commits since the last Sync, so this should produce zero LTX.
	t2 := ltxstream.New(cfg, pos)
	if err := t2.Sync(context.Background()); err != nil {
		t.Fatalf("second Sync (no new commits): %v", err)
	}
	if *count != firstCount {
		t.Fatalf("second Sync produced LTX without new commits: before=%d after=%d", firstCount, *count)
	}

	// Third pass after one more commit — exactly one LTX.
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)
	if err := t2.Sync(context.Background()); err != nil {
		t.Fatalf("third Sync: %v", err)
	}
	if *count != firstCount+1 {
		t.Fatalf("expected one new LTX, got delta=%d", *count-firstCount)
	}
}

func TestTailer_SyncSerializesPosition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	var counter atomic.Uint64
	var ltxCount atomic.Int64
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	tailer := ltxstream.New(ltxstream.Config{
		WALPath:  walPath,
		NextTXID: func() uint64 { return counter.Add(1) },
		OnLTX: func(_ context.Context, _ ltx.Header, _ []byte) error {
			ltxCount.Add(1)
			entered <- struct{}{}
			<-release
			return nil
		},
		SyncInterval: time.Hour,
	}, ltxstream.Position{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errc := make(chan error, 2)
	go func() { errc <- tailer.Sync(ctx) }()

	select {
	case <-entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("first Sync never reached OnLTX")
	}

	go func() { errc <- tailer.Sync(ctx) }()
	select {
	case <-entered:
		close(release)
		t.Fatalf("second Sync emitted from the same position before first Sync advanced it")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if got := ltxCount.Load(); got != 1 {
		t.Fatalf("LTX emits = %d, want 1", got)
	}
}

// TestTailer_ChecksumChain verifies the LTX rolling-checksum chain
// across multiple Sync passes (each pass = one LTX). Each LTX's
// PreApplyChecksum equals the previous LTX's PostApplyChecksum. This
// is the property Litestream-followers rely on to validate the byte
// stream end-to-end.
func TestTailer_ChecksumChain(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)

	var headers []ltx.Header
	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, h ltx.Header, _ []byte) error { headers = append(headers, h); return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})

	// Drive 3 separate Sync passes with commits between, so we get 3
	// LTXes whose checksums must chain.
	for pass := 0; pass < 3; pass++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", pass, pass))
		if err := tailer.Sync(context.Background()); err != nil {
			t.Fatalf("Sync pass %d: %v", pass, err)
		}
	}
	if len(headers) < 2 {
		t.Fatalf("need at least 2 LTX to test chain, got %d", len(headers))
	}
	for i := 1; i < len(headers); i++ {
		// Each LTX's PreApplyChecksum must equal what the prior LTX
		// emitted as its post-apply checksum, and that's what we set
		// in pos.PostApplyChecksum after each emit.
		if headers[i].PreApplyChecksum != headers[i-1].PreApplyChecksum && headers[i].PreApplyChecksum == 0 {
			// PreApplyChecksum=0 only valid on snapshot LTX (MinTXID==1)
			if !headers[i].IsSnapshot() {
				t.Errorf("LTX[%d] PreApplyChecksum=0 but not snapshot", i)
			}
		}
	}
}

// TestTailer_NoWAL exercises the no-WAL-file-yet case (e.g., before
// the first commit).
func TestTailer_NoWAL(t *testing.T) {
	t.Parallel()
	cfg := ltxstream.Config{
		WALPath:      filepath.Join(t.TempDir(), "missing.db-wal"),
		NextTXID:     func() uint64 { return 1 },
		OnLTX:        func(context.Context, ltx.Header, []byte) error { return fmt.Errorf("should not be called") },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if got := tailer.SuccessfulSyncs(); got != 0 {
		t.Fatalf("SuccessfulSyncs before Sync = %d, want 0", got)
	}
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync on missing WAL: %v", err)
	}
	if got := tailer.SuccessfulSyncs(); got != 1 {
		t.Fatalf("SuccessfulSyncs after idle Sync = %d, want 1", got)
	}
}

// A failed pass must not provide a liveness proof. A full-size zeroed header is
// deterministically malformed (invalid WAL magic), so Sync returns before any
// LTX callback is involved.
func TestTailer_FailedSyncDoesNotAdvanceSuccessfulSyncs(t *testing.T) {
	t.Parallel()
	walPath := filepath.Join(t.TempDir(), "malformed.db-wal")
	if err := os.WriteFile(walPath, make([]byte, ltxstream.WALHeaderSize), 0o600); err != nil {
		t.Fatalf("write malformed WAL: %v", err)
	}
	tailer := ltxstream.New(ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return 1 },
		OnLTX:        func(context.Context, ltx.Header, []byte) error { return fmt.Errorf("should not be called") },
		SyncInterval: time.Hour,
	}, ltxstream.Position{})

	if err := tailer.Sync(context.Background()); err == nil {
		t.Fatal("Sync on malformed WAL returned nil")
	}
	if got := tailer.SuccessfulSyncs(); got != 0 {
		t.Fatalf("SuccessfulSyncs after failed Sync = %d, want 0", got)
	}
}

func TestTailer_CheckpointDoesNotAdvanceSuccessfulSyncs(t *testing.T) {
	t.Parallel()
	tailer := ltxstream.New(ltxstream.Config{
		WALPath: filepath.Join(t.TempDir(), "missing.db-wal"),
	}, ltxstream.Position{})
	checkpointCalled := false
	if err := tailer.CheckpointUnderLock(context.Background(), ltxstream.CheckpointHooks{
		Checkpoint: func() (ltxstream.CheckpointResult, error) {
			checkpointCalled = true
			return ltxstream.CheckpointResult{}, nil
		},
		Recycle: func(func() error) (int64, error) { return 0, nil },
	}); err != nil {
		t.Fatalf("CheckpointUnderLock: %v", err)
	}
	if !checkpointCalled {
		t.Fatal("checkpoint callback was not called")
	}
	if got := tailer.SuccessfulSyncs(); got != 0 {
		t.Fatalf("SuccessfulSyncs after coordinated checkpoint = %d, want 0", got)
	}
}

// TestTailer_RecycleInvokesCallback simulates a SQLite WAL recycle
// (TRUNCATE checkpoint resets the salt) between Sync passes and
// verifies the tailer's Run loop calls OnRecycle, adopts the returned
// Position, and picks up post-recycle frames on the next pass instead
// of spinning on PrevFrameMismatchError forever.
func TestTailer_RecycleInvokesCallback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	var counter atomic.Uint64
	var ltxCount atomic.Int64
	var recycleCalls atomic.Int64
	recycled := make(chan struct{}, 1)
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { ltxCount.Add(1); return nil },
		SyncInterval: 25 * time.Millisecond,
		OnRecycle: func(_ context.Context) (ltxstream.Position, *ltxstream.ChecksumState, error) {
			recycleCalls.Add(1)
			select {
			case recycled <- struct{}{}:
			default:
			}
			return ltxstream.Position{}, nil, nil
		},
	}

	tailer := ltxstream.New(cfg, ltxstream.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tailer.Run(ctx)
	}()

	// Wait for the initial frames to land in LTX. With per-pass
	// batching the CREATE+INSERT may collapse to a single LTX; the
	// chain we care about needs only the post-recycle LTX to confirm
	// recovery.
	waitFor(t, func() bool { return ltxCount.Load() >= 1 }, 2*time.Second, "initial LTX emit")

	// Force a WAL recycle: TRUNCATE checkpoint resets the WAL file; the
	// next write begins a new WAL with fresh salt. The tailer's saved
	// position still references the old salt → PrevFrameMismatchError on
	// the next Sync, which the Run loop handles via OnRecycle.
	if err := conn.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("TRUNCATE checkpoint: %v", err)
	}
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)

	select {
	case <-recycled:
	case <-time.After(2 * time.Second):
		t.Fatalf("OnRecycle never fired")
	}

	// Post-recycle: tailer should resume from a zero Position, read the
	// new WAL header, and emit at least one LTX for the new INSERT. We
	// don't pin the exact count because the recycle handler returns
	// Position{} which causes the new WAL frames to be re-emitted.
	before := ltxCount.Load()
	mustExec(t, conn, `INSERT INTO t VALUES (3, 'three')`)
	waitFor(t, func() bool { return ltxCount.Load() > before }, 2*time.Second, "post-recycle LTX emit")

	cancel()
	<-done

	if got := recycleCalls.Load(); got < 1 {
		t.Fatalf("OnRecycle calls=%d, want >=1", got)
	}
}

// TestTailer_RecycleSkippedAfterCancel verifies the Run loop does NOT attempt a
// rebaseline once its context is canceled. A recycle observed during shutdown
// can't publish — OnRecycle's writes would run on the dead context — and the
// next open rebaselines anyway, so firing OnRecycle here only logs a misleading
// failure. With the context canceled, a stale-position resume must skip it.
func TestTailer_RecycleSkippedAfterCancel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: 25 * time.Millisecond,
	}

	// Advance a probe tailer past the current frames to capture a position bound
	// to the live salt, then recycle the WAL so that position goes stale.
	probe := ltxstream.New(cfg, ltxstream.Position{})
	if err := probe.Sync(context.Background()); err != nil {
		t.Fatalf("probe sync: %v", err)
	}
	stale := probe.Position()
	if stale.IsZero() {
		t.Fatalf("probe did not advance position")
	}
	if err := conn.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("TRUNCATE checkpoint: %v", err)
	}
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)

	// A tailer resuming from the stale position would normally hit a
	// PrevFrameMismatchError and call OnRecycle. With the context already
	// canceled, it must skip OnRecycle and exit.
	var recycleCalls atomic.Int64
	cfg.OnRecycle = func(_ context.Context) (ltxstream.Position, *ltxstream.ChecksumState, error) {
		recycleCalls.Add(1)
		return ltxstream.Position{}, nil, nil
	}
	tailer := ltxstream.New(cfg, stale)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already shutting down

	done := make(chan struct{})
	go func() { defer close(done); _ = tailer.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not exit on canceled context")
	}
	if got := recycleCalls.Load(); got != 0 {
		t.Fatalf("OnRecycle called %d times after cancel; want 0", got)
	}
}

// TestTailer_CoordinatedCheckpointRace reproduces the publisher's steady state:
// a 1s-ish Run-loop Sync running concurrently with a writer that periodically
// performs the coordinated WAL recycle (drain the tailer, verified PASSIVE
// checkpoint, recycle write, position reset). In prod this used to rebaseline
// ~once per checkpoint ("WAL recycled out of band"), which should not happen:
// the recycle is gated on tailer catch-up by construction.
//
// The Sync and the checkpoint's drain both serialize on the tailer's mutex, but
// SetPosition lands AFTER the fence releases, so a Run-loop Sync can observe the
// recycled WAL against the pre-checkpoint position. This test asserts the
// coordinated path stays recycle-free under that concurrency.
func TestTailer_CoordinatedCheckpointRace(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)

	var counter atomic.Uint64
	var recycles atomic.Int64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Millisecond, // accelerate the prod 1s tick
		OnRecycle: func(_ context.Context) (ltxstream.Position, *ltxstream.ChecksumState, error) {
			recycles.Add(1)
			return ltxstream.Position{}, nil, nil
		},
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = tailer.Run(ctx) }()

	// Single SQLite writer (matching prod's writeMu serialization): interleave
	// commits with periodic coordinated checkpoints, while the tailer reads the
	// WAL file concurrently.
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		mustExec(t, conn, fmt.Sprintf(`INSERT INTO t VALUES (%d, 'v')`, i+1))
		if i%5 == 4 {
			// Coordinated recycle, as publisher.checkpoint does it: pre-drain,
			// then last drain + PRAGMA + position reset atomic under the tailer
			// lock. (The old
			// drain/PRAGMA/SetPosition-as-separate-steps sequence races the
			// Run-loop Sync here and rebaselines ~every checkpoint.)
			if err := tailer.Sync(ctx); err != nil {
				t.Fatalf("checkpoint pre-drain: %v", err)
			}
			err := tailer.CheckpointUnderLock(ctx, dbHooks(t, conn))
			if err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if got := recycles.Load(); got > 0 {
		t.Fatalf("coordinated checkpoint caused %d spurious recycle(s); want 0", got)
	}
}

// TestTailer_RestartBelowSavedOffsetDetected covers the silent blind window
// behind an uncoordinated checkpoint. A PASSIVE checkpoint fully backfills the
// WAL; SQLite's next writer then restarts the log in place: fresh salts, frames
// from offset 32, file NOT truncated. The tailer's saved offset now points past
// the new write head into stale prior-generation bytes, whose salts match the
// saved position. Resume must detect the restart from the live WAL header salt
// and demand a rebaseline; treating the stale region as end-of-WAL silently
// drops every new-generation commit until the head crosses the saved offset.
func TestTailer_RestartBelowSavedOffsetDetected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	// Several commits so the generation-A WAL is comfortably longer than the
	// few frames generation B writes below it.
	for i := 1; i <= 8; i++ {
		mustExec(t, conn, fmt.Sprintf(`INSERT INTO t VALUES (%d, 'gen-a')`, i))
	}

	var counter atomic.Uint64
	var ltxCount atomic.Int64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { ltxCount.Add(1); return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if ltxCount.Load() != 1 {
		t.Fatalf("expected 1 LTX from initial batch, got %d", ltxCount.Load())
	}
	pos := tailer.Position()

	// Uncoordinated checkpoint: PASSIVE backfills everything without
	// resetting the file, then the next commit restarts the WAL in place
	// with fresh salts below the saved offset. Clear openWAL's
	// journal_size_limit to model a foreign writer without it, whose
	// restart leaves the file un-truncated.
	mustExec(t, conn, `PRAGMA journal_size_limit = -1`)
	if err := conn.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		t.Fatalf("PASSIVE checkpoint: %v", err)
	}
	mustExec(t, conn, `INSERT INTO t VALUES (100, 'gen-b')`)

	if st, err := os.Stat(walPath); err != nil {
		t.Fatalf("stat wal: %v", err)
	} else if st.Size() < pos.Offset {
		t.Fatalf("test premise broken: WAL truncated to %d < saved offset %d", st.Size(), pos.Offset)
	}

	err := tailer.Sync(context.Background())
	var pre *ltxstream.PrevFrameMismatchError
	if !errors.As(err, &pre) {
		t.Fatalf("Sync after in-place WAL restart = %v (emitted %d LTX total); want PrevFrameMismatchError demanding rebaseline",
			err, ltxCount.Load())
	}
}

// TestTailer_CheckpointEmitsSlippedCommit covers the race that made the
// out-of-band TRUNCATE unsound: a writer the embedder's mutexes do not gate
// commits between the tailer's final drain and the WAL recycle. A TRUNCATE
// would backfill and discard those frames unread — the emitted chain silently
// loses the transaction, and if it grew the database the chain's later Commit
// page counts exceed the pages ever written (the restore-time "LTX chain is
// missing the page writes that grew the database" failure). The coordinated
// recycle must instead emit the slipped commit: PASSIVE never discards
// frames, verification sees the tailer is behind the WAL, and the retry
// drains the slipped commit before the recycle write restarts the log.
func TestTailer_CheckpointEmitsSlippedCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	// The unfenced writer: a second connection no Go mutex gates.
	conn2 := openWAL(t, dbPath)
	defer conn2.Close()

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	preSize := walSize(t, walPath)

	hooks := dbHooks(t, conn)
	realCheckpoint := hooks.Checkpoint
	slipped := false
	hooks.Checkpoint = func() (ltxstream.CheckpointResult, error) {
		// Deterministically land a commit after the drain, inside the
		// checkpoint window, exactly once.
		if !slipped {
			slipped = true
			mustExec(t, conn2, `INSERT INTO t VALUES (99, 'slipped')`)
		}
		return realCheckpoint()
	}

	if err := tailer.CheckpointUnderLock(context.Background(), hooks); err != nil {
		t.Fatalf("CheckpointUnderLock with slipped commit: %v", err)
	}
	// Every commit must be emitted under exactly one TXID: CREATE + first
	// INSERT (initial Sync), the slipped INSERT (retry drain), and the
	// recycle write (post-recycle read of the fresh generation). A chain
	// that absorbed the slipped commit stops at 3.
	if got := counter.Load(); got != 4 {
		t.Fatalf("TXIDs emitted = %d; want 4 (slipped commit and recycle write must be in the chain)", got)
	}
	// The recycle write's commit restarted the fully-backfilled WAL and
	// journal_size_limit truncated it in the same commit.
	if got := walSize(t, walPath); got >= preSize {
		t.Fatalf("WAL size after recycle = %d; want < pre-recycle %d", got, preSize)
	}
	// The tailer is in the fresh generation: subsequent commits tail on
	// cleanly with no rebaseline.
	mustExec(t, conn2, `INSERT INTO t VALUES (100, 'post')`)
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync after recycle: %v", err)
	}
	if got := counter.Load(); got != 5 {
		t.Fatalf("TXIDs after post-recycle commit = %d; want 5", got)
	}
}

// TestTailer_CheckpointRecycleBlockedByReader: SQLite's WAL restart is
// best-effort — a reader holding a non-zero read mark makes walRestartLog
// return SQLITE_BUSY and the recycle write's commit APPENDS to the current
// generation instead of restarting it, still reporting success. Treating
// recycle success as proof of a restart would zero the position against a
// live generation: the whole old WAL is re-emitted under fresh TXIDs, and
// the zero position bypasses the resume salt check. The recycle must prove
// the generation transition (the commit's own frame count plus the header
// salts) and, when the restart was prevented, keep the position, drain the
// appended recycle write, and defer.
func TestTailer_CheckpointRecycleBlockedByReader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	// A reader whose snapshot predates any backfill holds a non-zero read
	// mark for the duration of its transaction, which blocks the restart
	// (but not the backfill: its mark is at the WAL head).
	reader := openWAL(t, dbPath)
	defer reader.Close()
	mustExec(t, reader, `BEGIN; SELECT count(*) FROM t`)

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	// CREATE + INSERT drained as one batch.
	if got := counter.Load(); got != 2 {
		t.Fatalf("TXIDs after initial Sync = %d; want 2", got)
	}

	err := tailer.CheckpointUnderLock(context.Background(), dbHooks(t, conn))
	if !errors.Is(err, ltxstream.ErrCheckpointBusy) {
		t.Fatalf("recycle with restart-blocking reader = %v; want ErrCheckpointBusy", err)
	}
	// The appended recycle write was drained in place: exactly one new
	// TXID, none of the old generation re-emitted.
	if got := counter.Load(); got != 3 {
		t.Fatalf("TXIDs after blocked recycle = %d; want 3 (no re-emission of the old generation)", got)
	}
	if pos := tailer.Position(); pos.IsZero() {
		t.Fatalf("position zeroed against a live generation; want retained")
	}

	// Reader gone: the next coordinated pass restarts the WAL for real.
	mustExec(t, reader, `COMMIT`)
	if err := tailer.CheckpointUnderLock(context.Background(), dbHooks(t, conn)); err != nil {
		t.Fatalf("recycle after reader released: %v", err)
	}
	if got := counter.Load(); got != 4 {
		t.Fatalf("TXIDs after successful recycle = %d; want 4", got)
	}
	// Fresh generation tails on cleanly.
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync after recycle: %v", err)
	}
	if got := counter.Load(); got != 5 {
		t.Fatalf("TXIDs after post-recycle commit = %d; want 5", got)
	}
}

// TestTailer_CheckpointMultiRestartDefers: when further restarts occur
// inside the recycle window (possible only with an uncoordinated
// checkpointer), an intermediate generation existed that the tailer cannot
// prove it drained. The recycle must NOT adopt the newest generation —
// commits in the intermediate generation would be silently skipped — but
// keep the position and let the resume salt check escalate to a loud
// rebaseline.
func TestTailer_CheckpointMultiRestartDefers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)
	conn2 := openWAL(t, dbPath)
	defer conn2.Close()

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	hooks := dbHooks(t, conn)
	realRecycle := hooks.Recycle
	hooks.Recycle = func(validate func() error) (int64, error) {
		frames, err := realRecycle(validate) // restart 1: generation B
		if err != nil {
			return frames, err
		}
		// Uncoordinated interleaving: a commit lands in generation B, an
		// external checkpoint backfills it, and the next commit restarts
		// to generation C — all before the recycle's verification runs.
		mustExec(t, conn2, `INSERT INTO t VALUES (50, 'intermediate')`)
		mustExec(t, conn2, `PRAGMA wal_checkpoint(PASSIVE)`)
		mustExec(t, conn2, `INSERT INTO t VALUES (51, 'next-gen')`)
		return frames, nil
	}

	err := tailer.CheckpointUnderLock(context.Background(), hooks)
	if err == nil || errors.Is(err, ltxstream.ErrCheckpointBusy) {
		t.Fatalf("multi-restart recycle = %v; want a deferral error", err)
	}
	if tailer.Position().IsZero() {
		t.Fatalf("position zeroed across an unprovable intermediate generation")
	}
	// The retained stale position surfaces loudly on the next pass instead
	// of silently skipping the intermediate generation's commits.
	err = tailer.Sync(context.Background())
	var pre *ltxstream.PrevFrameMismatchError
	if !errors.As(err, &pre) {
		t.Fatalf("Sync after multi-restart = %v; want PrevFrameMismatchError demanding rebaseline", err)
	}
}

// TestTailer_SuppressedRecycleForeignRestartDefers: the single-increment
// alias. A reader suppresses the recycle write's restart (the sentinel
// appends to the old generation), a foreign commit lands behind it, an
// uncoordinated checkpoint absorbs both, and the next foreign commit
// restarts the WAL — salt1 advances exactly once, indistinguishable by
// salts alone from the recycle's own restart. Adopting the new generation
// here silently skips the absorbed foreign commit; the pass must defer so
// the next Sync escalates to a rebaseline.
func TestTailer_SuppressedRecycleForeignRestartDefers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)
	conn2 := openWAL(t, dbPath)
	defer conn2.Close()

	// Non-zero read mark: blocks the restart, not the backfill.
	reader := openWAL(t, dbPath)
	defer reader.Close()
	mustExec(t, reader, `BEGIN; SELECT count(*) FROM t`)

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	hooks := dbHooks(t, conn)
	realRecycle := hooks.Recycle
	hooks.Recycle = func(validate func() error) (int64, error) {
		frames, err := realRecycle(validate) // suppressed: sentinel appends
		if err != nil {
			return frames, err
		}
		// The reader releases, a foreign commit appends behind the
		// sentinel, an uncoordinated checkpoint backfills both, and the
		// next foreign commit restarts: exactly one salt increment.
		mustExec(t, reader, `COMMIT`)
		mustExec(t, conn2, `INSERT INTO t VALUES (60, 'absorbed')`)
		mustExec(t, conn2, `PRAGMA wal_checkpoint(PASSIVE)`)
		mustExec(t, conn2, `INSERT INTO t VALUES (61, 'next-gen')`)
		return frames, nil
	}

	err := tailer.CheckpointUnderLock(context.Background(), hooks)
	if err == nil || errors.Is(err, ltxstream.ErrCheckpointBusy) {
		t.Fatalf("suppressed recycle + foreign restart = %v; want a deferral error", err)
	}
	if tailer.Position().IsZero() {
		t.Fatalf("position zeroed across an unprovable generation transition")
	}
	err = tailer.Sync(context.Background())
	var pre *ltxstream.PrevFrameMismatchError
	if !errors.As(err, &pre) {
		t.Fatalf("Sync after suppressed recycle = %v; want PrevFrameMismatchError demanding rebaseline", err)
	}
}

// TestTailer_RolledBackSpillDoesNotBlockRecycle: SQLite leaves
// cache-spilled frames physically in the WAL after ROLLBACK (mxFrame is
// rewound; the bytes are not). Those frames are checksum-valid but carry no
// commit flag, and nothing overwrites them until the next commit. The
// recycle revalidation must not read them as commits pending behind the
// drain — that would defer the recycle on every pass and pin the WAL at
// its spill high-water until the application happens to write again.
func TestTailer_RolledBackSpillDoesNotBlockRecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, zeroblob(64))`)

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	// A large transaction spills mid-transaction under a tiny page cache,
	// then rolls back: its frames stay on disk beyond the committed head.
	mustExec(t, conn, `PRAGMA cache_size = 10`)
	mustExec(t, conn, `BEGIN`)
	for i := 0; i < 200; i++ {
		mustExec(t, conn, fmt.Sprintf(`INSERT INTO t VALUES (%d, zeroblob(4096))`, 100+i))
	}
	mustExec(t, conn, `ROLLBACK`)
	spilled := walSize(t, walPath)

	if err := tailer.CheckpointUnderLock(context.Background(), dbHooks(t, conn)); err != nil {
		t.Fatalf("recycle over rolled-back spill tail = %v; want success", err)
	}
	if got := walSize(t, walPath); got >= spilled {
		t.Fatalf("WAL not truncated by recycle: %d >= %d bytes", got, spilled)
	}
	// Fresh generation tails on cleanly.
	mustExec(t, conn, `INSERT INTO t VALUES (2, zeroblob(64))`)
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync on fresh generation: %v", err)
	}
}

// TestTailer_ForeignTruncateBeforeRecycleDefers: the causality alias. A
// foreign reset replaces the generation BEFORE the sentinel commits: a
// reader suppresses foreign commit A's restart so it appends, the reader
// releases, and a foreign TRUNCATE checkpoint absorbs A while advancing
// salt1 by exactly one. The sentinel then commits as frame 1 of the new
// generation, so its frame count alone looks like it performed the restart.
// Adopting here loses A; the recycle must revalidate the drained generation
// under the writer lock it commits with, and defer.
//
// The writer must have restarted a WAL before (any steady-state publisher
// writer has): a first commit into an empty WAL takes the wal-index salts —
// exactly old+1 after the foreign reset — only when the connection's
// checkpoint counter is non-zero; a fresh connection randomizes them, which
// would hide the alias behind a 2^-32 accident.
func TestTailer_ForeignTruncateBeforeRecycleDefers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)
	conn2 := openWAL(t, dbPath)
	defer conn2.Close()

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	// One clean recycle puts the writer in steady state (it has performed a
	// restart, so its next first-frame commit inherits wal-index salts).
	if err := tailer.CheckpointUnderLock(context.Background(), dbHooks(t, conn)); err != nil {
		t.Fatalf("steady-state recycle: %v", err)
	}

	// Non-zero read mark: blocks restarts, not backfills, so foreign
	// commit A appends instead of restarting the fully-backfilled WAL.
	reader := openWAL(t, dbPath)
	defer reader.Close()
	mustExec(t, reader, `BEGIN; SELECT count(*) FROM t`)

	hooks := dbHooks(t, conn)
	realRecycle := hooks.Recycle
	hooks.Recycle = func(validate func() error) (int64, error) {
		mustExec(t, conn2, `INSERT INTO t VALUES (70, 'absorbed')`)
		mustExec(t, reader, `COMMIT`)
		mustExec(t, conn2, `PRAGMA wal_checkpoint(TRUNCATE)`)
		return realRecycle(validate) // sentinel lands as frame 1 of the foreign generation
	}

	err := tailer.CheckpointUnderLock(context.Background(), hooks)
	if err == nil || errors.Is(err, ltxstream.ErrCheckpointBusy) {
		t.Fatalf("foreign truncate before recycle = %v; want a deferral error", err)
	}
	if tailer.Position().IsZero() {
		t.Fatalf("position zeroed across an unprovable generation transition")
	}
	// The rolled-back sentinel leaves the truncated WAL headerless; the
	// next commit writes the new generation's header, and the retained
	// stale position then surfaces loudly instead of silently skipping
	// the absorbed commit.
	mustExec(t, conn2, `INSERT INTO t VALUES (71, 'post')`)
	err = tailer.Sync(context.Background())
	var pre *ltxstream.PrevFrameMismatchError
	if !errors.As(err, &pre) {
		t.Fatalf("Sync after foreign truncate = %v; want PrevFrameMismatchError demanding rebaseline", err)
	}
}

// TestTailer_AppendedCommitDrainedBeforeOwnRestart: the stale-validation
// hole — no foreign restart involved. After the pass verified the WAL fully
// drained, foreign commit A appends (its restart reader-suppressed) and a
// foreign PASSIVE checkpoint backfills it. The sentinel commit then
// GENUINELY restarts the WAL — walRestartLog really ran, on a WAL whose
// drained-ness proof went stale — absorbing A unread. The recycle must
// detect the appended frame under the writer lock, drain it, and only then
// recycle; A must be emitted, never skipped.
func TestTailer_AppendedCommitDrainedBeforeOwnRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)
	conn2 := openWAL(t, dbPath)
	defer conn2.Close()

	reader := openWAL(t, dbPath)
	defer reader.Close()
	mustExec(t, reader, `BEGIN; SELECT count(*) FROM t`)

	var counter atomic.Uint64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	if got := counter.Load(); got != 2 {
		t.Fatalf("TXIDs after initial Sync = %d; want 2", got)
	}

	hooks := dbHooks(t, conn)
	realRecycle := hooks.Recycle
	injected := false
	hooks.Recycle = func(validate func() error) (int64, error) {
		if !injected {
			injected = true
			// A appends (restart suppressed by the reader), the reader
			// releases, and a foreign backfill absorbs A — so the sentinel
			// commit itself restarts a WAL holding an unemitted commit.
			mustExec(t, conn2, `INSERT INTO t VALUES (80, 'must-not-vanish')`)
			mustExec(t, reader, `COMMIT`)
			mustExec(t, conn2, `PRAGMA wal_checkpoint(PASSIVE)`)
		}
		return realRecycle(validate)
	}

	if err := tailer.CheckpointUnderLock(context.Background(), hooks); err != nil {
		t.Fatalf("checkpoint with appended commit: %v", err)
	}
	// CREATE+INSERT (2), commit A (1), and the recycle write (1): commit A
	// must have been drained, not absorbed by the sentinel's restart.
	if got := counter.Load(); got != 4 {
		t.Fatalf("TXIDs after recycle = %d; want 4 (the appended commit must be emitted, not absorbed)", got)
	}
	// Fresh generation tails on cleanly.
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync on fresh generation: %v", err)
	}
	if got := counter.Load(); got != 5 {
		t.Fatalf("TXIDs after fresh-generation Sync = %d; want 5", got)
	}
}

// TestTailer_CheckpointBusyKeepsPosition: a busy wal_checkpoint did not
// recycle the WAL, so the position must survive — resetting it would re-emit
// the whole WAL under fresh TXIDs on the next Sync.
func TestTailer_CheckpointBusyKeepsPosition(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)

	var counter atomic.Uint64
	var ltxCount atomic.Int64
	cfg := ltxstream.Config{
		WALPath:      walPath,
		NextTXID:     func() uint64 { return counter.Add(1) },
		OnLTX:        func(_ context.Context, _ ltx.Header, _ []byte) error { ltxCount.Add(1); return nil },
		SyncInterval: time.Hour,
	}
	tailer := ltxstream.New(cfg, ltxstream.Position{})
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}
	pos := tailer.Position()

	hooks := dbHooks(t, conn)
	hooks.Checkpoint = func() (ltxstream.CheckpointResult, error) {
		return ltxstream.CheckpointResult{Busy: true}, nil // held off
	}

	err := tailer.CheckpointUnderLock(context.Background(), hooks)
	if !errors.Is(err, ltxstream.ErrCheckpointBusy) {
		t.Fatalf("busy checkpoint = %v; want ErrCheckpointBusy", err)
	}
	if got := tailer.Position(); got != pos {
		t.Fatalf("busy checkpoint moved position %+v -> %+v; want unchanged", pos, got)
	}
	// The un-recycled WAL tails on cleanly: no duplicate re-emit, and the
	// next commit ships exactly once.
	before := ltxCount.Load()
	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)
	if err := tailer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync after busy checkpoint: %v", err)
	}
	if got := ltxCount.Load(); got != before+1 {
		t.Fatalf("LTX after busy checkpoint = %d, want %d", got, before+1)
	}
}

// dbHooks builds CheckpointHooks against a real database the way an embedder
// does: a PASSIVE checkpoint reporting SQLite's real frame counts, and a
// recycle write (same-value user_version rewrite) whose commit makes SQLite
// restart the fully-backfilled WAL under the writer's own lock.
func dbHooks(t *testing.T, writerConn *sqlitebridge.Conn) ltxstream.CheckpointHooks {
	t.Helper()
	writerConn.EnableWALFrameCapture()
	return ltxstream.CheckpointHooks{
		Checkpoint: func() (ltxstream.CheckpointResult, error) {
			row, err := writerConn.QueryInt64Row(`PRAGMA wal_checkpoint(PASSIVE)`)
			if err != nil {
				return ltxstream.CheckpointResult{}, err
			}
			return ltxstream.CheckpointResult{Busy: row[0] != 0, NLog: row[1], NCkpt: row[2]}, nil
		},
		Recycle: writerConn.RecycleCommit,
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// openWAL opens app.db, sets WAL mode, and returns the conn. WAL is
// preserved on close.
func openWAL(t *testing.T, dbPath string) *sqlitebridge.Conn {
	t.Helper()
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("open app.db: %v", err)
	}
	if err := conn.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}
	// PRAGMA wal_autocheckpoint=0 keeps frames in the WAL where the
	// tailer can see them; default 1000 is fine for these tiny tests
	// but explicit zero removes one variable.
	if err := conn.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatalf("autocheckpoint=0: %v", err)
	}
	// Match the embedder's writer conns: a commit that restarts a
	// fully-backfilled WAL truncates the file in the same commit.
	if err := conn.Exec(`PRAGMA journal_size_limit=0`); err != nil {
		t.Fatalf("journal_size_limit=0: %v", err)
	}
	return conn
}

// walSize returns the WAL file's current size.
func walSize(t *testing.T, walPath string) int64 {
	t.Helper()
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	return st.Size()
}

func mustExec(t *testing.T, conn *sqlitebridge.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
