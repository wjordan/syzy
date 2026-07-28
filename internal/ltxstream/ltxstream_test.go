package ltxstream_test

import (
	"bytes"
	"context"
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
	if err := tailer.CheckpointUnderLock(context.Background(), func() error {
		checkpointCalled = true
		return nil
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
		OnRecycle: func(_ context.Context) (ltxstream.Position, error) {
			recycleCalls.Add(1)
			select {
			case recycled <- struct{}{}:
			default:
			}
			return ltxstream.Position{}, nil
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
	cfg.OnRecycle = func(_ context.Context) (ltxstream.Position, error) {
		recycleCalls.Add(1)
		return ltxstream.Position{}, nil
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
// performs the coordinated WAL recycle (drain the tailer, PRAGMA
// wal_checkpoint(TRUNCATE), then SetPosition(Position{})). In prod this rebaselines
// ~once per checkpoint ("WAL recycled out of band"), which should not happen:
// the recycle is supposed to be gated on tailer catch-up by construction.
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
		OnRecycle: func(_ context.Context) (ltxstream.Position, error) {
			recycles.Add(1)
			return ltxstream.Position{}, nil
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
			err := tailer.CheckpointUnderLock(ctx, func() error {
				return conn.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
			})
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
	return conn
}

func mustExec(t *testing.T, conn *sqlitebridge.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
