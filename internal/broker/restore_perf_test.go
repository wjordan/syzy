package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestColdRestorePerf compares cold-restore strategies on a fresh
// empty target:
//
//	apply         per-changeset commit, full broker apply path.
//	apply-ceil    raw UPSERT (the same statement the broker emits) in
//	              one big txn, no broker / LWW / cache work — ceiling
//	              for "logical apply at this schema."
//	backup        sqlite3_backup_init/step from a populated source.
//	raw           io.Copy of the source app.db file (lower bound).
//
// Each path produces a logically-equivalent target. The print line
// reports wall time, rows/s, and source-DB-bytes/s.
//
// Skipped unless SYZY_RESTORE_PERF_N is set (comma-separated row
// counts, e.g. 50000,200000). val=64 bytes; override via
// SYZY_RESTORE_PERF_VAL_BYTES.
func TestColdRestorePerf(t *testing.T) {
	if testing.Short() {
		t.Skip("perf comparison; skipped under -short")
	}
	env := os.Getenv("SYZY_RESTORE_PERF_N")
	if env == "" {
		t.Skip("perf comparison; set SYZY_RESTORE_PERF_N (e.g. 50000,200000) to run")
	}
	var sizes []int
	for _, p := range strings.Split(env, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || n <= 0 {
			t.Fatalf("SYZY_RESTORE_PERF_N: %q invalid: %v", p, err)
		}
		sizes = append(sizes, n)
	}
	valBytes := 64
	if env := os.Getenv("SYZY_RESTORE_PERF_VAL_BYTES"); env != "" {
		v, err := strconv.Atoi(env)
		if err != nil || v < 0 {
			t.Fatalf("SYZY_RESTORE_PERF_VAL_BYTES: %q invalid", env)
		}
		valBytes = v
	}
	val := strings.Repeat("x", valBytes)
	for _, n := range sizes {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			runRestorePerf(t, n, val)
		})
	}
}

func runRestorePerf(t *testing.T, n int, val string) {
	t.Helper()

	// Build N changeset payloads in memory off a throwaway fixture used
	// only to acquire a *catalog.Table handle. Apply throws away this
	// fixture's app.db; the real source DB is rebuilt in timeApplyReplay
	// below so the apply timing covers a fresh empty target.
	scratch := newApplier(t, 1, nil)
	tab := scratch.tab
	payloads := make([][]byte, n)
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	for i := 0; i < n; i++ {
		// Big-endian PK so inserts append at the b-tree right edge —
		// matches syzy's recommended uuidv7() pattern (time-ordered).
		var idVal [8]byte
		binary.BigEndian.PutUint64(idVal[:], uint64(i+1))
		cs := buildInsert(t, tab, crdt.Dot{Origin: src, Seq: crdt.Seq(i + 1)}, stamp, 1, idVal[:], val)
		payloads[i] = cs.Encoded()
	}

	// Path A: apply replay into a fresh empty target. Source DB ends
	// up at sourcePath after this run.
	applyDir := t.TempDir()
	sourcePath := filepath.Join(applyDir, "app.db")
	applyDur := timeApplyReplay(t, sourcePath, payloads)

	// Checkpoint+close already happened inside timeApplyReplay; the
	// file at sourcePath is the realistic post-restore state.
	srcSize := mustFileSize(t, sourcePath)

	// Path A': "apply ceiling" — bypass the broker entirely. Run the
	// same UPSERT statement the broker produces against a fresh empty
	// target, all in one BEGIN/COMMIT, with no LWW / cache / journal
	// work. Lower bound for what an N-changeset cold-restore could
	// achieve at this schema if we maximally batched + dropped CRDT
	// bookkeeping.
	idVals := make([][8]byte, n)
	for i := range idVals {
		binary.BigEndian.PutUint64(idVals[i][:], uint64(i+1))
	}
	ceilingDur := timeApplyCeiling(t, idVals, val)

	// Path B: sqlite3_backup from sourcePath to a fresh empty target.
	backupDur := timeBackup(t, sourcePath)

	// Path C: io.Copy the source file, then open it with the same WAL
	// pragmas the apply path uses — fairer comparison than a bare cp.
	rawDur := timeRawCopy(t, sourcePath)

	rows := float64(n)
	bytes := float64(srcSize)
	t.Logf("N=%d val=%dB source=%s pages≈%d",
		n, len(val), fmtBytes(srcSize), srcSize/4096)
	t.Logf("  apply        %12s  %10.0f rows/s  %10s/s",
		applyDur.Round(time.Millisecond),
		rows/applyDur.Seconds(),
		fmtBytes(int64(bytes/applyDur.Seconds())))
	t.Logf("  apply-ceil   %12s  %10.0f rows/s  %10s/s  (%.1fx vs apply)",
		ceilingDur.Round(time.Millisecond),
		rows/ceilingDur.Seconds(),
		fmtBytes(int64(bytes/ceilingDur.Seconds())),
		float64(applyDur)/float64(ceilingDur))
	t.Logf("  backup       %12s  %10.0f rows/s  %10s/s  (%.1fx vs apply, %.1fx vs ceil)",
		backupDur.Round(time.Millisecond),
		rows/backupDur.Seconds(),
		fmtBytes(int64(bytes/backupDur.Seconds())),
		float64(applyDur)/float64(backupDur),
		float64(ceilingDur)/float64(backupDur))
	t.Logf("  raw          %12s  %10.0f rows/s  %10s/s  (%.1fx vs apply)",
		rawDur.Round(time.Millisecond),
		rows/rawDur.Seconds(),
		fmtBytes(int64(bytes/rawDur.Seconds())),
		float64(applyDur)/float64(rawDur))
}

// newPerfBroker creates an event(id BLOB PK, n TEXT) target at
// dbPath with the apply broker wired up. Cleanup is registered on t.
func newPerfBroker(t *testing.T, dbPath string) (*sqlitebridge.Conn, *Broker) {
	t.Helper()
	app, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	sc, err := metadata.Open(filepath.Join(filepath.Dir(dbPath), "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(testCluster); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	br, err := New(Config{
		AppApply: app,
		Meta:     sc,
		Catalog:  cat,
		Cache:    nodestate.New(crdt.Origin(1)),
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	return app, br
}

// timeApplyReplay applies all payloads through the broker's
// per-changeset commit path and returns the wall time of the apply
// loop only — fixture setup and final checkpoint are excluded.
// Checkpoints sourcePath at the end so timeBackup / timeRawCopy run
// against a byte-stable file.
func timeApplyReplay(t *testing.T, sourcePath string, payloads [][]byte) time.Duration {
	t.Helper()
	app, br := newPerfBroker(t, sourcePath)
	ctx := context.Background()
	t0 := time.Now()
	for i, p := range payloads {
		if err := br.applyPayload(ctx, p); err != nil {
			t.Fatalf("apply: payload %d: %v", i, err)
		}
	}
	dur := time.Since(t0)
	if err := app.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("apply: checkpoint: %v", err)
	}
	return dur
}

// timeApplyCeiling runs the same UPSERT the broker generates for
// event(id BLOB PK, n TEXT) — INSERT ... ON CONFLICT(id) DO UPDATE SET
// n = excluded.n — directly against a fresh empty target, all rows in
// one BEGIN IMMEDIATE/COMMIT, with no broker / LWW / cache / mirror
// journal work. This is the ceiling for "logical apply at this schema"
// once we drop both per-changeset commit overhead and CRDT bookkeeping.
func timeApplyCeiling(t *testing.T, idVals [][8]byte, val string) time.Duration {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "app.db")
	app, err := sqlitebridge.Open(dst, 0)
	if err != nil {
		t.Fatalf("ceiling: open: %v", err)
	}
	defer app.Close()
	if err := app.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		t.Fatalf("ceiling: schema: %v", err)
	}
	stmt, _, err := app.Prepare(`INSERT INTO event (id, n) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET n = excluded.n`)
	if err != nil {
		t.Fatalf("ceiling: prepare: %v", err)
	}
	defer stmt.Finalize()

	t0 := time.Now()
	if err := app.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("ceiling: begin: %v", err)
	}
	for i := range idVals {
		if err := stmt.Reset(); err != nil {
			t.Fatalf("ceiling: reset %d: %v", i, err)
		}
		if err := stmt.BindBlob(1, idVals[i][:]); err != nil {
			t.Fatalf("ceiling: bind id %d: %v", i, err)
		}
		if err := stmt.BindText(2, val); err != nil {
			t.Fatalf("ceiling: bind n %d: %v", i, err)
		}
		if _, err := stmt.Step(); err != nil {
			t.Fatalf("ceiling: step %d: %v", i, err)
		}
	}
	if err := app.Exec(`COMMIT`); err != nil {
		t.Fatalf("ceiling: commit: %v", err)
	}
	if err := app.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("ceiling: checkpoint: %v", err)
	}
	return time.Since(t0)
}

// timeBackup opens sourcePath read-only, runs sqlite3_backup_init +
// step-to-completion against a fresh empty target, and times the
// step+finish loop.
func timeBackup(t *testing.T, sourcePath string) time.Duration {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "app.db")

	src, err := sqlitebridge.Open(sourcePath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		t.Fatalf("backup: open src: %v", err)
	}
	defer src.Close()
	target, err := sqlitebridge.Open(dst, 0)
	if err != nil {
		t.Fatalf("backup: open dst: %v", err)
	}
	defer target.Close()
	if err := target.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL`); err != nil {
		t.Fatalf("backup: dst pragma: %v", err)
	}

	t0 := time.Now()
	bk, err := sqlitebridge.BackupInit(target, "main", src, "main")
	if err != nil {
		t.Fatalf("backup: init: %v", err)
	}
	for {
		err := bk.Step(256)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = bk.Finish()
			t.Fatalf("backup: step: %v", err)
		}
	}
	if err := bk.Finish(); err != nil {
		t.Fatalf("backup: finish: %v", err)
	}
	if err := target.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("backup: checkpoint: %v", err)
	}
	return time.Since(t0)
}

// timeRawCopy reads sourcePath as raw bytes into a fresh target file
// and fsyncs. No SQLite involvement; the theoretical lower bound for
// "ship the database from A to B."
func timeRawCopy(t *testing.T, sourcePath string) time.Duration {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "app.db")

	t0 := time.Now()
	in, err := os.Open(sourcePath)
	if err != nil {
		t.Fatalf("raw: open src: %v", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("raw: open dst: %v", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("raw: copy: %v", err)
	}
	if err := out.Sync(); err != nil {
		t.Fatalf("raw: fsync: %v", err)
	}
	return time.Since(t0)
}

func mustFileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

func fmtBytes(n int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GiB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MiB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KiB", float64(n)/KB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
