//go:build linux

package producer

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wjordan/syzy/sqlitebridge"
)

// Inotify cost attribution benchmarks. Documents the experiment that
// drove the decision to drop the WAL tailer entirely (the producer
// originally ran a tailer with an inotify watch on app.db-wal as a
// confirmation cross-check for journal records).
//
// Goal at the time: cleanly separate the kernel-side fsnotify cost
// paid on the writer thread (per-fsync event allocation + watch list
// walk) from the goroutine-side cost of having a tailer consume the
// events. Earlier diagnostics measured a ~1.3µs gap between
// "tailer running" and "tailer closed" but couldn't say which side
// owned the cost.
//
// Method: hold the SQLite + touch-journal + no-op walHook fixture
// constant (matches openHookFixtureNoOp) and add inotify components
// independently:
//
//	A. FixtureOnly                       — no watch, no goroutine
//	B. RawWatchNoReader                  — watch installed, no goroutine
//	C. WatchAndDrainerGoroutine          — watch + minimal goroutine
//
// Findings on Ryzen 9 7900X / Linux 6.17 (50000 iters × 5 counts):
//
//	A median: 10,418 ns/op
//	B median: 11,736 ns/op   (B - A = +1,318 ns: kernel fsnotify cost)
//	C median: 11,645 ns/op   (C - B = within noise: goroutine cost ~0)
//
// Conclusion: the entire ~1.3µs gap is kernel-side fsnotify work
// performed synchronously on the writer thread when fsync wakes any
// inotify watch on the WAL inode. The watcher goroutine itself adds
// no measurable overhead. Pooling/persisting the inotify FD does not
// help — the kernel cost is per-event-generation, not per-FD-creation.
// The only way to remove it is to remove the watch.
//
// Variant A is intentionally identical to BenchmarkHookFixtureNoOpInsert
// so the three variants can be run as a single suite via a shared regex.
//
// Run: go test -run x -bench BenchmarkInotifyAttribution -benchtime=50000x
// ./internal/producer

const inotifyDrainBufSize = 4096

// openInotifyFixture opens an openHookFixtureNoOp-equivalent connection
// and returns the WAL file path so callers can install raw inotify
// watches against it. The DDL (CREATE TABLE) implicitly creates the
// WAL file, so InotifyAddWatch on the WAL path will succeed.
//
// Intentionally identical to openHookFixtureNoOp so the FixtureOnly
// variant matches BenchmarkHookFixtureNoOpInsert exactly — earlier
// versions seeded an INSERT+DELETE which bumped the baseline by
// ~1,000ns relative to the equivalent fixture in producer_bench_test.go.
func openInotifyFixture(b *testing.B) (*sqlitebridge.Conn, string) {
	b.Helper()
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	c, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	if err := c.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		b.Fatalf("schema: %v", err)
	}
	c.EnableTouchJournal()
	c.SetWALHook(func(string, int) int {
		c.ClearTouchJournal()
		return 0
	})
	return c, dbPath + "-wal"
}

// runInsertLoop runs the bench-timed INSERT loop against the prepared
// conn. Caller is responsible for fixture setup before, and any
// teardown after, the timed region.
func runInsertLoop(b *testing.B, c *sqlitebridge.Conn) {
	b.Helper()
	stmt, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkInotifyAttribution_FixtureOnly is variant A: pure SQLite +
// touch journal + no-op walHook, no inotify watch. Reference baseline.
// Should match BenchmarkHookFixtureNoOpInsert within noise.
func BenchmarkInotifyAttribution_FixtureOnly(b *testing.B) {
	c, _ := openInotifyFixture(b)
	runInsertLoop(b, c)
}

// BenchmarkInotifyAttribution_RawWatchNoReader is variant B: same
// fixture + raw inotify watch installed on the WAL file, no goroutine
// reading. Events queue up in the kernel; we drain periodically with
// timer paused so the kernel's queue-management doesn't dominate.
//
// Caveat: at very high b.N the kernel may hit /proc/sys/fs/inotify
// /max_queued_events (default 16384) between drains and start
// short-circuiting per-event work, biasing this measurement low. The
// 1024-iter drain interval keeps us comfortably under that ceiling.
func BenchmarkInotifyAttribution_RawWatchNoReader(b *testing.B) {
	c, walPath := openInotifyFixture(b)
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		b.Fatalf("InotifyInit1: %v", err)
	}
	defer syscall.Close(fd)
	wd, err := syscall.InotifyAddWatch(fd, walPath, syscall.IN_MODIFY)
	if err != nil {
		b.Fatalf("InotifyAddWatch: %v", err)
	}
	defer func() { _, _ = syscall.InotifyRmWatch(fd, uint32(wd)) }()

	stmt, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	buf := make([]byte, inotifyDrainBufSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
		if i&1023 == 1023 {
			b.StopTimer()
			for {
				_, rerr := syscall.Read(fd, buf)
				if rerr != nil {
					break
				}
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
}

// BenchmarkInotifyAttribution_WatchAndDrainerGoroutine is variant C:
// same fixture + watch + a goroutine that drains events but does no
// other work. The goroutine uses non-blocking reads + 1ms poll, the
// same pattern the production waker used (since removed).
//
// Diff vs RawWatchNoReader = goroutine activity overhead (scheduler
// wakeups, cache disruption). Empirically this comes out within noise
// of zero, indicating the kernel-side fsnotify cost dominates and the
// goroutine is essentially free under modest CPU contention.
func BenchmarkInotifyAttribution_WatchAndDrainerGoroutine(b *testing.B) {
	c, walPath := openInotifyFixture(b)
	fd, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if err != nil {
		b.Fatalf("InotifyInit1: %v", err)
	}
	wd, err := syscall.InotifyAddWatch(fd, walPath, syscall.IN_MODIFY)
	if err != nil {
		_ = syscall.Close(fd)
		b.Fatalf("InotifyAddWatch: %v", err)
	}

	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, inotifyDrainBufSize)
		for !stop.Load() {
			_, err := syscall.Read(fd, buf)
			if err == nil {
				continue
			}
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				time.Sleep(time.Millisecond)
				continue
			}
			return
		}
	}()
	defer func() {
		stop.Store(true)
		_, _ = syscall.InotifyRmWatch(fd, uint32(wd))
		_ = syscall.Close(fd)
		<-done
	}()

	runInsertLoop(b, c)
}
