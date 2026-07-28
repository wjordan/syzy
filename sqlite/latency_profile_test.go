package sqlite_test

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestPublishSnapshot_LatencyProfile is a repeatable diagnostic for
// PublishSnapshot's writeMu-hold window and its phase breakdown. It
// does NOT assert thresholds — it logs metrics so before/after fixes
// can be compared mechanically.
//
// Two angles of measurement:
//
//   - Internal (white-box): a slog handler captures the per-phase Debug
//     events emitted by snapshotPinnedLocked and PublishSnapshot
//     (pre_drain, barrier_acq, tail_drain, snapshot_flush, pin_init,
//     barrier_rel, bundle_files, obj_publish, writemu_hold).
//   - External (black-box): a concurrent goroutine issues
//     `db.BeginTx → INSERT → Commit` in a tight loop on the same node
//     and records the max BeginTx latency it observes. This is exactly
//     the path a write-heavy companion takes — `BeginTx` acquires
//     writeMu, so its latency is dominated by how long the publisher
//     holds writeMu.
//
// The S3 upload uses a file:// backend, so obj_publish reflects local
// file I/O rather than network round-trips. The barrier-window numbers
// are transport-independent; the writemu_hold number understates real
// S3 by the network gap.
//
// Run with: go test -v -run TestPublishSnapshot_LatencyProfile .
func TestPublishSnapshot_LatencyProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("latency profile, skipped in -short mode")
	}

	bucket := t.TempDir()
	be, err := objectstore.OpenFS(bucket)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}

	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "app.db")

	cap := newCapturingHandler()
	log := slog.New(cap)

	ctx := context.Background()
	node, err := syzy.Open(ctx, syzy.Config{
		Path:          dbPath,
		ObjectBackend: be,
		Log:           log,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	// Seed 200 rows in a single multi-row INSERT (one txn): the profile
	// only needs a non-trivial page set to flush, not 200 commits.
	var seed strings.Builder
	seed.WriteString(`INSERT INTO t VALUES `)
	for i := 0; i < 200; i++ {
		if i > 0 {
			seed.WriteByte(',')
		}
		fmt.Fprintf(&seed, `(%d, 'seed-%d')`, i, i)
	}
	if err := node.Exec(seed.String()); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}

	// External writer mirrors the fly-demo heartbeat: db.BeginTx, one
	// INSERT, Commit. db.BeginTx takes writeMu, so this measures how
	// long the publisher holds writeMu — exactly the symptom the
	// heartbeat sees as "beginImmediate=Xms".
	db := syzy.NewDB(node)
	defer db.Close()

	var (
		stop    atomic.Bool
		wg      sync.WaitGroup
		maxWait atomic.Int64 // ns observed in BeginTx
		writes  atomic.Int64
		errs    atomic.Int64
		nextWID atomic.Int64
	)
	nextWID.Store(1_000_000)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			id := nextWID.Add(1)
			t0 := time.Now()
			tx, err := db.BeginTx(ctx, nil)
			waited := time.Since(t0)
			if err != nil {
				errs.Add(1)
				continue
			}
			recordMax(&maxWait, waited.Nanoseconds())
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO t (id, v) VALUES (?, ?)`, id, "w",
			); err != nil {
				_ = tx.Rollback()
				errs.Add(1)
				continue
			}
			if err := tx.Commit(); err != nil {
				errs.Add(1)
				continue
			}
			writes.Add(1)
			// Loose loop, not pegged. Plenty of BeginTx samples per
			// publish window without saturating CPU.
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(10 * time.Millisecond) // warmup

	// Clear any log records from Open: the publisher's initial
	// takeBaseline emits its own "syzy: snapshot pinned" event,
	// which would otherwise be counted as if it were one of the
	// per-iteration PublishSnapshot pins below.
	cap.reset()

	const iterations = 5
	for i := 0; i < iterations; i++ {
		if err := node.PublishSnapshot(ctx); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("PublishSnapshot[%d]: %v", i, err)
		}
		// Touch the DB between iterations so the next pin has fresh
		// dirty state to flush.
		if err := node.Exec(fmt.Sprintf(`INSERT INTO t VALUES (%d, 'p-%d')`, 2_000_000+i, i)); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("between-iter INSERT: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()

	pins := cap.byMessage("syzy: snapshot pinned")
	publishes := cap.byMessage("syzy: publish snapshot")
	if len(pins) != iterations || len(publishes) != iterations {
		t.Fatalf("expected %d pin and %d publish records, got %d and %d",
			iterations, iterations, len(pins), len(publishes))
	}

	t.Logf("=== publish-snapshot latency profile (%d iterations) ===", iterations)
	t.Logf("external concurrent writer (db.BeginTx → INSERT → Commit):")
	t.Logf("  successful writes:                 %d", writes.Load())
	t.Logf("  errors:                            %d", errs.Load())
	t.Logf("  MAX observed BeginTx latency:      %v", time.Duration(maxWait.Load()))
	t.Logf("")
	t.Logf("per-iteration phase breakdown:")
	for i := 0; i < iterations; i++ {
		p := decodeDurations(pins[i])
		pub := decodeDurations(publishes[i])
		t.Logf("  iter[%d]:", i)
		t.Logf("    snapshotPinnedLocked total      = %v", p["total"])
		t.Logf("      pre_drain      (out)          = %v", p["pre_drain"])
		t.Logf("      barrier_acq    (in)           = %v", p["barrier_acq"])
		t.Logf("      tail_drain     (in)           = %v", p["tail_drain"])
		t.Logf("      snapshot_flush (in)           = %v", p["snapshot_flush"])
		t.Logf("      pin_init       (in)           = %v", p["pin_init"])
		t.Logf("      barrier_rel    (in)           = %v", p["barrier_rel"])
		t.Logf("      barrier_hold (sum of in)      = %v", p["barrier_hold"])
		t.Logf("    PublishSnapshot:")
		t.Logf("      writemu_wait                  = %v", pub["writemu_wait"])
		t.Logf("      writemu_hold (the bottleneck) = %v", pub["writemu_hold"])
		t.Logf("      bundle_files                  = %v", pub["bundle_files"])
		t.Logf("      obj_publish                   = %v", pub["obj_publish"])
		t.Logf("      total                         = %v", pub["total"])
	}

	t.Logf("")
	t.Logf("aggregate (across %d iterations):", iterations)
	t.Logf("  median barrier_hold = %v", median(durationsBy(pins, "barrier_hold")))
	t.Logf("  max    barrier_hold = %v", maxOf(durationsBy(pins, "barrier_hold")))
	t.Logf("  median writemu_hold = %v", median(durationsBy(publishes, "writemu_hold")))
	t.Logf("  max    writemu_hold = %v", maxOf(durationsBy(publishes, "writemu_hold")))
}

func recordMax(m *atomic.Int64, candidate int64) {
	for {
		cur := m.Load()
		if candidate <= cur {
			return
		}
		if m.CompareAndSwap(cur, candidate) {
			return
		}
	}
}

// capturingHandler buffers slog records for inspection.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newCapturingHandler() *capturingHandler { return &capturingHandler{} }

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) reset() {
	h.mu.Lock()
	h.records = h.records[:0]
	h.mu.Unlock()
}

func (h *capturingHandler) byMessage(msg string) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, 0, len(h.records))
	for _, r := range h.records {
		if r.Message == msg {
			out = append(out, r)
		}
	}
	return out
}

func decodeDurations(r slog.Record) map[string]time.Duration {
	out := map[string]time.Duration{}
	r.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindDuration {
			out[a.Key] = a.Value.Duration()
		}
		return true
	})
	return out
}

func durationsBy(rs []slog.Record, key string) []time.Duration {
	out := make([]time.Duration, 0, len(rs))
	for _, r := range rs {
		if d, ok := decodeDurations(r)[key]; ok {
			out = append(out, d)
		}
	}
	return out
}

func median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), ds...)
	for i := 1; i < len(cp); i++ {
		for j := i; j > 0 && cp[j-1] > cp[j]; j-- {
			cp[j-1], cp[j] = cp[j], cp[j-1]
		}
	}
	return cp[len(cp)/2]
}

func maxOf(ds []time.Duration) time.Duration {
	var m time.Duration
	for _, d := range ds {
		if d > m {
			m = d
		}
	}
	return m
}
