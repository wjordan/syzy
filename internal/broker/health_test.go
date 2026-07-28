package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
)

func TestInboundHealth_TracksAppliedOrigins(t *testing.T) {
	t.Parallel()
	a := newUniqueApplier(t, 1)

	if h := a.br.InboundHealth(); len(h.Origins) != 0 || h.ApplyStalled ||
		h.ConsecutiveLocked != 0 || h.SelfHeals != 0 || h.LastSubscribeError != "" {
		t.Fatalf("fresh broker health not zero: %+v", h)
	}

	remote := crdt.Origin(2)
	clk := crdt.Clock{WallTime: 12345}
	before := time.Now()
	ins := buildUniqueInsert(t, a.tab, crdt.Dot{Origin: remote, Seq: 1},
		crdt.Stamp{Origin: remote, Clock: clk}, []byte{0xAA}, "slug-1", "n-1")
	if err := a.br.applyPayloadCache(ins, ins.Encoded(), false); err != nil {
		t.Fatalf("apply: %v", err)
	}
	a.br.setLastSubscribeError(errors.New("boom"))

	h := a.br.InboundHealth()
	if len(h.Origins) != 1 {
		t.Fatalf("origins = %+v, want 1 entry", h.Origins)
	}
	o := h.Origins[0]
	if o.Origin != remote || o.AppliedSeq != 1 || o.AppliedTip != 1 {
		t.Fatalf("origin entry = %+v", o)
	}
	if !o.LastHLC.Equal(clk) {
		t.Fatalf("LastHLC = %+v, want %+v", o.LastHLC, clk)
	}
	if o.LastApplied.Before(before) {
		t.Fatalf("LastApplied = %v, want >= %v", o.LastApplied, before)
	}
	if h.LastSubscribeError != "boom" {
		t.Fatalf("LastSubscribeError = %q", h.LastSubscribeError)
	}
	if h.ApplyStalled || h.ConsecutiveLocked != 0 {
		t.Fatalf("healthy broker reports stall: %+v", h)
	}
}

// warnCounter counts WARN records emitted through a slog handler.
type warnCounter struct {
	mu sync.Mutex
	n  int
}

func (h *warnCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.n++
		h.mu.Unlock()
	}
	return nil
}
func (h *warnCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCounter) WithGroup(string) slog.Handler      { return h }
func (h *warnCounter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func TestWarnFetchError_RateLimited(t *testing.T) {
	t.Parallel()
	a := newUniqueApplier(t, 1)
	wc := &warnCounter{}
	a.br.log = slog.New(wc)

	errA := errors.New("s3: connection refused")
	a.br.warnFetchError("gap fetch", errA)
	a.br.warnFetchError("gap fetch", errA)
	a.br.warnFetchError("gap fetch", errA)
	if got := wc.count(); got != 1 {
		t.Fatalf("repeated identical error logged %d times, want 1", got)
	}
	// A changed error string logs immediately.
	a.br.warnFetchError("gap fetch", errors.New("s3: 403 forbidden"))
	if got := wc.count(); got != 2 {
		t.Fatalf("changed error logged %d times total, want 2", got)
	}
	// After the rate-limit interval elapses, the same error logs again.
	a.br.fetchErrMu.Lock()
	a.br.fetchErrAt = time.Now().Add(-2 * fetchErrLogEvery)
	a.br.fetchErrMu.Unlock()
	a.br.warnFetchError("gap fetch", errors.New("s3: 403 forbidden"))
	if got := wc.count(); got != 3 {
		t.Fatalf("post-interval repeat logged %d times total, want 3", got)
	}
	// Context-cancellation noise is suppressed entirely.
	a.br.warnFetchError("gap fetch", context.Canceled)
	if got := wc.count(); got != 3 {
		t.Fatalf("ctx cancellation logged; count = %d, want 3", got)
	}
}
