package producer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wjordan/syzy/unique"
)

// fakeReg is a unique.Registry whose Reserve returns ErrUnavailable for the
// first failN calls, then the configured terminal result.
type fakeReg struct {
	failN    int
	calls    int
	ok       bool
	conflict *unique.Claim
	termErr  error // non-nil terminal error instead of (ok, conflict)
}

func (f *fakeReg) Reserve(context.Context, []unique.Claim) (bool, *unique.Claim, error) {
	f.calls++
	if f.calls <= f.failN {
		return false, nil, unique.ErrUnavailable
	}
	if f.termErr != nil {
		return false, nil, f.termErr
	}
	return f.ok, f.conflict, nil
}

func (f *fakeReg) Release(context.Context, []unique.Claim) error { return nil }

func noSleep(time.Duration) {}

// A transient ErrUnavailable (a leaseholder handover/drain) is waited out, not
// surfaced as a commit failure — the whole point of the fix.
func TestReserveWithRetry_RecoversAfterTransient(t *testing.T) {
	r := &fakeReg{failN: 3, ok: true}
	ok, conflict, retries, err := reserveWithRetry(context.Background(), r, []unique.Claim{{}}, time.Second, noSleep)
	if err != nil || !ok || conflict != nil {
		t.Fatalf("got ok=%v conflict=%v err=%v; want ok=true, no conflict, no err", ok, conflict, err)
	}
	if retries != 3 || r.calls != 4 {
		t.Fatalf("retries=%d calls=%d; want 3 retries over 4 calls", retries, r.calls)
	}
}

// A genuine conflict is a definitive answer: returned immediately, never
// retried (retrying could only flip it after the holder releases — wrong).
func TestReserveWithRetry_ConflictReturnsImmediately(t *testing.T) {
	r := &fakeReg{ok: false, conflict: &unique.Claim{}}
	ok, conflict, retries, err := reserveWithRetry(context.Background(), r, []unique.Claim{{}}, time.Second, noSleep)
	if err != nil || ok || conflict == nil {
		t.Fatalf("got ok=%v conflict=%v err=%v; want a conflict", ok, conflict, err)
	}
	if retries != 0 || r.calls != 1 {
		t.Fatalf("retries=%d calls=%d; want a single call", retries, r.calls)
	}
}

// A non-transient error is surfaced as-is, never masked or retried.
func TestReserveWithRetry_NonTransientNotRetried(t *testing.T) {
	boom := errors.New("boom")
	r := &fakeReg{termErr: boom}
	_, _, retries, err := reserveWithRetry(context.Background(), r, []unique.Claim{{}}, time.Second, noSleep)
	if !errors.Is(err, boom) || retries != 0 || r.calls != 1 {
		t.Fatalf("err=%v retries=%d calls=%d; want boom returned on first call", err, retries, r.calls)
	}
}

// Past the budget a persistently-unavailable backend surfaces ErrUnavailable —
// a real outage, not a spurious blip.
func TestReserveWithRetry_GivesUpAfterBudget(t *testing.T) {
	r := &fakeReg{failN: 1 << 30}
	tinySleep := func(time.Duration) { time.Sleep(200 * time.Microsecond) }
	_, _, retries, err := reserveWithRetry(context.Background(), r, []unique.Claim{{}}, 2*time.Millisecond, tinySleep)
	if !errors.Is(err, unique.ErrUnavailable) || retries < 1 {
		t.Fatalf("err=%v retries=%d; want ErrUnavailable after at least one retry", err, retries)
	}
}

// A cancelled context (producer closing) stops the retry promptly.
func TestReserveWithRetry_CtxCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &fakeReg{failN: 1 << 30}
	_, _, retries, err := reserveWithRetry(ctx, r, []unique.Claim{{}}, time.Minute, noSleep)
	if !errors.Is(err, unique.ErrUnavailable) || retries != 1 || r.calls != 1 {
		t.Fatalf("err=%v retries=%d calls=%d; want a single attempt then ctx-cancel exit", err, retries, r.calls)
	}
}
