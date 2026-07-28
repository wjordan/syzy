package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// blockingTransport is the minimal Transport for fetcher tests:
// Broadcast no-ops, Subscribe blocks until ctx is cancelled. Fetch and
// gap-fill behavior live on a separate fakeGapFiller wired via
// broker.Config.GapFiller.
type blockingTransport struct{}

func (blockingTransport) Broadcast(_ context.Context, _ []byte) error { return nil }
func (blockingTransport) Subscribe(ctx context.Context, _ transport.ApplyFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

// fakeGapFiller is a transport.GapFiller whose Fetch records the
// requested ranges and replays caller-staged payloads through apply.
// Lets the fetcher unit tests drive arbitrary repair scenarios without
// standing up a real peer.
type fakeGapFiller struct {
	mu sync.Mutex
	// rounds is appended-to per Fetch call.
	rounds []fakeRound
	// replyByOrigin is consulted on each Fetch: matching ranges'
	// payloads are passed through apply.
	replyByOrigin map[crdt.Origin][]fakeReply
	// fetchErr, when non-nil, is returned from Fetch after replies
	// run. Used to test error-path bookkeeping.
	fetchErr error
}

type fakeRound struct {
	ranges []transport.Range
}

type fakeReply struct {
	seq     crdt.Seq
	payload []byte
}

func newFakeGapFiller() *fakeGapFiller {
	return &fakeGapFiller{
		replyByOrigin: map[crdt.Origin][]fakeReply{},
	}
}

func (f *fakeGapFiller) Fetch(ctx context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	f.mu.Lock()
	rangesCp := append([]transport.Range(nil), ranges...)
	f.rounds = append(f.rounds, fakeRound{ranges: rangesCp})
	replies := map[crdt.Origin][]fakeReply{}
	for o, rs := range f.replyByOrigin {
		replies[o] = append(replies[o], rs...)
	}
	err := f.fetchErr
	f.mu.Unlock()
	for _, r := range ranges {
		for _, reply := range replies[r.Origin] {
			if reply.seq < r.Lo || reply.seq > r.Hi {
				continue
			}
			if applyErr := apply(ctx, reply.payload); applyErr != nil {
				return applyErr
			}
		}
	}
	return err
}

func (f *fakeGapFiller) addReply(origin crdt.Origin, seq crdt.Seq, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replyByOrigin[origin] = append(f.replyByOrigin[origin], fakeReply{seq: seq, payload: payload})
}

func (f *fakeGapFiller) roundsSnapshot() []fakeRound {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRound(nil), f.rounds...)
}

// TestFetcherFillsInternalGap is the canonical scenario: a live
// broadcast delivers seq 5 first, creating applied_gaps={[5,5]} above
// frontier=0. The local cache's AppliedTip for origin 7 is now 5.
// The fetcher computes missing=[1,4], asks GapFiller, which replays the
// four payloads through apply directly — the broker's
// applyPayloadWithRetry path serializes them with the (idle) Subscribe
// loop via the cache mutex + sqlite writer slot. Frontier promotes to 5.
func TestFetcherFillsInternalGap(t *testing.T) {
	t.Parallel()
	gap := newFakeGapFiller()
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap))

	src := crdt.Origin(7)

	// Pre-stage replies for seqs 1..4. Build real changesets so
	// applyPayload is happy with them.
	for seq := crdt.Seq(1); seq <= 4; seq++ {
		stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + seq)}, Origin: src}
		cs := buildInsert(t, f.tab,
			crdt.Dot{Origin: src, Seq: seq},
			stamp, 1, []byte{byte(seq)}, "v")
		gap.addReply(src, seq, cs.Encoded())
	}

	// Apply seq 5 directly — this is the live broadcast that creates
	// the gap. Use applyPayload so we go through the gap-creation
	// wake check (frontier was 0; seq 5 > 1, so wake fires) and the
	// cache's AppliedTip advances to 5.
	stamp5 := crdt.Stamp{Clock: crdt.Clock{WallTime: 5000}, Origin: src}
	cs5 := buildInsert(t, f.tab,
		crdt.Dot{Origin: src, Seq: 5},
		stamp5, 1, []byte{0x05}, "v")
	if err := f.br.applyPayload(context.Background(), cs5.Encoded()); err != nil {
		t.Fatalf("apply seq=5: %v", err)
	}

	// Tight test interval so the timer doesn't dominate.
	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 5*time.Millisecond, 5*time.Second, 32

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if front, ok := f.cache.FrontierFor(src); ok && front.LastSeq == 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	front, _ := f.cache.FrontierFor(src)
	t.Fatalf("frontier never reached 5; got %+v after rounds=%d", front, len(gap.roundsSnapshot()))
}

// TestFetcherNoOpWhenNothingMissing: when there's nothing missing
// (cache.AppliedTip == frontier for every observed origin), the
// planner skips Fetch and leaves its interval at base. We can observe
// this indirectly by confirming no Fetch calls happen.
func TestFetcherNoOpWhenNothingMissing(t *testing.T) {
	gap := newFakeGapFiller()
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap))

	// Apply seq 1 contiguously. frontier = tip = 1.
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs := buildInsert(t, f.tab,
		crdt.Dot{Origin: src, Seq: 1},
		stamp, 1, []byte{0x01}, "v")
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply: %v", err)
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 5*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	// Wait long enough for several rounds; expect zero Fetches.
	time.Sleep(50 * time.Millisecond)
	if got := len(gap.roundsSnapshot()); got != 0 {
		t.Errorf("rounds = %d; want 0 (nothing missing → no Fetch calls)", got)
	}
}

// TestFetcherSkipsSelfOrigin: the planner must never emit ranges for
// the self origin. Self seqs are contiguous by producer construction;
// any apparent gap is a bug, not something Fetch can repair. We
// allocate a few self seqs to populate cache.SenderNextSeq[self] and
// confirm no self ranges appear in any Fetch round.
func TestFetcherSkipsSelfOrigin(t *testing.T) {
	gap := newFakeGapFiller()
	f := newApplier(t, crdt.Origin(7), blockingTransport{}, withGapFiller(gap))

	for i := 0; i < 3; i++ {
		_ = f.cache.AllocSelfSeq(f.cache.Self())
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 5*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	time.Sleep(50 * time.Millisecond)
	for _, r := range gap.roundsSnapshot() {
		for _, rg := range r.ranges {
			if rg.Origin == crdt.Origin(7) {
				t.Errorf("planner emitted self-origin range %+v", rg)
			}
		}
	}
}

// TestFetcherPropagatesTransportError: a Fetch error is recorded in
// LastSubscribeError so operators can see the planner is wedged.
func TestFetcherPropagatesTransportError(t *testing.T) {
	t.Parallel()
	gap := newFakeGapFiller()
	gap.fetchErr = errors.New("transport down")
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap))

	// Apply seq 5 to create a gap (frontier=0, tip=5) so the fetcher
	// has a real range to ask for.
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 5000}, Origin: src}
	cs := buildInsert(t, f.tab,
		crdt.Dot{Origin: src, Seq: 5},
		stamp, 1, []byte{0x05}, "v")
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("apply seq=5: %v", err)
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 5*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if err := f.br.LastSubscribeError(); err != nil && err.Error() == "transport down" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("LastSubscribeError never surfaced fetch error; got %v", f.br.LastSubscribeError())
}

// TestConcurrentApplySerialized stresses the AppApply txn boundary by
// driving many applyPayload goroutines simultaneously. Production runs
// the subscribe loop, the gap-fill fetcher, and schema-catchup against
// the same single AppApply Conn (sqlitebridge.Conn is not safe for
// concurrent use); without applyMu, two BEGIN IMMEDIATE calls racing on
// the same conn produce "cannot start a transaction within a
// transaction" and corrupt prepared-statement state. With the lock, all
// payloads land cleanly and frontier reaches the contiguous tip.
func TestConcurrentApplySerialized(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, blockingTransport{})
	src := crdt.Origin(7)

	const N = 64
	payloads := make([][]byte, N)
	for i := 0; i < N; i++ {
		seq := crdt.Seq(i + 1)
		stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + i)}, Origin: src}
		cs := buildInsert(t, f.tab,
			crdt.Dot{Origin: src, Seq: seq},
			stamp, 1, []byte{byte(i + 1)}, "v")
		payloads[i] = cs.Encoded()
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, N)
	ctx := context.Background()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			for i := off; i < N; i += goroutines {
				if err := f.br.applyPayload(ctx, payloads[i]); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("apply: %v", err)
	}

	for i := 0; i < N; i++ {
		if !f.cache.IsAppliedRemote(src, crdt.Seq(i+1)) {
			t.Errorf("seq %d not marked applied", i+1)
		}
	}
	front, ok := f.cache.FrontierFor(src)
	if !ok || front.LastSeq != crdt.Seq(N) {
		t.Errorf("FrontierFor(%d) = %+v ok=%v; want LastSeq=%d", src, front, ok, N)
	}
}

// fakeTipCoverageSource reports fixed tips and coverage claims, standing
// in for the s3fetch source (possibly wrapped in mergedTips).
type fakeTipCoverageSource struct {
	tips     map[crdt.Origin]crdt.Seq
	coverage map[crdt.Origin][]transport.Range
}

func (f *fakeTipCoverageSource) DiscoverTips(context.Context) (map[crdt.Origin]crdt.Seq, error) {
	out := map[crdt.Origin]crdt.Seq{}
	for o, t := range f.tips {
		out[o] = t
	}
	return out, nil
}

func (f *fakeTipCoverageSource) Coverage(context.Context) (map[crdt.Origin][]transport.Range, error) {
	out := map[crdt.Origin][]transport.Range{}
	for o, iv := range f.coverage {
		out[o] = iv
	}
	return out, nil
}

// TestFetcherDemotesUnserveable: a missing range the bucket's surviving
// epochs cannot cover gets exactly one immediate full-chain probe (peers
// might hold it), then leaves the per-round plan — no re-fetch every
// round, and a probe coming back empty (or erroring "0 frames") never
// lands in LastSubscribeError / the fetch-round WARN path. KickFetcher
// (the peer-connect hook) re-arms the probe.
func TestFetcherDemotesUnserveable(t *testing.T) {
	gap := newFakeGapFiller()
	gap.fetchErr = fmt.Errorf("mux: catchup x: peer delivered 0 frames: %w", transport.ErrUnfilled)
	src := crdt.Origin(7)
	tips := &fakeTipCoverageSource{
		tips:     map[crdt.Origin]crdt.Seq{src: 5},
		coverage: map[crdt.Origin][]transport.Range{src: {{Origin: src, Lo: 5, Hi: 6}}}, // [1,4] uncovered
	}
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap), withTipSource(tips))

	// Live-apply seq 5: frontier 0, gap [1,4], tip 5.
	stamp5 := crdt.Stamp{Clock: crdt.Clock{WallTime: 5000}, Origin: src}
	cs5 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 5}, stamp5, 1, []byte{0x05}, "v")
	if err := f.br.applyPayload(context.Background(), cs5.Encoded()); err != nil {
		t.Fatalf("apply seq=5: %v", err)
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 30*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	// Many rounds elapse; only the immediate first probe may Fetch.
	time.Sleep(250 * time.Millisecond)
	rounds := gap.roundsSnapshot()
	if len(rounds) != 1 {
		t.Fatalf("Fetch rounds = %d; want exactly 1 (one immediate probe, then demoted)", len(rounds))
	}
	want := transport.Range{Origin: src, Lo: 1, Hi: 4}
	if len(rounds[0].ranges) != 1 || rounds[0].ranges[0] != want {
		t.Errorf("probe ranges = %+v; want [%+v]", rounds[0].ranges, want)
	}
	if err := f.br.LastSubscribeError(); err != nil {
		t.Errorf("LastSubscribeError = %v; probe outcomes must not be recorded as errors", err)
	}
	f.br.fetchErrMu.Lock()
	warned := f.br.fetchErrMsg
	f.br.fetchErrMu.Unlock()
	if warned != "" {
		t.Errorf("fetchErrMsg = %q; a clean-empty (ErrUnfilled) probe must not WARN", warned)
	}

	// Peer connect re-arms the probe.
	f.br.KickFetcher()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(gap.roundsSnapshot()) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("KickFetcher did not trigger a re-probe; rounds=%d", len(gap.roundsSnapshot()))
}

// TestFetcherProbeFillsFromPeer: demotion must not strand a range a peer
// still holds — the immediate first probe drains it through the normal
// apply path and the frontier converges.
func TestFetcherProbeFillsFromPeer(t *testing.T) {
	gap := newFakeGapFiller()
	src := crdt.Origin(7)
	tips := &fakeTipCoverageSource{
		tips:     map[crdt.Origin]crdt.Seq{src: 5},
		coverage: map[crdt.Origin][]transport.Range{src: {{Origin: src, Lo: 5, Hi: 6}}},
	}
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap), withTipSource(tips))

	for seq := crdt.Seq(1); seq <= 4; seq++ {
		stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + seq)}, Origin: src}
		cs := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: seq}, stamp, 1, []byte{byte(seq)}, "v")
		gap.addReply(src, seq, cs.Encoded())
	}
	stamp5 := crdt.Stamp{Clock: crdt.Clock{WallTime: 5000}, Origin: src}
	cs5 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 5}, stamp5, 1, []byte{0x05}, "v")
	if err := f.br.applyPayload(context.Background(), cs5.Encoded()); err != nil {
		t.Fatalf("apply seq=5: %v", err)
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 30*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if front, ok := f.cache.FrontierFor(src); ok && front.LastSeq == 5 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	front, _ := f.cache.FrontierFor(src)
	t.Fatalf("frontier never reached 5 via probe; got %+v", front)
}

// TestFetcherProbeSubstantiveErrorWarns: only clean-empty (ErrUnfilled)
// probe results demote to INFO; a real failure — decode, dial, corruption —
// keeps its WARN visibility via the rate-limited fetch-error path.
func TestFetcherProbeSubstantiveErrorWarns(t *testing.T) {
	gap := newFakeGapFiller()
	gap.fetchErr = errors.New("tls: certificate expired")
	src := crdt.Origin(7)
	tips := &fakeTipCoverageSource{
		tips:     map[crdt.Origin]crdt.Seq{src: 5},
		coverage: map[crdt.Origin][]transport.Range{src: {{Origin: src, Lo: 5, Hi: 6}}},
	}
	f := newApplier(t, 1, blockingTransport{}, withGapFiller(gap), withTipSource(tips))

	stamp5 := crdt.Stamp{Clock: crdt.Clock{WallTime: 5000}, Origin: src}
	cs5 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 5}, stamp5, 1, []byte{0x05}, "v")
	if err := f.br.applyPayload(context.Background(), cs5.Encoded()); err != nil {
		t.Fatalf("apply seq=5: %v", err)
	}

	f.br.fetcherInterval, f.br.fetcherMaxInterval, f.br.fetcherMaxRanges = 30*time.Millisecond, 5*time.Second, 32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.br.fetchErrMu.Lock()
		warned := f.br.fetchErrMsg
		f.br.fetchErrMu.Unlock()
		if warned != "" {
			if !strings.Contains(warned, "unserveable-range probe") || !strings.Contains(warned, "certificate expired") {
				t.Fatalf("fetchErrMsg = %q; want unserveable-range probe WARN with the real error", warned)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("substantive probe error never reached the WARN path")
}
