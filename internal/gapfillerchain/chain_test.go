package gapfillerchain

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

// payload synthesizes the 17-byte wire prefix the chain uses to read
// (origin, seq). The body byte distinguishes payloads in tests.
func payload(origin crdt.Origin, seq crdt.Seq, tag byte) []byte {
	b := make([]byte, 18)
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], uint64(origin))
	binary.BigEndian.PutUint64(b[9:17], uint64(seq))
	b[17] = tag
	return b
}

type fakeFiller struct {
	deliver []byte
	err     error
	// seen records the ranges this filler was called with so tests can
	// confirm filler 2 only got the residual after filler 1 satisfied
	// part of the request.
	seen []transport.Range
}

func (f *fakeFiller) Fetch(_ context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	f.seen = append([]transport.Range(nil), ranges...)
	for i := 0; i+18 <= len(f.deliver); i += 18 {
		if err := apply(context.Background(), f.deliver[i:i+18]); err != nil {
			return err
		}
	}
	return f.err
}

func TestChain_PassesResidualToFallback(t *testing.T) {
	// Caller asks for [1,5]. Filler 1 delivers seqs {1,2,3}. Filler 2
	// should receive only [4,5] — the still-missing sub-range.
	f1 := &fakeFiller{deliver: concat(payload(7, 1, 0), payload(7, 2, 0), payload(7, 3, 0))}
	f2 := &fakeFiller{deliver: concat(payload(7, 4, 0), payload(7, 5, 0))}
	chain := New(f1, f2)
	var got []crdt.Seq
	err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 5}},
		func(_ context.Context, p []byte) error {
			got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
			return nil
		})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want := []crdt.Seq{1, 2, 3, 4, 5}
	if !seqEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if len(f2.seen) != 1 || f2.seen[0].Lo != 4 || f2.seen[0].Hi != 5 {
		t.Fatalf("f2 should have seen [4,5], saw %v", f2.seen)
	}
}

func TestChain_SkipsFallbackWhenFirstFillerCovers(t *testing.T) {
	f1 := &fakeFiller{deliver: concat(payload(7, 1, 0), payload(7, 2, 0))}
	f2 := &fakeFiller{}
	chain := New(f1, f2)
	if err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 2}},
		func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(f2.seen) != 0 {
		t.Fatalf("f2 should not have been called, saw %v", f2.seen)
	}
}

func TestChain_FallbackOnFirstFillerError(t *testing.T) {
	want := errors.New("peer unavailable")
	f1 := &fakeFiller{err: want}
	f2 := &fakeFiller{deliver: payload(7, 1, 0)}
	chain := New(f1, f2)
	var got []crdt.Seq
	err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
		func(_ context.Context, p []byte) error {
			got = append(got, crdt.Seq(binary.BigEndian.Uint64(p[9:17])))
			return nil
		})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !seqEqual(got, []crdt.Seq{1}) {
		t.Fatalf("got %v want [1]", got)
	}
}

func TestChain_ReturnsLastErrorWhenStillMissing(t *testing.T) {
	want := errors.New("s3 down")
	f1 := &fakeFiller{}
	f2 := &fakeFiller{err: want}
	chain := New(f1, f2)
	err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
		func(context.Context, []byte) error { return nil })
	if !errors.Is(err, want) {
		t.Fatalf("got err %v, want %v", err, want)
	}
}

func TestChain_NilFillerCollapses(t *testing.T) {
	f := &fakeFiller{deliver: payload(7, 1, 0)}
	chain := New(nil, f, nil)
	if err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
		func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if New(nil, nil) != nil {
		t.Fatalf("New(nil, nil) should return nil")
	}
	single := New(nil, f, nil)
	if single != transport.GapFiller(f) {
		t.Fatalf("single-filler chain should return the filler directly")
	}
}

func TestChain_FailedApplyDoesNotMarkSeen(t *testing.T) {
	// A corrupt payload from filler 1 returns an apply error. The
	// chain MUST NOT mark that seq as seen — otherwise filler 2 is
	// asked for a narrowed residual missing the seq the caller still
	// needs.
	f1 := &fakeFiller{deliver: payload(7, 1, 0)}
	f2 := &fakeFiller{deliver: payload(7, 1, 0)}
	chain := New(f1, f2)
	calls := 0
	err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 1}},
		func(_ context.Context, _ []byte) error {
			calls++
			if calls == 1 {
				return errors.New("simulated apply error")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(f2.seen) != 1 || f2.seen[0].Lo != 1 || f2.seen[0].Hi != 1 {
		t.Fatalf("f2 should have seen [1,1], saw %v", f2.seen)
	}
	if calls != 2 {
		t.Fatalf("apply called %d times, want 2 (one per filler)", calls)
	}
}

func TestChain_OpenEndedRangeForwardsToFallback(t *testing.T) {
	// Open-ended request: filler 1 covers some prefix; filler 2 must
	// still see the open-ended remainder (we can't prove "covered to
	// the end" without an upper bound).
	f1 := &fakeFiller{deliver: concat(payload(7, 1, 0), payload(7, 2, 0))}
	f2 := &fakeFiller{}
	chain := New(f1, f2)
	if err := chain.Fetch(context.Background(),
		[]transport.Range{{Origin: 7, Lo: 1, Hi: 0}},
		func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(f2.seen) != 1 {
		t.Fatalf("f2 should still have been called once, saw %v", f2.seen)
	}
	if f2.seen[0].Lo != 3 || !f2.seen[0].OpenEnded() {
		t.Fatalf("f2 should have seen open-ended [3,∞), saw %v", f2.seen[0])
	}
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func seqEqual(a, b []crdt.Seq) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
