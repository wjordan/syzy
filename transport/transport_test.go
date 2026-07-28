package transport

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestRangeOpenEndedAndContains(t *testing.T) {
	r := Range{Origin: 1, Lo: 5, Hi: 10}
	if r.OpenEnded() {
		t.Errorf("[5,10] reported as open-ended")
	}
	for _, seq := range []crdt.Seq{5, 7, 10} {
		if !r.Contains(seq) {
			t.Errorf("Contains(%d) = false; want true", seq)
		}
	}
	for _, seq := range []crdt.Seq{0, 4, 11, 100} {
		if r.Contains(seq) {
			t.Errorf("Contains(%d) = true; want false", seq)
		}
	}

	open := Range{Origin: 1, Lo: 5, Hi: 0}
	if !open.OpenEnded() {
		t.Errorf("[5,_) reported as bounded")
	}
	for _, seq := range []crdt.Seq{5, 100, 1 << 40} {
		if !open.Contains(seq) {
			t.Errorf("open.Contains(%d) = false; want true", seq)
		}
	}
	if open.Contains(4) {
		t.Errorf("open.Contains(4) = true; want false")
	}
}
