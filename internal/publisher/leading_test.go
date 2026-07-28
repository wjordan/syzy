package publisher

import "testing"

// TestPublisherLeading: Leading reflects the leading flag that Run sets after
// claiming the lease and clears on exit. The standby WAL checkpoint reads this
// to stand down while a node is the active publisher.
func TestPublisherLeading(t *testing.T) {
	p := &Publisher{}
	if p.Leading() {
		t.Fatal("zero-value publisher must not report leading")
	}
	p.leading.Store(true)
	if !p.Leading() {
		t.Fatal("Leading should report true once the flag is set")
	}
	p.leading.Store(false)
	if p.Leading() {
		t.Fatal("Leading should report false once the flag is cleared")
	}
}
