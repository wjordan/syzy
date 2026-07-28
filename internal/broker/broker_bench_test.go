package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// BenchmarkApplyInsert measures one inbound applyPayload per iteration:
// decode + LWW vs Cache + app DML + Cache state advance. No transport in
// this path. Each iteration uses a unique (origin, seq) and PK so the
// apply path always runs the DML (no idempotent skip).
func BenchmarkApplyInsert(b *testing.B) {
	f := newApplier(b, 1, nil)

	// Pre-build b.N changeset payloads so the bench measures only the
	// apply path, not Build cost. Memory is O(b.N * payload-size); for
	// the standard event(id BLOB PK, n TEXT) workload a payload is ~80
	// bytes, so even -benchtime=1000000x stays under 100MB.
	payloads := make([][]byte, b.N)
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	for i := 0; i < b.N; i++ {
		var idVal [8]byte
		for j := 0; j < 8; j++ {
			idVal[j] = byte(i >> (8 * j))
		}
		cs := buildInsert(b, f.tab, crdt.Dot{Origin: src, Seq: crdt.Seq(i + 1)}, stamp, 1, idVal[:], "x")
		payloads[i] = cs.Encoded()
	}

	b.ReportAllocs()
	b.ResetTimer()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if err := f.br.applyPayload(ctx, payloads[i]); err != nil {
			b.Fatalf("applyPayload: %v", err)
		}
	}
}
