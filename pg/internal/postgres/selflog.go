package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pglogrepl"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/nodestate"
)

// The self-origin log makes the exact changeset bytes the durability boundary
// (pg-coordination-model.md §3). After folding a local commit the orchestrator
// appends the built changeset here and fsyncs it BEFORE the slot's
// confirmed_flush is allowed past that commit, so a crash never forgets a
// shipped commit's exact bytes. Recovery replays those bytes verbatim — never
// re-deriving Dot/Stamp — which is what keeps a node convergent with peers when
// its local folds interleaved with remote applies that bumped the HLC (a
// re-derived stamp would be lower than the one peers already saw).
//
// Each entry's payload is the source commit LSN (8 bytes, big-endian) followed
// by the changeset wire bytes. The LSN lets recovery report the self-log head —
// the dedup boundary: the slot may re-deliver commits at or below it (when a
// standby ack lagged the append), and those are already shipped, so the
// orchestrator drops them rather than re-build a duplicate Dot.

func encodeSelfLogPayload(lsn pglogrepl.LSN, encoded []byte) []byte {
	out := make([]byte, 8+len(encoded))
	binary.BigEndian.PutUint64(out[:8], uint64(lsn))
	copy(out[8:], encoded)
	return out
}

func decodeSelfLogPayload(p []byte) (pglogrepl.LSN, []byte, error) {
	if len(p) < 8 {
		return 0, nil, fmt.Errorf("postgres: self-log payload too short (%d bytes)", len(p))
	}
	return pglogrepl.LSN(binary.BigEndian.Uint64(p[:8])), p[8:], nil
}

// recoverSelf replays the self-origin log into the Cache after a restart,
// restoring row state, the HLC, and the sender-seq counter from the EXACT
// shipped changeset bytes. PutRowState is DominatedBy-gated and ObserveHLC /
// ObserveSelfSeq are maxes, so replaying entries already covered by the loaded
// snapshot — or interleaving with RecoverMirror for remote origins — converges
// regardless of order, and re-running recovery is idempotent.
//
// Returns the highest source LSN in the log: the dedup boundary the orchestrator
// uses to skip re-delivered, already-shipped commits.
func recoverSelf(cache *nodestate.Cache, j *journal.Journal) (pglogrepl.LSN, error) {
	self := cache.Self()
	var headLSN pglogrepl.LSN
	head := j.Head()
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("postgres: replay self-log: %w", err)
		}
		if it.Offset() > head {
			break
		}
		if rec.Kind != journal.KindLocalDML || rec.Aborted() {
			continue
		}
		lsn, encoded, err := decodeSelfLogPayload(rec.Payload)
		if err != nil {
			return 0, err
		}
		cs, err := crdt.Decode(encoded)
		if err != nil {
			return 0, fmt.Errorf("postgres: decode self-log changeset: %w", err)
		}
		if cs.Dot.Origin != self {
			return 0, fmt.Errorf("postgres: self-log origin mismatch: cache=%d payload=%d", self, cs.Dot.Origin)
		}
		if lsn > headLSN {
			headLSN = lsn
		}
		cache.ObserveSelfSeq(cs.Dot.Origin, cs.Dot.Seq)
		cache.ObserveHLC(cs.Stamp.Clock)
		for _, r := range cs.Records {
			if _, ok := r.(crdt.BlobPatch); ok {
				continue // defensive: a BlobPatch never advances row_clock
			}
			h := r.Header()
			if !cache.RowState(h.Table, h.PK).DominatedBy(h.CL, cs.Stamp) {
				continue
			}
			cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL, Base: cs.Stamp})
		}
	}
	return headLSN, nil
}
