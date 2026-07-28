package sqlite

import (
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/transport"
)

type FrontierEntry struct {
	LastSeq    uint64
	AppliedTip uint64
	LastHLC    uint64
}

// LastSubscribeError returns the most recent non-cancellation error
// from the broker's transport subscription loop. Diagnostics-only.
// Returns nil when no broker is configured.
func (n *Node) LastSubscribeError() error {
	if n.broker == nil {
		return nil
	}
	return n.broker.LastSubscribeError()
}

// OriginInboundHealth is one remote origin's inbound-apply state:
// AppliedSeq is the highest seq applied from that origin; AppliedTip is
// the cache's applied tip (>= AppliedSeq when gaps exist); LastHLC is
// the HLC of the last apply, packed as the wire encoding (same form as
// FrontierEntry.LastHLC); LastApplied is its wall-clock time, zero
// until the first apply this process.
type OriginInboundHealth struct {
	Origin      uint64
	AppliedSeq  uint64
	AppliedTip  uint64
	LastHLC     uint64
	LastApplied time.Time
}

// InboundHealth is a poll-friendly snapshot of the inbound apply path,
// intended for a ~30s health poll by the embedding orchestrator.
// ConsecutiveLocked is the apply-retry loop's current "database is
// locked" streak on the payload it is holding (0 = healthy);
// ApplyStalled indicates that streak has crossed the stall threshold —
// the wedged-inbound indicator. SelfHeals counts apply-connection
// state resets since start. LastSubscribeError mirrors
// Node.LastSubscribeError as a string ("" = none).
//
// QuarantineResident counts received-but-unapplied changesets parked
// in the apply quarantine (deterministic apply failures the frontier
// advanced past; each retries automatically once its missing
// dependency arrives). Transient non-zero values are normal during
// cross-origin delivery races; a steady-state resident count with
// QuarantineMaxAttempts climbing since QuarantineOldest means an entry
// can never apply and needs operator attention.
type InboundHealth struct {
	Origins               []OriginInboundHealth
	LastSubscribeError    string
	SchemaUnhealthy       bool
	SchemaUnhealthySeq    uint64
	SchemaUnhealthyReason string
	ConsecutiveLocked     int
	ApplyStalled          bool
	SelfHeals             uint64
	QuarantineResident    int
	QuarantineOldest      time.Time
	QuarantineMaxAttempts int64
}

// InboundHealth snapshots the broker's inbound-apply health. Returns
// the zero value when no broker is configured (single-node mode).
func (n *Node) InboundHealth() InboundHealth {
	if n.broker == nil {
		return InboundHealth{}
	}
	bh := n.broker.InboundHealth()
	out := InboundHealth{
		LastSubscribeError:    bh.LastSubscribeError,
		SchemaUnhealthy:       bh.SchemaUnhealthy,
		SchemaUnhealthySeq:    bh.SchemaUnhealthySeq,
		SchemaUnhealthyReason: bh.SchemaUnhealthyReason,
		ConsecutiveLocked:     bh.ConsecutiveLocked,
		ApplyStalled:          bh.ApplyStalled,
		SelfHeals:             bh.SelfHeals,
		QuarantineResident:    bh.QuarantineResident,
		QuarantineOldest:      bh.QuarantineOldest,
		QuarantineMaxAttempts: bh.QuarantineMaxAttempts,
	}
	if len(bh.Origins) > 0 {
		out.Origins = make([]OriginInboundHealth, len(bh.Origins))
		for i, o := range bh.Origins {
			out.Origins[i] = OriginInboundHealth{
				Origin:      uint64(o.Origin),
				AppliedSeq:  uint64(o.AppliedSeq),
				AppliedTip:  uint64(o.AppliedTip),
				LastHLC:     o.LastHLC.Pack(),
				LastApplied: o.LastApplied,
			}
		}
	}
	return out
}

// CoordinatedDuplicate is one coordinated unique value the leaseholder
// observed held by more than one live row — an out-of-gate duplicate
// (typically rows written on a partitioned node before the key was
// created, arriving afterwards). The reservation gate cannot repair it:
// grants for the value are fenced until the rows are fixed. Owners are
// the offending rows' canonical PK encodings. The fence lifts on the
// leaseholder's next clean enumeration after an operator deletes or
// updates the extra row(s); the runbook is in sqlite/docs/OPERATIONS.md
// ("Repairing a fenced coordinated duplicate").
type CoordinatedDuplicate struct {
	Table  [16]byte
	Key    [16]byte
	Value  []byte
	Owners [][]byte
	// TableName and KeyColumns are resolved from the local catalog for
	// operator display; empty if the table or key has since been dropped.
	TableName  string
	KeyColumns []string
}

// CoordinatedDuplicates reports the coordinated values currently fenced
// as duplicates (bounded; empty when healthy). Only the node holding the
// reservation lease observes them, so poll it fleet-wide and treat any
// non-empty result as needing operator attention.
func (n *Node) CoordinatedDuplicates() []CoordinatedDuplicate {
	if n.leaseholder == nil {
		return nil
	}
	dups := n.leaseholder.DuplicateValues()
	if len(dups) == 0 {
		return nil
	}
	out := make([]CoordinatedDuplicate, len(dups))
	for i, d := range dups {
		cd := CoordinatedDuplicate{Table: d.Table, Key: d.Key, Value: d.Value, Owners: d.Owners}
		if tab, ok := n.catalog.TableByID(crdt.TableID(d.Table)); ok {
			cd.TableName = tab.Name
			for _, uk := range tab.UniqueKeys {
				if uk.KeyID == crdt.KeyID(d.Key) {
					for _, c := range uk.Columns {
						cd.KeyColumns = append(cd.KeyColumns, c.Name)
					}
					break
				}
			}
		}
		out[i] = cd
	}
	return out
}

// UploadedSeq returns the highest seq of origin known to be durably
// uploaded to the configured ObjectBackend, or 0 when no sealer is
// configured. The self-journal GC gate is uploaded_seq[self], so this
// is the operator's view of "what's safe to forget locally." Origins
// not produced by this node always return 0 — peers seal their own
// records.
func (n *Node) UploadedSeq(origin uint64) uint64 {
	if n.sealer == nil {
		return 0
	}
	return n.sealer.UploadedSeq(origin)
}

// SchemaSeq returns the local meta.schema_seq (catalog generation).
// Used for debugging schema-chain catch-up state.
func (n *Node) SchemaSeq() uint64 {
	if n.meta == nil {
		return 0
	}
	seq, _, err := n.meta.GetSchemaSeq()
	if err != nil {
		return 0
	}
	return seq
}

// Frontier returns a snapshot of this node's per-origin frontier
// vector, including self. Peer entries come from the cache's
// contiguous-applied head plus applied_gaps tip; the self entry, when
// present, is synthesized from the highest seq the producer has
// allocated (cache.SenderNextSeq - 1) plus the latest HLC. Origin keys
// are the same opaque uint64 form Node.Origin returns.
//
// Self is omitted when this node has not yet written anything (no
// seq allocated). Peer entries are omitted when no records from that
// peer have ever been seen.
func (n *Node) Frontier() map[uint64]FrontierEntry {
	raw := n.cache.FrontierMap()
	out := make(map[uint64]FrontierEntry, len(raw)+1)
	for origin, entry := range raw {
		out[uint64(origin)] = FrontierEntry{
			LastSeq:    uint64(entry.LastSeq),
			AppliedTip: uint64(n.cache.AppliedTip(origin)),
			LastHLC:    entry.LastHLC.Pack(),
		}
	}
	self := n.originClaim.Origin
	if next := n.cache.SenderNextSeq(self); next > 1 {
		out[uint64(self)] = FrontierEntry{
			LastSeq:    uint64(next - 1),
			AppliedTip: uint64(next - 1),
			LastHLC:    n.cache.HLCLast().Pack(),
		}
	}
	return out
}

// PeerFrontiers reports each currently-connected peer's applied-frontier
// as last observed by the transport's frontier aggregation, one entry per
// peer — including peers whose observation is unknown (not yet queried) or
// errored, so fetch failures can never read as healthy. This is the
// idle-safe replication-lag signal: compare a peer's frontier against
// Frontier()/AppliedTip to measure lag without requiring a heartbeat
// writer on the topic. Consumers judge staleness from each entry's Age.
// Nil when the node has no peer-frontier capability (nil Transport, or a
// transport without transport.PeerFrontierBuilder).
func (n *Node) PeerFrontiers() []transport.FrontierObservation {
	if n.peerFrontier == nil {
		return nil
	}
	return n.peerFrontier.Observations()
}

// FrontierLen returns the count of remote origins tracked in the
// frontier — the origin-count signal for horizontal-scale observability.
// It reflects distinct origins ever applied (fleet churn), which origin
// GC bounds toward the live-peer count. Cheaper than len(Frontier())
// (no map build). See reapOrigins.
func (n *Node) FrontierLen() int { return n.cache.FrontierLen() }

// openWriterBarrier opens a fresh app.db connection and runs BEGIN
// IMMEDIATE to take the SQLite WAL writer slot. Concurrent writers
// across all connections (including extension processes) hit BUSY
// and retry under busy_timeout. busy_timeout on the barrier connection
// is set generously so transient contention from a peer-apply commit
// in flight doesn't fail the clone.
