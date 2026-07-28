// Package objstore defines the on-bucket layout for syzy's physical
// (LTX) replication, logical (per-origin epoch) replication, schema
// chain, and CRDT-state metadata checkpoints.
//
// Layout (all keys relative to the configured bucket/prefix; the
// cluster ID lives inside HEAD, not in the key path):
//
//	HEAD                                                  # mutable JSON
//	db/<level-4hex>/<min_txid>-<max_txid>.ltx             # immutable
//	metadata/<level-4hex>/<min_txid>-<max_txid>.ltx       # immutable
//	origins/<origin-hex>/epoch-<lo_seq>-<hi_seq>.zst      # immutable
//	events/<schema_seq>.bin                               # immutable (schemalog)
//
// # Streams
//
// Two LTX streams share one mechanism. db/ ships app.db (the user's
// database); metadata/ ships metadata.db (CRDT clocks, frontier,
// schema catalog). Both run L0/L1/baseline through the same Tailer →
// PublishLTX path; they differ only in which file is being tailed
// and which prefix the LTXes land under.
//
//   - db/ — app.db physical replication. LTX files (Litestream-
//     compatible). Levels match Litestream's convention:
//     0000 = L0 (per-tick deltas, ~1s cadence)
//     0001 = L1 (compacted runs, ~30min cadence)
//     0009 = baseline (full-DB snapshot LTX, on takeover).
//     Single producer (the elected publisher); multiple consumers.
//   - metadata/ — metadata.db physical replication. Same layout as
//     db/. Same producer. Steady-state egress is bytes-touched-per-
//     commit instead of full-file-per-tick.
//   - origins/ — logical replication. Each origin writes its own
//     prefix; multiple producers, one per origin.
//   - events/ — replicated DDL chain (the schemalog package owns this
//     prefix). Multiple producers (every node contributing a DDL).
//
// # Coordination
//
// One CAS-mutable object: HEAD. It carries (a) cluster_id beacon,
// (b) app.db baseline pointer, (c) metadata.db baseline pointer,
// (d) the publisher lease.
// The publisher holds a CAS-renewed lease in HEAD's publisher field;
// non-publisher nodes attempt CAS-takeover
// when the lease expires, then write a fresh coupled baseline. A new
// bucket gets its cluster_id minted by the first opener via
// If-None-Match CAS on HEAD.
//
// The cross-stream invariant: every metadata/ LTX stamps its
// matching app-side TXID via meta.parent_app_txid inside the file's
// pages (the snapshotter writes it inside the same metadata.db tx
// after draining the app.db tailer). Restore replays the metadata
// chain to its tip, reads parent_app_txid, and applies the db/ chain
// through that value before considering the restore consistent.
//
// # TXID
//
// TXID in this package is bucket-relative: a monotonic counter owned
// by whichever node is the current publisher, persisted in
// metadata.db.meta. A publisher takeover seeds its counter from
// max(L0 TXID in bucket) + 1 and writes a fresh baseline at that
// TXID. The chain is therefore byte-monotonic across publisher
// rotations, which keeps Litestream-followers happy.
//
// # Litestream compatibility
//
// db/<level>/<min>-<max>.ltx matches Litestream's S3 path convention,
// and our LTX is encoded with HeaderFlagNoChecksum (matching what
// Litestream writes — necessary for Litestream's restore-side compactor
// to validate the merged output). `litestream restore` pointed at a
// syzy bucket produces a valid app.db; metadata.db is syzy-specific and
// not recovered by Litestream tooling — the eject hatch is one-way.
//
// Object key helpers below are pure functions; nothing in this file
// touches the network.
package objstore

import (
	"fmt"
	"strconv"
	"strings"
)

// L0Level, L1Level, BaselineLevel are the level-numbers stamped into
// db/<level-4hex>/. Match Litestream's convention: L0 is the freshest
// per-tick stream, L1 is compacted, BaselineLevel is the snapshot
// level (Litestream hard-codes SnapshotLevel = 9).
const (
	L0Level       = 0x0000
	L1Level       = 0x0001
	BaselineLevel = 9
)

const (
	// HEADKey is the bucket-wide mutable pointer (manifest + lease).
	HEADKey = "HEAD"

	// DBPrefix is the prefix for physical-replication LTX files.
	DBPrefix = "db/"

	// MetadataPrefix is the prefix for metadata.db checkpoint files.
	MetadataPrefix = "metadata/"

	// OriginsPrefix is the prefix for per-origin logical epoch objects.
	OriginsPrefix = "origins/"

	// EventsPrefix is where the schemalog package writes DDL events.
	// Owned by schemalog/s3.go; named here so Classify can label it.
	EventsPrefix = "events/"
)

// LTXKey returns the object key for an LTX file at the given stream
// prefix and level covering [minTXID, maxTXID]. Level is stamped as
// 4 hex digits to match Litestream's path convention.
//
// streamPrefix is one of DBPrefix or MetadataPrefix.
func LTXKey(streamPrefix string, level int, minTXID, maxTXID uint64) string {
	return fmt.Sprintf("%s%04x/%016x-%016x.ltx", streamPrefix, level, minTXID, maxTXID)
}

// LTXLevelPrefix returns the LIST prefix for one LTX level inside one
// stream. streamPrefix is one of DBPrefix or MetadataPrefix.
func LTXLevelPrefix(streamPrefix string, level int) string {
	return fmt.Sprintf("%s%04x/", streamPrefix, level)
}

// EpochKey returns the object key for an epoch carrying Changesets
// [loSeq, hiSeq] of origin (16 hex chars).
func EpochKey(originHex string, loSeq, hiSeq uint64) string {
	return fmt.Sprintf("%s%s/epoch-%016x-%016x.zst", OriginsPrefix, originHex, loSeq, hiSeq)
}

// OriginPrefixOf returns the LIST prefix for one origin's epochs.
func OriginPrefixOf(originHex string) string {
	return fmt.Sprintf("%s%s/", OriginsPrefix, originHex)
}

// ParseOriginEpochKey extracts (originHex, loSeq, hiSeq) from an
// origins/<originHex>/epoch-<016x>-<016x>.zst key. Returns ok=false for
// keys that don't match the epoch layout. Inverse of EpochKey.
func ParseOriginEpochKey(key string) (originHex string, lo, hi uint64, ok bool) {
	rest, ok := strings.CutPrefix(key, OriginsPrefix)
	if !ok {
		return "", 0, 0, false
	}
	originHex, after, ok := strings.Cut(rest, "/")
	if !ok || len(originHex) != 16 {
		return "", 0, 0, false
	}
	body, ok := strings.CutPrefix(after, "epoch-")
	if !ok {
		return "", 0, 0, false
	}
	body = strings.TrimSuffix(body, ".zst")
	loStr, hiStr, ok := strings.Cut(body, "-")
	if !ok || len(loStr) != 16 || len(hiStr) != 16 {
		return "", 0, 0, false
	}
	lo, err := strconv.ParseUint(loStr, 16, 64)
	if err != nil {
		return "", 0, 0, false
	}
	hi, err = strconv.ParseUint(hiStr, 16, 64)
	if err != nil {
		return "", 0, 0, false
	}
	return originHex, lo, hi, true
}

// Classify maps a bucket key to a short label for metering and
// observability. Unknown keys fall through to "other". The label set
// matches the layout above plus "other".
func Classify(key string) string {
	switch {
	case key == HEADKey:
		return "head"
	case strings.HasPrefix(key, DBPrefix):
		return "db"
	case strings.HasPrefix(key, MetadataPrefix):
		return "metadata"
	case strings.HasPrefix(key, OriginsPrefix):
		return "origins"
	case strings.HasPrefix(key, EventsPrefix):
		return "events"
	default:
		return "other"
	}
}
