package publisher

import (
	"sync"
	"time"

	"github.com/wjordan/syzy/internal/objstore"
)

// Stats is a snapshot of the publisher's local state, returned by
// (*Publisher).Stats(). Only populated on the node that currently
// holds the publisher lease.
type Stats struct {
	Generation         uint64        `json:"generation"`
	LeaseAcquisitions  uint64        `json:"lease_acquisitions"`
	LastAppBaseline    OpStat        `json:"last_app_baseline"`
	LastMetaBaseline   OpStat        `json:"last_meta_baseline"`
	LastL0             OpStat        `json:"last_l0"`      // app.db L0 emit
	LastMetaL0         OpStat        `json:"last_meta_l0"` // metadata.db L0 emit
	LastDBCompaction   CompactStat   `json:"last_db_compaction"`
	LastMetaCompaction CompactStat   `json:"last_meta_compaction"`
	LastRetention      RetentionStat `json:"last_retention"`
}

// OpStat captures one upload's outcome. SizeBytes is the bytes
// written to the bucket (post-compression for metadata).
type OpStat struct {
	TXID       uint64    `json:"txid,omitempty"`
	MinTXID    uint64    `json:"min_txid,omitempty"`
	MaxTXID    uint64    `json:"max_txid,omitempty"`
	SizeBytes  int64     `json:"size_bytes"`
	DurationMs int64     `json:"duration_ms"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// CompactStat captures the most recent compactor pass. Runs counts
// the L1 files produced by that pass; 0 is normal when there isn't
// yet a long-enough contiguous L0 run to compact.
type CompactStat struct {
	Stream          string    `json:"stream,omitempty"`
	L0Files         int       `json:"l0_files"`
	L1Files         int       `json:"l1_files"`
	BaselineTXID    uint64    `json:"baseline_txid,omitempty"`
	L0ScanAfterTXID uint64    `json:"l0_scan_after_txid,omitempty"`
	BaselineSkipped int       `json:"baseline_skipped"`
	CoveredSkipped  int       `json:"covered_skipped"`
	EligibleFiles   int       `json:"eligible_files"`
	Runs            int       `json:"runs"`
	InputFiles      int       `json:"input_files"`
	DurationMs      int64     `json:"duration_ms"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
	Error           string    `json:"error,omitempty"`
}

// RetentionStat captures the most recent retention sweep.
type RetentionStat struct {
	L0Deleted       int       `json:"l0_deleted"`
	L1Deleted       int       `json:"l1_deleted"`
	MetadataDeleted int       `json:"metadata_deleted"`
	DurationMs      int64     `json:"duration_ms"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
	Error           string    `json:"error,omitempty"`
}

// statsTracker serializes writes from publisher goroutines against
// snapshot reads from HTTP handlers.
type statsTracker struct {
	mu sync.Mutex
	s  Stats
}

func (t *statsTracker) snapshot() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.s
}

func (t *statsTracker) recordLeaseClaim(generation uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.Generation = generation
	t.s.LeaseAcquisitions++
}

func (t *statsTracker) recordAppBaseline(txid uint64, size int64, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastAppBaseline = OpStat{
		TXID:       txid,
		SizeBytes:  size,
		DurationMs: dur.Milliseconds(),
		FinishedAt: time.Now(),
		Error:      errString(err),
	}
}

func (t *statsTracker) recordMetaBaseline(txid uint64, size int64, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastMetaBaseline = OpStat{
		TXID:       txid,
		SizeBytes:  size,
		DurationMs: dur.Milliseconds(),
		FinishedAt: time.Now(),
		Error:      errString(err),
	}
}

func (t *statsTracker) recordL0(minTXID, maxTXID uint64, size int64, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastL0 = OpStat{
		MinTXID:    minTXID,
		MaxTXID:    maxTXID,
		SizeBytes:  size,
		DurationMs: dur.Milliseconds(),
		FinishedAt: time.Now(),
		Error:      errString(err),
	}
}

func (t *statsTracker) recordMetaL0(minTXID, maxTXID uint64, size int64, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastMetaL0 = OpStat{
		MinTXID:    minTXID,
		MaxTXID:    maxTXID,
		SizeBytes:  size,
		DurationMs: dur.Milliseconds(),
		FinishedAt: time.Now(),
		Error:      errString(err),
	}
}

func (t *statsTracker) recordCompaction(stream string, res CompactionResult, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stat := CompactStat{
		Stream:          stream,
		L0Files:         res.L0Files,
		L1Files:         res.L1Files,
		BaselineTXID:    res.BaselineTXID,
		L0ScanAfterTXID: res.L0ScanAfterTXID,
		BaselineSkipped: res.BaselineSkipped,
		CoveredSkipped:  res.CoveredSkipped,
		EligibleFiles:   res.EligibleFiles,
		Runs:            res.Runs,
		InputFiles:      res.InputFiles,
		DurationMs:      dur.Milliseconds(),
		FinishedAt:      time.Now(),
		Error:           errString(err),
	}
	switch stream {
	case objstore.DBPrefix:
		t.s.LastDBCompaction = stat
	case objstore.MetadataPrefix:
		t.s.LastMetaCompaction = stat
	}
}

func (t *statsTracker) recordRetention(res Result, dur time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.LastRetention = RetentionStat{
		L0Deleted:       res.L0Deleted,
		L1Deleted:       res.L1Deleted,
		MetadataDeleted: res.MetadataDeleted,
		DurationMs:      dur.Milliseconds(),
		FinishedAt:      time.Now(),
		Error:           errString(err),
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
