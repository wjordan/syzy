package sqlite

import (
	"time"

	internalpublisher "github.com/wjordan/syzy/internal/publisher"
)

// PublisherStats is a snapshot of the active physical publisher.
type PublisherStats struct {
	Generation         uint64                  `json:"generation"`
	LeaseAcquisitions  uint64                  `json:"lease_acquisitions"`
	LastAppBaseline    PublisherOpStat         `json:"last_app_baseline"`
	LastMetaBaseline   PublisherOpStat         `json:"last_meta_baseline"`
	LastL0             PublisherOpStat         `json:"last_l0"`
	LastMetaL0         PublisherOpStat         `json:"last_meta_l0"`
	LastDBCompaction   PublisherCompactionStat `json:"last_db_compaction"`
	LastMetaCompaction PublisherCompactionStat `json:"last_meta_compaction"`
	LastRetention      PublisherRetentionStat  `json:"last_retention"`
}

type PublisherOpStat struct {
	TXID       uint64    `json:"txid,omitempty"`
	MinTXID    uint64    `json:"min_txid,omitempty"`
	MaxTXID    uint64    `json:"max_txid,omitempty"`
	SizeBytes  int64     `json:"size_bytes"`
	DurationMs int64     `json:"duration_ms"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type PublisherCompactionStat struct {
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

type PublisherRetentionStat struct {
	L0Deleted       int       `json:"l0_deleted"`
	L1Deleted       int       `json:"l1_deleted"`
	MetadataDeleted int       `json:"metadata_deleted"`
	DurationMs      int64     `json:"duration_ms"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
	Error           string    `json:"error,omitempty"`
}

func publicPublisherStats(s internalpublisher.Stats) PublisherStats {
	return PublisherStats{
		Generation:         s.Generation,
		LeaseAcquisitions:  s.LeaseAcquisitions,
		LastAppBaseline:    PublisherOpStat(s.LastAppBaseline),
		LastMetaBaseline:   PublisherOpStat(s.LastMetaBaseline),
		LastL0:             PublisherOpStat(s.LastL0),
		LastMetaL0:         PublisherOpStat(s.LastMetaL0),
		LastDBCompaction:   PublisherCompactionStat(s.LastDBCompaction),
		LastMetaCompaction: PublisherCompactionStat(s.LastMetaCompaction),
		LastRetention:      PublisherRetentionStat(s.LastRetention),
	}
}
