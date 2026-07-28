package sqlite

import (
	"context"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/publisher"
)

// SweepResult summarizes an on-demand retention sweep over a bucket.
type SweepResult struct {
	L0Deleted       int
	L1Deleted       int
	BaselineDeleted int
	MetadataDeleted int
}

// SweepBucket runs one retention pass over a topic-prefixed bucket on demand,
// instead of waiting for the elected publisher's periodic retention loop. It
// deletes L0/L1 objects superseded by L1/baseline coverage, superseded
// <stream>0009/ baselines below the active HEAD baseline — each only once aged
// past grace, so in-flight restore readers are unaffected. dryRun counts what
// would be deleted without deleting.
//
// Safe to run alongside the publisher's own retention loop: deletes are
// idempotent (an already-gone object is a no-op) and both sweeps key off the
// same HEAD horizon. Origin-epoch reclaim is intentionally skipped — it needs
// the live node's apply frontier, which an out-of-band caller lacks.
func SweepBucket(ctx context.Context, bucket objectstore.Bucket, grace time.Duration, dryRun bool) (SweepResult, error) {
	r := &publisher.Retention{Backend: bucket, Grace: grace, DryRun: dryRun}
	res, err := r.Sweep(ctx)
	return SweepResult{
		L0Deleted:       res.L0Deleted,
		L1Deleted:       res.L1Deleted,
		BaselineDeleted: res.BaselineDeleted,
		MetadataDeleted: res.MetadataDeleted,
	}, err
}
