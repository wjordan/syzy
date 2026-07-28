package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	syzy "github.com/wjordan/syzy/sqlite"
)

const s3gcUsage = `usage: syzy s3-gc --bucket <s3://...> [--grace 24h] [--dry-run]

  --bucket   target bucket prefix (s3:// or file:// for tests)
  --grace    minimum age before deleting superseded objects (default 24h)
  --dry-run  print what would be deleted, don't delete

Sweeps the LTX bucket layout:
  - L0 files covered by L1, after the covering L1 is older than --grace.
  - L0/L1 files at or below the active baseline, after the baseline is
    older than --grace.
  - metadata files at metadata/ whose TXID is below the baseline's
    TXID AND older than --grace.

Idempotent: re-runnable. Multipart upload aborts should be configured
at the bucket level via S3 Lifecycle (24h is recommended).
`

func s3GCCmd(args []string) error {
	fs := flag.NewFlagSet("s3-gc", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, s3gcUsage) }
	bucket := fs.String("bucket", "", "target bucket URL")
	grace := fs.String("grace", "24h", "minimum age before deletion")
	dryRun := fs.Bool("dry-run", false, "report without deleting")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *bucket == "" {
		fs.Usage()
		return errors.New("--bucket required")
	}
	graceDur, err := parseDuration(*grace)
	if err != nil {
		return fmt.Errorf("--grace: %w", err)
	}
	be, err := openBucketBackend(*bucket)
	if err != nil {
		return err
	}
	res, err := syzy.SweepBucket(context.Background(), be, graceDur, *dryRun)
	if err != nil {
		return err
	}
	mode := ""
	if *dryRun {
		mode = " (dry-run)"
	}
	fmt.Printf("s3-gc%s: l0_deleted=%d l1_deleted=%d baseline_deleted=%d metadata_deleted=%d\n",
		mode, res.L0Deleted, res.L1Deleted, res.BaselineDeleted, res.MetadataDeleted)
	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		var n int
		if _, err := fmt.Sscanf(days, "%d", &n); err != nil {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
