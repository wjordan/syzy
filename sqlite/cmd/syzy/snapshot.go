package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/clone"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/objstore"
)

const (
	offlineSnapshotLeaseDuration  = 30 * time.Minute
	offlineSnapshotReleaseTimeout = 5 * time.Second
)

const snapshotUsage = `usage: syzy snapshot --db <path/to/app.db> --bucket <s3://...>

  --db      app.db path of a stopped syzy node
  --bucket  destination URL: s3://my-bucket/<cluster-id>
            or               file:///abs/path/to/dir for testing

Reads metadata.db to discover cluster_id, reserves the target bucket,
derives its TXID counter, runs sqlite3_backup into temp copies, encodes
app.db as a baseline LTX (snapshot LTX with MinTXID=1), uploads it
plus metadata.db to db/0009/ + metadata/, and CASes HEAD.

The source daemon must be stopped (refuses if daemon.lock is held), and the
target bucket must not have an active publisher lease. The target reservation
is bounded to 30 minutes and is exact-released on every exit.
Online publishing — talking to a running daemon over its TCP port —
is the responsibility of Node.PublishSnapshot.
`

func snapshotCmd(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, snapshotUsage) }
	dbPath := fs.String("db", "", "app.db path of a stopped syzy node")
	bucket := fs.String("bucket", "", "destination bucket URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *bucket == "" {
		fs.Usage()
		return errors.New("--db and --bucket required")
	}

	claim, err := layout.ClaimDaemon(*dbPath)
	if errors.Is(err, layout.ErrDaemonLocked) {
		return fmt.Errorf("source %s has a running daemon; stop it before running snapshot (use Node.PublishSnapshot for online publishing)", *dbPath)
	}
	if err != nil {
		return fmt.Errorf("inspect daemon lock at %s: %w", *dbPath, err)
	}
	defer claim.Release()

	be, err := openBucketBackend(*bucket)
	if err != nil {
		return err
	}

	clusterID, err := readClusterID(*dbPath)
	if err != nil {
		return fmt.Errorf("read cluster_id: %w", err)
	}
	var txid uint64
	var appSize, metaSize int
	if err := func() (retErr error) {
		opCtx, cancel := context.WithTimeout(context.Background(), offlineSnapshotLeaseDuration)
		defer cancel()
		reservation, err := objstore.AcquireMaintenanceReservation(opCtx, be, clusterID, offlineSnapshotLeaseDuration)
		if err != nil {
			return fmt.Errorf("reserve target bucket: %w", err)
		}
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), offlineSnapshotReleaseTimeout)
			defer releaseCancel()
			if err := reservation.Release(releaseCtx); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("release target reservation: %w", err))
			}
		}()

		// Derive the next TXID only after acquiring HEAD. The bucket's
		// max(L0, L1, baseline) is the single source of truth, and the
		// reservation prevents another publisher from allocating beside us.
		txid, err = nextBucketTXID(opCtx, be)
		if err != nil {
			return fmt.Errorf("derive next TXID: %w", err)
		}

		tmpDir, err := os.MkdirTemp("", "syzy-snapshot-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)
		stagedApp := filepath.Join(tmpDir, "app.db")
		stagedMeta := filepath.Join(tmpDir, "metadata.db")
		if err := opCtx.Err(); err != nil {
			return fmt.Errorf("offline snapshot deadline: %w", err)
		}
		if err := clone.BackupTo(stagedApp, *dbPath); err != nil {
			return fmt.Errorf("backup app.db: %w", err)
		}
		if err := opCtx.Err(); err != nil {
			return fmt.Errorf("offline snapshot deadline after app backup: %w", err)
		}
		if err := clone.BackupTo(stagedMeta, layout.MetaDB(*dbPath)); err != nil {
			return fmt.Errorf("backup metadata.db: %w", err)
		}

		var appBuf, metaBuf bytes.Buffer
		if _, _, err := ltxstream.EncodeBaseline(opCtx, &appBuf, stagedApp, txid); err != nil {
			return fmt.Errorf("encode app baseline LTX: %w", err)
		}
		if _, _, err := ltxstream.EncodeBaseline(opCtx, &metaBuf, stagedMeta, txid); err != nil {
			return fmt.Errorf("encode meta baseline LTX: %w", err)
		}
		if err := reservation.PublishCoupledBaselines(opCtx, txid, appBuf.Bytes(), metaBuf.Bytes()); err != nil {
			return fmt.Errorf("publish coupled baselines: %w", err)
		}
		appSize, metaSize = appBuf.Len(), metaBuf.Len()
		return nil
	}(); err != nil {
		return err
	}

	fmt.Printf("snapshot published: cluster=%s baseline_txid=%d app_size=%d meta_size=%d\n",
		clusterID, txid, appSize, metaSize)
	return nil
}

// readClusterID opens metadata.db read-only and returns its cluster_id (hex).
func readClusterID(dbPath string) (string, error) {
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		return "", fmt.Errorf("open metadata: %w", err)
	}
	defer sc.Close()
	cid, ok, err := sc.GetClusterID()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("metadata.db has no cluster_id (uninitialized)")
	}
	return hex.EncodeToString(cid[:]), nil
}

// nextBucketTXID returns max(L0, L1, baseline) + 1 across both streams,
// or 1 if the bucket is empty. Mirrors publisher.seedFromBucket: the
// bucket itself is the single source of TXID monotonicity.
func nextBucketTXID(ctx context.Context, be objectstore.Bucket) (uint64, error) {
	var max uint64
	for _, prefix := range []string{objstore.DBPrefix, objstore.MetadataPrefix} {
		for _, level := range []int{objstore.L0Level, objstore.L1Level, objstore.BaselineLevel} {
			m, err := objstore.MaxLTXTXID(ctx, be, prefix, level)
			if err != nil {
				return 0, err
			}
			if m > max {
				max = m
			}
		}
	}
	return max + 1, nil
}

// openBucketBackend parses a bucket URL and returns a Backend.
func openBucketBackend(raw string) (objectstore.Bucket, error) {
	return objectstore.Open(context.Background(), raw)
}
