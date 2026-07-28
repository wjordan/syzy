package objstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/wjordan/objectstore"
)

// ResolveClusterID returns the cluster_id stored in the bucket's HEAD,
// creating a stub HEAD with a fresh random id if HEAD does not yet
// exist. The stub form has Snapshot: nil; subsequent Publish calls
// upgrade HEAD to populated form via CAS while preserving ClusterID.
//
// This is the cluster_id rendezvous primitive for nodes opening
// against a shared bucket. Concurrent first-time callers linearize
// through CASHead's If-None-Match: exactly one wins and writes the
// stub; the rest read the winner's id from the resulting HEAD.
//
// The returned string is the canonical 32-char lowercase hex form
// (matching the publish path's encoding).
func ResolveClusterID(ctx context.Context, b objectstore.Bucket) (string, error) {
	const maxAttempts = 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		head, _, err := LoadHEAD(ctx, b)
		if err == nil {
			if head.ClusterID == "" {
				return "", errors.New("objstore: HEAD missing cluster_id")
			}
			return head.ClusterID, nil
		}
		if !errors.Is(err, ErrNoHEAD) {
			return "", err
		}
		// No HEAD yet — try to claim it with a fresh id. Loser of the
		// race re-loads on the next iteration.
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return "", fmt.Errorf("objstore: mint cluster_id: %w", err)
		}
		cid := hex.EncodeToString(raw[:])
		stub := &HEAD{Version: HEADVersion, ClusterID: cid}
		_, err = CASHead(ctx, b, stub, objectstore.IfAbsent())
		if err == nil {
			return cid, nil
		}
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return "", err
		}
		// CAS lost: somebody else stubbed first. Retry the LoadHEAD.
	}
	return "", errors.New("objstore: ResolveClusterID retries exhausted")
}
