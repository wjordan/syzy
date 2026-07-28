package objstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/wjordan/objectstore"
)

// ErrMaintenanceReservationInactive reports an offline maintenance operation
// that has been released or has reached its bounded lease expiry.
var ErrMaintenanceReservationInactive = errors.New("objstore: maintenance reservation inactive")

// MaintenanceReservation is a temporary exact publisher generation acquired
// by an offline operator operation. It must be acquired before inspecting TXID
// state or uploading immutable objects and released on every exit.
type MaintenanceReservation struct {
	backend   objectstore.Bucket
	clusterID string
	identity  PublisherIdentity
	expiresAt time.Time
	clock     func() time.Time
	closed    atomic.Bool
}

// AcquireMaintenanceReservation CAS-acquires a temporary publisher generation
// in HEAD. Any active publisher causes ErrPublisherLeaseActive. The reservation
// expiry is bounded by leaseDuration and by an earlier caller deadline.
func AcquireMaintenanceReservation(
	ctx context.Context,
	backend objectstore.Bucket,
	clusterID string,
	leaseDuration time.Duration,
) (*MaintenanceReservation, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, fmt.Errorf("objstore: generate maintenance identity: %w", err)
	}
	nodeID := "maintenance:" + hex.EncodeToString(token[:])
	return acquireMaintenanceReservation(ctx, backend, clusterID, nodeID, leaseDuration, time.Now)
}

func acquireMaintenanceReservation(
	ctx context.Context,
	backend objectstore.Bucket,
	clusterID, nodeID string,
	leaseDuration time.Duration,
	clock func() time.Time,
) (*MaintenanceReservation, error) {
	if backend == nil {
		return nil, errors.New("objstore: maintenance Backend required")
	}
	if clusterID == "" {
		return nil, errors.New("objstore: maintenance ClusterID required")
	}
	if nodeID == "" {
		return nil, errors.New("objstore: maintenance NodeID required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("objstore: maintenance lease duration must be positive")
	}
	if clock == nil {
		clock = time.Now
	}

	now := clock()
	expiresAt := now.Add(leaseDuration)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(expiresAt) {
		expiresAt = deadline
	}
	// HEAD stores microseconds. Use that exact representable instant locally so
	// the operation fence and remote active-owner check agree at the boundary.
	expiresAt = time.UnixMicro(expiresAt.UnixMicro())
	if !expiresAt.After(now) {
		return nil, fmt.Errorf("%w: deadline leaves no maintenance lease window", ErrMaintenanceReservationInactive)
	}

	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		nowUS := clock().UnixMicro()
		if expiresAt.UnixMicro() <= nowUS {
			return nil, fmt.Errorf("%w: reservation expired during acquisition", ErrMaintenanceReservationInactive)
		}
		head, etag, err := LoadHEAD(ctx, backend)
		var ifMatch *string
		switch {
		case errors.Is(err, ErrNoHEAD):
			head = &HEAD{Version: HEADVersion, ClusterID: clusterID}
			ifMatch = objectstore.IfAbsent()
		case err != nil:
			return nil, fmt.Errorf("objstore: load HEAD for maintenance reservation: %w", err)
		default:
			if head.ClusterID != "" && head.ClusterID != clusterID {
				return nil, fmt.Errorf("%w: HEAD has %s, maintenance operation has %s", ErrClusterIDMismatch, head.ClusterID, clusterID)
			}
			if head.Publisher != nil && head.Publisher.ExpiresAtUS > nowUS {
				return nil, fmt.Errorf("%w: %s generation %d expires at %d",
					ErrPublisherLeaseActive, head.Publisher.NodeID, head.Publisher.Generation, head.Publisher.ExpiresAtUS)
			}
			ifMatch = &etag
		}

		var previousGeneration uint64
		if head.Publisher != nil {
			previousGeneration = head.Publisher.Generation
		}
		if previousGeneration == ^uint64(0) {
			return nil, errors.New("objstore: publisher generation exhausted")
		}
		identity := PublisherIdentity{NodeID: nodeID, Generation: previousGeneration + 1}
		next := *head
		next.Version = HEADVersion
		next.ClusterID = clusterID
		next.Publisher = &Publisher{
			NodeID:      identity.NodeID,
			Generation:  identity.Generation,
			ExpiresAtUS: expiresAt.UnixMicro(),
		}
		if _, err := CASHead(ctx, backend, &next, ifMatch); err != nil {
			if errors.Is(err, objectstore.ErrPreconditionFailed) {
				continue
			}
			return nil, fmt.Errorf("objstore: acquire maintenance reservation: %w", err)
		}
		return &MaintenanceReservation{
			backend: backend, clusterID: clusterID, identity: identity,
			expiresAt: expiresAt, clock: clock,
		}, nil
	}
	return nil, errors.New("objstore: maintenance reservation CAS retries exhausted")
}

// PublishCoupledBaselines publishes through the reservation's exact active
// identity. Every Put is synchronously checked against the fixed expiry.
func (r *MaintenanceReservation) PublishCoupledBaselines(
	ctx context.Context,
	txid uint64,
	appBaselineLTX, metaBaselineLTX []byte,
) error {
	if r == nil {
		return errors.New("objstore: nil maintenance reservation")
	}
	if err := r.checkActive(); err != nil {
		return err
	}
	opCtx, cancel := context.WithDeadline(ctx, r.expiresAt)
	defer cancel()
	return publishCoupledBaselines(
		opCtx,
		&GuardedBucket{Bucket: r.backend, Check: r.checkActive},
		r.clusterID,
		r.identity,
		r.clock,
		txid,
		appBaselineLTX,
		metaBaselineLTX,
	)
}

func (r *MaintenanceReservation) checkActive() error {
	if r.closed.Load() {
		return fmt.Errorf("%w: reservation released", ErrMaintenanceReservationInactive)
	}
	now := r.clock()
	if !now.Before(r.expiresAt) {
		return fmt.Errorf("%w: expired at %s", ErrMaintenanceReservationInactive, r.expiresAt.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// Release CAS-expires only this reservation's exact identity. It is safe after
// local expiry and never alters a successor that has already replaced it.
func (r *MaintenanceReservation) Release(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.closed.Store(true)
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		head, etag, err := LoadHEAD(ctx, r.backend)
		if errors.Is(err, ErrNoHEAD) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("objstore: load HEAD to release maintenance reservation: %w", err)
		}
		if head.ClusterID != "" && head.ClusterID != r.clusterID {
			return fmt.Errorf("%w: HEAD has %s, maintenance operation has %s", ErrClusterIDMismatch, head.ClusterID, r.clusterID)
		}
		if !r.identity.Matches(head.Publisher) || head.Publisher.ExpiresAtUS == 0 {
			return nil
		}
		next := *head
		publisher := *head.Publisher
		publisher.ExpiresAtUS = 0
		next.Publisher = &publisher
		if _, err := CASHead(ctx, r.backend, &next, &etag); err != nil {
			if errors.Is(err, objectstore.ErrPreconditionFailed) {
				continue
			}
			return fmt.Errorf("objstore: release maintenance reservation: %w", err)
		}
		return nil
	}
	return errors.New("objstore: maintenance reservation release CAS retries exhausted")
}
