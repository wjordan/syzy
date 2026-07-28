package objstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
)

func TestMaintenanceReservationAcquiresPublishesAndExactReleases(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedBaselineHEAD(t, ctx, be, PublisherIdentity{NodeID: "old", Generation: 4}, 0, 6, 6)

	reservation, err := acquireMaintenanceReservation(ctx, be, "cafef00d", "maintenance:fixed", time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got := reservation.identity; got.NodeID != "maintenance:fixed" || got.Generation != 5 {
		t.Fatalf("identity = %+v, want maintenance:fixed generation 5", got)
	}
	head, _, err := LoadHEAD(ctx, be)
	if err != nil {
		t.Fatal(err)
	}
	if !reservation.identity.ActiveAt(head.Publisher, now.UnixMicro()) {
		t.Fatalf("HEAD did not record active reservation: %+v", head.Publisher)
	}
	if head.Baseline.TXID != 6 || head.MetaBaseline.TXID != 6 {
		t.Fatalf("acquisition moved baselines: %+v", head)
	}

	if err := reservation.PublishCoupledBaselines(ctx, 7, []byte("app-7"), []byte("meta-7")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	head, _, err = LoadHEAD(ctx, be)
	if err != nil {
		t.Fatal(err)
	}
	if head.Baseline == nil || head.Baseline.TXID != 7 || head.MetaBaseline == nil || head.MetaBaseline.TXID != 7 {
		t.Fatalf("coupled baseline not promoted: %+v", head)
	}
	if !reservation.identity.Matches(head.Publisher) {
		t.Fatalf("publication changed reservation identity: %+v", head.Publisher)
	}

	if err := reservation.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	head, _, err = LoadHEAD(ctx, be)
	if err != nil {
		t.Fatal(err)
	}
	if !reservation.identity.Matches(head.Publisher) || head.Publisher.ExpiresAtUS != 0 {
		t.Fatalf("release did not expire exact identity: %+v", head.Publisher)
	}
}

func TestMaintenanceReservationFencesEveryMutationAtExpiry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		prepareNow func(*MaintenanceReservation, *time.Time)
	}{
		{
			name: "expired before operation",
			prepareNow: func(r *MaintenanceReservation, now *time.Time) {
				*now = r.expiresAt
			},
		},
		{
			name: "pause between admission and first Put",
			prepareNow: func(r *MaintenanceReservation, _ *time.Time) {
				calls := 0
				r.clock = func() time.Time {
					calls++
					if calls == 1 {
						return r.expiresAt.Add(-time.Second)
					}
					return r.expiresAt
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be, err := objectstore.OpenFS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Microsecond)
			reservation, err := acquireMaintenanceReservation(ctx, be, "cafef00d", "maintenance:fixed", time.Minute, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			defer reservation.Release(context.Background())
			tc.prepareNow(reservation, &now)

			err = reservation.PublishCoupledBaselines(ctx, 1, []byte("app"), []byte("meta"))
			if !errors.Is(err, ErrMaintenanceReservationInactive) {
				t.Fatalf("publish error = %v, want inactive reservation", err)
			}
			for _, prefix := range []string{DBPrefix, MetadataPrefix} {
				files, listErr := ListLTX(ctx, be, prefix, BaselineLevel)
				if listErr != nil {
					t.Fatal(listErr)
				}
				if len(files) != 0 {
					t.Fatalf("expired reservation uploaded %d %s objects", len(files), prefix)
				}
			}
		})
	}
}

func TestMaintenanceReleaseNeverExpiresSuccessor(t *testing.T) {
	t.Parallel()
	be, err := objectstore.OpenFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	reservation, err := acquireMaintenanceReservation(ctx, be, "cafef00d", "maintenance:fixed", time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	head, etag, err := LoadHEAD(ctx, be)
	if err != nil {
		t.Fatal(err)
	}
	next := *head
	next.Publisher = &Publisher{
		NodeID: "successor", Generation: reservation.identity.Generation + 1,
		ExpiresAtUS: now.Add(time.Hour).UnixMicro(),
	}
	if _, err := CASHead(ctx, be, &next, &etag); err != nil {
		t.Fatal(err)
	}
	if err := reservation.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	head, _, err = LoadHEAD(ctx, be)
	if err != nil {
		t.Fatal(err)
	}
	if head.Publisher == nil || head.Publisher.NodeID != "successor" || head.Publisher.ExpiresAtUS == 0 {
		t.Fatalf("exact release disturbed successor: %+v", head.Publisher)
	}
}
