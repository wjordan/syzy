package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/quarantine"
)

// quarantineRetryInterval paces the Run loop's re-apply sweep. The SQLite
// broker retries each fetcher round; the push-based orchestrator has no
// equivalent cadence, so it uses a timer.
const quarantineRetryInterval = 30 * time.Second

// errBlobPatchUnsupported marks a changeset carrying a crdt.BlobPatch record:
// large-object replication was cut from this engine (v1 documents bytea
// columns instead), and the same bytes fail identically on every redelivery,
// so apply fails closed with a deterministic error and the changeset
// quarantines instead of pinning its origin's frontier.
var errBlobPatchUnsupported = errors.New("postgres: blob patches are not supported by the postgres engine")

// isDeterministicApplyErr reports whether an apply error is deterministic and
// payload-specific — a Postgres integrity-constraint violation (SQLSTATE
// class 23: NOT NULL, UNIQUE, FK, CHECK, exclusion) or a record kind this
// engine refuses by construction (blob patches). These would otherwise pin
// the per-origin frontier forever — retrying the same bytes yields the same
// error. Everything else (connection loss, schema gate, serialization, disk)
// is treated as transient and stays fatal to Run: restart + redelivery is the
// recovery path.
func isDeterministicApplyErr(err error) bool {
	if errors.Is(err, errBlobPatchUnsupported) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "23")
}

// quarantinePolicy binds the shared quarantine behavior (internal/quarantine)
// to this engine's stores.
func (o *orchestrator) quarantinePolicy() quarantine.Policy {
	return quarantine.Policy{Meta: o.appl.cfg.Meta, Cache: o.appl.cfg.Cache}
}

// quarantineApplyFailure persists a deterministically-failing peer changeset,
// advances the per-origin frontier past it (so later seqs flow), and returns
// true; false keeps the hard failure (no Meta, or per-origin cap exceeded).
func (o *orchestrator) quarantineApplyFailure(cs *crdt.Changeset, applyErr error) bool {
	return o.quarantinePolicy().Quarantine(cs, cs.Encoded(), applyErr)
}

// retryQuarantined re-applies every quarantined changeset once (force: the
// frontier already advanced at quarantine time). Called from the Run loop on
// a timer.
func (o *orchestrator) retryQuarantined(ctx context.Context) {
	o.quarantinePolicy().Retry(ctx, isDeterministicApplyErr,
		func(ctx context.Context, cs *crdt.Changeset, _ []byte) error {
			return o.appl.apply(ctx, cs, true)
		})
}
