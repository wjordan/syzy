package broker

import (
	"context"
	"errors"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/quarantine"
)

// isConstraintError reports whether an apply error is a SQLite constraint
// violation (NOT NULL / UNIQUE / FOREIGN KEY / CHECK — all report
// "<kind> constraint failed"). These are the deterministic, payload-specific
// failures that would otherwise pin the per-origin frontier forever.
func isConstraintError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "constraint failed")
}

// isDeterministicApplyFailure reports whether an apply error is a
// deterministic, payload-specific failure eligible for quarantine (as
// opposed to a transient/environmental error that should hard-block and
// retry in place): SQLite constraint violations, counter apply failures
// (wire contract, overflow), and a row-group update that outran its
// row's INSERT.
func isDeterministicApplyFailure(err error) bool {
	return isConstraintError(err) ||
		errors.Is(err, errCounterApply) ||
		errors.Is(err, errUpdateOutranInsert)
}

// quarantinePolicy binds the shared quarantine behavior (internal/quarantine)
// to this broker's stores.
func (b *Broker) quarantinePolicy() quarantine.Policy {
	return quarantine.Policy{Meta: b.cfg.Meta, Cache: b.cfg.Cache, Cap: b.quarantineCap, Log: b.log}
}

// quarantineConstraintFailure persists a deterministically-failing changeset,
// advances the per-origin frontier past it, and returns true; false keeps the
// hard block (no Meta, or per-origin cap exceeded). The payload is NOT
// mirror-journaled here (it was not materialized); RetryQuarantined journals
// it once it applies cleanly.
func (b *Broker) quarantineConstraintFailure(cs *crdt.Changeset, payload []byte, applyErr error) bool {
	return b.quarantinePolicy().Quarantine(cs, payload, applyErr)
}

// RetryQuarantined re-applies every quarantined changeset once (force=true:
// the frontier already advanced past each seq at quarantine time, so the
// normal idempotency short-circuit would no-op the re-apply). Called from the
// fetcher loop each round.
func (b *Broker) RetryQuarantined(ctx context.Context) {
	b.quarantinePolicy().Retry(ctx, isDeterministicApplyFailure,
		func(_ context.Context, cs *crdt.Changeset, payload []byte) error {
			return b.applyPayloadCache(cs, payload, true)
		})
}
