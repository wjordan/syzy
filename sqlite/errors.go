package sqlite

import (
	"github.com/wjordan/syzy/internal/broker"
	"github.com/wjordan/syzy/sqlitebridge"
)

// ErrSchemaUnhealthy is returned by Open after a terminal schema-catch-up
// failure has been durably recorded. The node must be rebuilt with syzy_clone;
// local metadata edits cannot safely repair a broken schema chain.
var ErrSchemaUnhealthy = broker.ErrSchemaUnhealthy

// IsCoordinatedCommitRejected reports whether err is a commit vetoed by the
// coordinated-uniqueness commit hook: the transaction's reservation either
// lost to a concurrent claim on another node, or the reservation backend
// was unavailable past the producer's (deliberately small) in-commit wait.
//
// The in-commit wait is tiny because it holds the app.db write lock,
// starving the broker's inbound apply. Callers own the real retry: re-run
// the whole transaction off the writer with backoff. A genuine cross-node
// conflict fails each retry identically and still surfaces after the
// caller's budget; a transient handover heals within a retry or two.
func IsCoordinatedCommitRejected(err error) bool {
	return sqlitebridge.IsCode(err, sqlitebridge.ResultConstraintCommitHook)
}
