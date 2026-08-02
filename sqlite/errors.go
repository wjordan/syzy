package sqlite

import (
	"errors"

	"github.com/wjordan/syzy/internal/broker"
	"github.com/wjordan/syzy/unique"
)

// ErrSchemaUnhealthy is returned by Open after a terminal schema-catch-up
// failure has been durably recorded. The node must be rebuilt with syzy_clone;
// local metadata edits cannot safely repair a broken schema chain.
var ErrSchemaUnhealthy = broker.ErrSchemaUnhealthy

// IsCoordinatedConflict reports a commit vetoed because another row owns a
// coordinated value. Retrying the unchanged write cannot succeed.
func IsCoordinatedConflict(err error) bool {
	return errors.Is(err, unique.ErrConflict)
}

// IsCoordinatedUnavailable reports a commit vetoed because the reservation
// backend remained unavailable past the in-commit retry budget. Retry the
// whole transaction off the writer.
func IsCoordinatedUnavailable(err error) bool {
	return errors.Is(err, unique.ErrUnavailable)
}
