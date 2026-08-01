package postgres

import (
	"errors"
	"fmt"

	"github.com/wjordan/syzy/internal/metadata"
)

// ErrSchemaUnhealthy is returned by Open (and by the orchestrator's Run loop)
// when this node has committed a DDL it cannot replicate: its physical schema
// has diverged from the cluster chain and the only repair is syzy_clone (§6 F,
// the failed_local shape). Once durably marked, a restart refuses to resume
// rather than crash-looping on the same divergence.
var ErrSchemaUnhealthy = errors.New("postgres: schema unhealthy; run syzy_clone to repair")

// errUnsupportedDDL tags a DDL rejection as admission-class — the command
// committed locally but syzy cannot put it on the schema chain (an unsupported
// or unreplicable form), as opposed to a transient infrastructure error
// (introspection/connection failure) that should be retried. A node that
// commits one has diverged; appendDDLBundle turns it into ErrSchemaUnhealthy,
// while a transient error halts capture without poisoning the durable marker.
var errUnsupportedDDL = errors.New("postgres: unsupported DDL")

// admissionError carries a descriptive rejection message while matching
// errUnsupportedDDL under errors.Is — so the user-facing text stays clean (no
// sentinel suffix) yet the orchestrator can classify it.
type admissionError struct{ msg string }

func (e *admissionError) Error() string        { return e.msg }
func (e *admissionError) Is(target error) bool { return target == errUnsupportedDDL }

// unsupportedDDLf builds an admission-class rejection with a formatted message.
func unsupportedDDLf(format string, a ...any) error {
	return &admissionError{msg: fmt.Sprintf(format, a...)}
}

// metaSchemaUnhealthyKey holds the durable schema-unhealthy marker (its value is
// the human-readable reason). Set when a local DDL diverges this node; read at
// Open to refuse resuming until syzy_clone clears it.
const metaSchemaUnhealthyKey = "schema_unhealthy"

// markSchemaUnhealthy records that this node has diverged: it sets the in-memory
// flag and best-effort persists the reason so a restart refuses to resume.
// Called on the orchestrator goroutine (the sole metadata writer for schema
// state); the durable write is best-effort because the in-memory halt is the
// authoritative signal for the running process.
func (e *Engine) markSchemaUnhealthy(reason string) {
	e.schemaUnhealthy.Store(true)
	if e.cfg.Meta != nil {
		_ = e.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
			return tx.SetMeta(metaSchemaUnhealthyKey, []byte(reason))
		})
	}
}

// loadSchemaHealth reads the durable schema-unhealthy marker. ok is true when a
// non-empty reason is recorded, meaning the node must be repaired (syzy_clone)
// before it can resume.
func loadSchemaHealth(meta *metadata.Store) (reason string, unhealthy bool, err error) {
	if meta == nil {
		return "", false, nil
	}
	v, ok, err := meta.GetMeta(metaSchemaUnhealthyKey)
	if err != nil || !ok || len(v) == 0 {
		return "", false, err
	}
	return string(v), true, nil
}
