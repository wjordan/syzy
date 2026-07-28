// Package schemalog is the schema-event CAS log that serializes DDL
// across the cluster. Every replicated DDL goes through Append at the
// originating engine's admission boundary; receivers catch up via Read.
//
// The interface is intentionally minimal so any CAS-capable storage
// (object store, linearizable KV, elected leader, in-process for
// tests) can back it. v1 ships an in-memory backend (Local) plus a
// SQLite-file backend (File) that lets multiple test nodes share one
// log file and exercise real CAS contention.
//
// See docs/SCHEMA.md#schema-log for the model and rules.
package schemalog

import (
	"context"
	"errors"
)

// ErrHeadMoved is returned by Append when parentSeq != current head.
// Losers retry after Read brings them up to date.
var ErrHeadMoved = errors.New("schemalog: head moved")

// ErrBelowHorizon is returned by Read when fromSeq is below the
// log's retention window. Recovery is syzy_clone.
var ErrBelowHorizon = errors.New("schemalog: requested seq below retention horizon")

// Event is one durable SchemaEvent stored in the log. Encoding of
// CatalogOp is opaque to the log — the engine decodes it via
// crdt.DecodeCatalogOp.
type Event struct {
	SchemaSeq uint64
	ParentSeq uint64
	CatalogOp []byte
	RawSQL    string
}

// Log is the contract every backend implements. Append commits a new
// event at head+1 iff head == parentSeq (linearizable CAS); Read
// returns events strictly above fromSeq for catch-up.
type Log interface {
	// Append commits event at head+1 iff head == parentSeq. Returns the
	// assigned schema_seq on success. ErrHeadMoved on CAS conflict.
	Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error)

	// Read returns up to limit events with schema_seq strictly greater
	// than fromSeq, in ascending order. Returns ErrBelowHorizon when
	// fromSeq is below the log's retention window.
	Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error)

	// Head returns the current schema_seq head (0 if empty).
	Head(ctx context.Context) (uint64, error)
}
