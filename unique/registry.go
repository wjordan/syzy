// Package unique is the shared coordination substrate for constraints whose
// global exclusivity is guaranteed by construction rather than reconciled
// after the fact.
//
// A coordinated write claims its key values against a Registry at SQLite's
// admission gate before commit. The second writer to claim a value is
// rejected with no conflict-fallback logic. The interface is intentionally
// minimal so any linearizable backend can
// implement it: a leaseholder (the low-latency default), per-value
// object-store CAS, or in-process for tests.
//
// The Registry treats values as opaque, canonically-encoded bytes — the
// same encoding that produces pk_blob — and is decoupled from the crdt
// domain types, mirroring schemalog. Table and Key are crdt.TableID /
// crdt.KeyID byte arrays; the owner is a crdt.PKBlob.
//
// See docs/SCHEMA.md#unique-keys.
package unique

import (
	"context"
	"errors"
)

// ErrUnavailable is returned by Reserve/Release when the backend cannot
// serve the request (a lease handover, or a partition isolating the
// writer from the leaseholder). It is retryable: the caller surfaces it
// to the app as a transient error, never as a silent conflict. This is
// the CAP cost of coordinated uniqueness, made explicit.
var ErrUnavailable = errors.New("unique: reservation backend unavailable")

// Claim is one reservation: the value Value of unique key Key on table
// Table, owned by row Owner. Table and Key are crdt.TableID / crdt.KeyID
// bytes; Value is the canonical encoding of the (non-NULL) key tuple;
// Owner is the owning row's crdt.PKBlob. NULL tuples are never claimed —
// the caller skips them, since NULLs do not collide. A single
// transaction may touch rows with different PKs, so the owner is
// per-claim, letting one Reserve batch the whole transaction.
type Claim struct {
	Table [16]byte
	Key   [16]byte
	Value []byte
	Owner []byte
	// Prev, if set, is a prior owner the reservation may take the value
	// from: a PK-changing update that keeps the same coordinated value
	// transfers it from the old row (Prev) to the new (Owner). Reserve
	// grants when the value is free, already this Owner's, or currently
	// Prev's — never another owner's. Empty for ordinary inserts/updates.
	Prev []byte
}

// Registry is the contract every coordinated-uniqueness backend
// implements. Reserve is the linearization point: across the cluster, at
// most one owner holds a given (Table, Key, Value) at a time.
type Registry interface {
	// Reserve atomically claims every entry in claims. It is all-or-nothing:
	// if any claim's (Table, Key, Value) is held by a different owner,
	// nothing is reserved and the result is (false, &conflict, nil) naming
	// the first such claim. A claim already held by the same owner is an
	// idempotent success (replay / re-assert). A non-nil error
	// (ErrUnavailable) means the request could not be served and is
	// retryable. An empty claims slice returns (true, nil, nil).
	Reserve(ctx context.Context, claims []Claim) (ok bool, conflict *Claim, err error)

	// Release notifies the backend that claims' owners vacated their
	// values (the owning row was deleted or its value changed). It is
	// advisory: a backend that observes the replicated rows (the
	// leaseholder) ignores it entirely — a vacated value leaves its
	// derived taken-set when the rows show it gone, and re-enters the
	// free pool only after the release hold (quarantine-until-stable).
	// In-process backends (Local) free the value immediately, which is
	// correct with no replication lag. Releasing a claim not held by its
	// owner is a no-op, so Release is idempotent.
	Release(ctx context.Context, claims []Claim) error
}
