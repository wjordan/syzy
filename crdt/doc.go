// Package crdt defines the public replication values and canonical codecs
// shared by Syzy database engines, transports, journals, and object storage.
//
// The package contains pure values and arbitration helpers; it has no database
// dependency and performs no I/O. Engine modules map native transactions and
// catalogs to these types, then realize accepted records through their own
// transactional apply paths.
//
// # Specifications
//
// The authoritative shared documents are:
//
//   - docs/CRDT.md: consistency guarantees, named invariants, causal length,
//     and the row, cell, range, counter, and unique layers;
//   - docs/PROTOCOL.md: the changeset envelope, record and value encoding, HLC
//     packing, and engine durability obligations; and
//   - docs/SCHEMA.md: catalog-operation meaning, stable identities, and schema
//     dependency ordering.
//
// Mechanical Go detail lives with the type or function it describes:
//
//	identity.go    Origin, Seq, Dot, Clock, Stamp
//	causal.go      SeqRange, SeqSet
//	deps.go        Deps and schema-chain dependencies
//	changeset.go   Changeset and DML record values
//	codec.go       canonical changeset encoding and decoding
//	catalog_op.go  stable schema catalog operations
//	state.go       RowState, causal-generation transitions, effective stamps
//	interval.go    byte-range conflict layer
//
// Schema-chain totality, invariant (8), is enforced by schemalog compare-and-
// swap append and catalog sequence apply. Unique-key exclusivity, invariant (10),
// is enforced by the admitted engine apply or coordination path. Their
// shared meaning is specified here even though enforcement crosses package
// boundaries.
//
// # SQLite boundary
//
// The shared package specifies what a changeset means, while the SQLite module
// owns capture, apply, and physical recovery. Its linked and extension
// runtimes must preserve the transaction identity, encoding, arbitration, and
// durable-frontier ordering defined here.
//
// Producer, apply, metadata, and recovery implementations are deliberately
// outside this package.
package crdt
