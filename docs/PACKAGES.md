# Package map

Syzy is pre-1.0. These classifications describe package ownership and intended
extension surfaces, not stability promises.

## Application API

| Module/package | Role |
|---|---|
| `github.com/wjordan/syzy/sqlite` | Open, operate, clone, restore, subscribe to, and inspect replicated SQLite databases. |
| `github.com/wjordan/syzy/lazyrestore` | Optional sparse bootstrap and page-fault recovery for object-backed databases. |

Applications use the SQLite package. Public extension contracts are the
packages listed below; other packages primarily support Syzy itself.

## Internal ownership

- The `sqlite` package owns database lifecycle, hook capture, transactional
  apply, DDL realization, extension/daemon packaging, and physical recovery.
- Root packages own canonical changesets and CRDT state, stable catalog
  identities, transport, schema-log integration, object-storage integration,
  coordination, journals, and anti-entropy services.

The SQLite runtime composes these packages directly. This ownership seam is an
implementation rule, not an additional application or provider API.

## Extension contracts

| Package | Intended user |
|---|---|
| `crdt` | Protocol implementers that need canonical replication values and bytes. |
| `catalog` | Schema-authority integrations that allocate stable identities or encode canonical key tuples. |
| `transport` | Custom network transports and catch-up sources. |
| `github.com/wjordan/objectstore` | Custom durable object-store providers. This is an independently versioned module. |
| `schemalog` | Custom schema-authority backends. |
| `notify` | Cross-process readers and writers for the lossy shared-memory change feed. |
| `sqlitebridge` | Low-level SQLite integration used by the runtime and loadable extension. |
| `wake` | Cross-process or cross-kernel producer notification implementations. |

These packages contain contracts that external implementations may bind
against. Their wire, storage, and interface invariants are spec-authoritative.
The `objectstore.Bucket` contract and backend semantics are authoritative in
the external module's [storage specification](https://github.com/wjordan/objectstore/blob/main/docs/SEMANTICS.md).

## Built-in composition

`tcpmesh` (the built-in network transport), `transport/memtransport`,
`wake/vsock`, and `unique` are shipped implementations or advanced
composition helpers. They are public today because the daemon and companion
implementations compose them directly; they are not separate product
surfaces.

`sqlitebridge` is a low-level integration package, not a stable provider
contract.

New provider contracts belong in the relevant extension package.
Implementation helpers should remain internal to their owning module.

## Module boundary

The repository is one module. SQLite and lazy restore are packages versioned
with the replication runtime because they bind the same wire formats, on-disk
layout, and recovery invariants. Separately distributed companion programs
depend on the public contracts above instead of importing `internal` packages.
