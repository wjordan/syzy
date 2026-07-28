# Documentation

These guides describe how to run Syzy and specify its replication, recovery,
and extension contracts.

## Orientation

1. [Getting started](GETTING_STARTED.md) — start a SQLite replica.
2. [Concepts](CONCEPTS.md) — the replication mental model.
3. [Architecture](ARCHITECTURE.md) — end-to-end responsibilities and lifecycle.
4. [Package map](PACKAGES.md) — modules, application APIs, and provider
   contracts.

## Specifications

- [CRDT model](CRDT.md) — consistency guarantees, convergence invariants, and
  conflict layers.
- [Changeset protocol](PROTOCOL.md) — canonical transaction bytes and runtime
  obligations.
- [Schema replication](SCHEMA.md) — stable catalog identities, catalog
  operations, and schema-log ordering.
- [Transport](TRANSPORT.md) — the multiplexed TCP wire, peer catch-up,
  frontiers, bundles, and coordination RPC carrier.
- [Pruning](PRUNING.md) — bounded logical history and offline-peer contracts.
- [Incremental blob replication](BLOB_PATCH.md) — byte-range records,
  arbitration, and SQLite realization.

On-wire formats, public provider interfaces, stable catalog identities, and
stated consistency invariants are spec-authoritative. Internal Go shapes are
code-authoritative; these docs describe their responsibilities and ordering
without promising private APIs.

## Guides

- [Overview](../sqlite/README.md)
- [Getting started](../sqlite/docs/GETTING_STARTED.md)
- [Operations](../sqlite/docs/OPERATIONS.md)
- [Limitations](../sqlite/docs/LIMITATIONS.md)
- [Architecture](../sqlite/docs/ARCHITECTURE.md)
- [DDL realization](../sqlite/docs/DDL.md)

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the development workflow and the
repository-level rules for spec versus code authority.
