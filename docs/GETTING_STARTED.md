# Getting started

Syzy gives each SQLite replica a writable local database and converges
committed transactions across peers.

## Choose an integration

Applications can embed Syzy in a Go process or load its SQLite extension. Both
paths support:

- a linked Go API;
- a loadable extension with a companion daemon;
- row- and cell-level conflict clocks;
- counter columns and blob byte-range replication; and
- LTX physical history with full or optional lazy restore.

Start with the complete two-node program in the
[SQLite getting-started guide](../sqlite/docs/GETTING_STARTED.md), then use the
[SQLite operations guide](../sqlite/docs/OPERATIONS.md) for extension mode,
multi-host transport, object storage, restore, and health.

## Mental model

Syzy combines:

- changeset identity and binary protocol;
- causal row generations and base last-writer-wins arbitration;
- stable schema identities and schema-log ordering;
- transport and peer catch-up contracts; and
- per-origin history, frontier, and object-epoch model.

Read [Concepts](CONCEPTS.md) for the complete mental model.

Syzy is pre-1.0 and experimental. The SQLite guide links its current
limitations and operational requirements.
