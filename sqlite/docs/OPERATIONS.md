# Operations

This guide takes the local [Getting Started](GETTING_STARTED.md) example into
multi-host deployment: linked Go nodes, extension daemons, shared object
storage, bootstrap, restore, and health checks.

Syzy is pre-1.0 and experimental. Read [Limitations](LIMITATIONS.md), especially
the host-crash durability and uncaptured-write warnings, before operating a
cluster with important data.

## Choose a deployment mode

Syzy has two deployment modes with the same replication, recovery, and
durability model:

- **Linked Go node.** Capture and apply run inside the process through
  `github.com/wjordan/syzy/sqlite`. Use this when the service owns every writer
  connection. Set `InProcessOnly: true` when no extension process writes the
  same database.
- **Extension and daemon.** Applications load the Syzy SQLite extension; one
  `syzy daemon` beside each database handles replication. Use this for Python,
  the SQLite CLI, other non-Go clients, or when replication must outlive the
  application process. Every writing connection must load the extension.

Both modes leave application rows in an ordinary SQLite file and replication
state in `<db>-syzy/`. Standard SQLite tools can read the database. An unhooked
write to a replicated table is not captured and must be avoided.

## What a multi-host cluster needs

Pointing one transport at another host is only one part of a deployment. Nodes
that share a logical database also need:

1. **One cluster identity.** Nodes using the same object backend rendezvous on
   the identity stored there. Without object storage, seed a fresh database
   with `sqlite.JoinCluster`, `syzy join`, or clone an existing replica.
2. **Shared schema authority.** Use an S3-backed schema log across hosts. A file
   log is suitable only where every process can safely access the same file;
   `schemalog.NewLocal()` is for one process.
3. **Reachable peer transport.** Configure the mesh listener, its advertised
   address, and initial seeds. The one listener carries gossip plus clone,
   peer catch-up, frontier, and coordinated-uniqueness traffic.
4. **Durable history when required.** A shared object backend enables
   changeset epochs, physical LTX restore, and the leaseholder used by
   coordinated `NOT NULL UNIQUE` keys in a multi-writer cluster.

The `syzy daemon --cluster` path composes these pieces and adds object-store
peer discovery. Linked applications wire them explicitly and must manage their
seed list or discovery mechanism.

## Linked Go nodes across hosts

The following is the important part of a production `Open`. It is a wiring
reference rather than a complete program; use the complete local program in
[Getting Started](GETTING_STARTED.md) for imports and error handling.

```go
ctx := context.Background()

objects, err := objectstore.Open(ctx,
	"s3://my-bucket/my-cluster/objects")
if err != nil {
	log.Fatal(err)
}
schemaBucket, err := objectstore.Open(ctx,
	"s3://my-bucket/my-cluster/schema")
if err != nil {
	log.Fatal(err)
}

mesh, err := tcpmesh.New(tcpmesh.Config{
	Listen:    ":7000",
	Advertise: "node-a.example:7000",
	Seeds:     []string{"node-b.example:7000"},
	TLSConfig: tlsConfig,
})
if err != nil {
	log.Fatal(err)
}
defer mesh.Close()

channel, err := mesh.Channel(syzy.DefaultTopic)
if err != nil {
	log.Fatal(err)
}

node, err := syzy.Open(ctx, syzy.Config{
	Path:          "app.db",
	Transport:     channel,
	SchemaLog:     schemalog.NewS3WithBackend(schemaBucket),
	ObjectBackend: objects,
	InProcessOnly: true,
	ServeClones:   true, // let peers bootstrap full copies from this node
})
if err != nil {
	log.Fatal(err)
}
defer node.Close()

db := syzy.NewDB(node)
```

Imports come from `github.com/wjordan/objectstore`,
`github.com/wjordan/syzy/schemalog`, `github.com/wjordan/syzy/tcpmesh`,
and `github.com/wjordan/syzy/sqlite` (aliased to `syzy` above).

Use the same object and schema prefixes on every node. Each host needs a unique,
routable advertised address. A process serving several logical databases can
share one mux and open a different topic for each database.

The fields most deployments must decide are:

- `Path` — the application database.
- `Transport` — nil is single-node mode; no peers receive changes.
- `SchemaLog` — nil rejects replicated DDL.
- `ObjectBackend` — nil disables object-store history and clustered
  coordinated uniqueness.
- `InProcessOnly` — true only when every writer belongs to this linked node;
  leave it false when extension processes also write the database.

Advanced retention, publisher, mmap, wake, and uniqueness timing options are
documented with `sqlite.Config` in [`sqlite/config.go`](../config.go).
Keeping those details with the API avoids copying defaults into several guides.

## Extension and daemon

The extension path requires a host SQLite built with
`SQLITE_ENABLE_PREUPDATE_HOOK`. From the repository root:

```bash
make build ext
export PATH="$PWD/bin:$PATH"
```

See [`ext/README.md`](../../ext/README.md) for build requirements, environment
configuration, and the optional preload mode.

The [Getting Started](GETTING_STARTED.md) guide has the complete local
two-replica CLI walkthrough. The extension starts or attaches to a daemon for
each database. `clone` copies the cluster identity and current state, then
assigns the destination a new writer identity. Configure explicit seeds for
immediate connection; object-backed deployments can also discover peers through
their shared cluster root.

## Object-store clusters with the daemon

A cluster root URL derives the object backend (`<root>/objects`), schema log
(`<root>/schema`), and peer-discovery namespace:

```bash
syzy daemon --cluster s3://my-bucket/my-cluster --db app.db
```

Run one daemon per replica with the same cluster URL. S3 clusters listen on
`:7000` by default — one port carries gossip plus clone, catch-up, frontier,
and coordinated-uniqueness traffic. File-backed clusters use Unix sockets
under the cluster root. Use `--listen` and `--seeds` when the defaults do not
match the network; `syzy daemon -h` lists the complete flags.

The object backend stores a restorable current physical chain, not an immutable
archive of every historical LTX object. Compaction and retention remove objects
that are covered by newer baselines or compacted ranges after a grace period.

## Joining, cloning, and restoring

There are three bootstrap paths:

- **Empty join.** `sqlite.JoinCluster(path, clusterID)` or `syzy join` gives a
  fresh database the cluster identity. Schema and rows then arrive from peers
  and durable history. Use this for small or young databases.
- **Clone.** `syzy clone <source> <destination>` copies a stopped local replica
  or streams a consistent bundle from a live daemon, then assigns a new writer
  identity.
- **Restore.** `sqlite.Restore` tries sources in order. A typical call prefers a
  live peer and falls back to the bucket:

```go
err := syzy.Restore(ctx, "app.db",
	"tcp://peer-host:7000",
	"s3://my-bucket/my-cluster/objects",
)
```

`sqlite.RestoreFromBucket` accepts an already-open `objectstore.Bucket`.

### Full restore and lazy bootstrap

A full restore downloads, reconstructs, and verifies the application and
metadata databases before opening them. Afterwards, reads are entirely local.

The optional `github.com/wjordan/syzy/lazyrestore` package supports Linux hosts
that need to open before downloading the complete application database. It
restores metadata, creates a sparse backing file, and exposes it through a FUSE
mount that fetches absent pages on demand. This trades startup bytes for
cold-read latency and an object-store dependency until pages become local.

The sparse backing file is not directly usable: reading an unallocated region
bypasses hydration and returns zeroes. Keep it outside application-visible
paths and expose only the mounted file. See the object-store clone section of
[ARCHITECTURE.md](ARCHITECTURE.md#object-store-clone-s3--file) for the exact
restore contract.

The application `db/` LTX stream is readable by Litestream and can restore a
plain SQLite file without Syzy metadata. That file is an exit path or recovery
source; it must be cloned or joined before participating in the cluster again.

## Health and observability

For daemon deployments, start with:

```bash
syzy status --db app.db
```

It reports the applied frontier, gaps, and local schema sequence.

```bash
syzy wait app.db --timeout 30s
```

blocks until every change produced locally has been applied by every reachable
peer, and fails if a peer cannot be reached. It exists for scripts and tests
that need a read elsewhere to observe a specific earlier write; it is not part
of the write path, and a peer that is down makes it fail rather than hang.

Linked nodes also expose poll-friendly snapshots:

- `Frontier()` — the contiguously accepted sequence per origin.
- `InboundHealth()` — per-origin progress, apply stalls, self-heals, and
  quarantined changesets.
- `LastSubscribeError()` — the most recent transport subscription failure.
- `UploadedSeq(origin)` — the highest sequence known durable in object storage.
- `SchemaSeq()` — the local schema-catalog generation.
- `CoordinatedDuplicates()` — coordinated unique values currently fenced as
  duplicates (see the runbook below).

Brief quarantine residency can occur while cross-origin dependencies arrive.
Residency that persists while its attempt count rises requires investigation.
The failure classifications and recovery actions are specified in
[ARCHITECTURE.md](ARCHITECTURE.md#localized-failures).

### Repairing a fenced coordinated duplicate

A coordinated (`NOT NULL UNIQUE`) key is enforced by the pre-commit reservation
gate, not by a SQLite index on each replica, so a duplicate can only enter
out-of-gate — typically rows committed on a partitioned node before the key
existed, replicating in after it was created. The reservation leaseholder
detects this while enumerating rows: it fences the affected value (refuses all
further grants for it) and reports it through `CoordinatedDuplicates()`.

1. Poll `CoordinatedDuplicates()` on every node. Only the node currently
   holding the reservation lease observes duplicates, so treat any non-empty
   result fleet-wide as needing attention.
2. Each entry names the table and key columns and lists the primary-key
   encoding of every live row holding the value (`Owners`).
3. Decide which row keeps the value, then `DELETE` the other rows — or
   `UPDATE` them to a distinct value — on any writer. This is ordinary
   replicated DML.
4. The fence lifts on the leaseholder's next clean enumeration after the
   repair replicates. No restart or manual reset is involved.

While a value is fenced, existing rows serve reads normally and unrelated
values grant normally; only new claims of the affected value fail.

## Durability checklist

- Ensure every writing connection uses the linked API or extension.
- Use `synchronous=FULL` on the application database when a host crash must not
  leave a committed row outside the replication record. The default
  `synchronous=NORMAL` favors latency and has a documented desynchronization
  window.
- Monitor frontier progress, subscription errors, and persistent quarantine
  entries.
- Exercise clone and restore before relying on object history.
- Review foreign keys, virtual tables, primary-key changes, and supported DDL in
  [Limitations](LIMITATIONS.md).
