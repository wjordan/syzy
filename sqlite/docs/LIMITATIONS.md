# Limitations

This page describes Syzy's current SQLite SQL, capture, apply, and
recovery boundaries.

- Replicated DDL inside an explicit transaction is limited: all DDL must
  precede any DML in the transaction, and DDL under `SAVEPOINT` scope (or a
  `CREATE TABLE` carrying cascade `FOREIGN KEY` actions) is rejected inside a
  transaction. Any number of DDL statements may share one `BEGIN ... COMMIT`;
  they publish as a single schema event. See
  [DDL.md#transactional-ddl](DDL.md#transactional-ddl).

- Certain tables are local-only and never replicated:
  - tables whose names start with `_` or `sqlite_`
  - tables in non-`main` schemas (`ATTACH`ed, `temp`)
  - shadow tables created by virtual tables

- Virtual table DML is not replicated; use virtual tables only as derived indexes over replicated source rows, not primary storage.

- Concurrent `UPDATE`s to a primary key column and non-primary key column can result in the non-primary key update not being applied to the updated row.
- Foreign keys are not enforced on inbound changes, so remote convergence can still produce violations.
  Audit with `PRAGMA foreign_key_check` to enforce ongoing referential integrity.
- **Out-of-band writes** to replicated tables on SQLite connections without the hook
  installed will not be replicated properly.
- With default durability settings, host crashes can result in writes that end up durable in the database but not metadata and are not replicated properly. Configure the writer connection with `synchronous=FULL` to ensure consistency across host crashes.

## Primary key requirements

Replicated tables require a non-NULL primary key. The linked Go API and
`LD_PRELOAD` shim accept the ordinary SQLite/ORM shape:

```sqlite
CREATE TABLE mytable (
  id INTEGER PRIMARY KEY
);
```

Direct `.load` clients must use the explicit collision-resistant form:

```sqlite
CREATE TABLE mytable (
  id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('mytable'))
);
```

Generated IDs are sparse int63 values from the local random-partition
allocator. Use `INSERT ... RETURNING id` rather than `last_insert_rowid()`.
See [SQL preprocessing](DDL.md#sql-preprocessing) for rewrite eligibility and
integration-path details.

Use `BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7())` when opaque, time-ordered,
or externally portable identifiers are preferable.

## `UNIQUE` constraints

Two modes, picked automatically by nullability:

- **Eventual** (nullable `UNIQUE`): no coordination, zero added
  latency. For concurrent writes claiming the same non-NULL value, the
  highest-Stamp row keeps it; lower-Stamp rows have their key columns
  set to `NULL`. A successful local insert is *not* a guarantee of
  global uniqueness.

  An eventual `UNIQUE` is carried as replicated catalog state, not as a
  SQLite `UNIQUE INDEX` on every replica: the physical index exists only
  on the node that ran the `CREATE`. A replica-side index would fail the
  apply transaction on the duplicate before arbitration could null the
  loser. (This applies only to `UNIQUE`, in both the standalone
  `CREATE UNIQUE INDEX` and inline `UNIQUE` forms. Ordinary
  `CREATE INDEX` replicates normally and exists on every replica, and
  `PRIMARY KEY` is enforced physically everywhere.)

  The consequence: arbitration runs when a replica *applies* a peer's
  write, so it never sees a duplicate a node creates entirely by itself.
  On any node other than the one that ran the `CREATE`, inserting a value
  that node already holds is accepted at write time and never arbitrated
  — that node keeps both rows while its peers converge on one. If you
  need a duplicate rejected on the spot, that is what coordinated
  (`NOT NULL UNIQUE`) is for.

- **Coordinated** (`NOT NULL UNIQUE`): globally unique *by
  construction*. The value is reserved through a single global
  round-trip before the local commit, so the second writer to claim it
  never commits: the commit is vetoed and fails with
  `SQLITE_CONSTRAINT_COMMITHOOK` (check with
  `sqlite.IsCoordinatedCommitRejected`). Treat it as
  retryable-off-the-writer: re-run the transaction — a genuine conflict
  fails identically each time, while a brief unavailability during a
  leader handover (which surfaces as the same commit rejection) heals
  within a retry or two. This is the right default for user-supplied
  identifiers like an email address. The cost: one round-trip per
  write touching the column.
  A coordinated key may also be **partial** — `CREATE UNIQUE INDEX …
  WHERE <predicate>`, the soft-delete idiom `UNIQUE(email) WHERE
  deleted_at IS NULL`, where uniqueness applies only to the rows the
  predicate admits.

  A coordinated key is enforced by the reservation gate alone — **no
  node keeps a SQLite `UNIQUE` index for it**, including the node that
  ran the DDL. Expect the visible consequences:

  - The `UNIQUE` syntax disappears from `sqlite_master`: an inline
    constraint is stripped from the stored `CREATE TABLE` text right
    after the DDL commits (a one-time table rebuild; pre-existing
    databases converge at the next open), and a standalone
    `CREATE UNIQUE INDEX` is downgraded to a plain index of the same
    name. `.schema` therefore won't show the constraint — the
    replicated catalog is its record.
  - `DROP INDEX` removes the key when the named index matches it on both
    columns and `WHERE` predicate. To keep that unambiguous, no second
    index may cover exactly a total key's columns — `CREATE INDEX` over
    them is rejected, as is a coordinated key over columns an unfiltered
    index already covers. An inline-declared key has no index name to
    drop; remove it via `DROP TABLE` or by recreating the table without
    the constraint.
  - DDL admission protects the gate: no `BLOB` or generated columns as
    key members, no triggers that `INSERT`/`UPDATE` a coordinated-key
    table (in either creation order, including via `ALTER TABLE ... RENAME
    TO` onto a name some trigger writes), no FK action that rewrites the
    child (`ON DELETE`/`ON UPDATE SET NULL`/`SET DEFAULT`, `ON UPDATE
    CASCADE`), no DML on the table in the same transaction as the key's
    DDL, and composite or partial keys require the row clock group. Each
    rejection states its reason at DDL or commit time.
  - A transaction that uses `ROLLBACK TO <savepoint>` and writes a
    coordinated-key table is rejected at commit: SQLite does not
    un-report the undone row changes, so the gate cannot tell which
    values the commit actually lands. Retry without savepoints.
  - A duplicate that predates the key (rows committed on a partitioned
    node before the `CREATE`, arriving after) is detected by the
    reservation leaseholder: the value is fenced from further grants
    and surfaced via `CoordinatedDuplicates()` for manual repair — see
    the runbook in [OPERATIONS.md](OPERATIONS.md#repairing-a-fenced-coordinated-duplicate).

Not supported:

- `UNIQUE` on `BLOB` columns, in both modes (eventual interacts
  unsafely with `blob_patch`; coordinated cannot gate incremental
  `sqlite3_blob_write` rewrites of a key column).
- *Eventual* (nullable-member) [partial unique
  indexes](https://sqlite.org/partialindex.html#unique_partial_indexes)
  (`CREATE UNIQUE INDEX ... WHERE ...`); coordinated (`NOT NULL`) partial
  indexes are supported.

## Counter columns

Declare a column `INTEGER COUNTER` and concurrent updates to it merge
by **summation** instead of last-writer-wins — no increment is ever
lost to a concurrent writer:

```sqlite
CREATE TABLE inventory (
  sku      TEXT PRIMARY KEY NOT NULL,
  quantity INTEGER COUNTER NOT NULL DEFAULT 0
);
-- on two nodes at once:
UPDATE inventory SET quantity = quantity + 30 WHERE sku = ?;
UPDATE inventory SET quantity = quantity - 50 WHERE sku = ?;
-- every node converges to the net -20.
```

Every `UPDATE` to a counter column replicates as a relative
adjustment (`NEW − OLD`), so `SET quantity = 0` means "subtract what I
saw", not "erase concurrent increments"; reset absolutely by deleting
and re-inserting the row. Counter columns must be `NOT NULL` with
INTEGER affinity, and can't be PK or `UNIQUE` members. See
[DDL.md#counter-columns](DDL.md#counter-columns).
