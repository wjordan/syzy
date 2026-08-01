# Changeset Protocol

This document specifies the canonical changeset bytes used by Syzy's SQLite and
Postgres runtimes, transports, journals, and object storage. The format is
**spec-authoritative**: any encoder or decoder disagreement is a protocol bug.
The public [`crdt`](../crdt) package is the canonical Go implementation.

Capture and apply use this same representation across every durable and
network path. DDL does not ride the DML changeset wire; it is represented by
catalog operations in the ordered
[schema log](SCHEMA.md).

## Changeset envelope

A flat binary format carries one committed transaction. The bytes that travel
over `transport.Broadcast`, the bytes stored in per-origin journals and object
epochs, and the bytes decoded by a receiver are identical. There is no
decode-and-re-encode step.

A broadcast payload is exactly the changeset bytes; the first byte is the
format version. Frontier exchange and peer catch-up requests use the separate
[transport protocol](TRANSPORT.md).

### Header

Encoders emit version 2. Decoders also accept the retained version 1 format;
version 1 is decode-only and must never be emitted. Every other version is
rejected.

```text
1 byte   version
8 bytes  origin            (big-endian)         -- Dot.Origin
8 bytes  sender_seq        (big-endian)         -- Dot.Seq
8 bytes  hlc               (big-endian)         -- Stamp.Clock
16 bytes cluster_id        (UUID bytes)
1 byte   deps_count
[deps_count]:
  2 bytes  chain_id        (big-endian)         -- 0 = schema chain
  8 bytes  seq             (big-endian)
8 bytes  crc64
varint   payload_length
N bytes  payload
```

`Stamp.Origin` equals the header origin. `Deps` currently carries at most one
entry: `(ChainID=0, seq=required_schema_seq)`. It is omitted while the writer's
schema sequence is zero. There are no row-level causal dependencies on the
wire; receivers use row generations, stamps, per-origin sequence order, and
quarantine for unresolved cross-origin arrival dependencies.

### DML records

The payload is a sequence of records:

```text
1 byte   op                (1 = insert, 2 = update, 3 = delete, 4 = blob_patch)
16 bytes table_id
varint   pk_blob_len
N bytes  pk_blob
varint   cl                -- writer's view of the post-op row generation
op-specific payload:
  insert/update:
    varint   column_count
    [column_count]:
      16 bytes column_id
      4 bytes type_tag     (big-endian; 0=null, canonical classes:
                            1=int, 2=real, 3=text, 4=blob)
      1 byte  format       (0=text, 1=binary, 2=delta)
      varint  value_byte_len   (omitted when type_tag = 0)
      N bytes value_bytes      (omitted when type_tag = 0)
  delete:
    varint   column_count    -- always 0
  blob_patch:
    16 bytes blob_column_id
    varint   range_count
    [range_count]:
      varint  offset
      varint  byte_len
      N bytes range_bytes
```

For an **insert**, active non-generated columns are present as a full row image
and `cl` is the writer's next live generation, which is odd.

For an **update**, the record carries the columns the source engine reports as
changed and `cl` is the writer's current live generation. The receiver maps
stable table and column IDs through its catalog, ignores tombstoned columns,
and arbitrates surviving values according to the admitted conflict layers.

For a **delete**, `column_count` is zero. The tombstone is keyed by
`(table_id, pk_blob)`, and `cl` is the next even generation.

For a **blob patch**, `cl` is the current live row generation. The record
addresses one stable blob column and carries non-overlapping byte ranges. See
[BLOB_PATCH.md](BLOB_PATCH.md#wire-format) for range arbitration.

### Value encoding

- **INTEGER:** 8-byte two's-complement big-endian.
- **REAL:** IEEE 754 binary64, big-endian.
- **TEXT:** UTF-8 bytes.
- **BLOB:** raw bytes.
- **NULL:** type tag only.

The `format` byte selects the value representation. Format `2` is the semantic
variant `delta`: a signed integer adjustment to an admitted counter column
rather than an absolute value. A receiver that cannot apply counter semantics
must reject delta payloads rather than treating them as registers.

## Identity and time

`Dot (origin, sequence)` uniquely identifies a changeset. Sequences are dense
per origin, which permits compact frontiers and gap detection.

The HLC field packs 47 bits of physical milliseconds and a 16-bit logical
counter as `(ms << 16) | logical`, with bit 63 reserved zero. The
`(hlc, origin)` pair forms the `Stamp`; conflict ordering is strict
lexicographic order on `(Stamp.Clock, Stamp.Origin)`. The origin tie-breaker is
required when concurrent writers produce the same HLC value.

## Runtime obligations

Syzy must:

1. assign one origin and strictly increasing sequence to each produced
   changeset;
2. preserve the source transaction boundary in one changeset;
3. make the exact encoded bytes durable before releasing the source
   history required to reconstruct the commit;
4. validate cluster identity and schema dependencies before apply;
5. apply accepted records transactionally with the admitted CRDT layers;
6. persist apply state before advancing the durable frontier; and
7. reject payload classes or schema layers it cannot implement safely.

The convergence invariants behind these obligations are authoritative in
[CRDT.md](CRDT.md). Admission and apply details are specified in the
[SQLite architecture](../sqlite/docs/ARCHITECTURE.md) and
[Postgres engine](postgres.md).
