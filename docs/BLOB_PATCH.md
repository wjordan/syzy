# Incremental Blob Replication

Syzy's `blob_patch` record carries byte-range mutations without
reshipping a complete large value. Per-range last-writer-wins resolves
concurrent overlapping patches under the row's causal generation.

This document specifies the record and arbitration layer. Capture, SQLite blob
I/O, placeholder rows, crash recovery, and physical storage are detailed in:

- [SQLite blob patches](../sqlite/docs/BLOB_PATCH.md)

The range layer is spec-authoritative. The public
[`crdt.IntervalMap`](../crdt/interval.go) is its canonical implementation.

## Wire Format

`blob_patch` is operation `4` in the canonical
[DML record format](PROTOCOL.md#dml-records). After `table_id`, primary-key
bytes, and causal length, its payload is:

```text
16 bytes blob_column_id
varint   range_count
[range_count]:
  varint  offset
  varint  byte_len
  N bytes range_bytes
```

Ranges are half-open `[offset, offset+length)` byte intervals. A producer should
coalesce adjacent changed bytes and must not emit overlapping ranges within one
record. The record's causal length must name a live row generation; a patch
cannot create or resurrect a row by itself.

Syzy maps `blob_column_id` through the stable schema catalog to a SQLite BLOB
column. Receivers arbitrate canonical byte positions before writing winning
ranges.

## Range clock

For each logical blob cell, a replica tracks a sparse interval map from byte
ranges to winning changeset stamps. Absent ranges inherit the effective parent
stamp of the whole column or row.

Every explicit interval must strictly dominate that parent stamp. Entries at or
below the parent are redundant and are removed. Adjacent intervals with equal
stamps coalesce into one run.

## Apply

For an inbound patch at stamp `c` over `[start,end)`:

1. Reject or skip the record if its row generation is not the current live
   generation according to the base CRDT rules.
2. Compare `c` with every explicit interval overlapping the range.
3. `c` wins an overlap only when it strictly dominates that interval's stamp.
4. In uncovered gaps, `c` wins only when it strictly dominates the effective
   parent stamp.
5. Write only the winning subranges through SQLite's blob API.
6. Persist the updated interval map under the same apply durability ordering as
   the row state and frontier.

Equal-stamp replay wins no bytes and is therefore idempotent. Disjoint patches
commute; overlapping patches converge by stamp order.

## Composition with full values

A full-row or full-column write advances the parent row/cell stamp. Explicit
range entries dominated by the new parent are absorbed and pruned. Entries that
strictly dominate the full write remain as byte-range overrides.

A delete advances the row to a tombstoned generation and drops the active range
map. A later insert establishes a new live generation. Syzy must not apply a
patch from an older generation merely because its byte stamp is high; row
generation gates range arbitration.

Blob length is a property of the native full value, with surviving range
overrides layered on top. Applications that require independently convergent
logical length should store it in a separate replicated field rather than
inferring it solely from the current byte container.

## Runtime obligations

Syzy must:

- capture the changed bytes and logical row/column identity;
- preserve the source transaction boundary;
- map SQLite blob offsets to canonical byte offsets;
- apply winning ranges transactionally or with an equivalent crash-recoverable
  intent;
- persist range clocks before advancing the accepted frontier; and
- define how missing rows, blob extension, truncation, and native blob handles
  behave.

An unsupported blob-patch schema or record class must be rejected, never
silently applied in arrival order.

The layer's relationship to row and cell clocks is formalized as `F_range` in
[CRDT.md](CRDT.md#layer-composition).
