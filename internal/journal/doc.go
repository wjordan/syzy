// Package journal is the producer's deferred-drain ring journal: an
// append-only mmap'd file that records durable commit evidence from
// the producer's wal_hook. A drainer goroutine reads new records as
// they appear and applies them to the metadata in batched transactions.
//
// Records in the journal are durable by construction: wal_hook fires
// after WAL fsync, so the SQLite write that produced the record has
// already reached disk by the time the record is appended. The drainer
// can therefore consume a record as soon as its publish word becomes
// nonzero, with no separate confirmation step.
//
// The hot path is write-only: copy the record bytes, publish the kind
// word, and return. In-process producers also send a coalesced Go
// notification; extension producers enable a futex wake on the publish
// word so daemon drainers can wait cross-process. No SQL runs on the
// wal_hook critical path. Process-crash safety is provided by the
// kernel's page cache flushing dirty pages even if the producer
// process dies; host-crash safety is the existing tradeoff (records
// past the last fsync of either app.db's WAL or the journal file are
// lost together) — unless the journal is opened with SyncOn, in
// which case Sync() flushes each Append via msync before wal_hook
// returns to SQLite. The producer derives SyncMode from app.db's
// PRAGMA main.synchronous (FULL/EXTRA → SyncOn) at construction
// time, so one operator setting drives both files' durability. See
// ARCHITECTURE.md "Host-Level Desync".
//
// Package layout is intentionally minimal: this package owns the
// on-disk format and concurrency-safe record reservation/iteration.
// Higher-level concerns — metadata drainer, rollback_hook integration —
// live in the producer package.
package journal
