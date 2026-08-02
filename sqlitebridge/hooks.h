#ifndef SYZY_HOOKS_H
#define SYZY_HOOKS_H

#include <stddef.h>
#include <stdint.h>
#include "syzy_sqlite.h"

// syzy_conn_state is the per-Conn user_data passed to every SQLite hook on a
// connection. The has_go_* flags let trampolines skip cgo crossings on
// fast paths where only the touch journal needs the fire.
typedef struct syzy_conn_state {
	unsigned char *buf;
	size_t buf_len;
	size_t buf_cap;
	int journal_enabled;
	int journal_truncated;
	int has_go_preupdate;
	int has_go_rollback;
	// suppress_blob_capture > 0 tells the preupdate trampoline to skip
	// the SYZY_OP_BLOB_WRITE OLD-image emission for in-flight
	// sqlite3_blob_write fires. Set by the Go BlobWriteAt wrapper and
	// the syzy_blob_write SQL function around their internal blob_write
	// call, so the journal carries only the compact intent record they
	// already appended. Counter (not bool) to nest safely.
	int suppress_blob_capture;
	// suppress_dml_capture > 0 tells the preupdate trampoline to skip
	// the regular OLD/NEW row-image emission. Used by trusted writers
	// whose effect on peers is communicated via a paired record (e.g.,
	// SyzyFS wraps its `data || zeroblob(...)` chunk-extension UPDATE so
	// the journal carries only the BlobWriteAt's BLOB_INTENT — the
	// receiver's ensureBlobLen rederives the trailing zero extension).
	// Counter (not bool) to nest safely.
	int suppress_dml_capture;
	// wal_checkpoint_threshold overrides the WAL frame count at which
	// the wal_hook trampolines run their backstop PASSIVE checkpoint.
	// 0 selects the built-in default; negative disables the backstop
	// (the embedder owns WAL bounding).
	int wal_checkpoint_threshold;
	uintptr_t hook_handle;
} syzy_conn_state;

void syzy_set_wal_checkpoint_threshold(syzy_conn_state *s, int n);

syzy_conn_state *syzy_state_new(void);
void syzy_state_free(syzy_conn_state *s);

void syzy_journal_clear(syzy_conn_state *s);
size_t syzy_journal_len(const syzy_conn_state *s);
const unsigned char *syzy_journal_data(const syzy_conn_state *s);

// syzy_journal_view is the {data,len} pair returned by syzy_journal_take.
typedef struct {
	const unsigned char *data;
	size_t len;
} syzy_journal_view;

// syzy_journal_take returns the current buffer pointer + length AND
// atomically clears buf_len = 0 and the truncation flag. Combines
// three cgo crossings (len + data + clear) into one. Returned by
// value (not via out-pointer) so Go's escape analysis doesn't force a
// heap allocation for the call.
//
// The returned pointer remains valid until the next preupdate fire
// writes into the buffer; for the producer's wal_hook path that's
// safe because SQLite cannot dispatch the next preupdate until
// wal_hook returns.
syzy_journal_view syzy_journal_take(syzy_conn_state *s);

void syzy_install_commit_hook(sqlite3 *db, syzy_conn_state *state);
void syzy_install_rollback_hook(sqlite3 *db, syzy_conn_state *state);
void syzy_install_preupdate_hook(sqlite3 *db, syzy_conn_state *state);
void syzy_install_wal_hook(sqlite3 *db, syzy_conn_state *state);
// syzy_install_producer_wal_hook installs a specialized trampoline
// that reads + clears the touch journal buffer in C and passes its
// data + length directly into the Go callback (syzyGoProducerWALHook),
// eliminating the explicit TouchJournalTake cgo crossing inside the
// hot-path walHook. The Go callback signature is the producer-internal
// one (data slice + frame count), not the public WALHook(string, int).
void syzy_install_producer_wal_hook(sqlite3 *db, syzy_conn_state *state);
void syzy_install_trace_hook(sqlite3 *db, unsigned int mask, syzy_conn_state *state);

// syzy_journal_append_blob_intent appends a SYZY_OP_BLOB_INTENT record to
// the touch journal: (rowid, db_name, table_name, column_name, offset,
// length). Carries no column values — the drainer reads NEW bytes for the
// recorded range from the post-commit DB. Returns non-zero on OOM (sets
// journal_truncated). zDb defaults to "main" when NULL.
int syzy_journal_append_blob_intent(syzy_conn_state *s,
    const char *zDb, const char *zTable, const char *zColumn,
    int64_t rowid, uint64_t offset, uint32_t length);

// syzy_register_blob_write_func registers the syzy_blob_write SQL
// function on db with state as user_data. Signature in SQL:
//   syzy_blob_write(table TEXT, rowid INTEGER, col TEXT, offset INTEGER, bytes BLOB) -> NULL
// Body: appends SYZY_OP_BLOB_INTENT to the touch journal, suppresses
// the matching preupdate fire's OLD-image emission, then runs
// sqlite3_blob_open/write/close. Caller must wrap in BEGIN IMMEDIATE/
// COMMIT for replication to fire (sqlite3_blob_close does not commit).
int syzy_register_blob_write_func(sqlite3 *db, syzy_conn_state *state);

#endif
