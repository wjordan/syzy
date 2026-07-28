#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// hooks.h handles the sqlite3.h vs sqlite3ext.h selection per build.
#include "hooks.h"

extern void syzyGoCommitHook(uintptr_t handle, int *ret);
extern void syzyGoRollbackHook(uintptr_t handle);
extern void syzyGoPreupdateHook(uintptr_t handle, sqlite3 *db, int op,
    const char *zDb, const char *zName,
    sqlite3_int64 rowidOld, sqlite3_int64 rowidNew);
extern void syzyGoWALHook(uintptr_t handle, sqlite3 *db, const char *zDb,
    int nFrame, int *ret);
extern void syzyGoProducerWALHook(uintptr_t handle,
    const unsigned char *touchData, size_t touchLen, int nFrame,
    int *ret);
extern void syzyGoTraceHook(uintptr_t handle, unsigned int evt,
    void *x, int *ret);

syzy_conn_state *syzy_state_new(void) {
	return (syzy_conn_state *)calloc(1, sizeof(syzy_conn_state));
}

void syzy_state_free(syzy_conn_state *s) {
	if (!s) return;
	free(s->buf);
	free(s);
}

void syzy_journal_clear(syzy_conn_state *s) {
	if (s) {
		s->buf_len = 0;
		s->journal_truncated = 0;
	}
}

size_t syzy_journal_len(const syzy_conn_state *s) {
	return s ? s->buf_len : 0;
}

const unsigned char *syzy_journal_data(const syzy_conn_state *s) {
	return s ? s->buf : NULL;
}

syzy_journal_view syzy_journal_take(syzy_conn_state *s) {
	syzy_journal_view v = {NULL, 0};
	if (!s) return v;
	v.data = s->buf;
	v.len = s->buf_len;
	s->buf_len = 0;
	s->journal_truncated = 0;
	return v;
}

static int syzy_buf_reserve(syzy_conn_state *s, size_t need) {
	size_t want = s->buf_cap;
	if (want < 64) want = 64;
	while (s->buf_len + need > want) {
		if (want > SIZE_MAX / 2) return 1;
		want *= 2;
	}
	if (want == s->buf_cap) return 0;
	unsigned char *nb = (unsigned char *)realloc(s->buf, want);
	if (!nb) return 1;
	s->buf = nb;
	s->buf_cap = want;
	return 0;
}

static void put_u16_be(unsigned char *p, uint16_t v) {
	p[0] = (unsigned char)(v >> 8);
	p[1] = (unsigned char)(v & 0xff);
}

static void put_u32_be(unsigned char *p, uint32_t v) {
	p[0] = (unsigned char)(v >> 24);
	p[1] = (unsigned char)((v >> 16) & 0xff);
	p[2] = (unsigned char)((v >> 8) & 0xff);
	p[3] = (unsigned char)(v & 0xff);
}

static void put_u64_be(unsigned char *p, uint64_t v) {
	for (int i = 7; i >= 0; --i) {
		p[i] = (unsigned char)(v & 0xff);
		v >>= 8;
	}
}

static void put_i64_be(unsigned char *p, int64_t v) {
	put_u64_be(p, (uint64_t)v);
}

static int syzy_append_value(syzy_conn_state *s, sqlite3_value *v) {
	if (!v) {
		if (syzy_buf_reserve(s, 1)) return 1;
		s->buf[s->buf_len++] = 0;
		return 0;
	}
	switch (sqlite3_value_type(v)) {
	case SQLITE_NULL:
		if (syzy_buf_reserve(s, 1)) return 1;
		s->buf[s->buf_len++] = 0;
		return 0;
	case SQLITE_INTEGER: {
		if (syzy_buf_reserve(s, 1 + 8)) return 1;
		s->buf[s->buf_len++] = 1;
		put_i64_be(s->buf + s->buf_len, sqlite3_value_int64(v));
		s->buf_len += 8;
		return 0;
	}
	case SQLITE_FLOAT: {
		if (syzy_buf_reserve(s, 1 + 8)) return 1;
		s->buf[s->buf_len++] = 2;
		double dv = sqlite3_value_double(v);
		uint64_t u;
		memcpy(&u, &dv, 8);
		put_u64_be(s->buf + s->buf_len, u);
		s->buf_len += 8;
		return 0;
	}
	case SQLITE_TEXT: {
		int n = sqlite3_value_bytes(v);
		const unsigned char *p = sqlite3_value_text(v);
		if (syzy_buf_reserve(s, 1 + 4 + (size_t)n)) return 1;
		s->buf[s->buf_len++] = 3;
		put_u32_be(s->buf + s->buf_len, (uint32_t)n);
		s->buf_len += 4;
		if (n > 0) memcpy(s->buf + s->buf_len, p, (size_t)n);
		s->buf_len += (size_t)n;
		return 0;
	}
	case SQLITE_BLOB: {
		int n = sqlite3_value_bytes(v);
		const void *p = sqlite3_value_blob(v);
		if (syzy_buf_reserve(s, 1 + 4 + (size_t)n)) return 1;
		s->buf[s->buf_len++] = 4;
		put_u32_be(s->buf + s->buf_len, (uint32_t)n);
		s->buf_len += 4;
		if (n > 0) memcpy(s->buf + s->buf_len, p, (size_t)n);
		s->buf_len += (size_t)n;
		return 0;
	}
	}
	return 1;
}

// SYZY_OP_BLOB_WRITE is the syzy-internal op byte tagging blob_write
// preupdate fires. Distinct from SQLite's preupdate ops
// (SQLITE_INSERT=18, SQLITE_UPDATE=23, SQLITE_DELETE=9). When this op
// is the first byte of a journal entry the layout is identical to a
// DELETE record except a 4-byte signed blob_col follows the table
// name length (and precedes ncol), and the captured values are OLD
// (including the pre-write bytes of the targeted blob column).
#define SYZY_OP_BLOB_WRITE 5

// SYZY_OP_BLOB_INTENT is the compact alternative emitted by the
// Syzy-owned blob-write surfaces (Go BlobWriteAt, syzy_blob_write SQL
// function). The wrapper records the (column, offset, length) it is
// about to write before calling sqlite3_blob_write and sets
// suppress_blob_capture so the preupdate trampoline skips the OLD-image
// SYZY_OP_BLOB_WRITE branch. Layout:
//   1 byte  op (SYZY_OP_BLOB_INTENT)
//   8 bytes rowid (int64 BE)
//   2+N     db_name length + bytes
//   2+N     table_name length + bytes
//   2+N     column_name length + bytes
//   8 bytes offset (uint64 BE)
//   4 bytes length (uint32 BE)
// Carries no row values — the drainer reads NEW bytes from the
// post-commit DB for the recorded range.
#define SYZY_OP_BLOB_INTENT 6

// Captured values per op:
//   INSERT : [NEW] (ncol values)
//   UPDATE : [OLD][NEW] (each ncol values)
//   DELETE : [OLD]
// Format is documented on Conn.EnableTouchJournal.
static int syzy_journal_append_preupdate(syzy_conn_state *s, sqlite3 *db,
        int op, const char *zDb, const char *zName,
        sqlite3_int64 rowidOld, sqlite3_int64 rowidNew) {
	size_t db_n = zDb ? strlen(zDb) : 0;
	size_t tbl_n = zName ? strlen(zName) : 0;
	int ncol = sx_preupdate_count(db);

	if (syzy_buf_reserve(s, 1 + 8 + 8 + 2 + db_n + 2 + tbl_n + 2)) return 1;
	s->buf[s->buf_len++] = (unsigned char)op;
	put_i64_be(s->buf + s->buf_len, rowidOld); s->buf_len += 8;
	put_i64_be(s->buf + s->buf_len, rowidNew); s->buf_len += 8;
	put_u16_be(s->buf + s->buf_len, (uint16_t)db_n); s->buf_len += 2;
	if (db_n) memcpy(s->buf + s->buf_len, zDb, db_n);
	s->buf_len += db_n;
	put_u16_be(s->buf + s->buf_len, (uint16_t)tbl_n); s->buf_len += 2;
	if (tbl_n) memcpy(s->buf + s->buf_len, zName, tbl_n);
	s->buf_len += tbl_n;
	put_u16_be(s->buf + s->buf_len, (uint16_t)ncol); s->buf_len += 2;

	// First values section. For INSERT this is NEW; for UPDATE/DELETE OLD.
	for (int i = 0; i < ncol; i++) {
		sqlite3_value *v = NULL;
		if (op == SQLITE_INSERT) {
			sx_preupdate_new(db, i, &v);
		} else {
			sx_preupdate_old(db, i, &v);
		}
		if (syzy_append_value(s, v)) return 1;
	}
	// Second values section for UPDATE only: NEW values.
	if (op == SQLITE_UPDATE) {
		for (int i = 0; i < ncol; i++) {
			sqlite3_value *v = NULL;
			sx_preupdate_new(db, i, &v);
			if (syzy_append_value(s, v)) return 1;
		}
	}
	return 0;
}

// syzy_journal_append_blob_write emits a SYZY_OP_BLOB_WRITE entry with
// the OLD column values (including the pre-write bytes for the target
// blob column). Layout matches a DELETE record except for the 4-byte
// signed blob_col field inserted between the table name and ncol.
static int syzy_journal_append_blob_write(syzy_conn_state *s, sqlite3 *db,
        const char *zDb, const char *zName,
        sqlite3_int64 rowidOld, sqlite3_int64 rowidNew, int blob_col) {
	size_t db_n = zDb ? strlen(zDb) : 0;
	size_t tbl_n = zName ? strlen(zName) : 0;
	int ncol = sx_preupdate_count(db);

	if (syzy_buf_reserve(s, 1 + 8 + 8 + 2 + db_n + 2 + tbl_n + 4 + 2)) return 1;
	s->buf[s->buf_len++] = (unsigned char)SYZY_OP_BLOB_WRITE;
	put_i64_be(s->buf + s->buf_len, rowidOld); s->buf_len += 8;
	put_i64_be(s->buf + s->buf_len, rowidNew); s->buf_len += 8;
	put_u16_be(s->buf + s->buf_len, (uint16_t)db_n); s->buf_len += 2;
	if (db_n) memcpy(s->buf + s->buf_len, zDb, db_n);
	s->buf_len += db_n;
	put_u16_be(s->buf + s->buf_len, (uint16_t)tbl_n); s->buf_len += 2;
	if (tbl_n) memcpy(s->buf + s->buf_len, zName, tbl_n);
	s->buf_len += tbl_n;
	put_u32_be(s->buf + s->buf_len, (uint32_t)blob_col); s->buf_len += 4;
	put_u16_be(s->buf + s->buf_len, (uint16_t)ncol); s->buf_len += 2;

	for (int i = 0; i < ncol; i++) {
		sqlite3_value *v = NULL;
		sx_preupdate_old(db, i, &v);
		if (syzy_append_value(s, v)) return 1;
	}
	return 0;
}

int syzy_journal_append_blob_intent(syzy_conn_state *s,
        const char *zDb, const char *zTable, const char *zColumn,
        int64_t rowid, uint64_t offset, uint32_t length) {
	if (!s) return 1;
	if (!zDb) zDb = "main";
	size_t db_n = strlen(zDb);
	size_t tbl_n = zTable ? strlen(zTable) : 0;
	size_t col_n = zColumn ? strlen(zColumn) : 0;
	if (syzy_buf_reserve(s,
	        1 + 8 + 2 + db_n + 2 + tbl_n + 2 + col_n + 8 + 4)) {
		s->journal_truncated = 1;
		return 1;
	}
	s->buf[s->buf_len++] = (unsigned char)SYZY_OP_BLOB_INTENT;
	put_i64_be(s->buf + s->buf_len, rowid); s->buf_len += 8;
	put_u16_be(s->buf + s->buf_len, (uint16_t)db_n); s->buf_len += 2;
	if (db_n) memcpy(s->buf + s->buf_len, zDb, db_n);
	s->buf_len += db_n;
	put_u16_be(s->buf + s->buf_len, (uint16_t)tbl_n); s->buf_len += 2;
	if (tbl_n) memcpy(s->buf + s->buf_len, zTable, tbl_n);
	s->buf_len += tbl_n;
	put_u16_be(s->buf + s->buf_len, (uint16_t)col_n); s->buf_len += 2;
	if (col_n) memcpy(s->buf + s->buf_len, zColumn, col_n);
	s->buf_len += col_n;
	put_u64_be(s->buf + s->buf_len, offset); s->buf_len += 8;
	put_u32_be(s->buf + s->buf_len, length); s->buf_len += 4;
	return 0;
}

static int syzy_tramp_commit(void *p) {
	syzy_conn_state *s = (syzy_conn_state *)p;
	int ret = 0;
	syzyGoCommitHook(s->hook_handle, &ret);
	return ret;
}

static void syzy_tramp_rollback(void *p) {
	syzy_conn_state *s = (syzy_conn_state *)p;
	if (s->journal_enabled) syzy_journal_clear(s);
	if (s->has_go_rollback) syzyGoRollbackHook(s->hook_handle);
}

static void syzy_tramp_preupdate(void *p, sqlite3 *db, int op,
        const char *zDb, const char *zName,
        sqlite3_int64 rowidOld, sqlite3_int64 rowidNew) {
	syzy_conn_state *s = (syzy_conn_state *)p;
	// Filter trigger-induced writes (depth > 0). Cascade actions are
	// implemented as implicit triggers, so they're filtered too. The
	// effect is captured-changeset == top-level direct-DML writes only;
	// every replica re-derives trigger and cascade effects from the
	// captured source rows.
	if (sx_preupdate_depth(db) > 0) {
		return;
	}
	// sqlite3_blob_write fires preupdate with op=DELETE. Distinguish
	// via sqlite3_preupdate_blobwrite() which returns the column index
	// being mutated (>=0) or -1 for ordinary DML. Blob-write fires emit
	// a tagged journal entry (SYZY_OP_BLOB_WRITE) with OLD values; we
	// skip the regular DML touch path so dedupe doesn't see a bogus
	// DELETE for the row.
	int blob_col = sx_preupdate_blobwrite(db);
	if (blob_col >= 0) {
		// Syzy-owned blob-write surfaces append a SYZY_OP_BLOB_INTENT
		// before the in-flight sqlite3_blob_write and bump
		// suppress_blob_capture; skip the OLD-image emission so the
		// touch journal stays compact (intent record only).
		if (s->journal_enabled && s->suppress_blob_capture <= 0) {
			if (syzy_journal_append_blob_write(s, db, zDb, zName,
			        rowidOld, rowidNew, blob_col)) {
				s->journal_truncated = 1;
			}
		}
		// Go preupdate hooks still see the fire (they may want
		// blob-write awareness via PreupdateEvent.BlobWrite()).
		if (s->has_go_preupdate) {
			syzyGoPreupdateHook(s->hook_handle, db, op, zDb, zName,
			    rowidOld, rowidNew);
		}
		return;
	}
	if (s->journal_enabled && s->suppress_dml_capture <= 0) {
		if (syzy_journal_append_preupdate(s, db, op, zDb, zName,
		        rowidOld, rowidNew)) {
			s->journal_truncated = 1;
		}
	}
	if (s->has_go_preupdate) {
		syzyGoPreupdateHook(s->hook_handle, db, op, zDb, zName,
		    rowidOld, rowidNew);
	}
}

// SYZY_WAL_CHECKPOINT_THRESHOLD: WAL frame count above which our
// trampolines run a PASSIVE checkpoint, restoring the default
// auto-checkpoint behavior that sqlite3_wal_hook() displaces.
// SQLite's default is 1000 (sqlite3WalDefaultHook in sqlite3.c); we
// run at 2000 to halve the rate of the fdatasync that PASSIVE
// triggers on the main-DB write — fewer disk barriers on shared/
// underprovisioned storage at the cost of a slightly larger
// in-flight WAL position.
#define SYZY_WAL_CHECKPOINT_THRESHOLD 2000

static int syzy_tramp_wal(void *p, sqlite3 *db, const char *zDb, int nFrame) {
	syzy_conn_state *s = (syzy_conn_state *)p;
	int ret = 0;
	syzyGoWALHook(s->hook_handle, db, zDb, nFrame, &ret);
	if (ret == SQLITE_OK && nFrame >= SYZY_WAL_CHECKPOINT_THRESHOLD) {
		sqlite3_wal_checkpoint(db, zDb);
	}
	return ret;
}

// syzy_tramp_wal_producer is a specialized trampoline used by the
// producer's hot path. Before calling into Go it reads the C-side
// touch journal buffer and clears it, passing (data, len) directly as
// arguments. Combined with TouchJournalTake (which it replaces), this
// eliminates the second cgo Go→C crossing inside walHook entirely:
// the Go callback receives the touch slice as a function argument and
// no longer has to call back across the boundary to fetch + clear.
//
// The aliased pointer is valid for the duration of the Go callback —
// SQLite cannot fire the next preupdate until wal_hook returns, so
// the buffer's contents won't be overwritten.
static int syzy_tramp_wal_producer(void *p, sqlite3 *db, const char *zDb, int nFrame) {
	syzy_conn_state *s = (syzy_conn_state *)p;
	const unsigned char *touch = s->buf;
	size_t touch_len = s->buf_len;
	s->buf_len = 0;
	s->journal_truncated = 0;
	int ret = 0;
	syzyGoProducerWALHook(s->hook_handle, touch, touch_len, nFrame, &ret);
	if (ret == SQLITE_OK && nFrame >= SYZY_WAL_CHECKPOINT_THRESHOLD) {
		sqlite3_wal_checkpoint(db, zDb);
	}
	return ret;
}

static int syzy_tramp_trace(unsigned int evt, void *uctx, void *p, void *x) {
	(void)p;
	syzy_conn_state *s = (syzy_conn_state *)uctx;
	int ret = 0;
	syzyGoTraceHook(s->hook_handle, evt, x, &ret);
	return ret;
}

void syzy_install_commit_hook(sqlite3 *db, syzy_conn_state *state) {
	if (state) sqlite3_commit_hook(db, syzy_tramp_commit, state);
	else sqlite3_commit_hook(db, NULL, NULL);
}

void syzy_install_rollback_hook(sqlite3 *db, syzy_conn_state *state) {
	if (state) sqlite3_rollback_hook(db, syzy_tramp_rollback, state);
	else sqlite3_rollback_hook(db, NULL, NULL);
}

void syzy_install_preupdate_hook(sqlite3 *db, syzy_conn_state *state) {
	if (state) sx_preupdate_hook(db, syzy_tramp_preupdate, state);
	else sx_preupdate_hook(db, NULL, NULL);
}

void syzy_install_wal_hook(sqlite3 *db, syzy_conn_state *state) {
	if (state) sqlite3_wal_hook(db, syzy_tramp_wal, state);
	else sqlite3_wal_hook(db, NULL, NULL);
}

void syzy_install_producer_wal_hook(sqlite3 *db, syzy_conn_state *state) {
	if (state) sqlite3_wal_hook(db, syzy_tramp_wal_producer, state);
	else sqlite3_wal_hook(db, NULL, NULL);
}

void syzy_install_trace_hook(sqlite3 *db, unsigned int mask,
        syzy_conn_state *state) {
	if (state) sqlite3_trace_v2(db, mask, syzy_tramp_trace, state);
	else sqlite3_trace_v2(db, 0, NULL, NULL);
}

// syzy_blob_write_impl is the SQL function body for syzy_blob_write.
// Records compact intent into the touch journal, runs the underlying
// sqlite3_blob_write with the OLD-image preupdate path silenced, and
// returns NULL.
static void syzy_blob_write_impl(sqlite3_context *ctx, int argc,
        sqlite3_value **argv) {
	if (argc != 5) {
		sqlite3_result_error(ctx,
		    "syzy_blob_write: expects (table, rowid, col, offset, bytes)", -1);
		return;
	}
	syzy_conn_state *s = (syzy_conn_state *)sqlite3_user_data(ctx);
	if (!s) {
		sqlite3_result_error(ctx,
		    "syzy_blob_write: no syzy state on this connection", -1);
		return;
	}
	if (sqlite3_value_type(argv[0]) != SQLITE_TEXT
	    || sqlite3_value_type(argv[1]) != SQLITE_INTEGER
	    || sqlite3_value_type(argv[2]) != SQLITE_TEXT
	    || sqlite3_value_type(argv[3]) != SQLITE_INTEGER) {
		sqlite3_result_error(ctx,
		    "syzy_blob_write: arg types must be (TEXT, INTEGER, TEXT, INTEGER, BLOB)", -1);
		return;
	}
	int btype = sqlite3_value_type(argv[4]);
	if (btype != SQLITE_BLOB && btype != SQLITE_NULL) {
		sqlite3_result_error(ctx,
		    "syzy_blob_write: arg 5 (bytes) must be BLOB or NULL", -1);
		return;
	}
	const char *table = (const char *)sqlite3_value_text(argv[0]);
	sqlite3_int64 rowid = sqlite3_value_int64(argv[1]);
	const char *col = (const char *)sqlite3_value_text(argv[2]);
	sqlite3_int64 offset = sqlite3_value_int64(argv[3]);
	const void *bytes = btype == SQLITE_BLOB ? sqlite3_value_blob(argv[4]) : NULL;
	int len = btype == SQLITE_BLOB ? sqlite3_value_bytes(argv[4]) : 0;
	if (offset < 0) {
		sqlite3_result_error(ctx, "syzy_blob_write: negative offset", -1);
		return;
	}
	sqlite3 *db = sqlite3_context_db_handle(ctx);

	if (syzy_journal_append_blob_intent(s, "main", table, col, rowid,
	        (uint64_t)offset, (uint32_t)len)) {
		sqlite3_result_error(ctx,
		    "syzy_blob_write: out of memory recording intent", -1);
		return;
	}
	s->suppress_blob_capture++;
	sqlite3_blob *bh = NULL;
	int rc = sx_blob_open(db, "main", table, col, rowid, 1, &bh);
	if (rc == SQLITE_OK) {
		if (len > 0) rc = sx_blob_write(bh, bytes, len, (int)offset);
		int crc = sx_blob_close(bh);
		if (rc == SQLITE_OK) rc = crc;
	}
	s->suppress_blob_capture--;
	if (rc != SQLITE_OK) {
		sqlite3_result_error_code(ctx, rc);
		return;
	}
	sqlite3_result_null(ctx);
}

int syzy_register_blob_write_func(sqlite3 *db, syzy_conn_state *state) {
	return sqlite3_create_function_v2(db, "syzy_blob_write", 5,
	    SQLITE_UTF8, state, syzy_blob_write_impl, NULL, NULL, NULL);
}
