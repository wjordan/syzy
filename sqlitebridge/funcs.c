#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// funcs.h handles the sqlite3.h vs sqlite3ext.h selection per build.
#include "funcs.h"

// On success: *out is the generated id. On failure: *err_out is a
// malloc'd C string the caller must free().
extern void syzyGoGenID(sqlite3 *db, const char *table, int table_len,
    sqlite3_int64 *out, char **err_out);

// uuidv7: per-process state for monotonic-within-ms `rand_a` counter.
//
// rand_a is 12 bits. On each call:
//   - if the wall clock advanced since the last call, reset seq to 0 and
//     publish the new ms;
//   - if the clock is stalled or moved backwards, reuse the last published
//     ms and increment seq;
//   - if seq overflows the 12-bit field, advance the published ms by 1
//     (the "borrow from the future" trick from RFC 9562 §6.2 method 1) and
//     reset seq.
//
// A single mutex serializes the small critical section. uuidv7 is not on
// the per-row hot path (it's called only when a DEFAULT fires, i.e. once
// per inserted row), so the mutex is fine.
static pthread_mutex_t syzy_uuidv7_mu = PTHREAD_MUTEX_INITIALIZER;
static uint64_t syzy_uuidv7_last_ms = 0;
static uint16_t syzy_uuidv7_last_seq = 0;

static uint64_t syzy_now_ms(void) {
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static void syzy_uuidv7(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	(void)argc;
	(void)argv;

	uint64_t ms = syzy_now_ms();
	uint16_t seq;

	pthread_mutex_lock(&syzy_uuidv7_mu);
	if (ms > syzy_uuidv7_last_ms) {
		syzy_uuidv7_last_ms = ms;
		syzy_uuidv7_last_seq = 0;
		seq = 0;
	} else if (syzy_uuidv7_last_seq >= 0xFFF) {
		// 12-bit overflow within a single ms — borrow from the future.
		syzy_uuidv7_last_ms++;
		syzy_uuidv7_last_seq = 0;
		ms = syzy_uuidv7_last_ms;
		seq = 0;
	} else {
		ms = syzy_uuidv7_last_ms;
		syzy_uuidv7_last_seq++;
		seq = syzy_uuidv7_last_seq;
	}
	pthread_mutex_unlock(&syzy_uuidv7_mu);

	uint8_t buf[16];
	buf[0] = (uint8_t)(ms >> 40);
	buf[1] = (uint8_t)(ms >> 32);
	buf[2] = (uint8_t)(ms >> 24);
	buf[3] = (uint8_t)(ms >> 16);
	buf[4] = (uint8_t)(ms >> 8);
	buf[5] = (uint8_t)ms;
	// byte 6: ver(4) ‖ rand_a high 4. byte 7: rand_a low 8.
	buf[6] = 0x70 | (uint8_t)((seq >> 8) & 0x0F);
	buf[7] = (uint8_t)(seq & 0xFF);
	sqlite3_randomness(8, buf + 8);
	// variant = 10 in the top 2 bits of byte 8.
	buf[8] = (uint8_t)((buf[8] & 0x3F) | 0x80);

	sqlite3_result_blob(ctx, buf, 16, SQLITE_TRANSIENT);
}

static void syzy_gen_id(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	if (argc != 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT) {
		sqlite3_result_error(ctx,
		    "gen_id: expected single TEXT table-name argument", -1);
		return;
	}
	const char *table = (const char *)sqlite3_value_text(argv[0]);
	int table_len = sqlite3_value_bytes(argv[0]);
	sqlite3 *db = sqlite3_context_db_handle(ctx);

	sqlite3_int64 id = 0;
	char *err = NULL;
	syzyGoGenID(db, table, table_len, &id, &err);
	if (err != NULL) {
		sqlite3_result_error(ctx, err, -1);
		free(err);
		return;
	}
	sqlite3_result_int64(ctx, id);
}

int syzy_register_funcs(sqlite3 *db) {
	// SQLITE_INNOCUOUS allows use in DEFAULT clauses under trusted_schema.
	// Both functions read only process-global state (clock/randomness for
	// uuidv7; per-process partition counters for gen_id), not row content.
	int rc = sqlite3_create_function_v2(db, "uuidv7", 0,
	    SQLITE_UTF8 | SQLITE_INNOCUOUS, NULL,
	    syzy_uuidv7, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	return sqlite3_create_function_v2(db, "gen_id", 1,
	    SQLITE_UTF8 | SQLITE_INNOCUOUS, NULL,
	    syzy_gen_id, NULL, NULL, NULL);
}
