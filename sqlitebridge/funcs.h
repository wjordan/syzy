#ifndef SYZY_FUNCS_H
#define SYZY_FUNCS_H

#include "syzy_sqlite.h"

// syzy_register_funcs registers the application-defined SQL functions
// `uuidv7()` and `gen_id(table)` on db. Both are intended for use as
// PRIMARY KEY DEFAULT expressions in user schemas. Returns SQLITE_OK
// or the failing sqlite3_create_function_v2 result code.
int syzy_register_funcs(sqlite3 *db);

#endif
