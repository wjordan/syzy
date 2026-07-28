package sqlitebridge

/*
#include "syzy_sqlite.h"
*/
import "C"

// LibVersion returns the linked SQLite version string (e.g. "3.53.0").
func LibVersion() string {
	return C.GoString(C.sx_libversion())
}

// LibVersionNumber returns the linked SQLite version as an integer
// (major*1_000_000 + minor*1_000 + patch). 3.53.0 → 3_053_000.
func LibVersionNumber() int {
	return int(C.sx_libversion_number())
}
