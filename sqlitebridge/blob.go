package sqlitebridge

/*
#include <stdlib.h>
#include "syzy_sqlite.h"
*/
import "C"

import (
	"unsafe"
)

// Blob is a handle returned by Conn.OpenBlob; it wraps sqlite3_blob*
// for incremental BLOB I/O. Used by the blob_patch capture path
// (read-only, NEW bytes post-commit) and by the apply path
// (read-write, sqlite3_blob_write).
type Blob struct {
	b *C.sqlite3_blob
}

// OpenBlob opens an incremental BLOB handle on (dbName, table, column,
// rowid). dbName "" defaults to "main". writable=true requests
// read-write; false is read-only.
func (c *Conn) OpenBlob(dbName, table, column string, rowid int64, writable bool) (*Blob, error) {
	if dbName == "" {
		dbName = "main"
	}
	cdb := C.CString(dbName)
	defer C.free(unsafe.Pointer(cdb))
	ctab := C.CString(table)
	defer C.free(unsafe.Pointer(ctab))
	ccol := C.CString(column)
	defer C.free(unsafe.Pointer(ccol))
	flags := C.int(0)
	if writable {
		flags = 1
	}
	var bh *C.sqlite3_blob
	rc := C.sx_blob_open(c.db, cdb, ctab, ccol, C.sqlite3_int64(rowid), flags, &bh)
	if rc != C.SQLITE_OK {
		return nil, newErrorFromDB(rc, c.db)
	}
	return &Blob{b: bh}, nil
}

// Bytes returns the byte size of the blob.
func (b *Blob) Bytes() int { return int(C.sx_blob_bytes(b.b)) }

// Read reads len(p) bytes from offset off into p.
func (b *Blob) Read(p []byte, off int) error {
	if len(p) == 0 {
		return nil
	}
	rc := C.sx_blob_read(b.b, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off))
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}

// Write writes len(p) bytes from p at offset off.
func (b *Blob) Write(p []byte, off int) error {
	if len(p) == 0 {
		return nil
	}
	rc := C.sx_blob_write(b.b, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off))
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}

// Reopen redirects the handle to (same column on) a different rowid.
// Cheaper than Close + OpenBlob when iterating rows.
func (b *Blob) Reopen(rowid int64) error {
	rc := C.sx_blob_reopen(b.b, C.sqlite3_int64(rowid))
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}

// Close releases the handle. Idempotent.
func (b *Blob) Close() error {
	if b.b == nil {
		return nil
	}
	rc := C.sx_blob_close(b.b)
	b.b = nil
	if rc != C.SQLITE_OK {
		return newErrorFromCode(rc)
	}
	return nil
}
