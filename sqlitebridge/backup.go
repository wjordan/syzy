package sqlitebridge

/*
#include <stdlib.h>
#include "syzy_sqlite.h"
*/
import "C"

import (
	"io"
	"unsafe"
)

// Backup wraps a sqlite3_backup* and copies pages from a source Conn to
// a destination Conn. One Backup serves one source/destination pair;
// callers must Finish before reusing either connection for other work.
//
// Concurrency: backup_step takes a brief writer-lock on the source per
// step, so concurrent writers on the source remain unblocked between
// steps. If the source is modified between steps, SQLite restarts the
// affected pages internally.
type Backup struct {
	bp *C.sqlite3_backup
}

// BackupInit prepares a page-copy from src.dbName ("main", "temp", or
// an attached schema) into dst.dbName. nil error + non-nil *Backup on
// success.
func BackupInit(dst *Conn, dstSchema string, src *Conn, srcSchema string) (*Backup, error) {
	if dstSchema == "" {
		dstSchema = "main"
	}
	if srcSchema == "" {
		srcSchema = "main"
	}
	cDst := C.CString(dstSchema)
	defer C.free(unsafe.Pointer(cDst))
	cSrc := C.CString(srcSchema)
	defer C.free(unsafe.Pointer(cSrc))
	bp := C.sx_backup_init(dst.db, cDst, src.db, cSrc)
	if bp == nil {
		// backup_init reports errors on the destination handle.
		return nil, newErrorFromDB(C.sx_errcode(dst.db), dst.db)
	}
	return &Backup{bp: bp}, nil
}

// Step copies up to nPage pages. Returns io.EOF after the final page is
// copied (analog of SQLITE_DONE). Pass nPage <= 0 to copy every
// remaining page in one go.
func (b *Backup) Step(nPage int) error {
	rc := C.sx_backup_step(b.bp, C.int(nPage))
	switch rc {
	case C.SQLITE_OK:
		return nil
	case C.SQLITE_DONE:
		return io.EOF
	default:
		return newErrorFromCode(rc)
	}
}

// PageCount returns the total number of source pages, as observed at
// the most recent Step. Zero before the first step.
func (b *Backup) PageCount() int {
	return int(C.sx_backup_pagecount(b.bp))
}

// Remaining returns the number of source pages still to copy, as
// observed at the most recent Step.
func (b *Backup) Remaining() int {
	return int(C.sx_backup_remaining(b.bp))
}

// Finish releases the backup handle. Must be called exactly once per
// successful BackupInit. Returns the deferred error from any Step that
// failed (including the SQLITE_BUSY/SQLITE_LOCKED retryables); a final
// SQLITE_OK from Finish means every step succeeded. Idempotent.
func (b *Backup) Finish() error {
	if b == nil || b.bp == nil {
		return nil
	}
	rc := C.sx_backup_finish(b.bp)
	b.bp = nil
	if rc == C.SQLITE_OK {
		return nil
	}
	return newErrorFromCode(rc)
}
