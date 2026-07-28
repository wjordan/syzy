package sqlitebridge

/*
#include <stdlib.h>
#include "hooks.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// EnableTouchJournal turns on auto-capture of preupdate fires into the
// connection's C-side journal buffer. Each fire is appended without crossing
// cgo. Read with TouchJournal/TouchJournalLen and reset with
// ClearTouchJournal. Rollback automatically clears the buffer.
//
// The journal records, per fire:
//   - 1 byte op (SQLITE_INSERT=18, SQLITE_UPDATE=23, SQLITE_DELETE=9)
//   - 8 bytes rowid_old (big-endian int64)
//   - 8 bytes rowid_new (big-endian int64)
//   - 2 bytes db_name length (big-endian uint16) + UTF-8 bytes
//   - 2 bytes table_name length (big-endian uint16) + UTF-8 bytes
//   - 2 bytes column_count (big-endian uint16)
//   - column_count values (1 byte type tag {0=null,1=int,2=real,3=text,4=blob}
//     plus 8 bytes for int/real or 4-byte length + bytes for text/blob).
//   - For UPDATE only: an additional column_count values section with the
//     post-DML NEW values (same encoding).
//
// INSERT records NEW values. DELETE records OLD values. UPDATE records both
// OLD then NEW. Independent of any Go preupdate callback installed via
// SetPreupdateHook.
func (c *Conn) EnableTouchJournal() {
	s := c.ensureState()
	s.cstate.journal_enabled = 1
	c.reinstallPreupdate()
	c.reinstallRollback()
	// syzy_blob_write SQL function is the symmetric SQL surface for
	// the Go BlobWriteAt wrapper: same intent recording, same preupdate
	// suppression. Registered here so any conn that captures DML also
	// exposes the compact blob-write entrypoint.
	C.syzy_register_blob_write_func(c.db, s.cstate)
}

// DisableTouchJournal stops auto-capture. The buffer persists for read-out
// until ClearTouchJournal or Close. Re-enabling resumes appending after the
// existing contents.
func (c *Conn) DisableTouchJournal() {
	if c.state == nil {
		return
	}
	c.state.cstate.journal_enabled = 0
	c.reinstallPreupdate()
	c.reinstallRollback()
}

// TouchJournalEnabled reports whether auto-capture is currently on.
func (c *Conn) TouchJournalEnabled() bool {
	return c.state != nil && c.state.cstate.journal_enabled != 0
}

// TouchJournalLen returns the current journal byte length without copying.
func (c *Conn) TouchJournalLen() int {
	if c.state == nil {
		return 0
	}
	return int(C.syzy_journal_len(c.state.cstate))
}

// TouchJournalTruncated reports whether any append since the last clear hit
// an OOM and dropped data. Consumers should treat the buffer as suspect when
// this returns true and recover via the same mechanism as a metadata I/O
// failure (rollback + retry, or process exit + prepared recovery).
func (c *Conn) TouchJournalTruncated() bool {
	return c.state != nil && c.state.cstate.journal_truncated != 0
}

// TouchJournal returns a slice aliasing the C-side journal buffer. The
// slice is valid only until the next preupdate fire or call to
// ClearTouchJournal — both of which mutate the buffer underneath.
// Callers that need ownership past those points must copy.
//
// The producer's wal_hook hot path consumes the slice immediately by
// passing it to journal.Append (which copies into the mmap), then calls
// ClearTouchJournal — so aliasing is safe and skips the C.GoBytes
// allocation that a defensive copy would force.
func (c *Conn) TouchJournal() []byte {
	if c.state == nil {
		return nil
	}
	n := C.syzy_journal_len(c.state.cstate)
	if n == 0 {
		return nil
	}
	p := C.syzy_journal_data(c.state.cstate)
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}

// TouchJournalCopy returns a Go-owned copy of the current journal bytes.
// Use this when the caller needs the slice to outlive the next preupdate
// fire or ClearTouchJournal call. Empty slice if the journal is empty or
// never enabled.
func (c *Conn) TouchJournalCopy() []byte {
	if c.state == nil {
		return nil
	}
	n := C.syzy_journal_len(c.state.cstate)
	if n == 0 {
		return nil
	}
	p := C.syzy_journal_data(c.state.cstate)
	return C.GoBytes(unsafe.Pointer(p), C.int(n))
}

// TouchJournalTake returns a slice aliasing the C-side journal buffer
// AND clears the buffer in a single cgo crossing. Use this on hot
// paths that would otherwise call TouchJournal followed by
// ClearTouchJournal — combining them eliminates two cgo crossings
// (syzy_journal_len and syzy_journal_clear) per commit.
//
// The returned slice is valid until the next preupdate fire writes
// into the buffer. The producer's wal_hook is safe because SQLite
// cannot fire another preupdate until the hook returns.
func (c *Conn) TouchJournalTake() []byte {
	if c.state == nil {
		return nil
	}
	v := C.syzy_journal_take(c.state.cstate)
	if v.len == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(v.data)), int(v.len))
}

// ClearTouchJournal resets the journal byte length to zero (and clears the
// truncation flag) without freeing the underlying buffer.
func (c *Conn) ClearTouchJournal() {
	if c.state == nil {
		return
	}
	C.syzy_journal_clear(c.state.cstate)
}

// SuppressBlobCapture toggles the per-conn flag that tells the preupdate
// trampoline to skip the OLD-image SYZY_OP_BLOB_WRITE branch for the
// next sqlite3_blob_write fire(s). Use to wrap a Syzy-owned blob_write
// call paired with AppendBlobIntent: the wrapper records compact intent
// in the touch journal and the preupdate fire is silenced. Counter
// semantics — pair Suppress(true) with Suppress(false).
func (c *Conn) SuppressBlobCapture(on bool) {
	s := c.ensureState()
	if on {
		s.cstate.suppress_blob_capture++
	} else if s.cstate.suppress_blob_capture > 0 {
		s.cstate.suppress_blob_capture--
	}
}

// SuppressDMLCapture toggles the per-conn flag that tells the preupdate
// trampoline to skip the regular OLD/NEW row-image emission for the
// next ordinary DML fires. Use to silence captures for trusted writers
// whose effect on peers is communicated via a paired journal record —
// SyzyFS wraps its `data || zeroblob(...)` chunk-extension UPDATE so
// the journal carries only the BlobWriteAt's BLOB_INTENT and the
// receiver's ensureBlobLen rederives the extension. Counter semantics —
// pair Suppress(true) with Suppress(false).
func (c *Conn) SuppressDMLCapture(on bool) {
	s := c.ensureState()
	if on {
		s.cstate.suppress_dml_capture++
	} else if s.cstate.suppress_dml_capture > 0 {
		s.cstate.suppress_dml_capture--
	}
}

// AppendBlobIntent appends a SYZY_OP_BLOB_INTENT record to the touch
// journal: the (table, column, rowid, offset, length) the caller is
// about to write via sqlite3_blob_write. The drainer reads NEW bytes
// for the recorded range from the post-commit DB. dbName "" defaults to
// "main".
func (c *Conn) AppendBlobIntent(dbName, table, column string, rowid int64, offset uint64, length uint32) {
	s := c.ensureState()
	cdb := C.CString(dbName)
	defer C.free(unsafe.Pointer(cdb))
	ctab := C.CString(table)
	defer C.free(unsafe.Pointer(ctab))
	ccol := C.CString(column)
	defer C.free(unsafe.Pointer(ccol))
	C.syzy_journal_append_blob_intent(s.cstate, cdb, ctab, ccol,
		C.int64_t(rowid), C.uint64_t(offset), C.uint32_t(length))
}

// BlobWriteAt writes data at offset on (table, column, rowid) in the
// "main" schema as a syzy-compact blob-write: append a BLOB_INTENT to
// the touch journal and silence the preupdate trampoline's OLD-image
// emission so peers receive intent-only, not a full OLD/NEW row image.
// The row must already exist with the column allocated to at least
// offset+len(data) bytes; growing the column is the caller's job (see
// the SuppressDMLCapture-wrapped UPDATE pattern in syzy.Tx.BlobWriteAt-
// Extending and the SyzyFS adapter).
func (c *Conn) BlobWriteAt(table, column string, rowid int64, offset int, data []byte) error {
	if offset < 0 {
		return fmt.Errorf("sqlitebridge: BlobWriteAt: negative offset %d", offset)
	}
	c.AppendBlobIntent("main", table, column, rowid, uint64(offset), uint32(len(data)))
	c.SuppressBlobCapture(true)
	defer c.SuppressBlobCapture(false)
	blob, err := c.OpenBlob("main", table, column, rowid, true)
	if err != nil {
		return fmt.Errorf("sqlitebridge: BlobWriteAt: open %s.%s rowid=%d: %w", table, column, rowid, err)
	}
	defer blob.Close()
	return blob.Write(data, offset)
}
