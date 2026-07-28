package syncer

import (
	"encoding/binary"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// journalRecord is one decoded preupdate fire from the C-side touch
// journal. Mirrors the byte format documented on
// sqlitebridge.Conn.EnableTouchJournal.
//
// DBName and Table alias into the input buf (no string allocation).
// Buf must outlive the journalRecord.
type journalRecord struct {
	Op       int // SQLITE_INSERT=18, SQLITE_UPDATE=23, SQLITE_DELETE=9, syzyBlobWrite=5, syzyBlobIntent=6
	DBName   []byte
	Table    []byte
	OldRowID int64
	NewRowID int64
	// BlobCol is the 0-based column index targeted by sqlite3_blob_write.
	// Set only when Op == syzyBlobWrite; -1 otherwise.
	BlobCol int32
	// BlobColName is the targeted column name for syzyBlobIntent fires
	// (the wrapper records the name; the drainer resolves it via the
	// catalog at materialize time). Aliases into buf.
	BlobColName []byte
	// BlobOffset / BlobLen describe the byte range the syzyBlobIntent
	// caller is about to write. Unused for other ops.
	BlobOffset uint64
	BlobLen    uint32
	// Values is INSERT=NEW, UPDATE=OLD, DELETE=OLD, syzyBlobWrite=OLD
	// (with the OLD bytes of the targeted blob column intact). One
	// entry per table column in declared order; Bytes alias into the
	// touch-journal buffer. Empty for syzyBlobIntent.
	Values []crdt.ColValue
	// NewValues is populated only for UPDATE.
	NewValues []crdt.ColValue
}

// parseJournal decodes the bridge's touch-journal byte stream into
// journalRecord entries appended to scratch. Pass nil to allocate
// fresh, or a previously-returned slice (with len reset to 0) to reuse
// capacity. Returns the populated slice.
//
// ColValue.Bytes and rec.DBName/Table alias into buf — buf must
// outlive the returned records.
//
// Errors on a truncated buffer or unknown tags.
//
// To preserve the per-record Values/NewValues slice capacity across
// reuses, this grows scratch via re-slicing within capacity (rather
// than appending zero values, which would clobber the slice headers).
func parseJournal(buf []byte, scratch []journalRecord) ([]journalRecord, error) {
	out := scratch
	for off := 0; off < len(buf); {
		if off+1 > len(buf) {
			return nil, fmt.Errorf("syncer: truncated journal op at off=%d", off)
		}
		// Grow without zeroing if capacity is available — preserves the
		// existing rec.Values / rec.NewValues backing arrays.
		i := len(out)
		if i < cap(out) {
			out = out[:i+1]
		} else {
			out = append(out, journalRecord{})
		}
		rec := &out[i]
		rec.Op = int(buf[off])
		off++
		rec.BlobCol = -1
		rec.BlobColName = nil
		rec.BlobOffset = 0
		rec.BlobLen = 0

		if rec.Op == syzyBlobIntent {
			var err error
			off, err = parseBlobIntent(buf, off, rec)
			if err != nil {
				return nil, err
			}
			rec.Values = rec.Values[:0]
			rec.NewValues = rec.NewValues[:0]
			continue
		}

		if off+8+8+2 > len(buf) {
			return nil, fmt.Errorf("syncer: truncated journal header at off=%d", off)
		}
		rec.OldRowID = int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		rec.NewRowID = int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8

		dbN := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+dbN > len(buf) {
			return nil, fmt.Errorf("syncer: truncated db_name at off=%d", off)
		}
		rec.DBName = buf[off : off+dbN]
		off += dbN

		if off+2 > len(buf) {
			return nil, fmt.Errorf("syncer: truncated table_name length at off=%d", off)
		}
		tblN := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+tblN > len(buf) {
			return nil, fmt.Errorf("syncer: truncated table_name at off=%d", off)
		}
		rec.Table = buf[off : off+tblN]
		off += tblN

		// Blob-write fires carry an extra 4-byte signed blob_col field
		// before ncol; ordinary DML records skip directly to ncol.
		if rec.Op == syzyBlobWrite {
			if off+4 > len(buf) {
				return nil, fmt.Errorf("syncer: truncated blob_col at off=%d", off)
			}
			rec.BlobCol = int32(binary.BigEndian.Uint32(buf[off:]))
			off += 4
		}

		if off+2 > len(buf) {
			return nil, fmt.Errorf("syncer: truncated col count at off=%d", off)
		}
		ncol := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2

		var err error
		rec.Values, off, err = parseValues(buf, off, ncol, rec.Values[:0])
		if err != nil {
			return nil, err
		}
		if rec.Op == sqliteUpdate {
			rec.NewValues, off, err = parseValues(buf, off, ncol, rec.NewValues[:0])
			if err != nil {
				return nil, err
			}
		} else {
			rec.NewValues = rec.NewValues[:0]
		}
	}
	return out, nil
}

// parseBlobIntent decodes a SYZY_OP_BLOB_INTENT record body (the op byte
// has already been consumed). Layout: rowid (i64), db (u16+bytes), table
// (u16+bytes), column (u16+bytes), offset (u64), length (u32).
func parseBlobIntent(buf []byte, off int, rec *journalRecord) (int, error) {
	if off+8+2 > len(buf) {
		return 0, fmt.Errorf("syncer: truncated blob_intent header at off=%d", off)
	}
	rec.OldRowID = int64(binary.BigEndian.Uint64(buf[off:]))
	rec.NewRowID = rec.OldRowID
	off += 8
	dbN := int(binary.BigEndian.Uint16(buf[off:]))
	off += 2
	if off+dbN+2 > len(buf) {
		return 0, fmt.Errorf("syncer: truncated blob_intent db_name at off=%d", off)
	}
	rec.DBName = buf[off : off+dbN]
	off += dbN
	tblN := int(binary.BigEndian.Uint16(buf[off:]))
	off += 2
	if off+tblN+2 > len(buf) {
		return 0, fmt.Errorf("syncer: truncated blob_intent table_name at off=%d", off)
	}
	rec.Table = buf[off : off+tblN]
	off += tblN
	colN := int(binary.BigEndian.Uint16(buf[off:]))
	off += 2
	if off+colN+8+4 > len(buf) {
		return 0, fmt.Errorf("syncer: truncated blob_intent column_name at off=%d", off)
	}
	rec.BlobColName = buf[off : off+colN]
	off += colN
	rec.BlobOffset = binary.BigEndian.Uint64(buf[off:])
	off += 8
	rec.BlobLen = binary.BigEndian.Uint32(buf[off:])
	off += 4
	return off, nil
}

// parseValues decodes ncol typed column values starting at off.
// ColValue.Bytes alias into buf. Pass scratch to reuse capacity from
// a previous invocation.
func parseValues(buf []byte, off, ncol int, scratch []crdt.ColValue) ([]crdt.ColValue, int, error) {
	out := scratch
	if cap(out) < ncol {
		out = make([]crdt.ColValue, 0, ncol)
	}
	for i := 0; i < ncol; i++ {
		if off >= len(buf) {
			return nil, 0, fmt.Errorf("syncer: truncated tag at col %d", i)
		}
		tag := buf[off]
		off++
		switch tag {
		case 0:
			out = append(out, crdt.ColValue{TypeTag: crdt.ColNull})
		case 1:
			if off+8 > len(buf) {
				return nil, 0, fmt.Errorf("syncer: truncated int at col %d", i)
			}
			out = append(out, crdt.ColValue{TypeTag: crdt.ColInt, Bytes: buf[off : off+8]})
			off += 8
		case 2:
			if off+8 > len(buf) {
				return nil, 0, fmt.Errorf("syncer: truncated real at col %d", i)
			}
			out = append(out, crdt.ColValue{TypeTag: crdt.ColReal, Bytes: buf[off : off+8]})
			off += 8
		case 3, 4:
			if off+4 > len(buf) {
				return nil, 0, fmt.Errorf("syncer: truncated bytes length at col %d", i)
			}
			n := int(binary.BigEndian.Uint32(buf[off:]))
			off += 4
			if off+n > len(buf) {
				return nil, 0, fmt.Errorf("syncer: truncated bytes at col %d", i)
			}
			ct := crdt.ColText
			if tag == 4 {
				ct = crdt.ColBlob
			}
			out = append(out, crdt.ColValue{TypeTag: ct, Bytes: buf[off : off+n]})
			off += n
		default:
			return nil, 0, fmt.Errorf("syncer: unknown journal tag %d at col %d", tag, i)
		}
	}
	return out, off, nil
}
