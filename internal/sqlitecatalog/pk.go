package catalog

import (
	"fmt"

	corecatalog "github.com/wjordan/syzy/catalog"
	"github.com/wjordan/syzy/crdt"
)

// EncodePK builds the canonical PK blob for this table. byID maps each PK
// column's ID to its value; the encoder iterates t.PK in declared order.
//
// Format (one entry per PK column, in PK position order):
//
//	16 bytes column_id
//	1 byte   type_tag (1=int, 2=real, 3=text, 4=blob)
//	varint   value_byte_len
//	N bytes  value
//
// NULL is rejected (spec: PK columns must be NOT NULL).
func (t *Table) EncodePK(byID map[crdt.ColumnID]crdt.ColValue) (crdt.PKBlob, error) {
	out := make([]byte, 0, 32*len(t.PK))
	for _, col := range t.PK {
		v, ok := byID[col.ID]
		if !ok {
			return nil, fmt.Errorf("catalog: EncodePK missing value for PK column %q", col.Name)
		}
		v.Column = col.ID
		var err error
		out, err = corecatalog.AppendValue(out, v)
		if err != nil {
			return nil, fmt.Errorf("catalog: EncodePK column %q: %w", col.Name, err)
		}
	}
	return out, nil
}

// EncodePKFromSlice builds the canonical PK blob from cols, a slice of
// ColValues in t.Columns order (i.e., position-keyed, the same order
// readAppRow* and parseJournal produce). Avoids the map allocation that
// EncodePK needs.
//
// cols may be SHORTER than t.Columns: a journal record written before an
// ADD COLUMN migration carries only the columns that existed at write time
// (ADD COLUMN appends a non-PK column after the existing ones, so the PK
// columns are always present). Missing trailing columns are the migration's
// defaults, materialized by the apply path. A short slice is therefore fine
// — only a PK column falling outside it is an error.
//
// out, if non-nil with sufficient capacity, is reused; otherwise a fresh
// slice is allocated. The returned slice may alias out.
func (t *Table) EncodePKFromSlice(out []byte, cols []crdt.ColValue) (crdt.PKBlob, error) {
	if cap(out) < 32*len(t.PK) {
		out = make([]byte, 0, 32*len(t.PK))
	} else {
		out = out[:0]
	}
	for _, pkCol := range t.PK {
		// Look up by ColumnID, not position, so the catalog can rearrange
		// t.Columns without breaking PK encoding.
		idx := t.colIndex(pkCol.ID)
		if idx < 0 {
			return nil, fmt.Errorf("catalog: EncodePKFromSlice PK col %q not in t.Columns", pkCol.Name)
		}
		if idx >= len(cols) {
			return nil, fmt.Errorf("catalog: EncodePKFromSlice PK col %q at index %d beyond cols len=%d", pkCol.Name, idx, len(cols))
		}
		var err error
		v := cols[idx]
		v.Column = pkCol.ID
		out, err = corecatalog.AppendValue(out, v)
		if err != nil {
			return nil, fmt.Errorf("catalog: EncodePKFromSlice column %q: %w", pkCol.Name, err)
		}
	}
	return out, nil
}

// EncodeKeyFromSlice builds the canonical reservation tuple for a unique
// key from cols, a slice of ColValues in t.Columns order (the same
// position-keyed shape parseJournal/readAppRow produce). The encoding
// matches EncodePK's per-entry format (column_id, type_tag, varint len,
// bytes) over the key's member columns in declared order, so the same
// logical value encodes identically on every node.
//
// hasNull is true if any member is NULL; the caller skips NULL tuples
// (NULLs do not collide under a UNIQUE constraint). When hasNull is true
// the returned bytes are nil.
func (t *Table) EncodeKeyFromSlice(key UniqueKey, cols []crdt.ColValue) (value []byte, hasNull bool, err error) {
	out := make([]byte, 0, 32*len(key.Columns))
	for _, member := range key.Columns {
		idx := t.colIndex(member.ID)
		if idx < 0 || idx >= len(cols) {
			return nil, false, fmt.Errorf("catalog: EncodeKeyFromSlice key column %q not in row image", member.Name)
		}
		v := cols[idx]
		if v.TypeTag == crdt.ColNull {
			return nil, true, nil
		}
		v.Column = member.ID
		out, err = corecatalog.AppendValue(out, v)
		if err != nil {
			return nil, false, err
		}
	}
	return out, false, nil
}

// colIndex returns the position of the column with id in t.Columns, or -1.
func (t *Table) colIndex(id crdt.ColumnID) int {
	for i, c := range t.Columns {
		if c.ID == id {
			return i
		}
	}
	return -1
}

// DecodePK reverses EncodePK. Returns values keyed by ColumnID. The
// returned ColValue.Bytes alias into blob — callers must not mutate blob
// while the values are in use, and must copy if they need ownership past
// the blob's lifetime. Errors if the blob is truncated.
//
// Hot-path callers should prefer RangePK, which streams entries without
// allocating the result map.
func (t *Table) DecodePK(blob crdt.PKBlob) (map[crdt.ColumnID]crdt.ColValue, error) {
	out := make(map[crdt.ColumnID]crdt.ColValue, len(t.PK))
	if err := t.RangePK(blob, func(id crdt.ColumnID, v crdt.ColValue) error {
		out[id] = v
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// RangePK invokes fn once per PK entry decoded from blob, in encoded
// order. ColValue.Bytes alias into blob — callers must not mutate blob
// while the value is in use, and must copy if they need ownership past
// the blob's lifetime. fn returning a non-nil error halts the walk and
// returns that error to the caller. Errors if the blob is truncated.
func (t *Table) RangePK(blob crdt.PKBlob, fn func(crdt.ColumnID, crdt.ColValue) error) error {
	return corecatalog.RangeTuple(blob, func(value crdt.ColValue) error {
		return fn(value.Column, value)
	})
}
