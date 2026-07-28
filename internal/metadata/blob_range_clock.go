package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// BlobRangeClockEntry is one column's IntervalMap state for one row.
// Map is the parsed entries; the on-disk representation packs all
// columns of one row into a single intervals BLOB.
type BlobRangeClockEntry struct {
	Column  crdt.ColumnID
	Entries []crdt.IntervalEntry
}

// GetBlobRangeClock returns the per-column IntervalMap entries for the
// row, or nil if no row exists in blob_range_clock.
func (s *Store) GetBlobRangeClock(table crdt.TableID, pk crdt.PKBlob) ([]BlobRangeClockEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return getBlobRangeClock(s.stmts.getBlobRangeClock, table, pk)
}

// PutBlobRangeClock upserts the row's per-column intervals. Pass an
// empty cols slice to delete the row.
func (s *Store) PutBlobRangeClock(table crdt.TableID, pk crdt.PKBlob, cols []BlobRangeClockEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(cols) == 0 {
		return deleteBlobRangeClock(s.stmts.deleteBlobRangeClock, table, pk)
	}
	return putBlobRangeClock(s.stmts.putBlobRangeClock, table, pk, cols)
}

// PutBlobRangeClock inside an open WithTx — see Store.PutBlobRangeClock.
func (tx *Tx) PutBlobRangeClock(table crdt.TableID, pk crdt.PKBlob, cols []BlobRangeClockEntry) error {
	if len(cols) == 0 {
		return deleteBlobRangeClock(tx.stmts.deleteBlobRangeClock, table, pk)
	}
	return putBlobRangeClock(tx.stmts.putBlobRangeClock, table, pk, cols)
}

// DeleteBlobRangeClock drops the row's blob_range_clock entry. Called
// when a full-row DELETE/INSERT/UPDATE absorbs every interval entry.
func (tx *Tx) DeleteBlobRangeClock(table crdt.TableID, pk crdt.PKBlob) error {
	return deleteBlobRangeClock(tx.stmts.deleteBlobRangeClock, table, pk)
}

// GetBlobRangeClock inside an open WithTx.
func (tx *Tx) GetBlobRangeClock(table crdt.TableID, pk crdt.PKBlob) ([]BlobRangeClockEntry, error) {
	return getBlobRangeClock(tx.stmts.getBlobRangeClock, table, pk)
}

// HasAnyBlobRangeClock reports whether blob_range_clock has at least
// one row for the given table_id. Lets the apply path skip the per-row
// GetBlobRangeClock probe + DELETE when no entries can exist (e.g.,
// tables without BLOB columns or that have never received a
// blob_patch). Returns false on the empty-rowset case.
func (s *Store) HasAnyBlobRangeClock(table crdt.TableID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.hasAnyBlobRangeClock
	if err := stmt.Reset(); err != nil {
		return false, err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return false, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return false, err
	}
	if hasRow {
		// drain to completion so the stmt is reset cleanly next call
		if _, err := stmt.Step(); err != nil {
			return true, err
		}
	}
	return hasRow, nil
}

func getBlobRangeClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob) ([]BlobRangeClockEntry, error) {
	if err := stmt.Reset(); err != nil {
		return nil, err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return nil, err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return nil, err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return nil, err
	}
	raw := stmt.ColumnBlob(0)
	if _, err := stmt.Step(); err != nil {
		return nil, err
	}
	return DecodeBlobRangeClock(raw)
}

func putBlobRangeClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob, cols []BlobRangeClockEntry) error {
	buf := EncodeBlobRangeClock(cols)
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return err
	}
	if err := stmt.BindBlob(3, buf); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

func deleteBlobRangeClock(stmt *sqlitebridge.Stmt, table crdt.TableID, pk crdt.PKBlob) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindBlob(1, table[:]); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, pk); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

// EncodeBlobRangeClock packs per-column intervals into the canonical
// blob_range_clock.intervals format (big-endian):
//
//	per column entry with non-empty Entries:
//	  16 bytes column_id
//	   2 bytes n_intervals (uint16)
//	   n_intervals * (start u64, end u64, hlc u64, origin u64)
//
// Columns with zero entries are omitted. An all-empty input encodes to
// a zero-length buffer (callers should use DELETE instead of UPSERT in
// that case; PutBlobRangeClock handles the dispatch).
func EncodeBlobRangeClock(cols []BlobRangeClockEntry) []byte {
	size := 0
	for _, c := range cols {
		if len(c.Entries) == 0 {
			continue
		}
		size += 16 + 2 + 32*len(c.Entries)
	}
	buf := make([]byte, 0, size)
	for _, c := range cols {
		if len(c.Entries) == 0 {
			continue
		}
		buf = append(buf, c.Column[:]...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(c.Entries)))
		for _, e := range c.Entries {
			buf = binary.BigEndian.AppendUint64(buf, e.Range.Start)
			buf = binary.BigEndian.AppendUint64(buf, e.Range.End)
			buf = binary.BigEndian.AppendUint64(buf, e.Stamp.Clock.Pack())
			buf = binary.BigEndian.AppendUint64(buf, uint64(e.Stamp.Origin))
		}
	}
	return buf
}

// LoadIntervalMaps reconstructs per-column IntervalMaps from saved
// entries. Each entry's intervals are non-overlapping and sorted on
// disk, so replaying via Apply with c=entry.Stamp and baseline=Stamp{}
// reproduces the original map.
func LoadIntervalMaps(entries []BlobRangeClockEntry) map[crdt.ColumnID]crdt.IntervalMap {
	out := make(map[crdt.ColumnID]crdt.IntervalMap, len(entries))
	for _, e := range entries {
		m := crdt.NewIntervalMap()
		for _, ent := range e.Entries {
			m.Apply(ent.Range.Start, ent.Range.End, ent.Stamp, crdt.Stamp{})
		}
		out[e.Column] = m
	}
	return out
}

// EntriesFromMaps serializes per-column IntervalMaps into the persisted
// shape, dropping empty maps and sorting columns by ID so the on-disk
// encoding is stable across Go map-iteration orders.
func EntriesFromMaps(maps map[crdt.ColumnID]crdt.IntervalMap) []BlobRangeClockEntry {
	out := make([]BlobRangeClockEntry, 0, len(maps))
	for c, m := range maps {
		if m.IsEmpty() {
			continue
		}
		out = append(out, BlobRangeClockEntry{
			Column:  c,
			Entries: append([]crdt.IntervalEntry(nil), m.Entries()...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Column[:], out[j].Column[:]) < 0
	})
	return out
}

// DecodeBlobRangeClock parses the blob_range_clock.intervals BLOB. An
// empty buf returns nil, nil.
func DecodeBlobRangeClock(buf []byte) ([]BlobRangeClockEntry, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	out := []BlobRangeClockEntry{}
	for off := 0; off < len(buf); {
		if off+16+2 > len(buf) {
			return nil, fmt.Errorf("metadata: truncated blob_range_clock header at %d", off)
		}
		var col crdt.ColumnID
		copy(col[:], buf[off:off+16])
		off += 16
		n := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+32*n > len(buf) {
			return nil, fmt.Errorf("metadata: truncated blob_range_clock entries at %d", off)
		}
		entries := make([]crdt.IntervalEntry, n)
		for i := 0; i < n; i++ {
			start := binary.BigEndian.Uint64(buf[off:])
			off += 8
			end := binary.BigEndian.Uint64(buf[off:])
			off += 8
			hlc := binary.BigEndian.Uint64(buf[off:])
			off += 8
			origin := binary.BigEndian.Uint64(buf[off:])
			off += 8
			entries[i] = crdt.IntervalEntry{
				Range: crdt.ByteRange{Start: start, End: end},
				Stamp: crdt.Stamp{
					Clock:  crdt.UnpackClock(hlc),
					Origin: crdt.Origin(origin),
				},
			}
		}
		out = append(out, BlobRangeClockEntry{Column: col, Entries: entries})
	}
	return out, nil
}
