package metadata

import (
	"encoding/binary"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// Meta keys for the snapshotter's checkpoint blobs: per-origin
// applied_gaps SeqSets and per-origin journal-offset markers.
const (
	keyAppliedGaps     = "applied_gaps"
	keySnapshotMarkers = "snapshot_markers"
)

// FullRowClock returns every row_clock entry. Used by recovery to
// rehydrate the in-memory cache. Caller iterates without holding the
// metadata mutex past return — entries are detached.
type FullRowClockEntry struct {
	Table crdt.TableID
	PK    crdt.PKBlob
	CL    uint64
	Base  crdt.Stamp
}

// AllRowClocks scans the entire row_clock table. Allocates one slice
// for the result; for very large tables consider streaming, but for
// the recovery seed (one-shot) this is fine.
func (s *Store) AllRowClocks() ([]FullRowClockEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt, _, err := s.conn.Prepare(`SELECT table_id, pk_blob, cl, base_hlc, base_origin FROM row_clock`)
	if err != nil {
		return nil, fmt.Errorf("metadata: prepare AllRowClocks: %w", err)
	}
	defer stmt.Finalize()
	var out []FullRowClockEntry
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		tableBytes := stmt.ColumnBlob(0)
		pkBytes := stmt.ColumnBlob(1)
		var tableID crdt.TableID
		if len(tableBytes) != len(tableID) {
			return nil, fmt.Errorf("metadata: table_id wrong width: got %d, want %d", len(tableBytes), len(tableID))
		}
		copy(tableID[:], tableBytes)
		pk := make(crdt.PKBlob, len(pkBytes))
		copy(pk, pkBytes)
		out = append(out, FullRowClockEntry{
			Table: tableID,
			PK:    pk,
			CL:    uint64(stmt.ColumnInt64(2)),
			Base: crdt.Stamp{
				Clock:  crdt.UnpackClock(uint64(stmt.ColumnInt64(3))),
				Origin: crdt.Origin(stmt.ColumnInt64(4)),
			},
		})
	}
}

// GetAppliedGaps reads the applied_gaps blob and decodes per-origin
// SeqSets. Empty map when absent.
func (s *Store) GetAppliedGaps() (map[crdt.Origin]crdt.SeqSet, error) {
	v, ok, err := s.GetMeta(keyAppliedGaps)
	if err != nil || !ok {
		return map[crdt.Origin]crdt.SeqSet{}, err
	}
	return decodeAppliedGaps(v)
}

// SetAppliedGaps writes the applied_gaps blob inside an open WithTx.
// Encodes as a flat varint stream; see encodeAppliedGaps for layout.
func (tx *Tx) SetAppliedGaps(m map[crdt.Origin]crdt.SeqSet) error {
	return setMeta(tx.stmts.setMeta, keyAppliedGaps, encodeAppliedGaps(m))
}

// SetAppliedGaps standalone (autocommit) variant; tests use it.
func (s *Store) SetAppliedGaps(m map[crdt.Origin]crdt.SeqSet) error {
	return s.SetMeta(keyAppliedGaps, encodeAppliedGaps(m))
}

// GetSnapshotMarkers reads the per-origin journal-offset markers blob.
// Empty map when absent.
func (s *Store) GetSnapshotMarkers() (map[crdt.Origin]uint64, error) {
	v, ok, err := s.GetMeta(keySnapshotMarkers)
	if err != nil || !ok {
		return map[crdt.Origin]uint64{}, err
	}
	return decodeMarkers(v)
}

// SetSnapshotMarkers writes the markers blob inside an open WithTx.
func (tx *Tx) SetSnapshotMarkers(m map[crdt.Origin]uint64) error {
	return setMeta(tx.stmts.setMeta, keySnapshotMarkers, encodeMarkers(m))
}

// SetSnapshotMarkers standalone variant; tests + bootstrap.
func (s *Store) SetSnapshotMarkers(m map[crdt.Origin]uint64) error {
	return s.SetMeta(keySnapshotMarkers, encodeMarkers(m))
}

// encodeAppliedGaps lays out:
//
//	uint32 nOrigins
//	for each origin:
//	  uint64 origin_id
//	  uint32 nRanges
//	  for each range: uint64 lo, uint64 hi
//
// All big-endian. Compact and safe to evolve via a leading version byte
// later if needed; for v0 we keep it untagged and check length at decode.
func encodeAppliedGaps(m map[crdt.Origin]crdt.SeqSet) []byte {
	var buf []byte
	buf = appendU32(buf, uint32(len(m)))
	for origin, gs := range m {
		buf = appendU64(buf, uint64(origin))
		ranges := gs.Ranges()
		buf = appendU32(buf, uint32(len(ranges)))
		for _, r := range ranges {
			buf = appendU64(buf, uint64(r.Lo))
			buf = appendU64(buf, uint64(r.Hi))
		}
	}
	return buf
}

func decodeAppliedGaps(b []byte) (map[crdt.Origin]crdt.SeqSet, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("metadata: applied_gaps blob truncated (%d bytes)", len(b))
	}
	out := map[crdt.Origin]crdt.SeqSet{}
	n, b, err := readU32(b)
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < n; i++ {
		o, rest, err := readU64(b)
		if err != nil {
			return nil, err
		}
		nr, rest, err := readU32(rest)
		if err != nil {
			return nil, err
		}
		var ss crdt.SeqSet
		for j := uint32(0); j < nr; j++ {
			lo, r2, err := readU64(rest)
			if err != nil {
				return nil, err
			}
			hi, r3, err := readU64(r2)
			if err != nil {
				return nil, err
			}
			// Add each seq in [lo, hi] — SeqSet's internal ranges
			// reconstitute. Cheap for short ranges; if these grow we'll
			// add a bulk-ranges constructor.
			for s := lo; s <= hi; s++ {
				ss.Add(crdt.Seq(s))
			}
			rest = r3
		}
		out[crdt.Origin(o)] = ss
		b = rest
	}
	return out, nil
}

func encodeMarkers(m map[crdt.Origin]uint64) []byte {
	var buf []byte
	buf = appendU32(buf, uint32(len(m)))
	for origin, off := range m {
		buf = appendU64(buf, uint64(origin))
		buf = appendU64(buf, off)
	}
	return buf
}

func decodeMarkers(b []byte) (map[crdt.Origin]uint64, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("metadata: snapshot_markers blob truncated (%d bytes)", len(b))
	}
	n, b, err := readU32(b)
	if err != nil {
		return nil, err
	}
	out := make(map[crdt.Origin]uint64, n)
	for i := uint32(0); i < n; i++ {
		o, b1, err := readU64(b)
		if err != nil {
			return nil, err
		}
		off, b2, err := readU64(b1)
		if err != nil {
			return nil, err
		}
		out[crdt.Origin(o)] = off
		b = b2
	}
	return out, nil
}

func appendU32(b []byte, v uint32) []byte {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	return append(b, x[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	return append(b, x[:]...)
}

func readU32(b []byte) (uint32, []byte, error) {
	if len(b) < 4 {
		return 0, nil, fmt.Errorf("metadata: short read u32 (%d bytes)", len(b))
	}
	return binary.BigEndian.Uint32(b[:4]), b[4:], nil
}

func readU64(b []byte) (uint64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, fmt.Errorf("metadata: short read u64 (%d bytes)", len(b))
	}
	return binary.BigEndian.Uint64(b[:8]), b[8:], nil
}
