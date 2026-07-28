package metadata

import (
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

func TestBlobRangeClockEncodeRoundTrip(t *testing.T) {
	col1 := crdt.ColumnID{0xC1, 0xC2}
	col2 := crdt.ColumnID{0xD3, 0xD4}
	cols := []BlobRangeClockEntry{
		{
			Column: col1,
			Entries: []crdt.IntervalEntry{
				{
					Range: crdt.ByteRange{Start: 0, End: 4},
					Stamp: crdt.Stamp{Clock: crdt.Clock{WallTime: 100, Logical: 1}, Origin: 7},
				},
				{
					Range: crdt.ByteRange{Start: 12, End: 16},
					Stamp: crdt.Stamp{Clock: crdt.Clock{WallTime: 200, Logical: 0}, Origin: 11},
				},
			},
		},
		{
			Column: col2,
			Entries: []crdt.IntervalEntry{
				{
					Range: crdt.ByteRange{Start: 32, End: 64},
					Stamp: crdt.Stamp{Clock: crdt.Clock{WallTime: 300, Logical: 5}, Origin: 9},
				},
			},
		},
	}

	buf := EncodeBlobRangeClock(cols)
	got, err := DecodeBlobRangeClock(buf)
	if err != nil {
		t.Fatalf("DecodeBlobRangeClock: %v", err)
	}
	if len(got) != len(cols) {
		t.Fatalf("cols=%d; want %d", len(got), len(cols))
	}
	for i := range cols {
		if got[i].Column != cols[i].Column {
			t.Errorf("col[%d].Column = %x; want %x", i, got[i].Column, cols[i].Column)
		}
		if len(got[i].Entries) != len(cols[i].Entries) {
			t.Errorf("col[%d] entries=%d; want %d", i,
				len(got[i].Entries), len(cols[i].Entries))
		}
		for j, want := range cols[i].Entries {
			ge := got[i].Entries[j]
			if ge.Range != want.Range {
				t.Errorf("col[%d] entry[%d] range = %v; want %v", i, j, ge.Range, want.Range)
			}
			if ge.Stamp != want.Stamp {
				t.Errorf("col[%d] entry[%d] stamp = %v; want %v", i, j, ge.Stamp, want.Stamp)
			}
		}
	}
}

func TestBlobRangeClockEmptyEncodes(t *testing.T) {
	if got := EncodeBlobRangeClock(nil); len(got) != 0 {
		t.Errorf("empty cols → %x; want empty", got)
	}
	got, err := DecodeBlobRangeClock(nil)
	if err != nil || got != nil {
		t.Errorf("DecodeBlobRangeClock(nil) = (%v, %v)", got, err)
	}
}

func TestBlobRangeClockMetaPersist(t *testing.T) {
	sc, _ := openTemp(t)
	tab := crdt.TableID{0xa1, 0xa2}
	pk := crdt.PKBlob{0x10, 0x20}
	col := crdt.ColumnID{0xC1}

	got, err := sc.GetBlobRangeClock(tab, pk)
	if err != nil {
		t.Fatalf("GetBlobRangeClock empty: %v", err)
	}
	if got != nil {
		t.Fatalf("empty row returned %v; want nil", got)
	}
	cols := []BlobRangeClockEntry{
		{
			Column: col,
			Entries: []crdt.IntervalEntry{
				{
					Range: crdt.ByteRange{Start: 0, End: 8},
					Stamp: crdt.Stamp{Clock: crdt.Clock{WallTime: 99, Logical: 2}, Origin: 5},
				},
			},
		},
	}
	if err := sc.PutBlobRangeClock(tab, pk, cols); err != nil {
		t.Fatalf("PutBlobRangeClock: %v", err)
	}
	got, err = sc.GetBlobRangeClock(tab, pk)
	if err != nil {
		t.Fatalf("GetBlobRangeClock: %v", err)
	}
	if len(got) != 1 || len(got[0].Entries) != 1 {
		t.Fatalf("loaded cols = %+v; want 1 col, 1 entry", got)
	}
	if got[0].Entries[0].Stamp.Origin != 5 {
		t.Errorf("origin = %d; want 5", got[0].Entries[0].Stamp.Origin)
	}
	// Empty cols → DELETE.
	if err := sc.PutBlobRangeClock(tab, pk, nil); err != nil {
		t.Fatalf("PutBlobRangeClock(nil): %v", err)
	}
	got, err = sc.GetBlobRangeClock(tab, pk)
	if err != nil || got != nil {
		t.Errorf("after delete: got=%v err=%v; want nil/nil", got, err)
	}
}

// TestBlobRangeClockMultiPageOverflowSurvivesRestart drives the live
// production discipline against blob_range_clock rows whose intervals
// blob spans multiple SQLite overflow pages: wal_autocheckpoint=0 +
// periodic Checkpoint("RESTART"). A field metadata.db pulled
// from a Fly demo failed PRAGMA integrity_check with
//
//	Tree 12 page 481 cell 3: overflow list length is 1 but should be 4
//	Page 486: never used
//	Page 488: never used
//	Page 494: never used
//
// against blob_range_clock rows whose intervals exceeded one page.
// Both nodes had byte-identical corruption, so it was sourced from the
// publisher's snapshot baseline. This test reproduces locally.
func TestBlobRangeClockMultiPageOverflowSurvivesRestart(t *testing.T) {
	sc, path := openTemp(t)
	if err := sc.SetClusterID(crdt.ClusterID{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := sc.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatal(err)
	}
	if err := sc.DisableAutoCheckpoint(); err != nil {
		t.Fatal(err)
	}

	tab := crdt.TableID{0xab}
	col := crdt.ColumnID{0xC1}
	// 600 entries × 32 bytes = ~19 KB; with the 16-byte column header +
	// 2-byte count this is ~5 overflow pages on a 4 KB page DB.
	const nEntries = 600
	entries := make([]crdt.IntervalEntry, nEntries)
	for i := range entries {
		entries[i] = crdt.IntervalEntry{
			Range: crdt.ByteRange{Start: uint64(i * 100), End: uint64(i*100 + 50)},
			Stamp: crdt.Stamp{Clock: crdt.Clock{WallTime: int64(i)}, Origin: 7},
		}
	}
	cols := []BlobRangeClockEntry{{Column: col, Entries: entries}}

	// The broker apply path drives PutBlobRangeClock through WithTx,
	// often batching a PUT + a DELETE in the same outer transaction
	// (an UPSERT that absorbs every dominated entry, then a different
	// row's INSERT). Mirror that here: each "iteration" runs one WithTx
	// that PUTs three rows and DELETEs one previously-written row. This
	// produces the freelist churn (allocate overflow pages, free
	// overflow pages) that blob_range_clock sees in production.
	const nIterations = 200
	for i := 0; i < nIterations; i++ {
		err := sc.WithTx(func(tx *Tx) error {
			for k := 0; k < 3; k++ {
				idx := i*3 + k
				pk := crdt.PKBlob{byte(idx), byte(idx >> 8)}
				if err := tx.PutBlobRangeClock(tab, pk, cols); err != nil {
					return err
				}
			}
			if i > 0 {
				idx := (i - 1) * 3
				pk := crdt.PKBlob{byte(idx), byte(idx >> 8)}
				if err := tx.DeleteBlobRangeClock(tab, pk); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WithTx i=%d: %v", i, err)
		}
		// Periodic RESTART racing with apply traffic, matching the
		// publisher's checkpoint loop cadence.
		if i > 0 && i%17 == 0 {
			if err := sc.Checkpoint("RESTART", nil); err != nil {
				t.Fatalf("Checkpoint i=%d: %v", i, err)
			}
		}
	}
	if err := sc.Checkpoint("RESTART", nil); err != nil {
		t.Fatalf("final Checkpoint: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}

	conn, err := sqlitebridge.Open(path, 0)
	if err != nil {
		t.Fatalf("reopen for integrity_check: %v", err)
	}
	defer conn.Close()
	stmt, _, err := conn.Prepare("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("prepare integrity_check: %v", err)
	}
	defer stmt.Finalize()
	var report []string
	for {
		ok, err := stmt.Step()
		if err != nil {
			t.Fatalf("integrity_check step: %v", err)
		}
		if !ok {
			break
		}
		report = append(report, stmt.ColumnText(0))
	}
	if len(report) != 1 || report[0] != "ok" {
		t.Fatalf("integrity_check failed:\n  %s", report)
	}
}
