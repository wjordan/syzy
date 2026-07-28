package nodestate

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// fakeMirrorSource serves a single origin's journal to RecoverMirror.
type fakeMirrorSource struct {
	self crdt.Origin
	orig crdt.Origin
	j    *journal.Journal
}

func (f *fakeMirrorSource) Origins() []crdt.Origin { return []crdt.Origin{f.orig} }
func (f *fakeMirrorSource) Journal(o crdt.Origin) (*journal.Journal, error) {
	return f.j, nil
}

// TestRecoverMirrorToleratesTornTail is the node-boot regression: an
// unclean exit (OOM/power loss mid-append) leaves a torn trailing record
// in a SyncOff mirror journal. RecoverMirror must recover the valid
// prefix and succeed, not return the journal CRC mismatch as fatal and
// crash-loop the node. The torn tail is recoverable via peer
// re-delivery, so dropping it is safe.
func TestRecoverMirrorToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	const self crdt.Origin = 7
	const orig crdt.Origin = 9

	j, err := journal.Open(dir, 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	// Records RecoverMirror iterates but skips (non-KindMirror): enough
	// to exercise the torn-tail iteration without a real crdt payload.
	for i := 0; i < 4; i++ {
		if _, _, err := j.Append(journal.KindLocalDML, uint64(i), uint64(orig), []byte{byte(i)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the last (index 3) record's CRC: torn trailing append.
	tearLastRecordCRC(t, filepath.Join(dir, "seg-00000000.bin"), 3, 1)

	// Reopen through journal.Open recovery (truncates the torn tail).
	j2, err := journal.Open(dir, 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("reopen torn: %v", err)
	}
	defer j2.Close()

	cache := New(self)
	src := &fakeMirrorSource{self: self, orig: orig, j: j2}
	heads, err := RecoverMirror(cache, src, nil)
	if err != nil {
		t.Fatalf("RecoverMirror returned %v; torn tail must not be fatal", err)
	}
	if _, ok := heads[orig]; !ok {
		t.Errorf("RecoverMirror head missing for origin %d", orig)
	}
}

// TestRecoverMirrorToleratesStaleMarker is the restore regression: a
// node that restored metadata from the bucket adopts the producer's
// snapshot marker, a journal-physical byte offset that does not land on
// a record boundary in this node's own (physically distinct) mirror
// journal. RecoverMirror must align the marker to a real boundary and
// replay forward, not parse mid-record garbage and crash-loop the node
// (observed in prod: "recover mirror: ... record CRC mismatch at offset
// 4752 in segment 0", where 4752 sat 72 bytes inside a valid record).
func TestRecoverMirrorToleratesStaleMarker(t *testing.T) {
	dir := t.TempDir()
	const self crdt.Origin = 7
	const orig crdt.Origin = 9

	j, err := journal.Open(dir, 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	// Craft the payload so its first bytes look like a valid record
	// header (nonzero kind, small payloadLen): a marker pointing at the
	// payload start then parses a bogus record and fails CRC — the exact
	// loud prod symptom ("record CRC mismatch at offset N"), rather than
	// the silent EOF/pending stop a random byte pattern would give.
	payload := make([]byte, 64)
	binary.LittleEndian.PutUint32(payload[0:4], 1)  // kind: nonzero
	binary.LittleEndian.PutUint32(payload[8:12], 8) // payloadLen: small, so end<=limit
	var midRecord journal.Offset
	for i := 0; i < 6; i++ {
		start, _, err := j.Append(journal.KindLocalDML, uint64(i), uint64(orig), payload)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if i == 3 {
			// Record 3's payload start (40 bytes past its boundary): never
			// a boundary itself (segment 0, so composite Offset == byteOff).
			midRecord = start + 40
		}
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	j2, err := journal.Open(dir, 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()

	cache := New(self)
	cache.SetSnapshotMarker(orig, midRecord) // stale, mid-record marker
	src := &fakeMirrorSource{self: self, orig: orig, j: j2}
	if _, err := RecoverMirror(cache, src, nil); err != nil {
		t.Fatalf("RecoverMirror returned %v; a mid-record marker must align, not fail", err)
	}
}

// tearLastRecordCRC corrupts the CRC trailer of the recordIdx-th record
// (0-based) in a journal segment that holds fixed payloadLen-byte
// records, simulating a torn trailing append. Layout matches the
// journal on-disk format: 64B file header, 40B record header, payload,
// 4B CRC, padded to an 8B boundary.
func tearLastRecordCRC(t *testing.T, path string, recordIdx, payloadLen int) {
	t.Helper()
	const fileHeaderSize = 64
	const recordHeaderLen = 40
	const recordTrailerLen = 8 // crc(4) + pad(4)
	const recordAlign = 8
	raw := recordHeaderLen + payloadLen + recordTrailerLen
	total := (raw + recordAlign - 1) &^ (recordAlign - 1)
	recStart := fileHeaderSize + recordIdx*total
	crcOff := int64(recStart + recordHeaderLen + payloadLen)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, crcOff); err != nil {
		t.Fatalf("corrupt CRC: %v", err)
	}
}
