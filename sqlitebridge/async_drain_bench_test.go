package sqlitebridge

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"unsafe"
)

// Async-drain journal benchmarks: stock SQLite INSERT plus a separate
// per-process journal append (no fsync) to close the process-crash
// recovery gap for an async drainer. Read against BenchmarkBaselineInsert
// in bench_test.go for the floor (no journaling at all) and against
// BenchmarkTriggerJournalInsert for the same-DB-trigger comparison.
//
// Crash model:
//   - process crash, host alive: kernel still owns dirty pages, journal
//     bytes survive; drainer recovers from last-drained offset.
//   - host crash: bytes past last fsync are lost — accepted tradeoff.
//
// Workload mirrors the existing benches: event(id BLOB PK, n TEXT) with
// a unique 8-byte PK per iter. Each iter does one INSERT and emits one
// journal record carrying op tag, PK, and a 16-byte payload (synthetic
// stand-in for changeset bytes — n is small in this schema, but the
// journal would carry encoded record bytes in production).

const journalRecBytes = 1 /*op*/ + 8 /*pk*/ + 16 /*payload*/

// encodeJournalRec writes an op/pk/payload triple into b at offset 0,
// returning the byte count. Layout matches what a real producer would
// emit so the compare-against-baseline numbers reflect realistic write
// volume.
func encodeJournalRec(b []byte, op byte, pk [8]byte, payload [16]byte) int {
	b[0] = op
	copy(b[1:9], pk[:])
	copy(b[9:25], payload[:])
	return journalRecBytes
}

// --- Variant 1: write(2) with O_APPEND -------------------------------

// fileJournal: simplest portable shape. One write(2) per record into an
// O_APPEND file. Kernel handles ordering; no fsync. Recovery scans from
// a checkpoint offset stored elsewhere.
type fileJournal struct {
	f   *os.File
	buf [journalRecBytes]byte
}

func newFileJournal(path string) (*fileJournal, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileJournal{f: f}, nil
}

func (j *fileJournal) Append(op byte, pk [8]byte, payload [16]byte) error {
	encodeJournalRec(j.buf[:], op, pk, payload)
	_, err := j.f.Write(j.buf[:])
	return err
}

func (j *fileJournal) Close() error { return j.f.Close() }

// --- Variant 2: mmap segment, memcpy + atomic head bump --------------

// mmapJournal: pre-grown file, mmapped MAP_SHARED. Hot path is one
// memcpy plus an atomic head bump — no syscall. A background goroutine
// would handle segment rotation, msync, and tail signaling; the bench
// captures only the producer-side cost. Buffer is large enough that no
// rotation happens during the bench run.
//
// Crash semantics: on process crash, kernel-owned dirty pages still
// flush to disk eventually; the head pointer lives in the same segment
// (offset 0) so the drainer recovers exactly the records the producer
// claimed it wrote.
type mmapJournal struct {
	data []byte
	head *uint64 // points into data[0:8]
	body []byte  // data[8:]
}

const mmapJournalSize = 1 << 28 // 256 MiB; larger than any bench will write

func newMmapJournal(path string) (*mmapJournal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := f.Truncate(mmapJournalSize); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, mmapJournalSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	j := &mmapJournal{
		data: data,
		head: (*uint64)(unsafe.Pointer(&data[0])),
		body: data[8:],
	}
	atomic.StoreUint64(j.head, 0)
	return j, nil
}

func (j *mmapJournal) Append(op byte, pk [8]byte, payload [16]byte) {
	off := atomic.AddUint64(j.head, journalRecBytes) - journalRecBytes
	encodeJournalRec(j.body[off:off+journalRecBytes], op, pk, payload)
}

func (j *mmapJournal) Close() error { return syscall.Munmap(j.data) }

// --- Variant 3: in-process ring with batched flush -------------------

// ringJournal: writes into a fixed in-memory ring; a background drainer
// would copy out and apply. The bench measures only the enqueue cost —
// the absolute floor for "what does it cost the producer to record an
// entry that something else will eventually persist." This is the
// strictly-lower bound; it does not by itself close the process-crash
// gap (a process crash before flush loses the buffered tail), but it
// shows what's achievable if a same-process drainer flushes between
// commits.
type ringJournal struct {
	buf  []byte
	head uint64
}

func newRingJournal(capBytes int) *ringJournal {
	return &ringJournal{buf: make([]byte, capBytes)}
}

func (j *ringJournal) Append(op byte, pk [8]byte, payload [16]byte) {
	off := j.head
	j.head += journalRecBytes
	if int(j.head) > len(j.buf) {
		j.head = journalRecBytes
		off = 0
	}
	encodeJournalRec(j.buf[off:off+journalRecBytes], op, pk, payload)
}

// --- Benchmarks ------------------------------------------------------

func benchAsyncDrainInsert(b *testing.B, append func(op byte, pk [8]byte, payload [16]byte)) {
	c := openBaselineDB(b)
	stmt, _, err := c.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	var payload [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		binary.LittleEndian.PutUint64(id[:], uint64(i))
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
		append(1, id, payload)
	}
}

// BenchmarkAsyncDrainFile: O_APPEND write(2) per record. One syscall on
// the hot path, no fsync. Recovery scans the file from a checkpoint
// offset.
func BenchmarkAsyncDrainFile(b *testing.B) {
	dir := b.TempDir()
	j, err := newFileJournal(filepath.Join(dir, "journal.log"))
	if err != nil {
		b.Fatalf("newFileJournal: %v", err)
	}
	b.Cleanup(func() { _ = j.Close() })

	benchAsyncDrainInsert(b, func(op byte, pk [8]byte, payload [16]byte) {
		if err := j.Append(op, pk, payload); err != nil {
			b.Fatalf("journal append: %v", err)
		}
	})
}

// BenchmarkAsyncDrainMmap: memcpy + atomic head bump into a pre-grown
// MAP_SHARED segment. No syscall on the hot path. Process-crash safe
// (kernel flushes dirty pages); host-crash window is whatever sits
// between writeback intervals.
func BenchmarkAsyncDrainMmap(b *testing.B) {
	dir := b.TempDir()
	j, err := newMmapJournal(filepath.Join(dir, "journal.mmap"))
	if err != nil {
		b.Fatalf("newMmapJournal: %v", err)
	}
	b.Cleanup(func() { _ = j.Close() })

	benchAsyncDrainInsert(b, j.Append)
}

// BenchmarkAsyncDrainRing: in-process ring, no I/O. Pure floor; not by
// itself crash-safe across processes. Useful as the "trigger fire,
// memcpy, return" lower bound.
func BenchmarkAsyncDrainRing(b *testing.B) {
	j := newRingJournal(1 << 24) // 16 MiB
	benchAsyncDrainInsert(b, j.Append)
}
