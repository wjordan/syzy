package notify

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/wjordan/syzy/internal/futex"
)

// WriterConfig configures a Writer. Path is required; NumSlots
// defaults to DefaultNumSlots.
type WriterConfig struct {
	Path     string
	NumSlots uint32
}

// Writer owns the feed file and is the sole publisher. Append is the
// hot path; one mmap copy plus a single atomic head-store plus an
// (idempotent) futex wake. Safe for concurrent Append from multiple
// goroutines (mu serializes the slot-cursor advance).
type Writer struct {
	cfg      WriterConfig
	file     *os.File
	data     []byte
	numSlots uint32
	headPtr  *uint32 // points into data at hdrHeadOff

	// pollOnly: FUSE/virtiofs-backed feed — suppress futex wakes. No
	// same-kernel waiter can exist that a wake would reach (readers of
	// such feeds are pollOnly too, and cross-kernel readers were never
	// reachable), and a futex op pins the DAX page against fuse
	// invalidation. See futex.FileEligible.
	pollOnly bool

	mu     sync.Mutex
	closed bool
}

// NewWriter creates or rebinds the feed file at cfg.Path. Every call
// bumps generation, so live readers detect writer restarts (including
// crashes mid-publish) and resync via a Lossy notification.
func NewWriter(cfg WriterConfig) (*Writer, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("notify: WriterConfig.Path required")
	}
	numSlots := cfg.NumSlots
	if numSlots == 0 {
		numSlots = DefaultNumSlots
	}
	size := int64(HeaderSize) + int64(numSlots)*int64(SlotSize)

	f, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("notify: open feed: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("notify: stat feed: %w", err)
	}

	priorGen := uint64(0)
	reuse := stat.Size() == size
	if reuse {
		priorGen, err = peekGeneration(f)
		if err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("notify: truncate feed: %w", err)
		}
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("notify: mmap feed: %w", err)
	}

	// Zero the ring on reuse so leftover records from a prior writer
	// can't fool a reader that connects mid-restart. Fresh files are
	// already zero from f.Truncate.
	if reuse {
		clear(data[HeaderSize:])
	}

	copy(data[hdrMagicOff:hdrMagicOff+4], []byte(MagicBytes))
	binary.LittleEndian.PutUint16(data[hdrVersionOff:], FormatVersion)
	binary.LittleEndian.PutUint16(data[hdrHeaderSizeOff:], HeaderSize)
	binary.LittleEndian.PutUint32(data[hdrNumSlotsOff:], numSlots)
	binary.LittleEndian.PutUint32(data[hdrSlotSizeOff:], SlotSize)
	binary.LittleEndian.PutUint64(data[hdrGenerationOff:], priorGen+1)

	headPtr := (*uint32)(unsafe.Pointer(&data[hdrHeadOff]))
	atomic.StoreUint32(headPtr, 0)

	w := &Writer{
		cfg:      cfg,
		file:     f,
		data:     data,
		numSlots: numSlots,
		headPtr:  headPtr,
		pollOnly: !futex.FileEligible(f),
	}
	return w, nil
}

// peekGeneration reads the generation field via ReadAt before we
// mmap, returning 0 on short reads or wrong magic (the header will
// be rewritten anyway).
func peekGeneration(f *os.File) (uint64, error) {
	var hdr [HeaderSize]byte
	n, err := f.ReadAt(hdr[:], 0)
	if err != nil || n < HeaderSize {
		return 0, nil
	}
	if string(hdr[hdrMagicOff:hdrMagicOff+4]) != MagicBytes {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(hdr[hdrGenerationOff:]), nil
}

// Append publishes one Changeset's worth of changes as a contiguous
// run of slots, then atomically advances head and wakes any waiting
// readers. Records sharing one Append form one Notification on the
// reader side.
//
// Empty input is a no-op (no head advance, no wake).
func (w *Writer) Append(changes []Change) error {
	if len(changes) == 0 {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	// head is a monotonic uint32; the slot ring is keyed by head %
	// numSlots. Overrun detection on the reader side relies on
	// unsigned subtraction (head - lastSeen), which is correct across
	// the 2^32 wrap.
	head := atomic.LoadUint32(w.headPtr)
	for i, c := range changes {
		w.writeSlot((head+uint32(i))%w.numSlots, c)
	}
	atomic.StoreUint32(w.headPtr, head+uint32(len(changes)))
	w.mu.Unlock()

	if !w.pollOnly {
		_, _ = futex.WakeAll(w.headPtr)
	}
	return nil
}

func (w *Writer) writeSlot(slotIdx uint32, c Change) {
	off := HeaderSize + int(slotIdx)*SlotSize
	slot := w.data[off : off+SlotSize]
	binary.LittleEndian.PutUint64(slot[slotOriginOff:], c.Origin)
	binary.LittleEndian.PutUint64(slot[slotSeqOff:], c.Seq)
	slot[slotOpOff] = byte(c.Op)

	flags := uint8(0)
	tbl := c.Table
	if len(tbl) > TableNameMaxBytes {
		tbl = tbl[:TableNameMaxBytes]
		flags |= flagTableTruncated
	}
	pk := c.PK
	if len(pk) > PKMaxBytes {
		pk = pk[:PKMaxBytes]
		flags |= flagPKTruncated
	}
	slot[slotFlagsOff] = flags
	slot[slotTableLenOff] = uint8(len(tbl))
	binary.LittleEndian.PutUint16(slot[slotPKLenOff:], uint16(len(pk)))

	tblArea := slot[slotTableOff : slotTableOff+TableNameMaxBytes]
	n := copy(tblArea, tbl)
	// Clear past the value so a prior occupant's bytes don't leak.
	clear(tblArea[n:])
	pkArea := slot[slotPKOff : slotPKOff+PKMaxBytes]
	n = copy(pkArea, pk)
	clear(pkArea[n:])
}

// Close releases the mmap and file. Subsequent Append calls return
// ErrClosed. Idempotent.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	// Wake sleeping readers so they re-check ctx and exit. (pollOnly
	// readers ride their bounded timer; no same-kernel futex waiter exists.)
	if !w.pollOnly {
		_, _ = futex.WakeAll(w.headPtr)
	}
	var firstErr error
	if w.data != nil {
		if err := syscall.Munmap(w.data); err != nil {
			firstErr = fmt.Errorf("notify: munmap: %w", err)
		}
		w.data = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("notify: close feed: %w", err)
		}
		w.file = nil
	}
	return firstErr
}
