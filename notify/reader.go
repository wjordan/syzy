package notify

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/wjordan/syzy/internal/futex"
)

// readerWakeInterval bounds how long Read blocks in the futex before
// re-checking ctx; futex_wait is uninterruptible from Go.
const readerWakeInterval = 500 * time.Millisecond

type ReaderConfig struct {
	Path string
}

// Reader consumes a feed. On writer restart (generation change) or
// ring overrun, the next Read returns a single Lossy notification
// and resumes from the new head.
type Reader struct {
	cfg  ReaderConfig
	file *os.File
	data []byte

	numSlots   uint32
	headPtr    *uint32
	genPtr     *uint64
	generation uint64

	// pollOnly: the feed is FUSE/virtiofs-backed (see futex.FileEligible),
	// so Read waits on wake+timer instead of futex_wait — a futex wake
	// could never cross from the writer's kernel anyway, and futex_wait's
	// page pin on a DAX mapping is a guest-kernel hazard. wake carries
	// Interrupt (and same-process writer wakes); capacity 1, non-blocking
	// send, so a wake can never be lost between drain-check and wait.
	pollOnly bool
	wake     chan struct{}

	mu       sync.Mutex
	lastSeen uint32
	closed   bool

	// scratch backs returned Change values; pkBuf backs Change.PK
	// slices; notifBuf backs returned Notification slices. All are
	// reset per Read; consumers retaining values past the next call
	// must copy.
	scratch  []Change
	pkBuf    []byte
	notifBuf []Notification
}

// NewReader opens the feed file at cfg.Path read-only. The file must
// already exist; the reader starts caught up to the writer's current
// head, so events published before NewReader returns are not
// delivered. Returns ErrFormatMismatch on bad header.
func NewReader(cfg ReaderConfig) (*Reader, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("notify: ReaderConfig.Path required")
	}
	f, err := os.OpenFile(cfg.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("notify: open feed: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("notify: stat feed: %w", err)
	}
	if stat.Size() < int64(HeaderSize) {
		f.Close()
		return nil, ErrFormatMismatch
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("notify: mmap feed: %w", err)
	}

	bad := string(data[hdrMagicOff:hdrMagicOff+4]) != MagicBytes ||
		binary.LittleEndian.Uint16(data[hdrVersionOff:]) != FormatVersion ||
		binary.LittleEndian.Uint32(data[hdrSlotSizeOff:]) != SlotSize ||
		binary.LittleEndian.Uint32(data[hdrNumSlotsOff:]) == 0
	if bad {
		_ = syscall.Munmap(data)
		f.Close()
		return nil, ErrFormatMismatch
	}

	headPtr := (*uint32)(unsafe.Pointer(&data[hdrHeadOff]))
	genPtr := (*uint64)(unsafe.Pointer(&data[hdrGenerationOff]))

	r := &Reader{
		cfg:        cfg,
		file:       f,
		data:       data,
		numSlots:   binary.LittleEndian.Uint32(data[hdrNumSlotsOff:]),
		headPtr:    headPtr,
		genPtr:     genPtr,
		generation: atomic.LoadUint64(genPtr),
		lastSeen:   atomic.LoadUint32(headPtr),
		pollOnly:   !futex.FileEligible(f),
		wake:       make(chan struct{}, 1),
	}
	return r, nil
}

// Read blocks until at least one Notification is available, ctx is
// cancelled, or the writer closes the feed. PK byte slices in the
// returned Changes alias reader-owned scratch and must be copied if
// retained past the next call.
//
// Ring overrun or writer restart (generation change) surface as a
// single Notification with Lossy=true; the reader then resumes from
// the new head.
func (r *Reader) Read(ctx context.Context) ([]Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		notifs, head, ready := r.tryDrainLocked()
		if ready {
			return notifs, nil
		}
		if err := r.waitHead(head); err != nil {
			return nil, err
		}
	}
}

// waitHead blocks until the head may have advanced past head, an
// Interrupt lands, or readerWakeInterval elapses. Called with r.mu held
// (same as the futex wait it wraps: Close blocks behind the bounded
// wait, Interrupt does not take the lock).
func (r *Reader) waitHead(head uint32) error {
	if !r.pollOnly {
		if err := futex.Wait(r.headPtr, head, readerWakeInterval); err != nil && err != futex.ErrTimeout {
			return fmt.Errorf("notify: futex wait: %w", err)
		}
		return nil
	}
	timer := time.NewTimer(readerWakeInterval)
	defer timer.Stop()
	select {
	case <-r.wake:
	case <-timer.C:
	}
	return nil
}

// TryRead is the non-blocking form of Read: returns whatever's
// pending past the last drain without futex_wait. Same scratch-
// aliasing rules as Read.
func (r *Reader) TryRead() ([]Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	notifs, _, _ := r.tryDrainLocked()
	return notifs, nil
}

// tryDrainLocked is the shared core of Read and TryRead. It checks
// for a generation bump (writer restart) or pending events; returns
// (lossy, _, true) on a generation change, (drained, _, true) when
// events are available, and (nil, head, false) when the ring is at
// rest — in which case head is the value Read should pass to
// futex.Wait. Caller must hold r.mu.
func (r *Reader) tryDrainLocked() (notifs []Notification, head uint32, ready bool) {
	gen := atomic.LoadUint64(r.genPtr)
	if gen != r.generation {
		r.generation = gen
		r.lastSeen = atomic.LoadUint32(r.headPtr)
		return r.lossyOnce(), 0, true
	}
	head = atomic.LoadUint32(r.headPtr)
	if head == r.lastSeen {
		return nil, head, false
	}
	return r.drain(head), head, true
}

// drain reads slots in [lastSeen, head), groups them by (Origin, Seq),
// and returns the resulting Notifications. A single end-of-loop head
// re-check detects ring overrun: if the writer lapped us during the
// read, slot bytes are potentially torn — return Lossy and resync.
func (r *Reader) drain(head uint32) []Notification {
	delta := head - r.lastSeen
	if delta > r.numSlots {
		r.lastSeen = head
		return r.lossyOnce()
	}

	r.scratch = r.scratch[:0]
	r.pkBuf = r.pkBuf[:0]
	if cap(r.pkBuf) < int(delta)*PKMaxBytes {
		r.pkBuf = make([]byte, 0, int(delta)*PKMaxBytes)
	}

	for i := uint32(0); i < delta; i++ {
		off := HeaderSize + int((r.lastSeen+i)%r.numSlots)*SlotSize
		slot := r.data[off : off+SlotSize]

		flags := slot[slotFlagsOff]
		tblLen := int(slot[slotTableLenOff])
		if tblLen > TableNameMaxBytes {
			tblLen = TableNameMaxBytes
		}
		pkLen := int(binary.LittleEndian.Uint16(slot[slotPKLenOff:]))
		if pkLen > PKMaxBytes {
			pkLen = PKMaxBytes
		}

		pkStart := len(r.pkBuf)
		r.pkBuf = append(r.pkBuf, slot[slotPKOff:slotPKOff+pkLen]...)

		r.scratch = append(r.scratch, Change{
			Origin:         binary.LittleEndian.Uint64(slot[slotOriginOff:]),
			Seq:            binary.LittleEndian.Uint64(slot[slotSeqOff:]),
			Op:             Op(slot[slotOpOff]),
			Table:          string(slot[slotTableOff : slotTableOff+tblLen]),
			PK:             r.pkBuf[pkStart : pkStart+pkLen],
			TableTruncated: flags&flagTableTruncated != 0,
			PKTruncated:    flags&flagPKTruncated != 0,
		})
	}

	if atomic.LoadUint32(r.headPtr)-r.lastSeen > r.numSlots {
		r.lastSeen = atomic.LoadUint32(r.headPtr)
		return r.lossyOnce()
	}
	r.lastSeen = head

	r.notifBuf = r.notifBuf[:0]
	for i := 0; i < len(r.scratch); {
		j := i + 1
		for j < len(r.scratch) &&
			r.scratch[j].Origin == r.scratch[i].Origin &&
			r.scratch[j].Seq == r.scratch[i].Seq {
			j++
		}
		r.notifBuf = append(r.notifBuf, Notification{
			Origin:  r.scratch[i].Origin,
			Seq:     r.scratch[i].Seq,
			Changes: r.scratch[i:j],
		})
		i = j
	}
	return r.notifBuf
}

// lossyOnce returns a single Lossy notification reusing notifBuf so
// the slice header is amortized like the normal-path return.
func (r *Reader) lossyOnce() []Notification {
	r.notifBuf = append(r.notifBuf[:0], Notification{Lossy: true})
	return r.notifBuf
}

// Interrupt wakes a Read blocked in its bounded wait so it re-checks
// its context immediately instead of waiting out readerWakeInterval.
// Callers that cancel a Read's context and need the Read to have
// returned (e.g. before unmapping the feed's backing mount) must call
// this, then join the reading goroutine. Does not take r.mu (Read
// holds it across the wait); headPtr is immutable after NewReader,
// and a futex wake on an already-unmapped address is a harmless EFAULT.
func (r *Reader) Interrupt() {
	if r.pollOnly {
		select {
		case r.wake <- struct{}{}:
		default:
		}
		return
	}
	_, _ = futex.WakeAll(r.headPtr)
}

// Close releases the mmap and file. Concurrent Read calls observe
// ErrClosed on their next loop iteration (after the futex timeout).
// Idempotent.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var firstErr error
	if r.data != nil {
		if err := syscall.Munmap(r.data); err != nil {
			firstErr = fmt.Errorf("notify: munmap: %w", err)
		}
		r.data = nil
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("notify: close feed: %w", err)
		}
		r.file = nil
	}
	return firstErr
}
