package journal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/wjordan/syzy/internal/futex"
)

// Kind identifies the kind of intent recorded.
type Kind uint8

const (
	KindUnknown  Kind = 0
	KindLocalDML Kind = 1
	KindLocalDDL Kind = 2
	// KindEmpty is a sentinel record appended for commits that produce
	// no replicated DML (DDL, writes to non-replicated tables). The
	// drainer skips it but it still advances the journal head and
	// drained offset.
	KindEmpty Kind = 3
	// KindMirror is an inbound mirror record: the wire-format
	// changeset payload received from a remote origin and applied
	// locally. The record's origin field carries the source origin id;
	// the payload is the encoded crdt.Changeset bytes verbatim.
	// Recovery on restart replays these payloads back through the
	// apply path's idempotency check + LWW.
	KindMirror Kind = 4
	// KindSeal closes a segment and tells iterators to continue at the
	// next segment. It is structural journal metadata, not a record a
	// sink should apply.
	KindSeal Kind = 255
)

// Flag bits for Record.Flags.
const (
	FlagAborted uint16 = 1 << 0
)

const (
	segmentFilePrefix = "seg-"
	segmentFileSuffix = ".bin"
	minPayloadReserve = 1024
	minSegmentSize    = fileHeaderSize + minPayloadReserve
)

// Record is the user-visible view of a journal entry. Payload is
// borrowed from the segment's mmap and is valid only until the
// owning segment is unmapped (Close, or RetainAfter past it).
type Record struct {
	Kind   Kind
	Flags  uint16
	Seq    uint64
	HLC    uint64
	Origin uint64
	// SchemaSeq is the writer's schema-chain position at capture time
	// (0 = pre-stamp record or schema-agnostic payload). Consumers of
	// positional payloads use it to select the capture-time column
	// layout; see AppendWithSchemaSeq.
	SchemaSeq uint32
	Payload   []byte
}

func (r Record) Aborted() bool { return r.Flags&FlagAborted != 0 }

// Offset packs (segmentNumber<<32 | byteOffset) into a single uint64.
// Offsets returned by Append/Iterator are stable for the journal's
// lifetime and totally ordered; consumers compare them with simple
// uint64 comparison. The packing is an implementation detail and
// callers should treat Offset values as opaque tokens.
type Offset uint64

func makeOffset(seg uint32, byteOff uint64) Offset {
	return Offset(uint64(seg)<<32 | (byteOff & 0xFFFFFFFF))
}

func (o Offset) seg() uint32     { return uint32(uint64(o) >> 32) }
func (o Offset) byteOff() uint64 { return uint64(o) & 0xFFFFFFFF }

// SegmentStart returns the Offset at which segment seg begins — the
// RetainAfter cutoff that drops every segment strictly below seg.
func SegmentStart(seg uint32) Offset { return makeOffset(seg, 0) }

// Seg returns the segment number an offset points into. Offsets within
// one segment are contiguous and a higher segment number always sorts
// after a lower one, so a consumer can group records by segment and
// skip whole segments without decoding them. Exposed for callers (e.g.
// the mirror seek index) that maintain per-segment metadata keyed on the
// segment a record landed in.
func (o Offset) Seg() uint32 { return o.seg() }

// ErrSegmentFull is returned by Append only as an internal signal that
// the active segment is full and rotation is needed; callers never
// see it because Journal handles rotation transparently. Exposed for
// tests that want to assert the rotation path was taken.
var ErrSegmentFull = errors.New("journal: segment full")

// ErrPending means an iterator reached a zero publish word: no complete
// record is currently available at that offset.
var ErrPending = errors.New("journal: pending record")

// Journal is a multi-segment append-only log file.
//
// One appender at a time (the producer's commit_hook, naturally
// serialized by the SQLite writer lock). Multiple readers may iterate
// concurrently with the appender; they observe records up to the
// active segment's published head.
type Journal struct {
	dir string
	// segmentSize is the target size for newly allocated regular
	// segments. Individual segment files may be larger when a single
	// record needs more room; the on-disk header carries the actual
	// per-segment size.
	segmentSize uint32
	notify      chan struct{}
	mode        SyncMode

	// wakeFn, when non-nil, is invoked from segment.append after the
	// publish-word atomic store with that word's address. Defaults to
	// nil (no cross-process wake; in-process consumers still see the
	// notify channel and the head pointer). EnableSharedWake installs
	// the futex.WakeAll default; SetWakeFunc replaces with a custom
	// transport (e.g., vsock for cross-VM producers).
	wakeFn atomic.Pointer[func(*uint32)]

	// waitFn, when non-nil, replaces futex.Wait in WaitAt. Cross-VM
	// consumers install a Go-channel-backed waiter fed by an external
	// wake source (the publish word's futex hash bucket lives in the
	// producer's kernel; futex.Wait on the consumer side would only
	// time out).
	waitFn atomic.Pointer[func(context.Context, *uint32, uint32, time.Duration) error]

	// pollOnly: the journal directory is FUSE/virtiofs-backed, so segment
	// mmaps may be DAX and futex(2) on them is both useless (wakes are
	// per-kernel; cross-kernel consumers use a vsock wake or WaitAt's
	// timeout) and hazardous (the page pin races fuse invalidation, and a
	// wait frozen across a VM snapshot restarts against a dead superblock).
	// EnableSharedWake and WaitAt degrade to their no-futex forms; an
	// explicitly-installed wake/wait transport is unaffected.
	pollOnly bool

	// dirFile is held open for fsync at segment rotation. nil under SyncOff.
	dirFile *os.File

	// mu guards segments. The active segment pointer is published via
	// atomic so the hot-path Append doesn't take mu in the common case.
	mu       sync.RWMutex
	segments []*segment

	active  atomic.Pointer[segment]
	nextSeq atomic.Uint64
}

// ErrNoSegments is returned by Open in read-only mode (segmentSize == 0)
// when the journal directory holds no initialized segment yet: the writer
// has created the directory but not its first segment. Segment creation is
// the writer's job, so a reader reports this instead of planting a 0-byte
// segment, letting the caller wait for the writer and retry.
var ErrNoSegments = errors.New("journal: no segments to read")

// HasDrainableSegment reports whether dir holds at least one initialized
// (non-zero-length) segment, i.e. a writer has begun this journal. A
// missing dir, an empty dir, or one holding only zero-length placeholder
// segments all read as not-yet-drainable. A reader of another process's
// journal uses this to skip an origin whose writer hasn't produced a
// segment yet (or never will) without opening — and thereby creating — it.
func HasDrainableSegment(dir string) bool {
	nums, err := listSegmentNumbers(dir)
	if err != nil {
		return false
	}
	for _, n := range nums {
		if fi, err := os.Stat(segmentPath(dir, n)); err == nil && fi.Size() > 0 {
			return true
		}
	}
	return false
}

// Open opens or creates the journal directory at dir. New journals get
// a fresh segment with the given segmentSize. Existing journals
// recover by listing seg-NNNNNNNN.bin files in dir and scanning each
// in order. A read-only handle passes segmentSize == 0; it never creates
// a segment and returns ErrNoSegments when none exists yet.
//
// mode controls per-Append durability: SyncOff (default for mirror
// journals) leaves trailing-record durability to the kernel page
// cache; SyncOn makes Sync() fsync new records to disk and is the
// symmetric knob to app.db's synchronous=FULL.
func Open(dir string, segmentSize uint32, mode SyncMode) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("journal: mkdir: %w", err)
	}
	j := &Journal{dir: dir, segmentSize: segmentSize, notify: make(chan struct{}, 1), mode: mode,
		pollOnly: !futex.PathEligible(dir)}
	if mode == SyncOn {
		df, err := os.Open(dir)
		if err != nil {
			return nil, fmt.Errorf("journal: open dir for fsync: %w", err)
		}
		j.dirFile = df
	}
	nums, err := listSegmentNumbers(dir)
	if err != nil {
		j.closeAll()
		return nil, err
	}
	if len(nums) == 0 {
		// A read-only handle must not create the writer's first segment.
		// Report ErrNoSegments so the caller can wait for the writer rather
		// than planting a 0-byte seg-0 here that then fails every open.
		if segmentSize == 0 {
			j.closeAll()
			return nil, ErrNoSegments
		}
		s, _, _, err := openSegment(segmentPath(dir, 0), 0, segmentSize)
		if err != nil {
			j.closeAll()
			return nil, err
		}
		j.segments = []*segment{s}
		j.active.Store(s)
		j.nextSeq.Store(1)
		// Make the freshly-created first segment durable under SyncOn.
		// Without this a host crash before the first Append could leave
		// the file absent (no dir-entry fsync) or zero-length.
		if mode == SyncOn {
			if err := j.fsyncSegmentMeta(s); err != nil {
				j.closeAll()
				return nil, err
			}
		}
		return j, nil
	}
	var maxSeq uint64
	for _, n := range nums {
		s, lastSeq, _, err := openSegment(segmentPath(dir, n), n, segmentSize)
		if err != nil {
			j.closeAll()
			return nil, err
		}
		j.segments = append(j.segments, s)
		if lastSeq > maxSeq {
			maxSeq = lastSeq
		}
	}
	last := j.segments[len(j.segments)-1]
	if j.segmentSize == 0 {
		// Read-only handles may pass 0 when opening an existing journal.
		// If they are ever used to append, continue with the active
		// segment's size rather than failing a later rotation with a
		// zero target.
		j.segmentSize = last.segmentSize
	}
	if last.sealed.Load() {
		s, _, _, err := openSegment(segmentPath(dir, last.num+1), last.num+1, j.segmentSize)
		if err != nil {
			j.closeAll()
			return nil, err
		}
		j.segments = append(j.segments, s)
		last = s
		if mode == SyncOn {
			if err := j.fsyncSegmentMeta(s); err != nil {
				j.closeAll()
				return nil, err
			}
		}
	}
	j.active.Store(last)
	j.nextSeq.Store(maxSeq + 1)
	return j, nil
}

func segmentPath(dir string, num uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%s%08d%s", segmentFilePrefix, num, segmentFileSuffix))
}

func listSegmentNumbers(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("journal: read dir: %w", err)
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, segmentFilePrefix) || !strings.HasSuffix(name, segmentFileSuffix) {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, segmentFilePrefix), segmentFileSuffix)
		n, err := strconv.ParseUint(mid, 10, 32)
		if err != nil {
			continue // not one of ours
		}
		nums = append(nums, uint32(n))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// Close unmaps and closes all open segments. The caller must ensure
// no Append is in progress.
func (j *Journal) Close() error { return j.closeAll() }

func (j *Journal) closeAll() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	var firstErr error
	for _, s := range j.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	j.segments = nil
	j.active.Store(nil)
	if j.dirFile != nil {
		if err := j.dirFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		j.dirFile = nil
	}
	return firstErr
}

// EnableSharedWake makes Append wake cross-process waiters parked on
// record publish words via futex. Producer-only extension writers
// enable this; in-process producers keep the cheaper Go-channel wake.
// Equivalent to SetWakeFunc(futexWakeAll) / SetWakeFunc(nil).
func (j *Journal) EnableSharedWake(on bool) {
	if on {
		// pollOnly journals never futex-wake: no same-kernel waiter is
		// reachable, and the wake would pin a DAX page. A cross-kernel
		// transport installed via SetWakeFunc still applies.
		if j.pollOnly {
			return
		}
		fn := futexWakeAll
		j.wakeFn.Store(&fn)
	} else {
		j.wakeFn.Store(nil)
	}
}

// SetWakeFunc replaces the per-record wake transport. fn is invoked
// from Append after the publish-word atomic store, with the word's
// address. Use nil to disable external wake (in-process notify still
// fires; cross-process waiters fall back to WaitAt's timeout
// backstop).
//
// Cross-VM producers install a vsock-backed fn here so the host-side
// daemon's WaitAt-with-waitFn gets woken without depending on
// futex (which is per-kernel; see the shared
// persistence design).
func (j *Journal) SetWakeFunc(fn func(*uint32)) {
	if fn == nil {
		j.wakeFn.Store(nil)
		return
	}
	j.wakeFn.Store(&fn)
}

// SetWaitFunc replaces futex.Wait inside WaitAt. fn must block until
// it observes a wake (return nil), the timeout elapses (return
// futex.ErrTimeout), or ctx is cancelled (return ctx.Err()). nil
// restores the futex.Wait default.
func (j *Journal) SetWaitFunc(fn func(context.Context, *uint32, uint32, time.Duration) error) {
	if fn == nil {
		j.waitFn.Store(nil)
		return
	}
	j.waitFn.Store(&fn)
}

// futexWakeAll adapts futex.WakeAll's (int, error) signature to the
// WakeFunc shape. Used by EnableSharedWake.
func futexWakeAll(addr *uint32) {
	_, _ = futex.WakeAll(addr)
}

// loadWakeFn returns the currently-installed wake function or nil.
// segment.append calls the returned fn (if any) after publishing.
func (j *Journal) loadWakeFn() func(*uint32) {
	if p := j.wakeFn.Load(); p != nil {
		return *p
	}
	return nil
}

// Sync flushes any post-Append bytes from the active segment to disk
// when the journal is in SyncOn mode; no-op in SyncOff. The caller
// must serialize Sync with Append (the producer's wal_hook serializes
// naturally through SQLite's writer lock).
//
// Segment-rotation durability (the new segment's directory entry and
// file metadata) is handled inside rotate(), not here, so Sync's only
// job in steady state is one msync over the dirty range from the last
// Sync to the current head.
func (j *Journal) Sync() error {
	if j.mode == SyncOff {
		return nil
	}
	s := j.active.Load()
	if s == nil {
		return errors.New("journal: closed")
	}
	return s.sync()
}

// fsyncSegmentMeta makes a freshly-created segment's file metadata
// (size, directory entry) durable: fdatasync on the segment's fd
// covers the size; fsync on the parent dir covers the entry. Called
// from rotate() and from Open() for a brand-new journal under
// SyncOn. Caller holds j.mu.
func (j *Journal) fsyncSegmentMeta(s *segment) error {
	if err := fdatasyncFile(s.file); err != nil {
		return fmt.Errorf("journal: segment %d fdatasync: %w", s.num, err)
	}
	if j.dirFile != nil {
		if err := j.dirFile.Sync(); err != nil {
			return fmt.Errorf("journal: dir fsync: %w", err)
		}
	}
	return nil
}

// Head returns this process's cached offset just past the last observed
// published record in the active segment. Cross-process readers update
// this cache by iterating or calling Refresh.
func (j *Journal) Head() Offset {
	s := j.active.Load()
	if s == nil {
		return 0
	}
	return makeOffset(s.num, s.head.Load())
}

// Refresh replays published record cells from every mapped segment and
// advances this process's cached segment heads. Normal drainers follow
// the publish words directly; Refresh remains useful for barrier-style
// callers that compare DrainedOffset to Head and for diagnostics.
// Returns the number of records discovered.
func (j *Journal) Refresh() int {
	added := 0
	j.mu.Lock()
	defer j.mu.Unlock()
	for i := 0; i < len(j.segments); i++ {
		s := j.segments[i]
		off := s.head.Load()
		limit := uint64(s.segmentSize)
		for off < limit && !s.sealed.Load() {
			rec, end, err := parseRecordAt(s.data, off, limit)
			if err != nil {
				break
			}
			off = end
			added++
			if rec.Kind == KindSeal {
				s.sealed.Store(true)
				if i == len(j.segments)-1 {
					if _, err := j.openExistingSegmentLocked(s.num + 1); err != nil {
						break
					}
				}
				break
			}
		}
		if off > s.head.Load() {
			s.head.Store(off)
		}
	}
	if len(j.segments) > 0 {
		j.active.Store(j.segments[len(j.segments)-1])
	}
	return added
}

// Dir returns the directory the journal's segments live in. Exposed
// for diagnostics + tests; do not mutate the directory contents
// directly.
func (j *Journal) Dir() string { return j.dir }

// SegmentSize returns the target segment file size in bytes used for
// newly allocated regular segments. Individual segment files may be
// larger when a single record needs more room.
func (j *Journal) SegmentSize() uint32 { return j.segmentSize }

// Notify returns a coalesced channel signaled after each successful
// Append. The signal is only a wake hint: consumers must re-check Head
// after receiving because multiple appends may collapse into one wake.
func (j *Journal) Notify() <-chan struct{} { return j.notify }

// Segments returns a snapshot of the current segment numbers, oldest
// first. Diagnostic.
func (j *Journal) Segments() []uint32 {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make([]uint32, len(j.segments))
	for i, s := range j.segments {
		out[i] = s.num
	}
	return out
}

// Append writes a record into the active segment, rotating to a fresh
// segment if necessary. Returns the new record's Offset and assigned
// sequence number. The record's SchemaSeq is 0 — use AppendWithSchemaSeq
// for payloads whose decode depends on the capture-time column layout.
//
// Not safe for concurrent use; callers must serialize.
func (j *Journal) Append(kind Kind, hlc, origin uint64, payload []byte) (Offset, uint64, error) {
	return j.AppendWithSchemaSeq(kind, hlc, origin, 0, payload)
}

// AppendWithSchemaSeq is Append with the writer's schema-chain position
// stamped into the record header, so a drain running under a later
// schema can decode the positional payload with the capture-time layout.
func (j *Journal) AppendWithSchemaSeq(kind Kind, hlc, origin uint64, schemaSeq uint32, payload []byte) (Offset, uint64, error) {
	if kind == KindUnknown || kind == KindSeal {
		return 0, 0, errors.New("journal: kind required")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return 0, 0, errors.New("journal: payload too large")
	}
	total := recordTotalLen(uint32(len(payload)))
	need, err := requiredSegmentSize(total)
	if err != nil {
		return 0, 0, err
	}
	for {
		s := j.active.Load()
		if s == nil {
			return 0, 0, errors.New("journal: closed")
		}
		if s.sealed.Load() ||
			uint64(s.segmentSize) < uint64(need) ||
			s.head.Load()+uint64(total)+uint64(recordTotalLen(0)) > uint64(s.segmentSize) {
			if err := j.rotate(s, need); err != nil {
				return 0, 0, err
			}
			continue
		}
		seq := j.nextSeq.Add(1) - 1
		off, err := s.append(kind, 0, seq, hlc, origin, schemaSeq, payload, j.loadWakeFn())
		if err == nil {
			j.notifyAppend()
			return makeOffset(s.num, off), seq, nil
		}
		if !errors.Is(err, errSegmentFull) {
			return 0, 0, err
		}
		// Roll back the seq we reserved; rotation is the caller's
		// natural retry, and we'll reserve a fresh seq on retry. This
		// only matters under contention with rotation, but Append is
		// single-writer so it's purely tidiness.
		j.nextSeq.Add(^uint64(0))
		if err := j.rotate(s, need); err != nil {
			return 0, 0, err
		}
	}
}

func requiredSegmentSize(recordLen int) (uint32, error) {
	need := uint64(fileHeaderSize) + uint64(recordLen) + uint64(recordTotalLen(0))
	if need > uint64(^uint32(0)) {
		return 0, errors.New("journal: payload too large for segment")
	}
	return uint32(need), nil
}

func (j *Journal) notifyAppend() {
	select {
	case j.notify <- struct{}{}:
	default:
	}
}

// rotate creates a new segment after expected and publishes it as
// active. expected is the segment whose append failed with
// ErrSegmentFull; if it's no longer active, another caller already
// rotated and we just retry.
//
// Under SyncOn the new segment file is made durable (fdatasync on the
// fd, fsync on the parent dir) before it is published as active, so a
// host crash after rotate returns cannot leave a hole between an
// fsync'd record and a missing-from-disk segment file.
func (j *Journal) rotate(expected *segment, minSize uint32) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.active.Load() != expected {
		return nil // someone else rotated; retry on next iteration
	}
	newSize := j.segmentSize
	if newSize < minSize {
		newSize = minSize
	}
	newNum := expected.num + 1
	s, _, _, err := openSegment(segmentPath(j.dir, newNum), newNum, newSize)
	if err != nil {
		return fmt.Errorf("journal: rotate: %w", err)
	}
	if s.segmentSize < newSize {
		_ = s.close()
		return fmt.Errorf("journal: rotate: segment %d size %d smaller than required %d", newNum, s.segmentSize, newSize)
	}
	if j.mode == SyncOn {
		if err := j.fsyncSegmentMeta(s); err != nil {
			_ = s.close()
			_ = os.Remove(segmentPath(j.dir, newNum))
			return fmt.Errorf("journal: rotate: %w", err)
		}
	}
	if existing := j.findSegment(newNum); existing != nil {
		_ = s.close()
		s = existing
	} else {
		j.segments = append(j.segments, s)
	}
	if !expected.sealed.Load() {
		off := expected.head.Load()
		if off+uint64(recordTotalLen(0)) > uint64(expected.segmentSize) {
			return fmt.Errorf("journal: rotate: no room for segment seal")
		}
		if _, err := expected.append(KindSeal, 0, 0, 0, 0, 0, nil, j.loadWakeFn()); err != nil {
			return fmt.Errorf("journal: rotate seal: %w", err)
		}
		if j.mode == SyncOn {
			if err := expected.sync(); err != nil {
				return fmt.Errorf("journal: rotate seal sync: %w", err)
			}
		}
	}
	j.active.Store(s)
	j.notifyAppend()
	return nil
}

func (j *Journal) openExistingSegmentLocked(num uint32) (*segment, error) {
	if s := j.findSegment(num); s != nil {
		return s, nil
	}
	s, lastSeq, _, err := openExistingSegment(segmentPath(j.dir, num), num)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(j.segments), func(i int) bool { return j.segments[i].num >= num })
	if i < len(j.segments) && j.segments[i].num == num {
		_ = s.close()
		return j.segments[i], nil
	}
	j.segments = slices.Insert(j.segments, i, s)
	if cur := j.active.Load(); cur == nil || s.num > cur.num {
		j.active.Store(s)
	}
	for {
		cur := j.nextSeq.Load()
		if lastSeq < cur || j.nextSeq.CompareAndSwap(cur, lastSeq+1) {
			break
		}
	}
	return s, nil
}

func (j *Journal) publishWord(off Offset) (*uint32, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	s := j.findSegment(off.seg())
	if s == nil {
		var err error
		s, err = j.openExistingSegmentLocked(off.seg())
		if err != nil {
			return nil, err
		}
	}
	byteOff := off.byteOff()
	if byteOff < fileHeaderSize || byteOff+4 > uint64(s.segmentSize) {
		return nil, fmt.Errorf("journal: wait offset %d out of range", off)
	}
	return (*uint32)(unsafe.Pointer(&s.data[byteOff])), nil
}

// WaitAt blocks until a record is published at off or ctx is cancelled.
// timeout bounds futex_wait so missed wakes and Go context cancellation
// are eventually observed; callers that pass <=0 get a conservative
// default.
func (j *Journal) WaitAt(ctx context.Context, off Offset, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		word, err := j.publishWord(off)
		if err != nil {
			return err
		}
		if atomic.LoadUint32(word) != 0 {
			return nil
		}
		if fnPtr := j.waitFn.Load(); fnPtr != nil {
			err = (*fnPtr)(ctx, word, 0, timeout)
		} else if j.pollOnly {
			// FUSE/virtiofs-backed segment: sleep-poll instead of pinning
			// the DAX page in futex_wait; the producer's wake lives in
			// another kernel and could never land here anyway.
			timer := time.NewTimer(timeout)
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
		} else {
			err = futex.Wait(word, 0, timeout)
		}
		if err != nil && err != futex.ErrTimeout {
			return fmt.Errorf("journal: wait: %w", err)
		}
	}
}

// MarkAborted sets FlagAborted on the record at offset.
func (j *Journal) MarkAborted(off Offset) error {
	seg := off.seg()
	j.mu.RLock()
	defer j.mu.RUnlock()
	s := j.findSegment(seg)
	if s == nil {
		return fmt.Errorf("journal: segment %d not found", seg)
	}
	return s.markAborted(off.byteOff())
}

// findSegment looks up segment by number under j.mu (caller holds the
// lock). Returns nil if not present (e.g., already retained-out).
func (j *Journal) findSegment(num uint32) *segment {
	for _, s := range j.segments {
		if s.num == num {
			return s
		}
	}
	return nil
}

// RetainAfter removes (closes and deletes) any segments strictly
// before the segment containing off. Snapshotter GC calls this only
// after a durable metadata snapshot marker and any configured peer-safe
// frontier have made the segment unnecessary for recovery/fetch. Safe
// to call concurrently with Append (segments don't overlap with the
// active one unless rotation already happened).
func (j *Journal) RetainAfter(off Offset) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	cutoff := off.seg()
	keep := j.segments[:0]
	var firstErr error
	for _, s := range j.segments {
		if s.num < cutoff {
			if err := s.close(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := os.Remove(segmentPath(j.dir, s.num)); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		keep = append(keep, s)
	}
	j.segments = keep
	return firstErr
}

// RetainAfterAged is RetainAfter with an age floor: a segment before the
// cutoff is removed only if its file mtime is also before olderThan. This
// keeps the most recent olderThan window of history on disk so a peer that
// fell behind within the retention window can still gap-fill incrementally
// instead of rebaselining. A zero olderThan disables the age floor (then it
// behaves exactly like RetainAfter).
//
// To preserve the "no gaps below the live tail" invariant the rest of the
// journal relies on, pruning stays a contiguous prefix: the cutoff is pulled
// back to the first below-cutoff segment that is too young (or whose mtime
// can't be read), so no younger segment is ever unlinked from beneath an
// older retained one.
func (j *Journal) RetainAfterAged(off Offset, olderThan time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	cutoff := off.seg()
	if !olderThan.IsZero() {
		for _, s := range j.segments {
			if s.num >= cutoff {
				break
			}
			fi, err := s.file.Stat()
			if err != nil || !fi.ModTime().Before(olderThan) {
				cutoff = s.num // young (or unstattable): stop the prefix here
				break
			}
		}
	}
	keep := j.segments[:0]
	var firstErr error
	for _, s := range j.segments {
		if s.num < cutoff {
			if err := s.close(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := os.Remove(segmentPath(j.dir, s.num)); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		keep = append(keep, s)
	}
	j.segments = keep
	return firstErr
}

// Iterator walks records starting at a given offset, crossing segment
// boundaries as needed.
type Iterator struct {
	j   *Journal
	off Offset
}

// Iterate returns an iterator starting at off. Pass 0 for the start
// of the oldest segment. Records appended after Iterate is called
// become visible as the active segment's head advances.
func (j *Journal) Iterate(off Offset) *Iterator {
	if off == 0 {
		j.mu.RLock()
		if len(j.segments) > 0 {
			off = makeOffset(j.segments[0].num, fileHeaderSize)
		}
		j.mu.RUnlock()
	}
	return &Iterator{j: j, off: off}
}

// AlignResume validates a persisted resume offset against the
// journal's actual record geometry and returns a safe equivalent.
// Persisted markers can outlive a journal generation: an origin dying
// uncleanly and reappearing recreates its journal with new record
// boundaries (or fewer segments) while the consumer's marker survives
// in metadata. Resuming at a stale marker either wedges the drainer
// (mid-record offset parses as garbage: EOF below the published head,
// with a nonzero "publish word" that defeats WaitAt) or kills it
// ("segment N not found"). Alignment rules:
//
//   - Marker's segment missing and newer than every existing segment
//     (journal regenerated): restart from the oldest segment.
//     Re-applying records a marker already covered is the documented
//     re-process-silently contract; skipping them is not.
//   - Marker's segment missing and older than the oldest (retained
//     out): snap to the oldest segment's start (Next's existing
//     trail-jump semantic, applied eagerly).
//   - Marker inside a segment: walk record boundaries from the
//     segment start and return the greatest boundary <= marker. A
//     marker on a real boundary comes back unchanged.
func (j *Journal) AlignResume(off Offset) Offset {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if len(j.segments) == 0 {
		return 0
	}
	oldestStart := makeOffset(j.segments[0].num, fileHeaderSize)
	if off == 0 {
		return oldestStart
	}
	s := j.findSegment(off.seg())
	if s == nil {
		return oldestStart
	}
	target := off.byteOff()
	if target < fileHeaderSize {
		return makeOffset(s.num, fileHeaderSize)
	}
	boundary := uint64(fileHeaderSize)
	for boundary < target {
		_, end, err := parseRecordAt(s.data, boundary, uint64(s.segmentSize))
		if err != nil || end > target {
			// Walk stops short of the marker (pending frontier,
			// torn tail, or the marker sits mid-record): the marker
			// does not correspond to this generation's geometry.
			break
		}
		boundary = end
	}
	return makeOffset(s.num, boundary)
}

// Next reads the next record. Returns io.EOF when no more records are
// currently available; callers may resume later as the journal grows.
func (it *Iterator) Next() (Record, Offset, error) {
	for {
		segNum := it.off.seg()
		byteOff := it.off.byteOff()

		it.j.mu.RLock()
		s := it.j.findSegment(segNum)
		// Look ahead: if the requested segment is gone (retained out),
		// advance to the oldest segment we still have.
		if s == nil {
			if len(it.j.segments) == 0 {
				it.j.mu.RUnlock()
				return Record{}, it.off, io.EOF
			}
			oldest := it.j.segments[0]
			if oldest.num > segNum {
				// We trail; jump forward.
				it.off = makeOffset(oldest.num, fileHeaderSize)
				it.j.mu.RUnlock()
				continue
			}
			it.j.mu.RUnlock()
			return Record{}, it.off, fmt.Errorf("journal: segment %d not found", segNum)
		}
		// Track whether a successor segment exists so we know whether
		// to bump to it on EOF.
		hasNext := false
		var nextNum uint32
		for _, ss := range it.j.segments {
			if ss.num > segNum {
				if !hasNext || ss.num < nextNum {
					hasNext = true
					nextNum = ss.num
				}
			}
		}
		// Read the record while holding the read lock so RetainAfter /
		// Close can't unmap s.data underneath us. The lock is released
		// only after parseRecordAt returns; Record.Payload still aliases
		// the mmap, so callers that retain the slice past the next
		// Iterate call must copy.
		rec, end, err := s.next(byteOff)
		it.j.mu.RUnlock()
		if err == nil {
			if rec.Kind == KindSeal {
				it.j.mu.Lock()
				_, ensureErr := it.j.openExistingSegmentLocked(segNum + 1)
				it.j.mu.Unlock()
				if ensureErr != nil {
					return Record{}, it.off, ensureErr
				}
				it.off = makeOffset(segNum+1, fileHeaderSize)
				continue
			}
			origOff := it.off
			it.off = makeOffset(segNum, end)
			return rec, origOff, nil
		}
		if errors.Is(err, ErrPending) {
			return Record{}, it.off, ErrPending
		}
		if !errors.Is(err, io.EOF) {
			return Record{}, it.off, err
		}
		// EOF in this segment: if there's a successor (rotation
		// already advanced past us), jump to its start. Otherwise
		// we're at the live tail — return EOF so the caller can wait.
		if hasNext {
			it.off = makeOffset(nextNum, fileHeaderSize)
			continue
		}
		return Record{}, it.off, io.EOF
	}
}

// Offset returns the iterator's current position. Records up to but
// not including Offset() have been returned by Next.
func (it *Iterator) Offset() Offset { return it.off }
