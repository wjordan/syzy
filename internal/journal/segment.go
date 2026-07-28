package journal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// pageSize is captured once at init for msync alignment math. msync
// requires the start address to be page-aligned; the segment mmap
// itself is always page-aligned (mmap with offset 0), so we round the
// start byte offset down to a page boundary using pageMask.
var (
	pageSize = uint64(os.Getpagesize())
	pageMask = pageSize - 1
)

func init() {
	if pageSize == 0 || pageSize&(pageSize-1) != 0 {
		panic(fmt.Sprintf("journal: page size %d is not a power of two", pageSize))
	}
}

// segment is one mmap'd file holding a contiguous range of records.
// Operations are file-relative byte offsets; segment numbering and
// cross-segment ordering live in Journal.
type segment struct {
	num         uint32
	file        *os.File
	data        []byte
	segmentSize uint32

	// head is the in-memory append offset within this segment's file.
	// Writers store head after publishing each record; readers use the
	// per-record publish word as the cross-process visibility primitive.
	head   atomic.Uint64
	sealed atomic.Bool

	// syncedHead is the byte offset already msync'd to disk.
	// Writer-exclusive (single-writer contract); no atomic needed.
	syncedHead uint64
}

// openSegment opens the segment file at path. If it does not exist, a
// fresh segment is created with size; if it exists, the file's
// existing length is used and segmentSize is ignored. The recovered
// head is the offset just past the last fully valid record (so partial
// trailing records get overwritten on the next Append).
func openSegment(path string, num uint32, size uint32) (*segment, uint64, bool, error) {
	return openSegmentFile(path, num, size, true)
}

func openExistingSegment(path string, num uint32) (*segment, uint64, bool, error) {
	return openSegmentFile(path, num, 0, false)
}

func openSegmentFile(path string, num uint32, size uint32, create bool) (*segment, uint64, bool, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, 0, false, fmt.Errorf("journal: open segment: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, false, fmt.Errorf("journal: stat segment: %w", err)
	}
	created := stat.Size() == 0
	if created {
		if size < minSegmentSize {
			f.Close()
			return nil, 0, false, fmt.Errorf("journal: segmentSize %d too small (min %d)", size, minSegmentSize)
		}
		var saltBuf [16]byte
		if _, err := rand.Read(saltBuf[:]); err != nil {
			f.Close()
			return nil, 0, false, fmt.Errorf("journal: salt: %w", err)
		}
		if err := f.Truncate(int64(size)); err != nil {
			f.Close()
			return nil, 0, false, fmt.Errorf("journal: truncate segment: %w", err)
		}
		data, err := mmapFile(f, int(size))
		if err != nil {
			f.Close()
			return nil, 0, false, err
		}
		hdr := fileHeader{
			Magic:       magic,
			Version:     formatVersion,
			SegmentSize: size,
			CreatedUs:   uint64(time.Now().UnixMicro()),
			Salt0:       binary.LittleEndian.Uint64(saltBuf[0:]),
			Salt1:       binary.LittleEndian.Uint64(saltBuf[8:]),
		}
		hdrBytes := encodeFileHeader(hdr)
		copy(data[:fileHeaderSize], hdrBytes[:])
		s := &segment{num: num, file: f, data: data, segmentSize: size, syncedHead: fileHeaderSize}
		s.head.Store(fileHeaderSize)
		return s, 0, false, nil
	}

	size = uint32(stat.Size())
	data, err := mmapFile(f, int(size))
	if err != nil {
		f.Close()
		return nil, 0, false, err
	}
	hdr, err := decodeFileHeader(data[:fileHeaderSize])
	if err != nil {
		_ = syscall.Munmap(data)
		f.Close()
		return nil, 0, false, fmt.Errorf("journal: header: %w", err)
	}
	if hdr.SegmentSize != size {
		_ = syscall.Munmap(data)
		f.Close()
		return nil, 0, false, fmt.Errorf("journal: header segmentSize %d != file size %d", hdr.SegmentSize, size)
	}
	head, lastSeq, sealed, stop := scanRecords(data, size)
	// A torn trailing record (publish word set, but payload/CRC not fully
	// flushed before an unclean exit) leaves an invalid record at head.
	// Under SyncOff a lost trailing append is recoverable from the peer,
	// so truncate the torn tail and recover the valid prefix rather than
	// failing every reader at the bad offset. Distinguish from genuine
	// mid-journal corruption (a valid record exists after the bad one),
	// which must still surface.
	if errors.Is(stop, errInvalidRecord) {
		if recordAfter(data, head, size) {
			_ = syscall.Munmap(data)
			f.Close()
			return nil, 0, false, fmt.Errorf("journal: mid-journal corruption at offset %d in segment %d", head, num)
		}
		slog.Warn("journal: truncating torn trailing record",
			"segment", num, "offset", head, "path", path)
		clear(data[head:size])
	}
	// Pre-existing records are presumed durable from the prior process
	// (or recoverable as the same kind of host-crash loss the journal's
	// SyncOff mode already accepts). Start tracking msync coverage from
	// the recovered head forward.
	s := &segment{num: num, file: f, data: data, segmentSize: size, syncedHead: head}
	s.head.Store(head)
	s.sealed.Store(sealed)
	return s, lastSeq, sealed, nil
}

func mmapFile(f *os.File, size int) ([]byte, error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("journal: mmap: %w", err)
	}
	return data, nil
}

// scanRecords walks records starting at fileHeaderSize, returning the
// offset just past the last fully valid record, the highest sequence
// observed (zero if none), and the error that stopped the scan. A nil
// or errEndOfData/ErrPending stop is a clean tail; errInvalidRecord
// means the record at head is torn or corrupt.
func scanRecords(data []byte, segmentSize uint32) (head, lastSeq uint64, sealed bool, stop error) {
	off := uint64(fileHeaderSize)
	head = off
	for {
		rec, end, err := parseRecordAt(data, off, uint64(segmentSize))
		if err != nil {
			return head, lastSeq, sealed, err
		}
		head = end
		if rec.Seq > lastSeq {
			lastSeq = rec.Seq
		}
		if rec.Kind == KindSeal {
			return head, lastSeq, true, nil
		}
		off = end
	}
}

// recordAfter reports whether any valid record exists at an aligned
// offset strictly after from within [from, segmentSize). A valid record
// past a CRC-failed one means the corruption is mid-journal (real data
// follows) rather than a torn trailing append; the recovery path must
// not truncate in that case. Records are recordAlign-aligned, so the
// scan steps by recordAlign.
func recordAfter(data []byte, from uint64, segmentSize uint32) bool {
	limit := uint64(segmentSize)
	for off := from + recordAlign; off+recordHeaderLen+crcLen <= limit; off += recordAlign {
		if _, _, err := parseRecordAt(data, off, limit); err == nil {
			return true
		}
	}
	return false
}

// parseRecordAt reads the record header at off and returns the parsed
// record plus the offset just past it.
func parseRecordAt(data []byte, off, limit uint64) (rec Record, end uint64, err error) {
	if off+recordHeaderLen+crcLen > limit {
		return Record{}, off, errEndOfData
	}
	hdrBytes := data[off : off+recordHeaderLen]
	kindWord := binary.LittleEndian.Uint32(hdrBytes[0:])
	if kindWord == 0 {
		return Record{}, off, ErrPending
	}
	rh := decodeRecordHeader(hdrBytes)
	if rh.Kind == 0 {
		return Record{}, off, errInvalidRecord
	}
	end = off + uint64(recordTotalLen(rh.PayloadLen))
	if end > limit {
		return Record{}, off, errEndOfData
	}
	hdrEnd := off + recordHeaderLen
	payloadEnd := hdrEnd + uint64(rh.PayloadLen)
	payload := data[hdrEnd:payloadEnd]
	if Kind(rh.Kind) == KindSeal && len(payload) != 0 {
		return Record{}, off, errInvalidRecord
	}
	got := binary.LittleEndian.Uint32(data[payloadEnd : payloadEnd+crcLen])
	want := recordCRC(hdrBytes, payload)
	if got != want {
		return Record{}, off, errInvalidRecord
	}
	flagsAddr := (*uint32)(unsafe.Pointer(&hdrBytes[flagsHeaderOffset]))
	flags := uint16(atomic.LoadUint32(flagsAddr))
	rec = Record{
		Kind:      Kind(rh.Kind),
		Flags:     flags,
		Seq:       rh.Seq,
		HLC:       rh.HLC,
		Origin:    rh.Origin,
		SchemaSeq: rh.SchemaSeq,
		Payload:   payload,
	}
	return rec, end, nil
}

var (
	errEndOfData     = errors.New("journal: no full record")
	errInvalidRecord = errors.New("journal: invalid record")
)

// errSegmentFull is the segment-private full signal; the public
// ErrSegmentFull lives in journal.go.
var errSegmentFull = errors.New("journal: segment full")

// append writes a record into the segment's mmap. Returns the
// file-relative byte offset of the new record. Not safe for concurrent
// use; callers serialize.
//
// wakeFn, when non-nil, is invoked with the publish-word address
// immediately after the atomic store that publishes the record. It's
// the cross-process notification (futex.WakeAll for same-host
// extension producers, vsock send for cross-VM producers).
func (s *segment) append(kind Kind, flags uint16, seq, hlc, origin uint64, schemaSeq uint32, payload []byte, wakeFn func(*uint32)) (uint64, error) {
	if s.sealed.Load() {
		return 0, errSegmentFull
	}
	total := recordTotalLen(uint32(len(payload)))
	off := s.head.Load()
	if off+uint64(total) > uint64(s.segmentSize) {
		return 0, errSegmentFull
	}

	hdr := s.data[off : off+recordHeaderLen]
	binary.LittleEndian.PutUint32(hdr[0:4], 0)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(flags))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[12:16], schemaSeq)
	binary.LittleEndian.PutUint64(hdr[16:24], seq)
	binary.LittleEndian.PutUint64(hdr[24:32], hlc)
	binary.LittleEndian.PutUint64(hdr[32:40], origin)

	hdrEnd := off + recordHeaderLen
	payloadEnd := hdrEnd + uint64(len(payload))
	copy(s.data[hdrEnd:payloadEnd], payload)
	crc := recordCRC(hdr, payload)
	binary.LittleEndian.PutUint32(s.data[payloadEnd:payloadEnd+crcLen], crc)
	clear(s.data[payloadEnd+crcLen : off+uint64(total)])

	kindAddr := (*uint32)(unsafe.Pointer(&s.data[off]))
	atomic.StoreUint32(kindAddr, uint32(kind))
	if wakeFn != nil {
		wakeFn(kindAddr)
	}
	s.head.Store(off + uint64(total))
	if kind == KindSeal {
		s.sealed.Store(true)
	}
	return off, nil
}

// markAborted sets FlagAborted on the record at byteOff.
func (s *segment) markAborted(byteOff uint64) error {
	if byteOff < fileHeaderSize || byteOff+recordHeaderLen > uint64(s.segmentSize) {
		return errors.New("journal: offset out of range")
	}
	flagsAddr := (*uint32)(unsafe.Pointer(&s.data[byteOff+flagsHeaderOffset]))
	atomic.OrUint32(flagsAddr, uint32(FlagAborted))
	return nil
}

// next reads the next record at byteOff. Returns ErrPending when the
// publish word is still zero and io.EOF when byteOff is outside the
// segment's readable range.
func (s *segment) next(byteOff uint64) (Record, uint64, error) {
	if byteOff >= uint64(s.segmentSize) {
		return Record{}, byteOff, io.EOF
	}
	rec, end, err := parseRecordAt(s.data, byteOff, uint64(s.segmentSize))
	switch {
	case errors.Is(err, ErrPending):
		return Record{}, byteOff, ErrPending
	case errors.Is(err, errEndOfData):
		return Record{}, byteOff, io.EOF
	case errors.Is(err, errInvalidRecord):
		return Record{}, byteOff, fmt.Errorf("journal: record CRC mismatch at offset %d in segment %d", byteOff, s.num)
	case err != nil:
		return Record{}, byteOff, err
	}
	if end > s.head.Load() {
		s.head.Store(end)
	}
	if rec.Kind == KindSeal {
		s.sealed.Store(true)
	}
	return rec, end, nil
}

// sync flushes any post-syncedHead bytes via msync(MS_SYNC). Caller
// must have serialized with append (no concurrent writer); concurrent
// readers are fine. Returns nil if there is nothing new to flush.
//
// Pre-allocated segments don't grow on append, so msync is sufficient
// for record bytes; durability of the segment file's metadata
// (existence, size) is the rotation path's responsibility — see
// Journal.rotate.
func (s *segment) sync() error {
	head := s.head.Load()
	if head <= s.syncedHead {
		return nil
	}
	start := s.syncedHead &^ pageMask
	if err := msyncRange(s.data[start:head]); err != nil {
		return fmt.Errorf("journal: segment %d msync: %w", s.num, err)
	}
	s.syncedHead = head
	return nil
}

// close unmaps and closes the segment file.
func (s *segment) close() error {
	var firstErr error
	if s.data != nil {
		if err := syscall.Munmap(s.data); err != nil && firstErr == nil {
			firstErr = err
		}
		s.data = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	return firstErr
}
