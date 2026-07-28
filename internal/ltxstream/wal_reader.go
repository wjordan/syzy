// SQLite WAL reader. Lifted with light edits from
// github.com/benbjohnson/litestream@v0.5.x (Apache License 2.0); the
// reader is the same one Litestream uses to build LTX files, so syzy's
// LTX byte stream is byte-equivalent to what Litestream would produce
// from the same WAL — preserving Litestream-compat for restore /
// follow-mode consumers.
//
// Edits from upstream:
//   - package renamed
//   - logger replaced with log/slog (no internal/hexdump dep)
//   - dropped Hexdump-on-failure debug helper (not load-bearing)
package ltxstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"

	"github.com/wjordan/syzy/internal/syzylog"
)

// SQLite WAL constants.
const (
	WALHeaderSize      = 32
	WALFrameHeaderSize = 24
)

// WALReader wraps an io.ReaderAt and parses SQLite WAL frames.
//
// This reader verifies salt + checksum integrity while it reads. It
// does not enforce transaction boundaries (i.e. it may return
// uncommitted frames). It is the responsibility of the caller to
// recognize commit records via the per-frame `commit` field and
// discard pending frames if a commit is not reached.
type WALReader struct {
	r      io.ReaderAt
	frameN int

	bo       binary.ByteOrder
	pageSize uint32
	seq      uint32

	salt1, salt2     uint32
	chksum1, chksum2 uint32

	logger *slog.Logger
}

// NewWALReader returns a new instance of WALReader.
func NewWALReader(rd io.ReaderAt, logger *slog.Logger) (*WALReader, error) {
	if logger == nil {
		logger = syzylog.Default()
	}
	r := &WALReader{r: rd, logger: logger}
	if err := r.readHeader(); err != nil {
		return nil, err
	}
	return r, nil
}

// NewWALReaderWithOffset returns a new WALReader positioned at
// offset, with the running checksum primed by reading the previous
// frame. Salt must match the supplied (salt1, salt2) — if it doesn't,
// the WAL has been recycled (TRUNCATE/RESTART checkpoint) and the
// caller's saved position is stale.
func NewWALReaderWithOffset(ctx context.Context, rd io.ReaderAt, offset int64, salt1, salt2 uint32, logger *slog.Logger) (*WALReader, error) {
	if offset <= WALHeaderSize {
		return nil, fmt.Errorf("offset (%d) must be greater than the wal header size (%d)", offset, WALHeaderSize)
	}
	if logger == nil {
		logger = syzylog.Default()
	}
	r := &WALReader{r: rd, logger: logger}
	if err := r.readHeader(); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	r.salt1, r.salt2 = salt1, salt2

	frameSize := int64(r.pageSize + WALFrameHeaderSize)
	if (offset-WALHeaderSize)%frameSize != 0 {
		return nil, fmt.Errorf("unaligned wal offset %d for page size %d", offset, r.pageSize)
	}
	r.frameN = int((offset - WALHeaderSize) / frameSize)

	// Read previous frame to prime the rolling checksum. If salt or
	// checksum doesn't match, the offset is stale; caller should
	// re-baseline.
	r.frameN--
	if _, _, err := r.readFrame(ctx, make([]byte, r.pageSize), false); err != nil {
		return nil, &PrevFrameMismatchError{Err: err}
	}
	return r, nil
}

func (r *WALReader) PageSize() uint32 { return r.pageSize }
func (r *WALReader) Salt1() uint32    { return r.salt1 }
func (r *WALReader) Salt2() uint32    { return r.salt2 }

// Offset returns the file offset of the last read frame's start.
// Zero if no frames have been read.
func (r *WALReader) Offset() int64 {
	if r.frameN == 0 {
		return 0
	}
	return WALHeaderSize + ((int64(r.frameN) - 1) * (WALFrameHeaderSize + int64(r.pageSize)))
}

// NextOffset returns the file offset just past the last read frame —
// i.e., where the next ReadFrame will look. Use this as the resume
// position passed back to NewWALReaderWithOffset later: the
// constructor's "prime by reading previous frame" logic expects the
// next-frame offset, not the current-frame offset.
func (r *WALReader) NextOffset() int64 {
	if r.frameN == 0 {
		return WALHeaderSize
	}
	return WALHeaderSize + (int64(r.frameN) * (WALFrameHeaderSize + int64(r.pageSize)))
}

// Checksums returns the running (chksum1, chksum2) after the most
// recent successful frame read. Persist alongside Offset/Salt to
// resume tailing later.
func (r *WALReader) Checksums() (uint32, uint32) { return r.chksum1, r.chksum2 }

func (r *WALReader) readHeader() error {
	hdr := make([]byte, WALHeaderSize)
	if n, err := r.r.ReadAt(hdr, 0); n < len(hdr) {
		return io.EOF
	} else if err != nil {
		return err
	}

	switch magic := binary.BigEndian.Uint32(hdr[0:]); magic {
	case 0x377f0682:
		r.bo = binary.LittleEndian
	case 0x377f0683:
		r.bo = binary.BigEndian
	default:
		return fmt.Errorf("invalid wal header magic: %x", magic)
	}

	chksum1 := binary.BigEndian.Uint32(hdr[24:])
	chksum2 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := WALChecksum(r.bo, 0, 0, hdr[:24]); v0 != chksum1 || v1 != chksum2 {
		return io.EOF
	}

	if version := binary.BigEndian.Uint32(hdr[4:]); version != 3007000 {
		return fmt.Errorf("unsupported wal version: %d", version)
	}

	r.pageSize = binary.BigEndian.Uint32(hdr[8:])
	r.seq = binary.BigEndian.Uint32(hdr[12:])
	r.salt1 = binary.BigEndian.Uint32(hdr[16:])
	r.salt2 = binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = chksum1, chksum2
	return nil
}

// ReadFrame reads the next frame from the WAL. Returns io.EOF at the
// end of the valid WAL. `commit` is non-zero on the trailer frame of a
// committed transaction (it carries the post-commit DB size in pages).
func (r *WALReader) ReadFrame(ctx context.Context, data []byte) (pgno, commit uint32, err error) {
	return r.readFrame(ctx, data, true)
}

func (r *WALReader) readFrame(_ context.Context, data []byte, verifyChecksum bool) (pgno, commit uint32, err error) {
	if len(data) != int(r.pageSize) {
		return 0, 0, fmt.Errorf("WALReader.ReadFrame: buffer size (%d) must match page size (%d)", len(data), r.pageSize)
	}

	frameSize := r.pageSize + WALFrameHeaderSize
	offset := WALHeaderSize + (int64(r.frameN) * int64(frameSize))

	hdr := make([]byte, WALFrameHeaderSize)
	if n, err := r.r.ReadAt(hdr, offset); n != len(hdr) {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	if n, err := r.r.ReadAt(data, offset+WALFrameHeaderSize); n != len(data) {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	salt1 := binary.BigEndian.Uint32(hdr[8:])
	salt2 := binary.BigEndian.Uint32(hdr[12:])
	if r.salt1 != salt1 || r.salt2 != salt2 {
		return 0, 0, io.EOF
	}

	chksum1 := binary.BigEndian.Uint32(hdr[16:])
	chksum2 := binary.BigEndian.Uint32(hdr[20:])
	if verifyChecksum {
		r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, hdr[:8])
		r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, data)
		if r.chksum1 != chksum1 || r.chksum2 != chksum2 {
			return 0, 0, io.EOF
		}
	} else {
		r.chksum1, r.chksum2 = chksum1, chksum2
	}

	pgno = binary.BigEndian.Uint32(hdr[0:])
	commit = binary.BigEndian.Uint32(hdr[4:])
	r.frameN++
	return pgno, commit, nil
}

// WALChecksum computes a running SQLite WAL checksum over a byte slice.
func WALChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	if len(b)%8 != 0 {
		panic("ltxstream: misaligned checksum byte slice")
	}
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

// PrevFrameMismatchError is returned by NewWALReaderWithOffset when
// priming the rolling checksum from the previous frame fails — i.e.,
// the saved (salt, offset, checksum) tuple no longer matches the
// current WAL. Callers treat this as "WAL was recycled; rebaseline."
type PrevFrameMismatchError struct {
	Err error
}

func (e *PrevFrameMismatchError) Error() string { return fmt.Sprintf("prev frame mismatch: %s", e.Err) }
func (e *PrevFrameMismatchError) Unwrap() error { return e.Err }
