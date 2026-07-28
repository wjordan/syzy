// Package epoch defines the wire format for syzy's per-origin epoch
// objects in object storage. An epoch contains a contiguous span of
// Changesets [lo_seq, hi_seq] for one origin, packaged as zstd-
// compressed frames with a footer index that lets readers locate
// records by seq with a single ranged GET.
//
// On-disk layout:
//
//	[ zstd frame 0 ]                  -- N records, raw Changeset bytes
//	[ zstd frame 1 ]
//	...
//	[ zstd frame K ]
//	[ index block (uncompressed) ]    -- (frame_offset, frame_size,
//	                                     uncompressed_size, lo_seq, hi_seq)
//	                                     per frame, varint-encoded
//	[ trailer (32 bytes) ]            -- magic, version, sha256(prefix),
//	                                     index_offset, index_size, frame_count,
//	                                     min_seq, max_seq
//
// The trailer is fixed size at the end of the object so readers can
// fetch it with a single suffix-range GET (Range: bytes=-32). It
// contains everything needed to seek to the index, validate the
// content hash, and bound the seq range.
//
// Frames are independent zstd frames concatenated. `zstd -d` on the
// raw bytes (after stripping the index + trailer) yields the
// uncompressed Changeset stream, so the format is operator-friendly:
// pull an epoch, run `tail -c +N | zstd -d`, get the records.
//
// The encoder writes frames eagerly as records arrive and seals the
// epoch on Close: append the index, append the trailer.
package epoch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	// Magic identifies a syzy epoch object's trailer. Chosen to be
	// unlikely to collide with random data and ASCII-recognizable in
	// hexdumps.
	Magic uint32 = 0x53595A45 // "SYZE"

	// FormatVersion is the current epoch format version. Readers
	// reject anything they don't recognize.
	FormatVersion uint8 = 1

	// TrailerSize is the fixed footer at the end of every epoch.
	// Layout (bytes, little-endian unless noted):
	//   0..3   magic (uint32)
	//   4      version (uint8)
	//   5..7   reserved (zero)
	//   8..15  index_offset (uint64)   -- byte offset of index block
	//   16..19 index_size (uint32)     -- bytes of index block
	//   20..23 frame_count (uint32)
	//   24..31 min_seq (uint64)
	// followed by max_seq in the next 8 bytes; total 40.
	TrailerSize = 40

	// DefaultFrameTargetBytes is the default uncompressed buffer size
	// per zstd frame. Tuned for zstd's window (default 8 MiB) with
	// some headroom for record framing overhead.
	DefaultFrameTargetBytes = 256 << 10
)

// FrameIndex is one entry in the epoch's footer index.
type FrameIndex struct {
	// Offset is the byte offset of the zstd frame within the epoch.
	Offset int64
	// CompressedSize is the byte length of the zstd frame.
	CompressedSize int64
	// UncompressedSize is the total decompressed size of the frame.
	UncompressedSize int64
	// LoSeq is the first record's seq within the frame.
	LoSeq uint64
	// HiSeq is the last record's seq within the frame (inclusive).
	HiSeq uint64
}

// Trailer holds the parsed footer of an epoch object.
type Trailer struct {
	Magic       uint32
	Version     uint8
	IndexOffset int64
	IndexSize   int32
	FrameCount  int32
	MinSeq      uint64
	MaxSeq      uint64
}

// Footer holds the trailer plus the per-frame index.
type Footer struct {
	Trailer
	Frames []FrameIndex
}

// Record is one Changeset to encode. Bytes are the canonical
// Changeset wire bytes; Seq is the Changeset's Dot.Seq for indexing.
type Record struct {
	Seq   uint64
	Bytes []byte
}

// Encoder builds an epoch by appending Records and sealing on Close.
//
// Records must arrive in strictly-ascending Seq order. The encoder
// buffers records until the running uncompressed size of the current
// frame reaches FrameTargetBytes, then writes one zstd frame to the
// underlying writer.
type Encoder struct {
	w  io.Writer
	cw *countingWriter
	zw *zstd.Encoder

	// FrameTargetBytes triggers a frame seal when the uncompressed
	// buffer reaches this many bytes. Defaults to
	// DefaultFrameTargetBytes if zero.
	FrameTargetBytes int

	frame   bytes.Buffer // current frame's uncompressed records
	frameLo uint64       // first seq in current frame (0 if empty)
	frameHi uint64       // last seq in current frame

	frames []FrameIndex
	minSeq uint64
	maxSeq uint64

	prevSeq   uint64
	closed    bool
	hadRecord bool
}

// NewEncoder constructs an Encoder writing to w. Caller is responsible
// for closing w after Encoder.Close.
func NewEncoder(w io.Writer) (*Encoder, error) {
	cw := &countingWriter{w: w}
	zw, err := zstd.NewWriter(cw, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("epoch: new zstd writer: %w", err)
	}
	return &Encoder{
		w:                w,
		cw:               cw,
		zw:               zw,
		FrameTargetBytes: DefaultFrameTargetBytes,
	}, nil
}

// Append encodes one record. Records must be strictly seq-ascending.
func (e *Encoder) Append(r Record) error {
	if e.closed {
		return errors.New("epoch: encoder closed")
	}
	if e.hadRecord && r.Seq <= e.prevSeq {
		return fmt.Errorf("epoch: non-monotonic seq: prev=%d cur=%d", e.prevSeq, r.Seq)
	}
	if !e.hadRecord {
		e.minSeq = r.Seq
		e.frameLo = r.Seq
	}
	if e.frame.Len() == 0 {
		e.frameLo = r.Seq
	}
	// One record on the wire: varint(seq) + varint(length) + bytes.
	// Storing seq explicitly makes the format robust to future gaps
	// in the sealer's input (e.g., if KindEmpty records ever start
	// allocating seqs).
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], r.Seq)
	if _, err := e.frame.Write(buf[:n]); err != nil {
		return err
	}
	n = binary.PutUvarint(buf[:], uint64(len(r.Bytes)))
	if _, err := e.frame.Write(buf[:n]); err != nil {
		return err
	}
	if _, err := e.frame.Write(r.Bytes); err != nil {
		return err
	}
	e.frameHi = r.Seq
	e.maxSeq = r.Seq
	e.prevSeq = r.Seq
	e.hadRecord = true
	if e.frame.Len() >= e.frameTarget() {
		if err := e.flushFrame(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) frameTarget() int {
	if e.FrameTargetBytes > 0 {
		return e.FrameTargetBytes
	}
	return DefaultFrameTargetBytes
}

// flushFrame writes the buffered records as one zstd frame.
func (e *Encoder) flushFrame() error {
	if e.frame.Len() == 0 {
		return nil
	}
	startOff := e.cw.n
	uncompressed := int64(e.frame.Len())

	// Reset zstd encoder over the counting writer for one self-contained
	// frame. zstd.Encoder finalizes a frame on Close.
	e.zw.Reset(e.cw)
	if _, err := e.zw.Write(e.frame.Bytes()); err != nil {
		return fmt.Errorf("epoch: zstd write: %w", err)
	}
	if err := e.zw.Close(); err != nil {
		return fmt.Errorf("epoch: zstd close frame: %w", err)
	}
	endOff := e.cw.n
	frameBytes := endOff - startOff

	e.frames = append(e.frames, FrameIndex{
		Offset:           startOff,
		CompressedSize:   frameBytes,
		UncompressedSize: uncompressed,
		LoSeq:            e.frameLo,
		HiSeq:            e.frameHi,
	})
	e.frame.Reset()
	e.frameLo, e.frameHi = 0, 0
	return nil
}

// Close seals the epoch: flushes any pending frame, writes the index
// block, and writes the trailer. After Close, the underlying writer
// holds the complete epoch object.
func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	if err := e.flushFrame(); err != nil {
		return err
	}
	if !e.hadRecord {
		return errors.New("epoch: no records appended")
	}

	// Write the index block.
	indexOff := e.cw.n
	idxBuf := encodeIndex(e.frames)
	if _, err := e.w.Write(idxBuf); err != nil {
		return fmt.Errorf("epoch: write index: %w", err)
	}
	e.cw.n += int64(len(idxBuf))

	// Write the trailer.
	tr := make([]byte, TrailerSize)
	binary.LittleEndian.PutUint32(tr[0:4], Magic)
	tr[4] = FormatVersion
	binary.LittleEndian.PutUint64(tr[8:16], uint64(indexOff))
	binary.LittleEndian.PutUint32(tr[16:20], uint32(len(idxBuf)))
	binary.LittleEndian.PutUint32(tr[20:24], uint32(len(e.frames)))
	binary.LittleEndian.PutUint64(tr[24:32], e.minSeq)
	binary.LittleEndian.PutUint64(tr[32:40], e.maxSeq)
	if _, err := e.w.Write(tr); err != nil {
		return fmt.Errorf("epoch: write trailer: %w", err)
	}
	return nil
}

// MinSeq returns the lowest seq written, or 0 if no records yet.
func (e *Encoder) MinSeq() uint64 { return e.minSeq }

// MaxSeq returns the highest seq written.
func (e *Encoder) MaxSeq() uint64 { return e.maxSeq }

// encodeIndex writes the per-frame index as a sequence of varints.
// Order: frame_count, then per-frame [offset, compressed_size,
// uncompressed_size, lo_seq, hi_seq]. All varint-encoded.
func encodeIndex(frames []FrameIndex) []byte {
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	put := func(v uint64) {
		n := binary.PutUvarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	put(uint64(len(frames)))
	for _, f := range frames {
		put(uint64(f.Offset))
		put(uint64(f.CompressedSize))
		put(uint64(f.UncompressedSize))
		put(f.LoSeq)
		put(f.HiSeq)
	}
	return buf.Bytes()
}

// decodeIndex parses the bytes produced by encodeIndex.
func decodeIndex(buf []byte) ([]FrameIndex, error) {
	r := bytes.NewReader(buf)
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("epoch: read index count: %w", err)
	}
	frames := make([]FrameIndex, 0, count)
	for i := uint64(0); i < count; i++ {
		off, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: index frame[%d] offset: %w", i, err)
		}
		csz, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: index frame[%d] csize: %w", i, err)
		}
		usz, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: index frame[%d] usize: %w", i, err)
		}
		lo, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: index frame[%d] lo: %w", i, err)
		}
		hi, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: index frame[%d] hi: %w", i, err)
		}
		frames = append(frames, FrameIndex{
			Offset:           int64(off),
			CompressedSize:   int64(csz),
			UncompressedSize: int64(usz),
			LoSeq:            lo,
			HiSeq:            hi,
		})
	}
	return frames, nil
}

// ReadTrailer parses the fixed-size trailer from buf, which must be
// at least TrailerSize bytes long. Returns an error on bad magic or
// unsupported version.
func ReadTrailer(buf []byte) (Trailer, error) {
	if len(buf) < TrailerSize {
		return Trailer{}, fmt.Errorf("epoch: trailer too short: %d < %d", len(buf), TrailerSize)
	}
	tr := Trailer{
		Magic:       binary.LittleEndian.Uint32(buf[0:4]),
		Version:     buf[4],
		IndexOffset: int64(binary.LittleEndian.Uint64(buf[8:16])),
		IndexSize:   int32(binary.LittleEndian.Uint32(buf[16:20])),
		FrameCount:  int32(binary.LittleEndian.Uint32(buf[20:24])),
		MinSeq:      binary.LittleEndian.Uint64(buf[24:32]),
		MaxSeq:      binary.LittleEndian.Uint64(buf[32:40]),
	}
	if tr.Magic != Magic {
		return tr, fmt.Errorf("epoch: bad magic 0x%08x", tr.Magic)
	}
	if tr.Version != FormatVersion {
		return tr, fmt.Errorf("epoch: unsupported version %d", tr.Version)
	}
	return tr, nil
}

// ReadFooter parses both the trailer and the index block from a
// random-access source. It issues two reads if the trailer's index is
// not already covered by tail; if tail covers everything, only the
// trailer is parsed in-memory.
//
// size is the total epoch size.
//
// readAt(off, length) returns up to length bytes starting at off.
func ReadFooter(size int64, readAt func(off, length int64) ([]byte, error)) (*Footer, error) {
	// 1. Fetch trailer.
	if size < int64(TrailerSize) {
		return nil, fmt.Errorf("epoch: object too small (%d < %d)", size, TrailerSize)
	}
	tail, err := readAt(size-int64(TrailerSize), int64(TrailerSize))
	if err != nil {
		return nil, fmt.Errorf("epoch: read trailer: %w", err)
	}
	tr, err := ReadTrailer(tail)
	if err != nil {
		return nil, err
	}
	// 2. Fetch index block.
	if tr.IndexOffset < 0 || tr.IndexSize <= 0 {
		return nil, fmt.Errorf("epoch: bogus index location off=%d sz=%d", tr.IndexOffset, tr.IndexSize)
	}
	idxBytes, err := readAt(tr.IndexOffset, int64(tr.IndexSize))
	if err != nil {
		return nil, fmt.Errorf("epoch: read index: %w", err)
	}
	frames, err := decodeIndex(idxBytes)
	if err != nil {
		return nil, err
	}
	if int32(len(frames)) != tr.FrameCount {
		return nil, fmt.Errorf("epoch: index frame count mismatch (%d vs %d)", len(frames), tr.FrameCount)
	}
	return &Footer{Trailer: tr, Frames: frames}, nil
}

// FrameForSeq returns the index entry whose [LoSeq, HiSeq] contains
// seq. Returns nil if no frame covers seq.
func (f *Footer) FrameForSeq(seq uint64) *FrameIndex {
	// Binary search; frames are seq-ordered.
	lo, hi := 0, len(f.Frames)
	for lo < hi {
		mid := (lo + hi) / 2
		fr := f.Frames[mid]
		if seq < fr.LoSeq {
			hi = mid
		} else if seq > fr.HiSeq {
			lo = mid + 1
		} else {
			return &f.Frames[mid]
		}
	}
	return nil
}

// FramesOverlapping returns the slice of frames whose [LoSeq, HiSeq]
// overlaps [loSeq, hiSeq]. Inclusive on both ends.
func (f *Footer) FramesOverlapping(loSeq, hiSeq uint64) []FrameIndex {
	if loSeq > hiSeq {
		return nil
	}
	out := make([]FrameIndex, 0, 2)
	for _, fr := range f.Frames {
		if fr.HiSeq < loSeq {
			continue
		}
		if fr.LoSeq > hiSeq {
			break
		}
		out = append(out, fr)
	}
	return out
}

// DecodeFrame reads one frame's records out of compressed bytes.
// The caller supplies the raw compressed bytes (CompressedSize from
// the index entry) and gets back the records in seq order.
//
// dec is shared zstd decoder; pass nil to allocate a one-shot.
func DecodeFrame(compressed []byte, dec *zstd.Decoder) ([]Record, error) {
	var raw []byte
	if dec != nil {
		out, err := dec.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("epoch: zstd decode: %w", err)
		}
		raw = out
	} else {
		d, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer d.Close()
		out, err := d.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("epoch: zstd decode: %w", err)
		}
		raw = out
	}
	return decodeFrameBody(raw)
}

func decodeFrameBody(raw []byte) ([]Record, error) {
	r := bytes.NewReader(raw)
	var out []Record
	for r.Len() > 0 {
		seq, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: read record seq: %w", err)
		}
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("epoch: read record length: %w", err)
		}
		if int64(n) > int64(r.Len()) {
			return nil, fmt.Errorf("epoch: record length %d exceeds remaining %d", n, r.Len())
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("epoch: read record body: %w", err)
		}
		out = append(out, Record{Seq: seq, Bytes: buf})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// helpers

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
