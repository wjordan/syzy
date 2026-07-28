package epoch

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// TestRoundTripSmall encodes a handful of records, parses the footer,
// and verifies each record is recoverable via FrameForSeq.
func TestRoundTripSmall(t *testing.T) {
	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	for i := uint64(1); i <= 100; i++ {
		body := []byte(fmt.Sprintf("record-%d-%s", i, bytes.Repeat([]byte("x"), int(i%17))))
		if err := enc.Append(Record{Seq: i, Bytes: body}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if enc.MinSeq() != 1 || enc.MaxSeq() != 100 {
		t.Fatalf("min/max seq: got %d/%d, want 1/100", enc.MinSeq(), enc.MaxSeq())
	}

	body := buf.Bytes()
	footer, err := ReadFooter(int64(len(body)), readAtFromBytes(body))
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if footer.MinSeq != 1 || footer.MaxSeq != 100 {
		t.Fatalf("footer min/max: got %d/%d, want 1/100", footer.MinSeq, footer.MaxSeq)
	}
	if int32(len(footer.Frames)) != footer.FrameCount {
		t.Fatalf("frames len %d != count %d", len(footer.Frames), footer.FrameCount)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd NewReader: %v", err)
	}
	defer dec.Close()

	for seq := uint64(1); seq <= 100; seq++ {
		fr := footer.FrameForSeq(seq)
		if fr == nil {
			t.Fatalf("FrameForSeq(%d) = nil", seq)
		}
		compressed := body[fr.Offset : fr.Offset+fr.CompressedSize]
		recs, err := DecodeFrame(compressed, dec)
		if err != nil {
			t.Fatalf("DecodeFrame at seq %d: %v", seq, err)
		}
		// Find our record in the frame.
		var got *Record
		for i := range recs {
			if recs[i].Seq == seq {
				got = &recs[i]
				break
			}
		}
		if got == nil {
			t.Fatalf("seq %d not in frame [%d, %d]", seq, fr.LoSeq, fr.HiSeq)
		}
		want := fmt.Sprintf("record-%d-%s", seq, bytes.Repeat([]byte("x"), int(seq%17)))
		if string(got.Bytes) != want {
			t.Fatalf("seq %d: got %q, want %q", seq, got.Bytes, want)
		}
	}
}

// TestRoundTripMultipleFrames forces a tiny frame target so the encoder
// emits many frames; verifies frame-boundary handling.
func TestRoundTripMultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	enc, err := NewEncoder(&buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	enc.FrameTargetBytes = 64 // very small; force many frames
	const N = 200
	payloads := make(map[uint64]string, N)
	for i := uint64(1); i <= N; i++ {
		s := fmt.Sprintf("payload-%03d-aaaaaaaaaaaaaaaaaaaaaaaaaa", i)
		payloads[i] = s
		if err := enc.Append(Record{Seq: i, Bytes: []byte(s)}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	body := buf.Bytes()
	footer, err := ReadFooter(int64(len(body)), readAtFromBytes(body))
	if err != nil {
		t.Fatalf("ReadFooter: %v", err)
	}
	if footer.FrameCount < 5 {
		t.Fatalf("expected many frames with tiny target, got %d", footer.FrameCount)
	}

	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	// Walk all frames in order.
	var seqsSeen []uint64
	for _, fr := range footer.Frames {
		compressed := body[fr.Offset : fr.Offset+fr.CompressedSize]
		recs, err := DecodeFrame(compressed, dec)
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		for _, r := range recs {
			seqsSeen = append(seqsSeen, r.Seq)
			if got := payloads[r.Seq]; got != string(r.Bytes) {
				t.Fatalf("seq %d body mismatch", r.Seq)
			}
		}
	}
	if len(seqsSeen) != N {
		t.Fatalf("recovered %d records, want %d", len(seqsSeen), N)
	}
	for i, s := range seqsSeen {
		if s != uint64(i+1) {
			t.Fatalf("seqsSeen[%d] = %d, want %d", i, s, i+1)
		}
	}
}

func TestNonMonotonicAppendRejected(t *testing.T) {
	var buf bytes.Buffer
	enc, _ := NewEncoder(&buf)
	if err := enc.Append(Record{Seq: 5, Bytes: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Append(Record{Seq: 5, Bytes: []byte("b")}); err == nil {
		t.Fatal("expected error on equal seq")
	}
	if err := enc.Append(Record{Seq: 4, Bytes: []byte("c")}); err == nil {
		t.Fatal("expected error on backward seq")
	}
}

func TestEmptyEncoderClose(t *testing.T) {
	var buf bytes.Buffer
	enc, _ := NewEncoder(&buf)
	if err := enc.Close(); err == nil {
		t.Fatal("expected error closing empty encoder")
	}
}

func TestFramesOverlapping(t *testing.T) {
	footer := &Footer{
		Frames: []FrameIndex{
			{LoSeq: 1, HiSeq: 10},
			{LoSeq: 11, HiSeq: 20},
			{LoSeq: 21, HiSeq: 30},
		},
	}
	cases := []struct {
		lo, hi uint64
		want   int
	}{
		{0, 0, 0},
		{1, 10, 1},
		{5, 15, 2},
		{1, 30, 3},
		{8, 22, 3},
		{31, 100, 0},
		{20, 21, 2},
	}
	for _, c := range cases {
		got := footer.FramesOverlapping(c.lo, c.hi)
		if len(got) != c.want {
			t.Errorf("FramesOverlapping(%d,%d): got %d frames, want %d", c.lo, c.hi, len(got), c.want)
		}
	}
}

func TestTrailerBadMagic(t *testing.T) {
	junk := make([]byte, TrailerSize)
	_, err := ReadTrailer(junk)
	if err == nil {
		t.Fatal("expected magic error")
	}
}

// readAtFromBytes returns a readAt closure over an in-memory buffer.
func readAtFromBytes(body []byte) func(off, length int64) ([]byte, error) {
	return func(off, length int64) ([]byte, error) {
		end := off + length
		if end > int64(len(body)) {
			end = int64(len(body))
		}
		return body[off:end], nil
	}
}
