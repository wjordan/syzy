package tcpmesh

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestHelloRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		h    Hello
	}{
		{"empty-topics", Hello{NodeID: 1}},
		{"single", Hello{NodeID: 0xCAFEBABE_DEADBEEF, Topics: []string{"app-topic"}}},
		{"multi", Hello{NodeID: ^uint64(0), Topics: []string{"app-topic", "cdn-topic", "app-uuid-xyz"}}},
		{"max-topic-len", Hello{NodeID: 42, Topics: []string{strings.Repeat("x", MaxTopicLen)}}},
		{"listen-tcp", Hello{NodeID: 7, ListenAddr: "10.0.0.1:9000", Topics: []string{"t"}}},
		{"listen-unix", Hello{NodeID: 8, ListenAddr: "unix:/var/run/syzy.sock"}},
		{"listen-max", Hello{NodeID: 9, ListenAddr: strings.Repeat("a", MaxListenAddrLen)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeHello(&buf, tc.h); err != nil {
				t.Fatalf("writeHello: %v", err)
			}
			got, err := readHello(&buf)
			if err != nil {
				t.Fatalf("readHello: %v", err)
			}
			if got.NodeID != tc.h.NodeID {
				t.Errorf("NodeID: got %x want %x", got.NodeID, tc.h.NodeID)
			}
			if got.ListenAddr != tc.h.ListenAddr {
				t.Errorf("ListenAddr: got %q want %q", got.ListenAddr, tc.h.ListenAddr)
			}
			if !slices.Equal(got.Topics, tc.h.Topics) {
				t.Errorf("Topics: got %v want %v", got.Topics, tc.h.Topics)
			}
			if buf.Len() != 0 {
				t.Errorf("trailing bytes after hello: %d", buf.Len())
			}
		})
	}
}

func TestHelloRejectsOversizeListenAddr(t *testing.T) {
	_, err := encodeHelloBody(Hello{NodeID: 1, ListenAddr: strings.Repeat("x", MaxListenAddrLen+1)})
	if err == nil {
		t.Fatalf("expected error on oversize hello ListenAddr")
	}
}

func TestHelloMagicMismatch(t *testing.T) {
	// Simulate a legacy pre-mux peer's first bytes on the wire: u32
	// frameLen followed by a payload. No magic preamble.
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(5))
	buf.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x42})
	_, err := readHello(&buf)
	if err == nil {
		t.Fatalf("expected error on missing magic")
	}
	if !strings.Contains(err.Error(), "magic") {
		t.Errorf("error %q does not mention magic", err)
	}
}

func TestHelloRejectsOversizeTopic(t *testing.T) {
	_, err := encodeHelloBody(Hello{NodeID: 1, Topics: []string{strings.Repeat("x", MaxTopicLen+1)}})
	if err == nil {
		t.Fatalf("expected error on oversize hello topic")
	}
}

func TestHelloRejectsOversizeFrameLen(t *testing.T) {
	// Craft a hello frame whose declared length exceeds MaxFrameSize.
	// readHello must refuse before allocating the body buffer.
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, Magic)
	_ = binary.Write(&buf, binary.BigEndian, MaxFrameSize+1)
	_, err := readHello(&buf)
	if err == nil {
		t.Fatalf("expected error on oversize hello frameLen")
	}
}

func TestHelloRejectsTruncatedBody(t *testing.T) {
	// Hello frame declares nTopics=1 but the topic-length header is
	// past EOF.
	var body bytes.Buffer
	body.WriteByte(msgHello)
	_ = binary.Write(&body, binary.BigEndian, uint64(7))
	_ = binary.Write(&body, binary.BigEndian, uint32(1))
	// Intentionally do not write the topic-length u16.

	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, Magic)
	_ = binary.Write(&buf, binary.BigEndian, uint32(body.Len()))
	buf.Write(body.Bytes())
	_, err := readHello(&buf)
	if err == nil {
		t.Fatalf("expected error on truncated hello topic header")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		msgType byte
		topic   string
		payload []byte
	}{
		{"data-small", msgData, "t", []byte("hello")},
		{"data-binary", msgData, "app", []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"data-empty-payload", msgData, "k", nil},
		{"topic-add", msgTopicAdd, "app", nil},
		{"topic-remove", msgTopicRemove, "cdn", nil},
		{"max-topic", msgData, strings.Repeat("x", MaxTopicLen), []byte("payload")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeFrame(&buf, tc.msgType, tc.topic, tc.payload); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}
			gotType, gotTopic, gotPayload, err := readFrame(&buf)
			if err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if gotType != tc.msgType {
				t.Errorf("msgType: got %x want %x", gotType, tc.msgType)
			}
			if gotTopic != tc.topic {
				t.Errorf("topic: got %q want %q", gotTopic, tc.topic)
			}
			if len(gotPayload) == 0 && len(tc.payload) == 0 {
				// Equivalent.
			} else if !bytes.Equal(gotPayload, tc.payload) {
				t.Errorf("payload: got %x want %x", gotPayload, tc.payload)
			}
			if buf.Len() != 0 {
				t.Errorf("trailing bytes after frame: %d", buf.Len())
			}
		})
	}
}

func TestFrameRejectsOversizeTopic(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, msgData, strings.Repeat("x", MaxTopicLen+1), nil); err == nil {
		t.Errorf("writeFrame with oversize topic should error")
	}
}

func TestFrameRejectsOversizePayload(t *testing.T) {
	var buf bytes.Buffer
	huge := make([]byte, MaxFrameSize+1)
	if err := writeFrame(&buf, msgData, "t", huge); err == nil {
		t.Errorf("writeFrame with oversize payload should error")
	}
}

func TestReadFrameRejectsZeroBody(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(0))
	if _, _, _, err := readFrame(&buf); err == nil {
		t.Errorf("readFrame should reject zero-length body")
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, MaxFrameSize+1)
	if _, _, _, err := readFrame(&buf); err == nil {
		t.Errorf("readFrame should reject body > MaxFrameSize")
	}
}

func TestReadFramePropagatesEOF(t *testing.T) {
	var buf bytes.Buffer
	if _, _, _, err := readFrame(&buf); !errors.Is(err, io.EOF) {
		t.Errorf("readFrame on empty stream err = %v, want io.EOF", err)
	}
}

func TestStatusTableStable(t *testing.T) {
	// Lock in the status byte values; the bundle/catchup wire
	// contract pins these and any drift must be a deliberate change.
	want := []struct {
		name string
		got  byte
		want byte
	}{
		{"OK", StatusOK, 0x00},
		{"UnknownTopic", StatusUnknownTopic, 0x01},
		{"NoHandler", StatusNoHandler, 0x02},
		{"BadRequest", StatusBadRequest, 0x04},
		{"InternalError", StatusInternalError, 0x05},
	}
	for _, c := range want {
		if c.got != c.want {
			t.Errorf("%s = 0x%02x, want 0x%02x", c.name, c.got, c.want)
		}
	}
}

func TestRandomNodeIDNonzero(t *testing.T) {
	for i := 0; i < 100; i++ {
		if randomNodeID() == 0 {
			t.Fatalf("randomNodeID returned 0")
		}
	}
}

func TestRandomNodeIDDistinct(t *testing.T) {
	// Cheap diversity smoke test — two calls in a row should
	// virtually never collide.
	a, b := randomNodeID(), randomNodeID()
	if a == b {
		t.Errorf("randomNodeID gave identical %x twice in a row", a)
	}
}
