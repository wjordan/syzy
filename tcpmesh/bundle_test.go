package tcpmesh

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBundle_RoundTripsHandlerOutput(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	c, err := a.Channel("app")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	want := bytes.Repeat([]byte("syzy-mux-bundle-"), 256) // ~4 KiB
	c.SetBundleHandler(func(w io.Writer) error {
		_, err := w.Write(want)
		return err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got bytes.Buffer
	if err := FetchBundle(ctx, c.Endpoint(), &got); err != nil {
		t.Fatalf("FetchBundle: %v", err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("got %d bytes, want %d", got.Len(), len(want))
	}
}

func TestBundle_UnknownTopicStatus(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	// No Channel call.

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	url := BuildEndpointURL(a.Addr(), "no-such-topic")
	err = FetchBundle(ctx, url, io.Discard)
	if err == nil {
		t.Fatalf("FetchBundle should fail with status; got nil")
	}
	var be *BundleError
	if !errors.As(err, &be) || be.Status != StatusUnknownTopic {
		t.Fatalf("err = %v, want BundleError{Status: 0x01 UnknownTopic}", err)
	}
}

func TestBundle_NoHandlerStatus(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("app")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	_ = c // no SetBundleHandler call

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = FetchBundle(ctx, c.Endpoint(), io.Discard)
	if err == nil {
		t.Fatalf("FetchBundle should fail with status; got nil")
	}
	var be *BundleError
	if !errors.As(err, &be) || be.Status != StatusNoHandler {
		t.Fatalf("err = %v, want BundleError{Status: 0x02 NoHandler}", err)
	}
}

func TestBundle_BadRequestStatus(t *testing.T) {
	// A URL with empty ?topic= is rejected client-side; if a client
	// sends an empty topic prefix on the wire, server returns
	// BadRequest. Force the wire by manually dialing.
	dir := t.TempDir()
	sock := filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: "unix:" + sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	conn, err := dialContext(context.Background(), "unix:"+sock, nil, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// Send op + topicLen=0 + (no topic) — empty topic.
	if err := writeRequestPrefix(conn, opBundleStream, ""); err != nil {
		t.Fatalf("writeRequestPrefix: %v", err)
	}
	if err := readStatus(conn); err == nil {
		t.Fatalf("expected BundleError for empty topic")
	} else {
		var be *BundleError
		if !errors.As(err, &be) || be.Status != StatusBadRequest {
			t.Fatalf("err = %v, want BadRequest", err)
		}
	}
}

func TestBundle_FetcherPreBindsTopic(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("topic-x")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	c.SetBundleHandler(func(w io.Writer) error {
		_, err := w.Write([]byte("from-topic-x"))
		return err
	})

	fetcher := c.Fetcher()
	// Channel.Fetcher takes a bare addr (the mesh's bundle addr),
	// not a URL — the topic is implicit in the channel.
	var got bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := fetcher(ctx, a.Addr(), &got); err != nil {
		t.Fatalf("Fetcher: %v", err)
	}
	if got.String() != "from-topic-x" {
		t.Errorf("got %q, want %q", got.String(), "from-topic-x")
	}
}

func TestBundle_ParseEndpointURL(t *testing.T) {
	cases := []struct {
		raw       string
		wantAddr  string
		wantTopic string
		wantErr   bool
	}{
		{"tcp://host:7001?topic=app", "host:7001", "app", false},
		{"tcp://10.0.0.1:7001?topic=app-uuid", "10.0.0.1:7001", "app-uuid", false},
		{"unix:///var/run/syzy.sock?topic=cdn", "unix:/var/run/syzy.sock", "cdn", false},
		{"tcp://host:7001", "", "", true},        // missing topic
		{"tcp://host:7001?topic=", "", "", true}, // empty topic
		{"tcp://?topic=foo", "", "", true},       // missing host
		{"s3://bucket?topic=foo", "", "", true},  // unsupported scheme
		{"://broken?topic=foo", "", "", true},    // broken URL
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			gotAddr, gotTopic, err := ParseEndpointURL(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if gotAddr != tc.wantAddr {
				t.Errorf("addr = %q, want %q", gotAddr, tc.wantAddr)
			}
			if gotTopic != tc.wantTopic {
				t.Errorf("topic = %q, want %q", gotTopic, tc.wantTopic)
			}
		})
	}
}

func TestBundle_BuildAndParseRoundTrip(t *testing.T) {
	cases := []struct {
		addr, topic string
	}{
		{"host:7001", "app"},
		{"10.0.0.1:7001", "app-uuid"},
		{"unix:/var/run/syzy.sock", "cdn"},
	}
	for _, tc := range cases {
		t.Run(tc.addr, func(t *testing.T) {
			built := BuildEndpointURL(tc.addr, tc.topic)
			gotAddr, gotTopic, err := ParseEndpointURL(built)
			if err != nil {
				t.Fatalf("ParseEndpointURL(%q) err = %v", built, err)
			}
			if gotAddr != tc.addr {
				t.Errorf("addr round-trip: got %q want %q (built=%q)", gotAddr, tc.addr, built)
			}
			if gotTopic != tc.topic {
				t.Errorf("topic round-trip: got %q want %q", gotTopic, tc.topic)
			}
		})
	}
}

func TestBundle_ChannelEndpointEmptyWhenNoListener(t *testing.T) {
	a, err := New(Config{NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("t")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	if got := c.Endpoint(); got != "" {
		t.Errorf("Endpoint without listener = %q, want empty", got)
	}
}

func TestBundle_HandlerErrorClosesStream(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("k")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	c.SetBundleHandler(func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return errors.New("handler oops")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got bytes.Buffer
	// Mid-stream error closes the conn; client sees a clean EOF
	// after the partial bytes were copied through.
	if err := FetchBundle(ctx, c.Endpoint(), &got); err != nil {
		t.Fatalf("FetchBundle: %v", err)
	}
	if got.String() != "partial" {
		t.Errorf("got %q, want %q", got.String(), "partial")
	}
}

// Compile-time guard: BundleError implements error.
var _ error = (*BundleError)(nil)

// Compile-time guard: ParseEndpointURL's "unsupported scheme" branch
// fires on http (sanity check that the switch is exhaustive in the
// directions we care about).
func TestBundle_ParseEndpointURL_HTTPScheme(t *testing.T) {
	_, _, err := ParseEndpointURL("http://host?topic=t")
	if err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Errorf("ParseEndpointURL http err = %v, want scheme error", err)
	}
}

// Sanity check on the encoder so we don't accidentally double-
// encode the topic when it contains URL-special chars.
func TestBundle_BuildEndpointURLEncodesTopic(t *testing.T) {
	got := BuildEndpointURL("host:7001", "topic with space")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("Parse %q: %v", got, err)
	}
	if u.Query().Get("topic") != "topic with space" {
		t.Errorf("topic = %q, want %q", u.Query().Get("topic"), "topic with space")
	}
}

func TestBundle_ChannelCloseBreaksInFlightServe(t *testing.T) {
	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "bundle.sock")
	a, err := New(Config{Listen: sock, NodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	c, err := a.Channel("app")
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	started := make(chan struct{})
	c.SetBundleHandler(func(w io.Writer) error {
		close(started)
		buf := make([]byte, 1024)
		for {
			if _, err := w.Write(buf); err != nil {
				return err // conn broken by channel close
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fetchDone := make(chan error, 1)
	go func() { fetchDone <- FetchBundle(ctx, c.Endpoint(), io.Discard) }()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatalf("handler never started")
	}
	_ = c.Close()
	select {
	case <-fetchDone:
		// Stream broke: error or truncated-clean EOF both prove the
		// serve did not outlive the channel.
	case <-ctx.Done():
		t.Fatalf("in-flight bundle serve outlived channel close")
	}
}
