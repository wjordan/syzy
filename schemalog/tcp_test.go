package schemalog

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestStreamRPCOverUnix(t *testing.T) {
	ln, err := net.Listen("unix", t.TempDir()+"/schema.sock")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := Serve(ln, NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client, err := DialFunc("unix-test", func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", ln.Addr().String())
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if seq, err := client.Append(context.Background(), 0, []byte("op"), "CREATE TABLE t (id)"); err != nil || seq != 1 {
		t.Fatalf("Append = %d, %v", seq, err)
	}
	if head, err := client.Head(context.Background()); err != nil || head != 1 {
		t.Fatalf("Head = %d, %v", head, err)
	}
}

// blockingBackend blocks every call until release is closed. Used to
// exercise the cancel-mid-call paths.
type blockingBackend struct {
	release chan struct{}
}

func newBlockingBackend() *blockingBackend {
	return &blockingBackend{release: make(chan struct{})}
}

func (b *blockingBackend) wait(ctx context.Context) error {
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingBackend) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	return 1, b.wait(ctx)
}

func (b *blockingBackend) Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error) {
	return nil, b.wait(ctx)
}

func (b *blockingBackend) Head(ctx context.Context) (uint64, error) {
	return 0, b.wait(ctx)
}

func TestTCP_ContextCancelMidCall(t *testing.T) {
	backend := newBlockingBackend()
	srv, err := ListenTCP("127.0.0.1:0", backend)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client, err := DialTCP(srv.Addr())
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		err error
	}
	out := make(chan result, 1)
	go func() {
		_, err := client.Head(ctx)
		out <- result{err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case r := <-out:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Head error = %v; want context.Canceled", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Head did not return after cancel")
	}
	close(backend.release)
}

func TestTCP_ServerCloseMidCall(t *testing.T) {
	backend := newBlockingBackend()
	srv, err := ListenTCP("127.0.0.1:0", backend)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	client, err := DialTCP(srv.Addr())
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	out := make(chan error, 1)
	go func() {
		_, err := client.Head(context.Background())
		out <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := srv.Close(); err != nil {
		t.Fatalf("server Close: %v", err)
	}
	select {
	case err := <-out:
		if err == nil {
			t.Fatal("Head returned nil; want transport error after server close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Head did not return after server close")
	}
	close(backend.release)
}

func TestTCP_ReconnectAfterServerBounce(t *testing.T) {
	// Pre-bind to grab a free port the second server can rebind on.
	// SO_REUSEADDR is the default on Linux once the first listener closes.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	srv1, err := ListenTCP(addr, NewLocal())
	if err != nil {
		t.Fatalf("ListenTCP 1: %v", err)
	}

	client, err := DialTCP(addr)
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Head(context.Background()); err != nil {
		t.Fatalf("first Head: %v", err)
	}
	if err := srv1.Close(); err != nil {
		t.Fatalf("close srv1: %v", err)
	}

	if _, err := client.Head(context.Background()); err == nil {
		t.Fatal("Head after server close returned nil; want transport error")
	}

	time.Sleep(minRedialInterval + 50*time.Millisecond)
	srv2, err := ListenTCP(addr, NewLocal())
	if err != nil {
		t.Fatalf("ListenTCP 2: %v", err)
	}
	t.Cleanup(func() { _ = srv2.Close() })

	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := client.Head(context.Background())
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client never reconnected: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestTCP_OversizeFrameRejected(t *testing.T) {
	srv, err := ListenTCP("127.0.0.1:0", NewLocal())
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Oversize length prefix; server's readFrame must refuse before alloc.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrameSize+1)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write hdr: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("read after oversize header succeeded; want EOF/conn-closed")
	}
}

func TestTCP_DialTCPInvalidAddr(t *testing.T) {
	_, err := DialTCP("not a valid address")
	if err == nil {
		t.Fatal("DialTCP accepted bogus address")
	}
	if !strings.Contains(err.Error(), "resolve") {
		t.Errorf("error message = %q; want it to mention resolve", err.Error())
	}
}
