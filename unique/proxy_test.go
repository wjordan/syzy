package unique

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
)

func startProxyPair(t *testing.T, reg Registry) *ProxyClient {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "unique.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := ServeProxy(ln, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client, err := NewProxyClient("test", func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func proxyClaim(value, owner string) Claim {
	return Claim{
		Table: [16]byte{1},
		Key:   [16]byte{2},
		Value: []byte(value),
		Owner: []byte(owner),
	}
}

func TestProxyReserveAndConflict(t *testing.T) {
	client := startProxyPair(t, NewLocal())
	ctx := context.Background()

	ok, conflict, err := client.Reserve(ctx, []Claim{proxyClaim("a", "row1")})
	if err != nil || !ok || conflict != nil {
		t.Fatalf("first reserve: ok=%v conflict=%v err=%v", ok, conflict, err)
	}
	// Same owner re-asserts idempotently.
	ok, _, err = client.Reserve(ctx, []Claim{proxyClaim("a", "row1")})
	if err != nil || !ok {
		t.Fatalf("re-assert: ok=%v err=%v", ok, err)
	}
	// A different owner conflicts, with the losing claim named.
	ok, conflict, err = client.Reserve(ctx, []Claim{proxyClaim("a", "row2")})
	if err != nil || ok || conflict == nil || string(conflict.Value) != "a" {
		t.Fatalf("conflict reserve: ok=%v conflict=%v err=%v", ok, conflict, err)
	}
	// Release frees the value for a new owner (Local frees immediately).
	if err := client.Release(ctx, []Claim{proxyClaim("a", "row1")}); err != nil {
		t.Fatalf("release: %v", err)
	}
	ok, _, err = client.Reserve(ctx, []Claim{proxyClaim("a", "row2")})
	if err != nil || !ok {
		t.Fatalf("reserve after release: ok=%v err=%v", ok, err)
	}
}

func TestProxyEmptyClaimsSkipDial(t *testing.T) {
	client, err := NewProxyClient("test", func(context.Context) (net.Conn, error) {
		return nil, errors.New("must not dial")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if ok, conflict, err := client.Reserve(context.Background(), nil); err != nil || !ok || conflict != nil {
		t.Fatalf("empty reserve: ok=%v conflict=%v err=%v", ok, conflict, err)
	}
	if err := client.Release(context.Background(), nil); err != nil {
		t.Fatalf("empty release: %v", err)
	}
}

func TestProxyDialFailureIsUnavailable(t *testing.T) {
	client, err := NewProxyClient("test", func(context.Context) (net.Conn, error) {
		return nil, errors.New("refused")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, _, err = client.Reserve(context.Background(), []Claim{proxyClaim("a", "row1")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

// unavailableRegistry models a backend outage (e.g. a lease handover).
type unavailableRegistry struct{}

func (unavailableRegistry) Reserve(context.Context, []Claim) (bool, *Claim, error) {
	return false, nil, ErrUnavailable
}
func (unavailableRegistry) Release(context.Context, []Claim) error { return nil }

func TestProxyBackendOutageIsUnavailable(t *testing.T) {
	client := startProxyPair(t, unavailableRegistry{})
	_, _, err := client.Reserve(context.Background(), []Claim{proxyClaim("a", "row1")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestProxyServerCloseIsUnavailable(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "unique.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := ServeProxy(ln, NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewProxyClient("test", func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if ok, _, err := client.Reserve(context.Background(), []Claim{proxyClaim("a", "row1")}); err != nil || !ok {
		t.Fatalf("reserve before close: ok=%v err=%v", ok, err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}
	_, _, err = client.Reserve(context.Background(), []Claim{proxyClaim("b", "row1")})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err after server close = %v, want ErrUnavailable", err)
	}
}
