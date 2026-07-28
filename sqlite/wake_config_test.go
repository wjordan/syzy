package sqlite_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/wake"
)

// fakeWaker counts Wake calls. Stand-in for vsock.Waker in tests that
// just verify the wiring without needing a real cross-process
// transport.
type fakeWaker struct {
	calls atomic.Int32
}

func (f *fakeWaker) Wake(*uint32) { f.calls.Add(1) }
func (f *fakeWaker) Close() error { return nil }
func (f *fakeWaker) Calls() int32 { return f.calls.Load() }

// fakeListener counts Register / Unregister calls and returns Waiters
// that no-op.
type fakeListener struct {
	registered   atomic.Int32
	unregistered atomic.Int32
}

func (f *fakeListener) Register(string) wake.Waiter {
	f.registered.Add(1)
	return &fakeWaiter{}
}
func (f *fakeListener) Unregister(string) { f.unregistered.Add(1) }
func (f *fakeListener) Close() error      { return nil }

type fakeWaiter struct{}

func (fakeWaiter) Wait(ctx context.Context, _ *uint32, _ uint32, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}
func (fakeWaiter) Close() error { return nil }

func TestConfigWakeIsInvokedOnAppend(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")

	waker := &fakeWaker{}
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath, Wake: waker})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	if err := node.Exec(`CREATE TABLE t (id BLOB PRIMARY KEY DEFAULT (uuidv7()), v INT)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := node.Exec(`INSERT INTO t (v) VALUES (1), (2), (3)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	if got := waker.Calls(); got == 0 {
		t.Fatalf("waker.Wake never called after INSERTs; want > 0")
	}
}

func TestConfigWakeListenerAccepted(t *testing.T) {
	// Smoke test: a configured WakeListener doesn't break Open even
	// when no secondary origins are present. Register/Unregister are
	// exercised end-to-end by wake/vsock tests; here we just verify
	// the syzy.Config field flows through.
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	listener := &fakeListener{}
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath, WakeListener: listener})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
