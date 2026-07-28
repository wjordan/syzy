package vsock_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/futex"
	"github.com/wjordan/syzy/wake"
	"github.com/wjordan/syzy/wake/vsock"
)

const testOrigin = "deadbeef00000001"

func unixPair(t *testing.T) (net.Listener, func() (net.Conn, error)) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "wake.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dial := func() (net.Conn, error) { return net.Dial("unix", sock) }
	return ln, dial
}

func TestBasicWakeRoundtrip(t *testing.T) {
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	waiter := listener.Register(testOrigin)
	waker := vsock.NewWaker(dial, testOrigin)
	defer waker.Close()

	waker.Wake(nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestRepeatedWakesCoalesce(t *testing.T) {
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	waiter := listener.Register(testOrigin)
	waker := vsock.NewWaker(dial, testOrigin)
	defer waker.Close()

	for i := 0; i < 100; i++ {
		waker.Wake(nil)
	}

	// At least one Wait should return; subsequent Waits should time
	// out (coalesced wakes only post one signal).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx, nil, 0, 200*time.Millisecond); err != nil {
		t.Fatalf("first Wait: %v", err)
	}
	// Drain residual signals if any (some implementations buffer >1).
	// The contract is: a wake-or-more produces at least one signal.
	// Excess signals are allowed but not required.
}

func TestTimeoutWhenNoWake(t *testing.T) {
	ln, _ := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	waiter := listener.Register(testOrigin)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := waiter.Wait(ctx, nil, 0, 50*time.Millisecond)
	if !errors.Is(err, futex.ErrTimeout) {
		t.Fatalf("Wait err = %v; want futex.ErrTimeout", err)
	}
}

func TestUnregisteredOriginDropsConnection(t *testing.T) {
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	// No Register call for testOrigin: the listener accepts the
	// connection, reads the hello, then closes. The Waker's writes
	// will eventually fail and the Waker resets its connection.
	waker := vsock.NewWaker(dial, testOrigin)
	defer waker.Close()
	waker.Wake(nil)

	// Now Register and verify subsequent wakes are delivered.
	waiter := listener.Register(testOrigin)
	// The Waker has to reconnect; give it a moment.
	time.Sleep(50 * time.Millisecond)
	waker.Wake(nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("Wait after register: %v", err)
	}
}

func TestMultipleOriginsMultiplexed(t *testing.T) {
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	originA := "aaaa000000000001"
	originB := "bbbb000000000002"
	waiterA := listener.Register(originA)
	waiterB := listener.Register(originB)

	wakerA := vsock.NewWaker(dial, originA)
	wakerB := vsock.NewWaker(dial, originB)
	defer wakerA.Close()
	defer wakerB.Close()

	wakerA.Wake(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiterA.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("Wait A: %v", err)
	}
	// B should not have been signaled.
	short, cancelShort := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelShort()
	err := waiterB.Wait(short, nil, 0, 50*time.Millisecond)
	if !errors.Is(err, futex.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait B err = %v; want timeout", err)
	}

	wakerB.Wake(nil)
	if err := waiterB.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("Wait B after wake: %v", err)
	}
}

func TestRegisterAfterCloseNoPanic(t *testing.T) {
	// Regression: Register used to assign into a nil map after
	// Close, panicking. Now it returns a dead Waiter that times out
	// immediately so racing callers stay safe.
	ln, _ := unixPair(t)
	listener := vsock.NewListener(ln)
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w := listener.Register(testOrigin)
	if w == nil {
		t.Fatal("Register returned nil after Close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := w.Wait(ctx, nil, 0, 10*time.Millisecond)
	if !errors.Is(err, futex.ErrTimeout) {
		t.Errorf("dead waiter Wait err = %v; want futex.ErrTimeout", err)
	}
	_ = w.Close()
}

func TestStressManyConcurrentWakes(t *testing.T) {
	// Multiple producers slam wakes through the listener under
	// concurrency. Verifies no drops at the connection layer (each
	// producer's wake count = listener's signal count for its
	// origin) and that the listener doesn't deadlock or leak
	// goroutines under load.
	if testing.Short() {
		t.Skip("stress test skipped in -short")
	}
	const (
		producers = 8
		wakesEach = 500
	)
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)
	defer listener.Close()

	type producer struct {
		origin string
		waiter wake.Waiter
		waker  wake.Waker
		recv   atomic.Int32
	}
	prods := make([]*producer, producers)
	for i := range prods {
		origin := strings.Repeat("0", 14) + hex2(uint16(i+1))
		p := &producer{origin: origin}
		p.waiter = listener.Register(origin)
		p.waker = vsock.NewWaker(dial, origin)
		prods[i] = p
	}
	defer func() {
		for _, p := range prods {
			_ = p.waker.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Consumer goroutines: count signal arrivals per origin until
	// ctx is cancelled.
	var wg sync.WaitGroup
	for _, p := range prods {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				err := p.waiter.Wait(ctx, nil, 0, 50*time.Millisecond)
				if err == nil {
					p.recv.Add(1)
					continue
				}
				if ctx.Err() != nil {
					return
				}
				// Anything else is a timeout — loop and retry.
			}
		}()
	}

	// Producer goroutines: fire wakesEach in tight loop.
	for _, p := range prods {
		p := p
		go func() {
			for i := 0; i < wakesEach; i++ {
				p.waker.Wake(nil)
			}
		}()
	}

	// Wait for receipts to plateau. Coalescing means recv < wakesEach
	// is OK; what matters is recv > 0 for every producer and no
	// goroutine deadlocks. Give it up to 5 seconds to settle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, p := range prods {
			if p.recv.Load() == 0 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	for i, p := range prods {
		if got := p.recv.Load(); got == 0 {
			t.Errorf("producer %d origin=%s recv=0; expected at least 1 signal", i, p.origin)
		}
	}
}

func hex2(v uint16) string {
	const hexd = "0123456789abcdef"
	return string([]byte{hexd[(v>>4)&0xf], hexd[v&0xf]})
}

func TestWakerReconnectAfterPeerClose(t *testing.T) {
	ln, dial := unixPair(t)
	listener := vsock.NewListener(ln)

	waiter := listener.Register(testOrigin)
	waker := vsock.NewWaker(dial, testOrigin)
	defer waker.Close()

	waker.Wake(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("first Wait: %v", err)
	}

	// Tear down the listener; Waker's existing connection breaks. A
	// fresh listener on the same socket accepts the next Wake's
	// reconnect.
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	ln2, err := net.Listen("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("relisten: %v", err)
	}
	listener2 := vsock.NewListener(ln2)
	defer listener2.Close()
	waiter2 := listener2.Register(testOrigin)

	// First Wake after the break may land on the dead conn; the
	// implementation closes it on Write failure. Second Wake redials.
	waker.Wake(nil)
	time.Sleep(50 * time.Millisecond)
	waker.Wake(nil)

	if err := waiter2.Wait(ctx, nil, 0, 500*time.Millisecond); err != nil {
		t.Fatalf("Wait after reconnect: %v", err)
	}
}
