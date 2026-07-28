// Package wake defines the interfaces for the syzy journal's
// cross-process record-published notification. The journal's
// durability primitive is an atomic store to a publish word in a
// shared mmap; this package's job is the wake side: how a producer
// signals "look now," and how a consumer parked on WaitAt observes
// the signal.
//
// Two implementations ship:
//
//   - Default (no wake configured): the journal's
//     EnableSharedWake(true) installs futex-based wake. Suitable
//     when producer and consumer share a kernel.
//
//   - wake/vsock: a vsock-shaped Waker/Listener pair. Suitable when
//     producer and consumer live in different kernels (e.g., a guest
//     VM's syzy.so extension producing into a journal whose host-side
//     syzy daemon drains it). Required because futex wait/wake
//     cannot meet across kernels: shared-mapping futex keys derive
//     from the per-kernel inode->i_sequence (linux/kernel/futex/
//     core.c).
package wake

import (
	"context"
	"time"
)

// Waker is the producer-side interface. Wake is called once per
// published record with the publish-word address. Implementations are
// best-effort: errors are not returned to the commit path (durability
// is already in the journal; the consumer's WaitAt timeout backstop
// covers missed wakes).
type Waker interface {
	Wake(publishAddr *uint32)
	Close() error
}

// Waiter is the consumer-side interface. Wait blocks until the
// transport reports a wake, the timeout elapses, or ctx is cancelled.
// On timeout, Wait must return a sentinel that the journal recognizes
// (futex.ErrTimeout); other errors are propagated.
type Waiter interface {
	Wait(ctx context.Context, publishAddr *uint32, expected uint32, timeout time.Duration) error
	Close() error
}

// Listener is the daemon-side multiplexer. The syzy daemon discovers
// origins by scanning the per-DB origins directory; for each origin
// it Registers a Waiter and installs the Waiter's Wait on the
// origin's journal via journal.SetWaitFunc. On drainer teardown the
// daemon calls Unregister.
//
// vsock.Listener satisfies this interface for the cross-VM transport.
type Listener interface {
	Register(originHex string) Waiter
	Unregister(originHex string)
	Close() error
}
