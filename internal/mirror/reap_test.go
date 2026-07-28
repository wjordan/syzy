package mirror_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/mirror"
)

func originDir(root string, o crdt.Origin) string {
	return filepath.Join(root, fmt.Sprintf("origin_%d", o))
}

func newManagerAt(t *testing.T, root string) *mirror.Manager {
	t.Helper()
	mgr, err := mirror.New(mirror.Config{Root: root})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

// TestReap_RemovesDirAndHandle: after Reap the on-disk dir, the in-memory
// handle, and the Origins() entry are all gone.
func TestReap_RemovesDirAndHandle(t *testing.T) {
	root := t.TempDir()
	mgr := newManagerAt(t, root)

	const o = crdt.Origin(7)
	for i := crdt.Seq(1); i <= 3; i++ {
		if err := mgr.Append(o, payload(o, i, byte(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	drainMirror(t, mgr, o, 3)

	if _, err := os.Stat(originDir(root, o)); err != nil {
		t.Fatalf("dir should exist pre-reap: %v", err)
	}
	if err := mgr.Reap(o); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if _, err := os.Stat(originDir(root, o)); !os.IsNotExist(err) {
		t.Fatalf("dir should be gone post-reap, stat err=%v", err)
	}
	if _, ok := mgr.LookupJournal(o); ok {
		t.Fatalf("LookupJournal should be false post-reap")
	}
	for _, got := range mgr.Origins() {
		if got == o {
			t.Fatalf("Origins() should not include reaped origin %d", o)
		}
	}
}

// TestReap_ResurrectionRecreates: a reaped origin is re-created by a later
// Append (the reversible-eviction property the Node reaper relies on).
func TestReap_ResurrectionRecreates(t *testing.T) {
	root := t.TempDir()
	mgr := newManagerAt(t, root)

	const o = crdt.Origin(9)
	if err := mgr.Append(o, payload(o, 1, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	drainMirror(t, mgr, o, 1)
	if err := mgr.Reap(o); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	if err := mgr.Append(o, payload(o, 2, 2)); err != nil {
		t.Fatalf("Append post-reap: %v", err)
	}
	drainMirror(t, mgr, o, 1)
	if _, ok := mgr.LookupJournal(o); !ok {
		t.Fatalf("journal should be re-created post-resurrection")
	}
	if _, err := os.Stat(originDir(root, o)); err != nil {
		t.Fatalf("dir should exist post-resurrection: %v", err)
	}
}

// TestReap_NoHandleIsNoError: reaping an origin we never journaled is a no-op.
func TestReap_NoHandleIsNoError(t *testing.T) {
	mgr := newManagerAt(t, t.TempDir())
	if err := mgr.Reap(123); err != nil {
		t.Fatalf("Reap of unknown origin should be a no-op: %v", err)
	}
}

// TestReap_RaceWithAppend: concurrent Append + Reap must not panic, deadlock,
// or trip the race detector (run with -race). The quit-channel teardown (never
// closing in) is what keeps a racing Append off a closed channel.
func TestReap_RaceWithAppend(t *testing.T) {
	mgr := newManagerAt(t, t.TempDir())

	var wg sync.WaitGroup
	for o := crdt.Origin(1); o <= 20; o++ {
		o := o
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := crdt.Seq(1); i <= 10; i++ {
				// May error after a concurrent reap; that's expected and fine.
				_ = mgr.Append(o, payload(o, i, byte(i)))
			}
		}()
		go func() {
			defer wg.Done()
			_ = mgr.Reap(o)
		}()
	}
	wg.Wait()
}
