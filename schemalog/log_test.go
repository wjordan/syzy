package schemalog

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// backends returns the matrix of backends every contract test runs
// against. Each entry returns a fresh schema log + cleanup.
type backendCase struct {
	name string
	make func(t *testing.T) Log
}

func backends(t *testing.T) []backendCase {
	return []backendCase{
		{
			name: "Local",
			make: func(t *testing.T) Log { return NewLocal() },
		},
		{
			name: "File",
			make: func(t *testing.T) Log {
				dir := t.TempDir()
				f, err := OpenFile(filepath.Join(dir, "schemalog.db"))
				if err != nil {
					t.Fatalf("OpenFile: %v", err)
				}
				t.Cleanup(func() { _ = f.Close() })
				return f
			},
		},
		{
			name: "TCP",
			make: func(t *testing.T) Log {
				backend := NewLocal()
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
				return client
			},
		},
		{
			name: "S3",
			make: func(t *testing.T) Log {
				return newS3OnFile(t)
			},
		},
	}
}

func TestLog_Empty(t *testing.T) {
	for _, bc := range backends(t) {
		t.Run(bc.name, func(t *testing.T) {
			a := bc.make(t)
			ctx := context.Background()
			head, err := a.Head(ctx)
			if err != nil || head != 0 {
				t.Errorf("Head empty: got (%d, %v)", head, err)
			}
			evs, err := a.Read(ctx, 0, 10)
			if err != nil || len(evs) != 0 {
				t.Errorf("Read empty: got (%d evs, %v)", len(evs), err)
			}
		})
	}
}

func TestLog_AppendAdvancesHead(t *testing.T) {
	for _, bc := range backends(t) {
		t.Run(bc.name, func(t *testing.T) {
			a := bc.make(t)
			ctx := context.Background()
			n1, err := a.Append(ctx, 0, []byte("op1"), "raw1")
			if err != nil || n1 != 1 {
				t.Fatalf("Append1: (%d, %v)", n1, err)
			}
			n2, err := a.Append(ctx, 1, []byte("op2"), "raw2")
			if err != nil || n2 != 2 {
				t.Fatalf("Append2: (%d, %v)", n2, err)
			}
			head, _ := a.Head(ctx)
			if head != 2 {
				t.Errorf("Head = %d; want 2", head)
			}
		})
	}
}

func TestLog_HeadMovedRejected(t *testing.T) {
	for _, bc := range backends(t) {
		t.Run(bc.name, func(t *testing.T) {
			a := bc.make(t)
			ctx := context.Background()
			if _, err := a.Append(ctx, 0, []byte("op1"), ""); err != nil {
				t.Fatalf("first append: %v", err)
			}
			// Stale parentSeq=0 against head=1 must lose the CAS.
			if _, err := a.Append(ctx, 0, []byte("op2"), ""); !errors.Is(err, ErrHeadMoved) {
				t.Errorf("stale Append: err = %v; want ErrHeadMoved", err)
			}
		})
	}
}

func TestLog_ReadReturnsEventsAboveFromSeq(t *testing.T) {
	for _, bc := range backends(t) {
		t.Run(bc.name, func(t *testing.T) {
			a := bc.make(t)
			ctx := context.Background()
			ops := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
			for i, op := range ops {
				parent := uint64(i)
				if _, err := a.Append(ctx, parent, op, ""); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			evs, err := a.Read(ctx, 1, 10)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if len(evs) != 3 {
				t.Fatalf("len(evs) = %d; want 3", len(evs))
			}
			for i, e := range evs {
				wantSeq := uint64(i + 2)
				if e.SchemaSeq != wantSeq {
					t.Errorf("evs[%d].SchemaSeq = %d; want %d", i, e.SchemaSeq, wantSeq)
				}
			}
			// limit=2 truncates.
			evs2, _ := a.Read(ctx, 0, 2)
			if len(evs2) != 2 || evs2[0].SchemaSeq != 1 || evs2[1].SchemaSeq != 2 {
				t.Errorf("limited Read = %+v", evs2)
			}
		})
	}
}

func TestLog_ConcurrentAppendCASContention(t *testing.T) {
	// Both backends serialize Append internally, so concurrent calls
	// produce a contiguous sequence with no gaps. Each call observes
	// the latest head via the CAS and either commits or returns
	// ErrHeadMoved; this test just verifies the surviving sequence is
	// dense.
	for _, bc := range backends(t) {
		t.Run(bc.name, func(t *testing.T) {
			a := bc.make(t)
			ctx := context.Background()
			const writers = 4
			const each = 25
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < each; i++ {
						for {
							head, _ := a.Head(ctx)
							_, err := a.Append(ctx, head, []byte("x"), "")
							if err == nil {
								break
							}
							if !errors.Is(err, ErrHeadMoved) {
								t.Errorf("unexpected: %v", err)
								return
							}
						}
					}
				}()
			}
			wg.Wait()
			head, _ := a.Head(ctx)
			if head != uint64(writers*each) {
				t.Errorf("head = %d; want %d", head, writers*each)
			}
			evs, _ := a.Read(ctx, 0, writers*each*2)
			if len(evs) != writers*each {
				t.Fatalf("events = %d; want %d", len(evs), writers*each)
			}
			// Sequence is dense and ordered.
			for i, e := range evs {
				want := uint64(i + 1)
				if e.SchemaSeq != want {
					t.Fatalf("evs[%d].SchemaSeq = %d; want %d", i, e.SchemaSeq, want)
				}
				if e.ParentSeq != want-1 {
					t.Fatalf("evs[%d].ParentSeq = %d; want %d", i, e.ParentSeq, want-1)
				}
			}
		})
	}
}

// TestFile_ConcurrentProcessAppend verifies that two independent *File
// handles on the same DB (simulating two independent producer
// processes in the default-on persistence
// path) can both Append without one immediately failing with
// SQLITE_BUSY. Without PRAGMA busy_timeout on the schema.db conn, the
// second BEGIN IMMEDIATE returns SQLITE_BUSY immediately and the
// producer surfaces SQLITE_INTERRUPT to the user.
func TestFile_ConcurrentProcessAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schemalog.db")
	a, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile a: %v", err)
	}
	defer a.Close()
	b, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile b: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	const perWorker = 25
	results := make(chan error, 2*perWorker)
	var wg sync.WaitGroup
	worker := func(f *File, payload string) {
		defer wg.Done()
		for i := 0; i < perWorker; i++ {
			// Re-read head every attempt; either handle may have advanced
			// it. Mirrors the producer's parent_seq=current_head usage.
			head, err := f.Head(ctx)
			if err != nil {
				results <- err
				return
			}
			if _, err := f.Append(ctx, head, []byte(payload), payload); err != nil {
				if errors.Is(err, ErrHeadMoved) {
					// Lost the CAS to the other writer; the next iteration
					// re-reads head and retries.
					continue
				}
				results <- err
				return
			}
		}
		results <- nil
	}
	wg.Add(2)
	go worker(a, "from-a")
	go worker(b, "from-b")
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent Append: %v", err)
		}
	}

	head, err := a.Head(ctx)
	if err != nil {
		t.Fatalf("final Head: %v", err)
	}
	if head == 0 {
		t.Fatalf("expected at least one successful Append, head=0")
	}
}

func TestFile_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schemalog.db")
	a, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	ctx := context.Background()
	if _, err := a.Append(ctx, 0, []byte("x"), "raw"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := OpenFile(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b.Close()
	head, _ := b.Head(ctx)
	if head != 1 {
		t.Errorf("head after reopen = %d; want 1", head)
	}
	evs, _ := b.Read(ctx, 0, 10)
	if len(evs) != 1 || evs[0].RawSQL != "raw" {
		t.Errorf("evs = %+v", evs)
	}
}
