package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/schemalog"
)

// blockingLog wraps a schemalog.Log, parking the first Read until
// release is closed. entered is closed once Read is in flight.
type blockingLog struct {
	inner   schemalog.Log
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingLog) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	return l.inner.Append(ctx, parentSeq, op, raw)
}

func (l *blockingLog) Read(ctx context.Context, fromSeq uint64, limit int) ([]schemalog.Event, error) {
	l.once.Do(func() { close(l.entered) })
	select {
	case <-l.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return l.inner.Read(ctx, fromSeq, limit)
}

func (l *blockingLog) Head(ctx context.Context) (uint64, error) {
	return l.inner.Head(ctx)
}

// TestRunSchemaCatchup_ReadOutsideApplyMu locks in the catchup lock
// scope: SchemaLog.Read (object-store GETs with retries, up to minutes
// in prod) must run OUTSIDE applyMu so a slow read can't starve the
// apply path on every 500ms tick. Before the fix this test deadlocks
// at the applyMu acquisition below.
func TestRunSchemaCatchup_ReadOutsideApplyMu(t *testing.T) {
	t.Parallel()
	f := newCatchupFixture(t)
	f.appendCreateTable(t, 0, "t_lockscope")
	bl := &blockingLog{inner: f.log, entered: make(chan struct{}), release: make(chan struct{})}
	f.br.cfg.SchemaLog = bl

	done := make(chan error, 1)
	go func() { done <- f.br.RunSchemaCatchupOnce(context.Background()) }()

	select {
	case <-bl.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SchemaLog.Read never entered")
	}

	// While the log read is parked, an applier must be able to take
	// applyMu immediately.
	acquired := make(chan struct{})
	go func() {
		f.br.applyMu.Lock()
		f.br.applyMu.Unlock() //nolint:staticcheck // empty critical section is the probe
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("applyMu held across SchemaLog.Read; appliers starved")
	}

	close(bl.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("catchup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("catchup did not finish after release")
	}
	// The apply itself still happened (serialization invariant intact).
	if got := f.localSchemaSeq(t); got != 1 {
		t.Fatalf("schema_seq = %d, want 1", got)
	}
}
