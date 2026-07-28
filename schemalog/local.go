package schemalog

import (
	"context"
	"sync"
)

// Local is an in-process schema log. Append/Read/Head all serialize
// through a single mutex. Suitable for single-process deployments,
// tests, and the local-log dev/test mode in docs/SCHEMA.md.
//
// Events are kept indefinitely (no retention horizon). Multi-process
// deployments need a backend with a CAS-capable shared store; see
// File for a SQLite-file-backed implementation.
type Local struct {
	mu     sync.Mutex
	events []Event
}

// NewLocal returns a Local schema log with no events.
func NewLocal() *Local { return &Local{} }

func (l *Local) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	head := uint64(0)
	if n := len(l.events); n > 0 {
		head = l.events[n-1].SchemaSeq
	}
	if parentSeq != head {
		return 0, ErrHeadMoved
	}
	next := head + 1
	cp := append([]byte(nil), op...)
	l.events = append(l.events, Event{
		SchemaSeq: next,
		ParentSeq: parentSeq,
		CatalogOp: cp,
		RawSQL:    raw,
	})
	return next, nil
}

func (l *Local) Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, 0, limit)
	for _, e := range l.events {
		if e.SchemaSeq <= fromSeq {
			continue
		}
		// Defensive copy: callers may decode and mutate buffers.
		op := append([]byte(nil), e.CatalogOp...)
		out = append(out, Event{
			SchemaSeq: e.SchemaSeq,
			ParentSeq: e.ParentSeq,
			CatalogOp: op,
			RawSQL:    e.RawSQL,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (l *Local) Head(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := len(l.events); n > 0 {
		return l.events[n-1].SchemaSeq, nil
	}
	return 0, nil
}
