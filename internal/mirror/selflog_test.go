package mirror_test

import (
	"errors"
	"io"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/mirror"
)

const selfOrigin = crdt.Origin(42)

func newSelfManager(t *testing.T) *mirror.Manager {
	t.Helper()
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir(), Self: selfOrigin})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

// readSelf returns every KindMirror record in the self-log, in order. The
// self journal has no writer goroutine — AppendSelf is inline — so records
// are visible immediately, no busy-wait.
func readSelf(t *testing.T, m *mirror.Manager) []journal.Record {
	t.Helper()
	j, ok := m.LookupJournal(selfOrigin)
	if !ok {
		return nil
	}
	var recs []journal.Record
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			t.Fatalf("Iterate: %v", err)
		}
		if rec.Kind == journal.KindMirror {
			recs = append(recs, rec)
		}
	}
	return recs
}

// TestAppendSelf_CarriesEndOffsetPristinePayload is the on-disk contract:
// AppendSelf stashes the source self-journal endOffset in the record HEADER
// hlc field while the payload stays the byte-identical wire changeset, so
// recovery reads the offset back and every payload parser (payloadSeq, the
// sealer, serve-verbatim) still sees a pristine changeset.
func TestAppendSelf_CarriesEndOffsetPristinePayload(t *testing.T) {
	mgr := newSelfManager(t)

	type app struct {
		payload []byte
		endOff  journal.Offset
	}
	apps := []app{
		{payload(selfOrigin, 1, 0xa1), 100},
		{payload(selfOrigin, 2, 0xa2), 240},
		{payload(selfOrigin, 3, 0xa3), 375},
	}
	for _, a := range apps {
		if err := mgr.AppendSelf(a.payload, a.endOff); err != nil {
			t.Fatalf("AppendSelf(endOff=%d): %v", a.endOff, err)
		}
	}
	if err := mgr.SyncSelf(); err != nil {
		t.Fatalf("SyncSelf: %v", err)
	}

	recs := readSelf(t, mgr)
	if len(recs) != len(apps) {
		t.Fatalf("readSelf: got %d records, want %d", len(recs), len(apps))
	}
	for i, rec := range recs {
		if journal.Offset(rec.HLC) != apps[i].endOff {
			t.Errorf("record %d: header endOffset = %d, want %d", i, rec.HLC, apps[i].endOff)
		}
		if rec.Origin != uint64(selfOrigin) {
			t.Errorf("record %d: origin = %d, want %d", i, rec.Origin, selfOrigin)
		}
		if string(rec.Payload) != string(apps[i].payload) {
			t.Errorf("record %d: payload not pristine (endOffset must live in the header, not the payload)", i)
		}
	}
}

func TestAppendSelf_RejectsZeroEndOffset(t *testing.T) {
	mgr := newSelfManager(t)
	if err := mgr.AppendSelf(payload(selfOrigin, 1, 0), 0); err == nil {
		t.Fatal("AppendSelf(endOffset=0) succeeded; want error")
	}
	if recs := readSelf(t, mgr); len(recs) != 0 {
		t.Fatalf("rejected append still wrote %d records", len(recs))
	}
}

// TestAppend_RejectsSelfOrigin: the async Append path must never touch the
// self origin — its writer goroutine would defer durability past return,
// defeating capture-before-publish. Self writes go through AppendSelf.
func TestAppend_RejectsSelfOrigin(t *testing.T) {
	mgr := newSelfManager(t)
	if err := mgr.Append(selfOrigin, payload(selfOrigin, 1, 0)); err == nil {
		t.Fatal("Append(self) succeeded; want error steering to AppendSelf")
	}
	// A remote origin still uses the async path fine.
	const remote = crdt.Origin(9)
	if err := mgr.Append(remote, payload(remote, 1, 0)); err != nil {
		t.Fatalf("Append(remote): %v", err)
	}
	drainMirror(t, mgr, remote, 1)
}

// TestReap_RejectsSelfOrigin: the self-log is trimmed via RetainSealed, not
// reaped; Reap would try to stop a nonexistent writer goroutine.
func TestReap_RejectsSelfOrigin(t *testing.T) {
	mgr := newSelfManager(t)
	if err := mgr.AppendSelf(payload(selfOrigin, 1, 0xff), 50); err != nil {
		t.Fatalf("AppendSelf: %v", err)
	}
	if err := mgr.Reap(selfOrigin); err == nil {
		t.Fatal("Reap(self) succeeded; want error")
	}
}

// TestSelfLog_Unconfigured: with no Self origin, the self-only entry points
// error rather than silently writing to origin 0.
func TestSelfLog_Unconfigured(t *testing.T) {
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir()}) // Self == 0
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.AppendSelf(payload(1, 1, 0), 10); err == nil {
		t.Fatal("AppendSelf with no self origin succeeded; want error")
	}
	if err := mgr.SyncSelf(); err != nil {
		t.Fatalf("SyncSelf with no self origin should be a no-op, got %v", err)
	}
}
