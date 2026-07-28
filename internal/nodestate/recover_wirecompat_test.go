package nodestate

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// TestRecoverMirrorSkipsUndecodablePayload is the rolling-upgrade /
// rollback regression: a mirror journal holding a payload this binary
// cannot decode (unknown wire version, corruption) must not be fatal at
// node open. RecoverMirror logs, skips the record, and keeps the seqs
// it could decode; the skipped seq stays unapplied so the drainer
// re-fetches it from the cluster.
func TestRecoverMirrorSkipsUndecodablePayload(t *testing.T) {
	dir := t.TempDir()
	const self crdt.Origin = 7
	const orig crdt.Origin = 9

	j, err := journal.Open(dir, 64*1024, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	defer j.Close()

	mkCS := func(seq crdt.Seq) *crdt.Changeset {
		cs, err := crdt.Build(
			crdt.Dot{Origin: orig, Seq: seq},
			crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + seq), Logical: 0}, Origin: orig},
			nil, crdt.ClusterID{0xCC},
			[]crdt.Record{crdt.Delete{Table: crdt.TableID{0x01}, PK: crdt.PKBlob{byte(seq)}, CL: 2}},
		)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return cs
	}

	// Valid payload (seq 1), then an undecodable one (future wire
	// version 0xFF), then another valid payload (seq 2) to prove the
	// walk continues past the bad record.
	garbage := append([]byte{0xFF}, mkCS(1).Encoded()[1:]...)
	for _, payload := range [][]byte{mkCS(1).Encoded(), garbage, mkCS(2).Encoded()} {
		if _, _, err := j.Append(journal.KindMirror, 0, uint64(orig), payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Capture the Warn the skip path emits on the default logger.
	var logbuf logRecorder
	prev := slog.Default()
	slog.SetDefault(slog.New(&logbuf))
	defer slog.SetDefault(prev)

	cache := New(self)
	src := &fakeMirrorSource{self: self, orig: orig, j: j}
	if _, err := RecoverMirror(cache, src, nil); err != nil {
		t.Fatalf("RecoverMirror returned %v; undecodable payload must not be fatal", err)
	}

	if !cache.IsAppliedRemote(orig, 1) {
		t.Error("seq 1 (valid, before bad record) not applied")
	}
	if !cache.IsAppliedRemote(orig, 2) {
		t.Error("seq 2 (valid, after bad record) not applied — walk stopped at bad record")
	}

	warns := logbuf.matching(slog.LevelWarn, "undecodable mirror payload")
	if len(warns) != 1 {
		t.Errorf("want exactly 1 skip warning, got %d (%q)", len(warns), logbuf.lines())
	}
}

// logRecorder is a minimal slog.Handler capturing records for assertions.
type logRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (l *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, r)
	return nil
}
func (l *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logRecorder) WithGroup(string) slog.Handler      { return l }

func (l *logRecorder) matching(lvl slog.Level, substr string) []slog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []slog.Record
	for _, r := range l.recs {
		if r.Level == lvl && strings.Contains(r.Message, substr) {
			out = append(out, r)
		}
	}
	return out
}

func (l *logRecorder) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.recs))
	for i, r := range l.recs {
		out[i] = r.Message
	}
	return out
}
