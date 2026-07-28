package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestApplyRetrySelfHealsPinnedSnapshot recreates the prod inbound-apply
// wedge shape directly on the broker's own statement cache (the pre-fix
// bug in readKeyColumns): a cached SELECT stepped to SQLITE_ROW and
// abandoned pins a read snapshot on AppApply. Once another connection
// advances the WAL, every BEGIN IMMEDIATE on AppApply fails
// SQLITE_BUSY_SNAPSHOT ("database is locked") forever — busy_timeout
// does not apply. applyPayloadWithRetry must self-heal (finalize cached
// statements + rollback) within its retry budget instead of retrying
// one payload for hours.
func TestApplyRetrySelfHealsPinnedSnapshot(t *testing.T) {
	a := newUniqueApplier(t, 1)
	a.br.retryBackoff = time.Millisecond

	remote := crdt.Origin(2)
	stamp := func(w int64) crdt.Stamp {
		return crdt.Stamp{Origin: remote, Clock: crdt.Clock{WallTime: w}}
	}

	// Seed a row so the cached unique-read SELECT pends at SQLITE_ROW.
	ins := buildUniqueInsert(t, a.tab, crdt.Dot{Origin: remote, Seq: 1}, stamp(100),
		[]byte{0xAA}, "slug-1", "n-1")
	if err := a.br.applyPayloadCache(ins, ins.Encoded(), false); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// Pre-fix bug shape: step the broker's cached readKeyColumns stmt to
	// SQLITE_ROW and abandon it (no Reset). The stmt now pins a read
	// snapshot on AppApply.
	stmt, err := a.br.uniqReadStmt(a.tab, a.tab.UniqueKeys[0])
	if err != nil {
		t.Fatalf("uniqReadStmt: %v", err)
	}
	if err := stmt.BindBlob(1, []byte{0xAA}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if has, err := stmt.Step(); err != nil || !has {
		t.Fatalf("step: has=%v err=%v", has, err)
	}

	// Another connection (the producer in prod) advances the WAL, making
	// the pinned snapshot stale.
	w, err := sqlitebridge.Open(dbFileOf(t, a.app), 0)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer w.Close()
	if err := w.Exec(`INSERT INTO u (id, slug, n) VALUES (x'BB', 'slug-b', 'n-b')`); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// Sanity: the wedge is live — a direct apply fails locked.
	ins2 := buildUniqueInsert(t, a.tab, crdt.Dot{Origin: remote, Seq: 2}, stamp(200),
		[]byte{0xCC}, "slug-c", "n-c")
	if err := a.br.applyPayloadCache(ins2, ins2.Encoded(), false); err == nil ||
		!strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("wedge not reproduced; direct apply err = %v", err)
	}

	// The retry loop must self-heal and land the payload within the
	// budget (~applySelfHealAfter retries at 1ms backoff), not wedge.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.br.applyPayloadWithRetry(ctx, ins2.Encoded()); err != nil {
		t.Fatalf("apply-retry loop did not self-heal: %v", err)
	}

	h := a.br.InboundHealth()
	if h.SelfHeals == 0 {
		t.Fatalf("expected at least one self-heal, health = %+v", h)
	}
	if h.ConsecutiveLocked != 0 {
		t.Fatalf("locked streak should reset after recovery, got %d", h.ConsecutiveLocked)
	}
	if seq, ok := a.br.AppliedSeq(remote); !ok || seq != 2 {
		t.Fatalf("AppliedSeq = %d, %v; want 2, true", seq, ok)
	}
	// The healed connection stays usable for the next apply.
	ins3 := buildUniqueInsert(t, a.tab, crdt.Dot{Origin: remote, Seq: 3}, stamp(300),
		[]byte{0xDD}, "slug-d", "n-d")
	if err := a.br.applyPayloadCache(ins3, ins3.Encoded(), false); err != nil {
		t.Fatalf("post-heal apply: %v", err)
	}
}
