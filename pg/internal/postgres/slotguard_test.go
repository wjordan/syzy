package postgres

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// TestOpenRefusesWhenSlotNoLongerCoversCheckpoint: the replication slot is the
// only durable record of which commits this node has yet to capture. If it is
// gone — dropped, or invalidated by max_slot_wal_keep_size — a replacement
// starts at the current WAL head and every commit since the last checkpoint
// becomes unreadable: never captured, never published, and invisible to peers
// who go on replicating everything after it.
//
// Postgres refuses the backwards slot advance on its own, so this does not
// silently diverge — but it surfaces as "cannot advance replication slot ...
// minimum is ...", which names neither the cause nor what to do about it. The
// assertion is therefore on the operator-facing part: startup must stop AND say
// that this node has to be re-cloned.
func TestOpenRefusesWhenSlotNoLongerCoversCheckpoint(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x5a}
	const db = "syzy_slotguard"
	createTestDB(t, ctx, db, schemaKV)

	metaPath := filepath.Join(t.TempDir(), "meta.db")
	openEngineAt := func() (*Engine, error) {
		meta, err := metadata.Open(metaPath)
		if err != nil {
			t.Fatalf("meta: %v", err)
		}
		t.Cleanup(func() { meta.Close() })
		cfg := baseTestConfig(db, 71, cluster)
		cfg.Tables = []string{"public.kv"}
		cfg.Meta = meta
		return Open(ctx, cfg)
	}

	e, err := openEngineAt()
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	// The final Open fails, so no engine tears the origin down; drop it here.
	t.Cleanup(func() { dropOrigin(t, e.cfg.OriginName) })
	// Capture a commit and checkpoint it, so persisted state names a position
	// the slot is expected to still cover.
	appExec(t, db, `INSERT INTO public.kv VALUES (1,'one')`)
	if got := captureAll(t, ctx, e); len(got) != 1 {
		t.Fatalf("captured %d changesets, want 1", len(got))
	}
	lsn, err := e.appl.currentWALLSN(ctx)
	if err != nil {
		t.Fatalf("current wal lsn: %v", err)
	}
	if err := e.capt.checkpoint(lsn); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// A restart with the slot intact is the ordinary case and must still work.
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again, err := openEngineAt()
	if err != nil {
		t.Fatalf("reopen with the slot intact must succeed: %v", err)
	}

	// Now lose the slot, as an operator or an invalidation would.
	if err := again.DropSlot(ctx); err != nil {
		t.Fatalf("drop slot: %v", err)
	}
	if err := again.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bad, err := openEngineAt()
	if err == nil {
		closeEngine(t, ctx, bad)
		t.Fatal("Open succeeded with a slot that no longer covers the checkpoint — " +
			"this node would silently skip every commit since it and never converge")
	}
	if !strings.Contains(err.Error(), "re-clone") {
		t.Errorf("error %q does not tell the operator what to do (expected a re-clone instruction)", err)
	}
}

// TestOpenAllowsSlotAheadOfCheckpointWithSelfLog: in live mode the slot is
// acked to the SHIPPED position (every changeset fsynced into the self-log)
// while the Cache snapshot is checkpointed only every CheckpointEvery folds. A
// crash in between leaves confirmed_flush ahead of the persisted capture LSN —
// an ordinary, fully recoverable state, because the self-log holds exactly the
// commits in the gap. Startup must resume, not send the operator to re-clone a
// node whose state is intact.
func TestOpenAllowsSlotAheadOfCheckpointWithSelfLog(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x5b}
	const db = "syzy_slotahead"
	createTestDB(t, ctx, db, schemaKV)

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "meta.db")
	journalDir := filepath.Join(tmp, "journal")
	openEngineAt := func() (*Engine, error) {
		meta, err := metadata.Open(metaPath)
		if err != nil {
			t.Fatalf("meta: %v", err)
		}
		t.Cleanup(func() { meta.Close() })
		cfg := baseTestConfig(db, 73, cluster)
		cfg.Tables = []string{"public.kv"}
		cfg.Meta = meta
		cfg.JournalDir = journalDir
		cfg.CheckpointEvery = 1000
		return Open(ctx, cfg)
	}

	e, err := openEngineAt()
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	t.Cleanup(func() { dropOrigin(t, e.cfg.OriginName) })
	appExec(t, db, `INSERT INTO public.kv VALUES (1,'one')`)
	if got := captureAll(t, ctx, e); len(got) != 1 {
		t.Fatalf("captured %d changesets, want 1", len(got))
	}
	// Checkpoint the snapshot here, then let the ack run ahead of it — exactly
	// the live orchestrator's ordering between two checkpoints.
	ckpt, err := e.appl.currentWALLSN(ctx)
	if err != nil {
		t.Fatalf("current wal lsn: %v", err)
	}
	if err := e.capt.checkpoint(ckpt); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	appExec(t, db, `INSERT INTO public.kv VALUES (2,'two')`)
	ahead, err := e.appl.currentWALLSN(ctx)
	if err != nil {
		t.Fatalf("current wal lsn: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	ackSlot(t, ctx, db, slotName(db), ahead)

	again, err := openEngineAt()
	if err != nil {
		t.Fatalf("reopen with the slot acked past the checkpoint must succeed "+
			"(the self-log covers the gap): %v", err)
	}
	closeEngine(t, ctx, again)
}

// ackSlot advances a slot's confirmed_flush the way a live ack would.
func ackSlot(t *testing.T, ctx context.Context, db, slot string, lsn pglogrepl.LSN) {
	t.Helper()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("ack connect: %v", err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, slot, lsn.String()); err != nil {
		t.Fatalf("ack slot: %v", err)
	}
}
