package postgres

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

func TestIsDeterministicApplyErr(t *testing.T) {
	if !isDeterministicApplyErr(&pgconn.PgError{Code: "23502"}) { // not_null_violation
		t.Fatal("23502 must classify deterministic")
	}
	if !isDeterministicApplyErr(&pgconn.PgError{Code: "23514"}) { // check_violation
		t.Fatal("23514 must classify deterministic")
	}
	if isDeterministicApplyErr(&pgconn.PgError{Code: "40001"}) { // serialization_failure
		t.Fatal("40001 must classify transient")
	}
	if isDeterministicApplyErr(io.ErrUnexpectedEOF) {
		t.Fatal("conn error must classify transient")
	}
	if isDeterministicApplyErr(nil) {
		t.Fatal("nil must not classify")
	}
}

// TestQuarantineDeterministicApplyFailure: a peer changeset that fails a local
// integrity constraint (deterministic, SQLSTATE class 23) must not kill the
// Run loop or pin the origin's frontier. It is quarantined, the frontier
// advances, later seqs keep applying — and once the local conflict is gone,
// retryQuarantined force-applies it and clears the entry.
func TestQuarantineDeterministicApplyFailure(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x9a, 0x7e}

	src := newTestEngine(t, ctx, "syzy_qsrc", 7, cluster)
	defer closeEngine(t, ctx, src)

	// dst carries an extra CHECK the source lacks (enforced even under
	// session_replication_role=replica, unlike triggers/FKs) — the poison row
	// fails deterministically on dst only. Extra constraint, same columns:
	// stable IDs still match the source's.
	const dstDB = "syzy_qdst"
	createTestDB(t, ctx, dstDB, schemaKV+`;
		ALTER TABLE public.kv ADD CONSTRAINT no_poison CHECK (val IS DISTINCT FROM 'poison')`)
	meta, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("meta open: %v", err)
	}
	defer meta.Close()
	dst := openDurable(t, ctx, dstDB, 8, cluster, nodestate.New(8), meta)
	defer closeEngine(t, ctx, dst)

	appExec(t, "syzy_qsrc", `INSERT INTO public.kv VALUES (1,'ok')`)
	appExec(t, "syzy_qsrc", `INSERT INTO public.kv VALUES (2,'poison')`)
	appExec(t, "syzy_qsrc", `INSERT INTO public.kv VALUES (3,'ok3')`)
	css := captureBacklog(t, ctx, src, 3)

	runCtx, cancel := context.WithCancel(ctx)
	inbox := make(chan *crdt.Changeset, 8)
	runDone := make(chan error, 1)
	go func() {
		runDone <- dst.Run(runCtx, inbox, func(context.Context, *crdt.Changeset) error { return nil })
	}()
	for _, cs := range css {
		inbox <- cs
	}
	waitApplied := func(seq crdt.Seq) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !dst.cfg.Cache.IsAppliedRemote(7, seq) {
			select {
			case err := <-runDone:
				t.Fatalf("Run exited while waiting for seq %d: %v", seq, err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("seq %d not applied within deadline", seq)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitApplied(3)

	if got := dumpKV(t, dstDB); got[1] != "ok" || got[3] != "ok3" || len(got) != 2 {
		t.Fatalf("post-poison state = %v; want rows 1,3 only", got)
	}
	entries, err := meta.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine: %v", err)
	}
	if len(entries) != 1 || entries[0].Origin != 7 || entries[0].Seq != 2 {
		t.Fatalf("quarantine = %+v; want one entry (origin 7, seq 2)", entries)
	}

	// Run is still alive: a later changeset applies.
	appExec(t, "syzy_qsrc", `INSERT INTO public.kv VALUES (4,'ok4')`)
	cs4 := captureBacklog(t, ctx, src, 1)
	inbox <- cs4[0]
	waitApplied(4)

	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}

	// Conflict removed: the retry sweep force-applies and clears the entry.
	appExec(t, dstDB, `ALTER TABLE public.kv DROP CONSTRAINT no_poison`)
	dst.orch.retryQuarantined(ctx)
	if entries, err = meta.ListQuarantine(); err != nil || len(entries) != 0 {
		t.Fatalf("quarantine after retry = %+v (err %v); want empty", entries, err)
	}
	if got := dumpKV(t, dstDB); got[2] != "poison" {
		t.Fatalf("retried row missing: %v", got)
	}
}
