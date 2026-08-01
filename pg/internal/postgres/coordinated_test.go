package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/unique"
)

func openCoordEngine(t *testing.T, ctx context.Context, db string, origin crdt.Origin, cluster crdt.ClusterID, log schemalog.Log) *Engine {
	t.Helper()
	createTestDB(t, ctx, db, schemaKV)
	cfg := baseTestConfig(db, origin, cluster)
	cfg.DDL = true
	cfg.SchemaLog = log
	cfg.CoordinatedUnique = true
	e, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", db, err)
	}
	return e
}

func isGateError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55006"
}

func execErr(t *testing.T, db, sql string) error {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), sql)
	return err
}

// TestCoordinatedKeyGateAndFollower drives the v1 leaseholder-routed design
// end to end: a NOT NULL UNIQUE key is admitted as Coordinated, the
// originator's gate trigger blocks coordinated-key writes until the gate
// opens, a follower applying the schema event builds the physical index +
// triggers, replicated rows land on the follower despite its closed gate
// (replica role bypasses the trigger), and the follower's own physical index
// backstops a genuine conflict.
func TestCoordinatedKeyGateAndFollower(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xc0, 0x0d}
	a := openCoordEngine(t, ctx, "syzy_coord_a", 81, cluster, log)
	defer closeEngine(t, ctx, a)
	b := openCoordEngine(t, ctx, "syzy_coord_b", 82, cluster, log)
	defer closeEngine(t, ctx, b)

	appExec(t, "syzy_coord_a", `CREATE TABLE public.acct (id bigint PRIMARY KEY, email text NOT NULL UNIQUE, note text)`)
	_ = captureAllWithin(t, ctx, a, 800*time.Millisecond)

	// The captured key is Coordinated on the wire.
	events, err := log.Read(ctx, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("schema log read: %v (%d events)", err, len(events))
	}
	op, err := crdt.DecodeCatalogOp(events[0].CatalogOp)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	coordSeen := false
	for _, sub := range op.SubOps {
		for _, k := range sub.Keys {
			coordSeen = coordSeen || k.Coordinated
		}
		for _, k := range op.Keys {
			_ = k
		}
	}
	if op.Kind != crdt.OpBundle {
		// single op form
		for _, k := range op.Keys {
			coordSeen = coordSeen || k.Coordinated
		}
	}
	if !coordSeen {
		t.Fatalf("captured op carries no Coordinated key: %+v", op)
	}

	// Originator gate: closed → coordinated-key writes rejected pre-commit.
	if err := execErr(t, "syzy_coord_a", `INSERT INTO public.acct VALUES (1,'x@y','n')`); !isGateError(err) {
		t.Fatalf("gated INSERT: got %v; want 55006 gate error", err)
	}
	if err := a.SetUniqueGate(ctx, true, time.Minute); err != nil {
		t.Fatalf("open gate: %v", err)
	}
	appExec(t, "syzy_coord_a", `INSERT INTO public.acct VALUES (1,'x@y','n')`)
	// Physical index enforces on the leaseholder.
	if err := execErr(t, "syzy_coord_a", `INSERT INTO public.acct VALUES (2,'x@y','dup')`); err == nil {
		t.Fatalf("duplicate coordinated insert must fail on the leaseholder")
	}
	// Non-key updates stay leaseholder-free.
	if err := a.SetUniqueGate(ctx, false, time.Minute); err != nil {
		t.Fatalf("close gate: %v", err)
	}
	appExec(t, "syzy_coord_a", `UPDATE public.acct SET note='edited' WHERE id=1`)
	if err := execErr(t, "syzy_coord_a", `UPDATE public.acct SET email='z@y' WHERE id=1`); !isGateError(err) {
		t.Fatalf("gated key UPDATE: got %v; want 55006 gate error", err)
	}

	// Follower: schema catch-up builds the physical index + triggers.
	if err := b.catchUpSchema(ctx); err != nil {
		t.Fatalf("follower catch-up: %v", err)
	}
	var idxCount int
	connB, err := pgx.Connect(ctx, dbURL("syzy_coord_b"))
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer connB.Close(ctx)
	if err := connB.QueryRow(ctx,
		`SELECT count(*) FROM pg_indexes WHERE tablename='acct' AND indexdef LIKE 'CREATE UNIQUE INDEX%'`).Scan(&idxCount); err != nil {
		t.Fatalf("index count: %v", err)
	}
	if idxCount == 0 {
		t.Fatalf("follower holds no physical unique index for the coordinated key")
	}
	var trigCount int
	if err := connB.QueryRow(ctx,
		`SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'syzy_coord_gate%' AND NOT tgisinternal`).Scan(&trigCount); err != nil {
		t.Fatalf("trigger count: %v", err)
	}
	if trigCount != 2 {
		t.Fatalf("follower gate triggers = %d, want 2", trigCount)
	}

	// Replicated DML lands on the follower with its gate closed (replica role
	// bypasses the gate trigger).
	css := captureBacklog(t, ctx, a, 2) // insert + non-key update (gated writes never committed)
	applyAll(t, ctx, b, css)
	var email string
	if err := connB.QueryRow(ctx, `SELECT email FROM public.acct WHERE id=1`).Scan(&email); err != nil || email != "x@y" {
		t.Fatalf("follower row: email=%q err=%v; want x@y", email, err)
	}
	// The follower's own gate still blocks local coordinated writes.
	if err := execErr(t, "syzy_coord_b", `INSERT INTO public.acct VALUES (3,'w@y','n')`); !isGateError(err) {
		t.Fatalf("follower gated INSERT: got %v; want 55006", err)
	}
	// And with its gate open, its physical index backstops a duplicate.
	if err := b.SetUniqueGate(ctx, true, time.Minute); err != nil {
		t.Fatalf("open b gate: %v", err)
	}
	if err := execErr(t, "syzy_coord_b", `INSERT INTO public.acct VALUES (4,'x@y','dup')`); err == nil || isGateError(err) {
		t.Fatalf("follower duplicate insert: got %v; want unique violation", err)
	}
}

// TestUniqueLeaseGate: two engines contend for one lease in a file bucket;
// exactly one gate opens, and after the holder stops the other takes over.
func TestUniqueLeaseGate(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	cluster := crdt.ClusterID{0x1e, 0xa5}
	log := schemalog.NewLocal()
	a := openCoordEngine(t, ctx, "syzy_lease_a", 83, cluster, log)
	defer closeEngine(t, ctx, a)
	b := openCoordEngine(t, ctx, "syzy_lease_b", 84, cluster, log)
	defer closeEngine(t, ctx, b)

	be, err := objectstore.Open(ctx, "file://"+t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	ls := unique.OpenLease(be, "unique/lease")

	gateOpen := func(e *Engine, db string) bool {
		conn, err := pgx.Connect(ctx, dbURL(db))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)
		var open bool
		if err := conn.QueryRow(ctx, `SELECT open AND expires_at > now() FROM syzy_unique_gate`).Scan(&open); err != nil {
			t.Fatalf("gate read: %v", err)
		}
		return open
	}

	actx, acancel := context.WithCancel(ctx)
	adone := make(chan struct{})
	go func() { defer close(adone); a.RunUniqueLease(actx, ls, "pg-83", 0) }()

	deadline := time.Now().Add(10 * time.Second)
	for !gateOpen(a, "syzy_lease_a") {
		if time.Now().After(deadline) {
			t.Fatal("A never opened its gate")
		}
		time.Sleep(20 * time.Millisecond)
	}

	bctx, bcancel := context.WithCancel(ctx)
	bdone := make(chan struct{})
	go func() { defer close(bdone); b.RunUniqueLease(bctx, ls, "pg-84", 0) }()
	defer func() { bcancel(); <-bdone }()

	// B must not open while A holds.
	time.Sleep(200 * time.Millisecond)
	if gateOpen(b, "syzy_lease_b") {
		t.Fatal("B opened its gate while A holds the lease")
	}

	// A stops (clean release) → B takes over.
	acancel()
	<-adone
	if gateOpen(a, "syzy_lease_a") {
		t.Fatal("A's gate still open after release")
	}
	deadline = time.Now().Add(30 * time.Second)
	for !gateOpen(b, "syzy_lease_b") {
		if time.Now().After(deadline) {
			t.Fatal("B never took over the lease")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
