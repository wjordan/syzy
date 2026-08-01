package postgres

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/schemalog"
)

var wideningPairs = [][2]string{
	{"smallint", "integer"},
	{"smallint", "bigint"},
	{"integer", "bigint"},
	{"real", "double precision"},
	{"character varying(10)", "character varying(20)"},
	{"character varying(10)", "character varying"},
	{"character varying(10)", "text"},
	{"numeric(10,2)", "numeric(12,2)"},
	{"numeric(10,2)", "numeric"},
}

var narrowingPairs = [][2]string{
	{"bigint", "integer"},
	{"integer", "smallint"},
	{"double precision", "real"},
	{"text", "character varying(20)"},
	{"character varying(20)", "character varying(10)"},
	{"numeric(12,2)", "numeric(10,2)"},
	{"numeric(10,2)", "numeric(12,3)"}, // scale change reinterprets the value
	{"numeric", "numeric(10,2)"},
	{"bigint", "text"},   // representable, but the text encoding changes
	{"text", "jsonb"},    // a cast that can fail per row
	{"integer", "money"}, // unrecognized: never assumed safe
	{"character(10)", "text"},
}

// TestTypeWidens pins the only ALTER COLUMN TYPE conversions that replicate:
// those where every value of the old type is a value of the new one.
func TestTypeWidens(t *testing.T) {
	for _, p := range wideningPairs {
		if !typeWidens(p[0], p[1]) {
			t.Errorf("typeWidens(%q, %q) = false, want true", p[0], p[1])
		}
	}
	for _, p := range narrowingPairs {
		if typeWidens(p[0], p[1]) {
			t.Errorf("typeWidens(%q, %q) = true, want false", p[0], p[1])
		}
	}
}

// TestBuildCatalogOpAlterColumn: the relaxing attribute changes each become one
// OpAlterColumn carrying the column's whole desired state.
func TestBuildCatalogOpAlterColumn(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_altcol", 90, crdt.ClusterID{0xe9})
	defer closeEngine(t, ctx, e)

	var ops []crdt.CatalogOp
	var buildErr error
	catalogOpCollector(t, ctx, e, "syzy_ddl_altcol", &ops, &buildErr)

	appExec(t, "syzy_ddl_altcol", `CREATE TABLE public.c (id bigint PRIMARY KEY, n integer NOT NULL, s varchar(10))`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_altcol", `ALTER TABLE public.c ALTER COLUMN n TYPE bigint`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_altcol", `ALTER TABLE public.c ALTER COLUMN n DROP NOT NULL`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_altcol", `ALTER TABLE public.c ALTER COLUMN s SET DEFAULT 'hi'`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
	appExec(t, "syzy_ddl_altcol", `ALTER TABLE public.c ALTER COLUMN s DROP DEFAULT`)
	_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

	if buildErr != nil {
		t.Fatalf("unexpected build error: %v", buildErr)
	}
	if len(ops) != 5 {
		t.Fatalf("built %d ops, want 5 (create + 4 alters): %+v", len(ops), ops)
	}
	var nID, sID crdt.ColumnID
	for _, c := range ops[0].Columns {
		switch c.Name {
		case "n":
			nID = c.ID
		case "s":
			sID = c.ID
		}
	}
	for i, want := range []crdt.CatalogColumn{
		{ID: nID, Name: "n", Type: "bigint", NotNull: true},
		{ID: nID, Name: "n", Type: "bigint"},
		{ID: sID, Name: "s", Type: "character varying(10)", Default: "'hi'::character varying"},
		{ID: sID, Name: "s", Type: "character varying(10)"},
	} {
		op := ops[i+1]
		if op.Kind != crdt.OpAlterColumn || op.TableID != ops[0].TableID || len(op.Columns) != 1 {
			t.Fatalf("op %d = %+v, want one AlterColumn on the created table", i+1, op)
		}
		got := op.Columns[0]
		if got.ID != want.ID || got.Name != want.Name || got.Type != want.Type ||
			got.NotNull != want.NotNull || got.Default != want.Default {
			t.Errorf("alter %d column = %+v, want %+v", i+1, got, want)
		}
	}
}

// TestClassifyColumnChange is the post-commit floor: whatever the admission
// gate lets through, capture re-judges before it can reach the schema log.
func TestClassifyColumnChange(t *testing.T) {
	base := func() *colInfo {
		return &colInfo{name: "c", typeName: "integer", def: "", notNull: false}
	}
	live := func(mut func(*pgColumn)) pgColumn {
		pc := pgColumn{name: "c", typeName: "integer", attnum: 2}
		mut(&pc)
		return pc
	}
	for _, tc := range []struct {
		name    string
		pc      pgColumn
		changed bool
		wantErr string
	}{
		{"no-change", live(func(*pgColumn) {}), false, ""},
		{"widen", live(func(p *pgColumn) { p.typeName = "bigint" }), true, ""},
		{"set-default", live(func(p *pgColumn) { p.def = "0" }), true, ""},
		{"drop-not-null", live(func(p *pgColumn) { p.notNull = false }), false, ""},
		{"narrow", live(func(p *pgColumn) { p.typeName = "smallint" }), false, "not a widening conversion"},
		{"set-not-null", live(func(p *pgColumn) { p.notNull = true }), false, "SET NOT NULL"},
		{"generated", live(func(p *pgColumn) { p.generated = true }), false, "GENERATED expression"},
		{"identity", live(func(p *pgColumn) { p.identity = 'd' }), false, "IDENTITY"},
		{"pk", live(func(p *pgColumn) { p.pkpos = 1 }), false, "PRIMARY KEY membership"},
		{"nextval", live(func(p *pgColumn) { p.def = "nextval('s'::regclass)" }), false, "node-local sequence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, err := classifyColumnChange("t", base(), tc.pc)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("classify = %v, want accepted", err)
			case tc.wantErr == "" && changed != tc.changed:
				t.Errorf("changed = %v, want %v", changed, tc.changed)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("classify accepted the change, want rejection mentioning %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("error = %v, want it to mention %q", err, tc.wantErr)
			case tc.wantErr != "" && !errors.Is(err, errUnsupportedDDL):
				t.Errorf("error %v is not errUnsupportedDDL", err)
			}
		})
	}
}

// TestAdmissionRejectsRestrictingAlter: a change that RESTRICTS a column is
// rejected PRE-COMMIT by the ddl_command_start snapshot + ddl_command_end
// admission pair, so the user's migration rolls back with a clear error and the
// node stays healthy — instead of committing locally and halting capture.
func TestAdmissionRejectsRestrictingAlter(t *testing.T) {
	requirePG(t)
	for _, tc := range []struct {
		name  string
		setup []string // run with the internal guard, so it spools no intent
		alter string
		want  string
	}{
		{"narrow-type", nil, `ALTER TABLE public.r ALTER COLUMN n TYPE integer`, "not a widening conversion"},
		{"set-not-null", nil, `ALTER TABLE public.r ALTER COLUMN s SET NOT NULL`, "SET NOT NULL"},
		{"nextval-default", []string{`CREATE SEQUENCE public.r_seq`},
			`ALTER TABLE public.r ALTER COLUMN n SET DEFAULT nextval('public.r_seq')`, "node-local sequence"},
		{"drop-expression", nil, `ALTER TABLE public.r ALTER COLUMN g DROP EXPRESSION`, "GENERATED expression"},
		{"drop-pk", nil, `ALTER TABLE public.r DROP CONSTRAINT r_pkey`, "PRIMARY KEY membership"},
		// ADD COLUMN backfills existing rows by evaluating the default locally,
		// so a volatile one (and a merely stable one, like now()) gives every
		// node different values for the rows it already has.
		{"volatile-add-default", nil, `ALTER TABLE public.r ADD COLUMN u uuid DEFAULT gen_random_uuid()`, "is not immutable"},
		{"stable-add-default", nil, `ALTER TABLE public.r ADD COLUMN at timestamptz DEFAULT now()`, "is not immutable"},
		// A constraint that restricts which rows the table accepts, added while
		// peers are writing rows that are legal under their own shape. It does not
		// replicate but IS enforced on apply, so those rows would fail here on
		// every redelivery.
		{"add-check", nil, `ALTER TABLE public.r ADD CONSTRAINT r_pos CHECK (n > 0)`, "adding constraint(s)"},
		{"add-exclude", nil, `ALTER TABLE public.r ADD CONSTRAINT r_ex EXCLUDE USING btree (s WITH =)`, "adding constraint(s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := "syzy_ddl_rej_" + strings.ReplaceAll(tc.name, "-", "")
			e := openDDLEngine(t, ctx, db, 91, crdt.ClusterID{0xea})
			defer closeEngine(t, ctx, e)

			var ops []crdt.CatalogOp
			var buildErr error
			catalogOpCollector(t, ctx, e, db, &ops, &buildErr)

			if len(tc.setup) > 0 {
				appTxn(t, db, append([]string{`SET syzy.internal = 'on'`}, tc.setup...)...)
			}
			appExec(t, db, `CREATE TABLE public.r (id bigint PRIMARY KEY, n bigint, s text, g bigint GENERATED ALWAYS AS (id * 2) STORED)`)
			_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

			err := appExecErr(t, db, tc.alter)
			if err == nil {
				t.Fatalf("%s committed, want a pre-commit rejection", tc.alter)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "0A000" {
				t.Fatalf("error = %v, want SQLSTATE 0A000 (feature_not_supported)", err)
			}
			if !strings.Contains(pgErr.Message, tc.want) {
				t.Errorf("message = %q, want it to mention %q", pgErr.Message, tc.want)
			}
			// Nothing committed, so nothing to capture and no reason to halt.
			_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
			if buildErr != nil {
				t.Errorf("node went unhealthy after a rejected ALTER: %v (ops=%+v)", buildErr, ops)
			}
		})
	}
}

// TestAdmissionRejectsDivergentAlter covers the changes that commit cleanly on
// the originator and then either brick every follower's apply or silently
// diverge it — the failure modes the column-attribute diff alone cannot see.
func TestAdmissionRejectsDivergentAlter(t *testing.T) {
	requirePG(t)
	for _, tc := range []struct {
		name   string
		create string
		setup  []string // extra statements run before the ALTER, gate included
		alter  string
		want   string
	}{
		// An op for a serial/identity column ships the CREATE-shaped type
		// ("bigserial", "… GENERATED … AS IDENTITY"); neither is spellable in
		// ALTER COLUMN … TYPE, so a follower would fail the statement forever.
		{"widen-serial", `CREATE TABLE public.d (id serial PRIMARY KEY, v text)`, nil,
			`ALTER TABLE public.d ALTER COLUMN id TYPE bigint`, "auto-increment column"},
		{"widen-identity", `CREATE TABLE public.d (id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text)`, nil,
			`ALTER TABLE public.d ALTER COLUMN id TYPE bigint`, "auto-increment column"},
		// USING rewrites the originator's rows; the op carries only the target
		// type, so a follower keeps its own values.
		{"type-using", `CREATE TABLE public.d (id bigint PRIMARY KEY, n integer)`, nil,
			`ALTER TABLE public.d ALTER COLUMN n TYPE bigint USING n + 1000`, "USING"},
		// An ALWAYS/REPLICA trigger also fires on applied peer writes.
		{"always-trigger", `CREATE TABLE public.d (id bigint PRIMARY KEY, n integer)`,
			[]string{
				`CREATE FUNCTION public.d_tf() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN RETURN NEW; END $f$`,
				`CREATE TRIGGER d_tg BEFORE INSERT ON public.d FOR EACH ROW EXECUTE FUNCTION public.d_tf()`,
			},
			`ALTER TABLE public.d ENABLE ALWAYS TRIGGER d_tg`, "ENABLE ALWAYS/REPLICA"},
		// Leaving replication scope writes no intent row, so without this the
		// table would just stop replicating with no error anywhere.
		{"set-unlogged", `CREATE TABLE public.d (id bigint PRIMARY KEY, n integer)`, nil,
			`ALTER TABLE public.d SET UNLOGGED`, "out of scope"},
		{"set-schema", `CREATE TABLE public.d (id bigint PRIMARY KEY, n integer)`,
			[]string{`CREATE SCHEMA elsewhere`},
			`ALTER TABLE public.d SET SCHEMA elsewhere`, "out of scope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := "syzy_ddl_div_" + strings.ReplaceAll(tc.name, "-", "")
			e := openDDLEngine(t, ctx, db, 95, crdt.ClusterID{0xee})
			defer closeEngine(t, ctx, e)

			var ops []crdt.CatalogOp
			var buildErr error
			catalogOpCollector(t, ctx, e, db, &ops, &buildErr)

			appExec(t, db, tc.create)
			for _, s := range tc.setup {
				appExec(t, db, s)
			}
			_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)

			err := appExecErr(t, db, tc.alter)
			if err == nil {
				t.Fatalf("%s committed, want a pre-commit rejection", tc.alter)
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "0A000" {
				t.Fatalf("error = %v, want SQLSTATE 0A000 (feature_not_supported)", err)
			}
			if !strings.Contains(pgErr.Message, tc.want) {
				t.Errorf("message = %q, want it to mention %q", pgErr.Message, tc.want)
			}
			_ = captureAllWithin(t, ctx, e, 800*time.Millisecond)
			if buildErr != nil {
				t.Errorf("node went unhealthy after a rejected ALTER: %v (ops=%+v)", buildErr, ops)
			}
		})
	}
}

// TestAdmissionAdmitsRelaxingAlter: the admission gate is not a blanket ban on
// ALTER — the relaxations still commit and replicate.
func TestAdmissionAdmitsRelaxingAlter(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	e := openDDLEngine(t, ctx, "syzy_ddl_admit_ok", 94, crdt.ClusterID{0xed})
	defer closeEngine(t, ctx, e)

	appExec(t, "syzy_ddl_admit_ok", `CREATE TABLE public.ok (id bigint PRIMARY KEY, n integer NOT NULL, s varchar(10))`)
	// A serial PK: every later ALTER on its table must still be admitted — the
	// column's standing nextval default is an admitted serial, not a new one.
	appExec(t, "syzy_ddl_admit_ok", `CREATE TABLE public.auto (id bigserial PRIMARY KEY, s text)`)
	// A CHECK that predates this install — created with the gate bypassed, the
	// way a table already carrying one would look to a node that installed DDL
	// support after the fact. The gate refuses ADDING a constraint, not carrying
	// one, so this table's other ALTERs must still be admitted.
	appTxn(t, "syzy_ddl_admit_ok", `SET syzy.internal = 'on'`,
		`CREATE TABLE public.legacy (id bigint PRIMARY KEY, n int CHECK (n > 0))`)
	for _, sql := range []string{
		`ALTER TABLE public.ok ALTER COLUMN n TYPE bigint`,
		`ALTER TABLE public.ok ALTER COLUMN n DROP NOT NULL`,
		`ALTER TABLE public.ok ALTER COLUMN s TYPE text`,
		`ALTER TABLE public.ok ALTER COLUMN s SET DEFAULT 'hi'`,
		`ALTER TABLE public.ok ADD COLUMN extra text`,
		`ALTER TABLE public.ok RENAME COLUMN s TO s2`,
		`ALTER TABLE public.ok DROP COLUMN extra`,
		// attnum-keyed, so a re-add under the same name is a drop plus an add,
		// not a (rejected) same-name type change.
		`ALTER TABLE public.ok ADD COLUMN again text`,
		`ALTER TABLE public.ok DROP COLUMN again, ADD COLUMN again bigint`,
		`ALTER TABLE public.auto ADD COLUMN note text`,
		// A foreign key is admitted: apply does not enforce it (replica role
		// disables its triggers), so it quarantines nothing and cannot diverge the
		// cluster — it is a documented limitation, not a refusal. The inline
		// REFERENCES also makes Postgres report the CREATE as an extra ALTER TABLE
		// event, on a table with no pre-command snapshot.
		`CREATE TABLE public.fk (id bigserial PRIMARY KEY, ok_id bigint REFERENCES public.ok(id))`,
		`ALTER TABLE public.fk ADD CONSTRAINT fk_ok2 FOREIGN KEY (ok_id) REFERENCES public.ok(id)`,
		`ALTER TABLE public.legacy ADD COLUMN note text`,
		// Constraints are snapshotted by oid, which a rename preserves — by name
		// this would read as dropping one and adding another.
		`ALTER TABLE public.legacy RENAME CONSTRAINT legacy_n_check TO legacy_pos`,
		`ALTER TABLE public.auto ALTER COLUMN s SET DEFAULT 'x'`,
		`ALTER TABLE public.auto RENAME COLUMN id TO pk`,
	} {
		if err := appExecErr(t, "syzy_ddl_admit_ok", sql); err != nil {
			t.Errorf("%s was rejected: %v", sql, err)
		}
	}
}

// TestTypeWidensMatchesSQL holds the two implementations of the widening rule —
// typeWidens() and syzy_type_widens() — to the same verdicts.
func TestTypeWidensMatchesSQL(t *testing.T) {
	requirePG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db := "syzy_ddl_widen"
	createTestDB(t, ctx, db, schemaKV)
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	if err := installDDLSupport(ctx, c); err != nil {
		t.Fatalf("install ddl support: %v", err)
	}
	for _, p := range append(append([][2]string{}, wideningPairs...), narrowingPairs...) {
		var got bool
		if err := c.QueryRow(ctx, `SELECT public.syzy_type_widens($1, $2)`, p[0], p[1]).Scan(&got); err != nil {
			t.Fatalf("syzy_type_widens(%q, %q): %v", p[0], p[1], err)
		}
		if want := typeWidens(p[0], p[1]); got != want {
			t.Errorf("syzy_type_widens(%q, %q) = %v, Go says %v", p[0], p[1], got, want)
		}
	}
}

func appExecErr(t *testing.T, db, sql string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("app connect: %v", err)
	}
	defer c.Close(ctx)
	_, err = c.Exec(ctx, sql)
	return err
}

// TestLiveAlterColumnConvergence: A widens a column and changes its default; B
// reaches the same physical shape by applying the op, and rows written on both
// sides — before and after — converge.
func TestLiveAlterColumnConvergence(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	log := schemalog.NewLocal()
	cluster := crdt.ClusterID{0xeb}
	a := openDDLEngineLog(t, ctx, "syzy_lalt_a", 92, cluster, 1, log)
	defer closeEngine(t, ctx, a)
	b := openDDLEngineLog(t, ctx, "syzy_lalt_b", 93, cluster, 2, log)
	defer closeEngine(t, ctx, b)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	aInbox := make(chan *crdt.Changeset, 256)
	bInbox := make(chan *crdt.Changeset, 256)
	run := func(node *Engine, inbox <-chan *crdt.Changeset, peer chan<- *crdt.Changeset) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
				select {
				case peer <- cs:
				case <-ctx.Done():
				}
				return nil
			}
			if err := node.Run(runCtx, inbox, broadcast); err != nil && runCtx.Err() == nil {
				t.Errorf("%s orchestrator: %v", node.cfg.Name, err)
			}
		}()
	}
	run(a, aInbox, bInbox)
	run(b, bInbox, aInbox)
	defer func() { cancel(); wg.Wait() }()

	appExec(t, "syzy_lalt_a", `CREATE TABLE public.evt (id bigint PRIMARY KEY, n integer NOT NULL, msg text)`)
	appExec(t, "syzy_lalt_a", `INSERT INTO public.evt VALUES (1,1,'a')`)
	if !waitRows(t, "syzy_lalt_b", "public.evt", 1, 20*time.Second) {
		t.Fatal("B never saw the created table")
	}

	appExec(t, "syzy_lalt_a", `ALTER TABLE public.evt ALTER COLUMN n TYPE bigint, ALTER COLUMN n DROP NOT NULL`)
	// A value only a bigint can hold, and a NULL only the relaxed column
	// accepts: B must have applied the ALTER before it applies these rows.
	appExec(t, "syzy_lalt_a", `INSERT INTO public.evt VALUES (2, 4294967296, 'b'), (3, NULL, 'c')`)

	deadline := time.Now().Add(20 * time.Second)
	var got map[int64]string
	want := map[int64]string{1: "a", 2: "b", 3: "c"}
	for time.Now().Before(deadline) {
		if got = idMsgRows(t, "syzy_lalt_b", "public.evt"); mapsEqual(got, want) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !mapsEqual(got, want) {
		t.Fatalf("B did not converge: got %v want %v", got, want)
	}
	if typ := columnType(t, "syzy_lalt_b", "evt", "n"); typ != "bigint" {
		t.Errorf("B's evt.n type = %q, want bigint", typ)
	}
	if notNull := columnNotNull(t, "syzy_lalt_b", "evt", "n"); notNull {
		t.Errorf("B's evt.n is still NOT NULL")
	}
	// The relaxed column now accepts a local NULL on B too, and that row
	// travels back to A under the same schema.
	appExec(t, "syzy_lalt_b", `INSERT INTO public.evt VALUES (4, NULL, 'd')`)
	want[4] = "d"
	for deadline = time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		if got = idMsgRows(t, "syzy_lalt_a", "public.evt"); mapsEqual(got, want) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !mapsEqual(got, want) {
		t.Fatalf("A did not converge: got %v want %v", got, want)
	}
}

func columnType(t *testing.T, db, table, col string) string {
	t.Helper()
	return columnAttr[string](t, db, table, col, `format_type(a.atttypid, a.atttypmod)`)
}

func columnNotNull(t *testing.T, db, table, col string) bool {
	t.Helper()
	return columnAttr[bool](t, db, table, col, `a.attnotnull`)
}

func columnAttr[T any](t *testing.T, db, table, col, expr string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)
	var out T
	if err := c.QueryRow(ctx, `
		SELECT `+expr+` FROM pg_attribute a
		WHERE a.attrelid = format('public.%I', $1::text)::regclass AND a.attname = $2`,
		table, col).Scan(&out); err != nil {
		t.Fatalf("read %s.%s attribute: %v", table, col, err)
	}
	return out
}

// TestSchemaRestorePropagatesCrashWindowAlterColumn: the sibling of
// TestSchemaRestoreToleratesCrashMidDDL for column ATTRIBUTES. An ALTER COLUMN
// that commits in Postgres but crashes before its schema event is persisted is
// recovered by diffing the live catalog against the cached one — so the cache
// must hold the last SHIPPED attributes, not the live ones. Priming them from
// the live relation would make the diff empty and the widening would silently
// never replicate, leaving peers with a column too narrow for the values that
// eventually arrive.
func TestSchemaRestorePropagatesCrashWindowAlterColumn(t *testing.T) {
	requirePG(t)
	ctx := context.Background()
	const (
		db     = "syzy_crashaltercol"
		origin = crdt.Origin(101)
	)
	cluster := crdt.ClusterID{0xe7}
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	log := schemalog.NewLocal()

	meta1, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta1: %v", err)
	}
	e1 := openDDLEngineLogMeta(t, ctx, db, origin, cluster, 0, log, meta1)
	appExec(t, db, `CREATE TABLE public.gizmo (id bigint PRIMARY KEY, qty integer NOT NULL)`)
	_ = captureAllWithin(t, ctx, e1, 800*time.Millisecond)
	var gizmoTID crdt.TableID
	for id, ti := range e1.cat.byID {
		if ti.name == "gizmo" {
			gizmoTID = id
		}
	}
	if gizmoTID == (crdt.TableID{}) {
		t.Fatal("gizmo not cataloged after create")
	}
	head := e1.schemaSeq.Load()
	_ = e1.Close()
	if err := meta1.Close(); err != nil {
		t.Fatalf("meta1 close: %v", err)
	}
	waitSlotInactive(t, ctx, dbURL(db), "syzy_slot_"+db)

	// The crash window: both attribute changes commit with the sidecar down.
	appExec(t, db, `ALTER TABLE public.gizmo ALTER COLUMN qty TYPE bigint`)
	appExec(t, db, `ALTER TABLE public.gizmo ALTER COLUMN qty DROP NOT NULL`)

	meta2, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("meta2: %v", err)
	}
	defer meta2.Close()
	e2, err := Open(ctx, Config{
		Name: db, Origin: origin, Cluster: cluster,
		Cache:       nodestate.New(origin),
		ConnURL:     dbURL(db),
		ReplConnURL: replURL(db),
		Publication: "syzy_pub",
		Slot:        "syzy_slot_" + db,
		OriginName:  "syzy_origin_" + db,
		Tables:      []string{"public.kv"},
		DDL:         true,
		SchemaLog:   log,
		Meta:        meta2,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeEngine(t, ctx, e2)

	qty := func() *colInfo {
		ti := e2.cat.byID[gizmoTID]
		if ti == nil {
			t.Fatal("gizmo lost on restart")
		}
		return ti.byName["qty"]
	}
	// Post-Open the cache holds the RECORDED attributes, so the pending change is
	// still a diff.
	if c := qty(); c.typeName != "integer" || !c.notNull {
		t.Fatalf("post-Open qty = %s notNull=%v, want the recorded integer NOT NULL", c.typeName, c.notNull)
	}
	// Recovery re-derives both changes from the un-pruned intent rows.
	_ = captureAllWithin(t, ctx, e2, 800*time.Millisecond)
	if got := e2.schemaSeq.Load(); got <= head {
		t.Fatalf("post-recovery schemaSeq = %d, want > %d (the crash-window ALTERs must propagate)", got, head)
	}
	if c := qty(); c.typeName != "bigint" || c.notNull {
		t.Fatalf("post-recovery qty = %s notNull=%v, want bigint nullable", c.typeName, c.notNull)
	}
}
