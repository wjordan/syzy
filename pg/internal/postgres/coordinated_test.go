package postgres

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/pg/internal/pgtest"
	"github.com/wjordan/syzy/pg/internal/pgwire"
	"github.com/wjordan/syzy/unique"
)

// These tests exercise the coordinated-uniqueness gate end to end against a
// live server: real BEFORE and constraint triggers, a real dblink call out of
// a real commit, and the real wire endpoint. The registry behind it is a
// recording fake, so what is under test is the Postgres-side machinery — that
// the veto lands before commit, that the batch carries the transaction's NET
// effect, and that the failure modes are ones applications can handle.

// recordingRegistry captures reservations and returns a scripted verdict.
type recordingRegistry struct {
	mu      sync.Mutex
	batches [][]unique.Claim
	deny    bool
	unavail bool
}

func (r *recordingRegistry) Reserve(_ context.Context, claims []unique.Claim) (bool, *unique.Claim, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, claims)
	switch {
	case r.unavail:
		return false, nil, unique.ErrUnavailable
	case r.deny:
		c := unique.Claim{}
		if len(claims) > 0 {
			c = claims[0]
		}
		return false, &c, nil
	}
	return true, nil, nil
}

func (r *recordingRegistry) Release(context.Context, []unique.Claim) error { return nil }

func (r *recordingRegistry) seen() [][]unique.Claim {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]unique.Claim(nil), r.batches...)
}

func (r *recordingRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = nil
}

func (r *recordingRegistry) setDeny(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deny = v
}

func (r *recordingRegistry) setUnavail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unavail = v
}

// coordFixture is a live database with one coordinated key installed and the
// reservation endpoint serving.
type coordFixture struct {
	db      string
	app     *pgx.Conn // ordinary writer session — triggers fire
	admin   *pgx.Conn // used for installation and for replica-role writes
	reg     *recordingRegistry
	tableID crdt.TableID
	keyID   crdt.KeyID
	key     []coordCol
	pk      []coordCol
}

const acctSchema = `CREATE TABLE public.acct (
	id    bigint PRIMARY KEY,
	email text NOT NULL,
	note  text
)`

// acctCols describes the coordinated key (email) and the PK (id). The column
// ids are arbitrary but fixed: they are part of the canonical encoding, so
// the test asserts against the same ids it installs.
func acctCols() (key, pk []coordCol) {
	var emailID, idID crdt.ColumnID
	emailID[0], idID[0] = 0xE1, 0x1D
	return []coordCol{{cid: emailID, name: "email", typeName: "text"}},
		[]coordCol{{cid: idID, name: "id", typeName: "bigint"}}
}

// newCoordFixture builds the gate directly rather than through Engine.Open,
// so these tests stay about the SQL and the endpoint rather than about
// replication startup.
func newCoordFixture(t *testing.T, db string) *coordFixture {
	t.Helper()
	requirePG(t)
	sockDir := pgtest.SocketDir(t)
	ctx := context.Background()

	// createTestDB drops and recreates, so it is also the teardown for the
	// previous run — the package's existing convention.
	createTestDB(t, ctx, db, acctSchema)

	key, pk := acctCols()
	f := &coordFixture{db: db, reg: &recordingRegistry{}, key: key, pk: pk}
	for i := range f.tableID {
		f.tableID[i] = byte(i + 1)
	}
	for i := range f.keyID {
		f.keyID[i] = byte(0xA0 + i)
	}

	idx := newCoordIndex()
	idx.tables[f.tableID] = coordTable{
		tid:  f.tableID,
		name: "acct",
		pk:   pk,
		keys: map[crdt.KeyID][]coordCol{f.keyID: key},
	}

	// The socket name is libpq's own form; the port is only part of that
	// name, since nothing listens on TCP. Unique per test so tests sharing
	// the mounted directory cannot collide.
	port := 6000 + int(time.Now().UnixNano()%3000)
	srv, err := pgwire.Listen(pgwire.Config{
		Socket:   filepath.Join(sockDir, fmt.Sprintf(".s.PGSQL.%d", port)),
		Reserver: &coordReserver{idx: idx, reg: f.reg},
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("reservation endpoint: %v", err)
	}
	t.Cleanup(func() { srv.Close() })

	admin, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	f.admin = admin

	conninfo := fmt.Sprintf("host=%s port=%d dbname=%s user=postgres password=syzy connect_timeout=5",
		sockDir, port, pgDB(db))
	if err := ensureCoordSchema(ctx, admin, pgDB(db), conninfo); err != nil {
		t.Fatalf("ensureCoordSchema: %v", err)
	}

	names := make([]string, len(key))
	for i, c := range key {
		names[i] = quoteString(c.name)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(
		"SELECT public.syzy_coord_install(%s, %s, %s, ARRAY[%s]::text[])",
		quoteString(`public."acct"`),
		quoteString(hexID(f.tableID[:])),
		quoteString(hexID(f.keyID[:])),
		strings.Join(names, ", "))); err != nil {
		t.Fatalf("syzy_coord_install: %v", err)
	}

	// Opened after the ALTER DATABASE: a database-level GUC is read when the
	// session starts, so an earlier connection would not see the endpoint.
	app, err := pgx.Connect(ctx, dbURL(db))
	if err != nil {
		t.Fatalf("app connect: %v", err)
	}
	t.Cleanup(func() { app.Close(context.Background()) })
	f.app = app
	return f
}

func hexID(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0xf])
	}
	return string(out)
}

func strptr(s string) *string { return &s }

// canon is the canonical encoding of a tuple, the form a claim must carry.
func canon(t *testing.T, cols []coordCol, vals ...string) []byte {
	t.Helper()
	ptrs := make([]*string, len(vals))
	for i := range vals {
		ptrs[i] = strptr(vals[i])
	}
	b, err := encodeCanonical(cols, ptrs)
	if err != nil {
		t.Fatalf("encodeCanonical: %v", err)
	}
	return b
}

// A granted reservation lets the commit through, and the registry sees the
// claim once for the whole transaction, in canonical bytes.
func TestCoordinated_GrantedCommitSucceeds(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_grant")
	ctx := context.Background()

	if _, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'a@example.com',null)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	batches := f.reg.seen()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("registry saw batches of %v claims, want exactly one batch of one", claimShape(batches))
	}
	c := batches[0][0]
	if c.Table != f.tableID || c.Key != f.keyID {
		t.Errorf("claim ids = %x/%x, want %x/%x", c.Table[:4], c.Key[:4], f.tableID[:4], f.keyID[:4])
	}
	if want := canon(t, f.key, "a@example.com"); string(c.Value) != string(want) {
		t.Errorf("claim value = %x, want canonical %x (raw text would not match capture)", c.Value, want)
	}
	if want := canon(t, f.pk, "1"); string(c.Owner) != string(want) {
		t.Errorf("claim owner = %x, want canonical %x", c.Owner, want)
	}
}

// A denial must abort the commit and surface as an ordinary unique
// violation, so application error handling works unchanged.
func TestCoordinated_DenialAbortsCommitAs23505(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_deny")
	ctx := context.Background()

	f.reg.setDeny(true)
	_, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'taken@example.com',null)`)
	if err == nil {
		t.Fatal("denied reservation still committed; uniqueness is not enforced")
	}
	if code := sqlState(err); code != "23505" {
		t.Fatalf("SQLSTATE = %s (%v), want 23505 — a denial must look like a duplicate key", code, err)
	}
	assertRowCount(t, f.app, 0)
}

// An unreachable registry must also abort — fail closed — but as a retryable
// class, since nothing was reserved and a retry is safe.
func TestCoordinated_UnavailableRegistryFailsClosed(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_unavail")
	ctx := context.Background()

	f.reg.setUnavail(true)
	_, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'x@example.com',null)`)
	if err == nil {
		t.Fatal("write committed while the registry was unavailable; it was never reserved")
	}
	if code := sqlState(err); code != "40001" {
		t.Fatalf("SQLSTATE = %s (%v), want 40001 — retryable, not a permanent duplicate", code, err)
	}
	assertRowCount(t, f.app, 0)
}

// The batch carries the transaction's NET effect: a value inserted and then
// deleted in one transaction was never externally visible, so reserving it
// would hold a value nobody owns.
func TestCoordinated_InsertThenDeleteReservesNothing(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_net")
	ctx := context.Background()

	tx, err := f.app.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.acct VALUES (1,'ghost@example.com',null)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.acct WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for _, b := range f.reg.seen() {
		if len(b) != 0 {
			t.Fatalf("reserved %d claims for a value that never existed outside the transaction", len(b))
		}
	}
}

// Many rows in one transaction cost ONE round trip, not one per row.
func TestCoordinated_BatchesWholeTransaction(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_batch")
	ctx := context.Background()

	tx, err := f.app.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 25; i++ {
		if _, err := tx.Exec(ctx, `INSERT INTO public.acct VALUES ($1,$2,null)`,
			i, fmt.Sprintf("u%d@example.com", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	batches := f.reg.seen()
	if len(batches) != 1 {
		t.Fatalf("registry saw %d batches for one transaction, want 1", len(batches))
	}
	if len(batches[0]) != 25 {
		t.Fatalf("batch carried %d claims, want 25", len(batches[0]))
	}
}

// Rewriting a row's key within one transaction reserves only the value the
// row ends up holding.
func TestCoordinated_OnlyFinalValueReserved(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_final")
	ctx := context.Background()

	tx, err := f.app.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.acct VALUES (1,'first@example.com',null)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.acct SET email='second@example.com' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	batches := f.reg.seen()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("want one batch of one claim, got %v", claimShape(batches))
	}
	if want := canon(t, f.key, "second@example.com"); string(batches[0][0].Value) != string(want) {
		t.Error("reserved an intermediate value; only the committed one is owned")
	}
}

// Moving a value between rows in one transaction is a TRANSFER: the claim
// must name the prior owner, or the registry sees the value as held by the
// row being vacated and refuses it.
func TestCoordinated_PKChangeCarriesPrevOwner(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_transfer")
	ctx := context.Background()

	if _, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'move@example.com',null)`); err != nil {
		t.Fatal(err)
	}
	f.reg.reset()

	if _, err := f.app.Exec(ctx, `UPDATE public.acct SET id = 2 WHERE id = 1`); err != nil {
		t.Fatalf("pk change: %v", err)
	}
	batches := f.reg.seen()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("want one batch of one claim, got %v", claimShape(batches))
	}
	c := batches[0][0]
	if want := canon(t, f.pk, "1"); string(c.Prev) != string(want) {
		t.Fatalf("claim Prev = %x, want the old row's PK %x — without it a transfer reads as a conflict",
			c.Prev, want)
	}
	if want := canon(t, f.pk, "2"); string(c.Owner) != string(want) {
		t.Errorf("claim Owner = %x, want the new row's PK %x", c.Owner, want)
	}
}

// An update that leaves the key value alone must not reserve: otherwise
// every unrelated column write would cost a mesh round trip.
func TestCoordinated_NonKeyUpdateSkipsReservation(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_nonkey")
	ctx := context.Background()

	if _, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'stay@example.com',null)`); err != nil {
		t.Fatal(err)
	}
	f.reg.reset()

	if _, err := f.app.Exec(ctx, `UPDATE public.acct SET note = 'edited' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	for _, b := range f.reg.seen() {
		if len(b) != 0 {
			t.Fatalf("an update that did not touch the key reserved %d claims", len(b))
		}
	}
}

// The apply path runs as session_replication_role = replica, which must
// bypass the gate: a replicated row was already reserved by the node that
// originated it, and re-reserving would make the cluster contend with itself.
func TestCoordinated_ReplicaRoleBypassesGate(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_replica")
	ctx := context.Background()

	f.reg.setDeny(true) // would abort any gated write
	if _, err := f.admin.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.admin.Exec(ctx, `INSERT INTO public.acct VALUES (9,'applied@example.com',null)`); err != nil {
		t.Fatalf("apply-path write was gated: %v", err)
	}
	for _, b := range f.reg.seen() {
		if len(b) != 0 {
			t.Fatalf("apply-path write consulted the registry (%d claims); replicated rows must not", len(b))
		}
	}
	assertRowCount(t, f.app, 1)
}

// The accumulator is per-transaction scratch: an aborted transaction, a
// committed one, and a delete must all leave it empty, or it grows without
// bound and eventually reserves values for transactions that never committed.
func TestCoordinated_AccumulatorDrains(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_drain")
	ctx := context.Background()

	f.reg.setDeny(true)
	_, _ = f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'denied@example.com',null)`)
	f.reg.setDeny(false)
	if _, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (2,'ok@example.com',null)`); err != nil {
		t.Fatal(err)
	}
	// A delete accumulates a vacancy row; it must be consumed too.
	if _, err := f.app.Exec(ctx, `DELETE FROM public.acct WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := f.app.QueryRow(ctx, `SELECT count(*) FROM public.syzy_coord_pending`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d accumulator rows left behind; the scratch table leaks", n)
	}
}

// The endpoint must refuse a key its catalog does not know rather than
// letting the write through: an unknown key means this node's schema is
// behind, and granting would be granting blind.
func TestCoordinated_UnknownKeyIsRefused(t *testing.T) {
	f := newCoordFixture(t, "syzy_coord_unknown")
	ctx := context.Background()

	// Reinstall the trigger under a key id the endpoint's index lacks.
	var bogus crdt.KeyID
	bogus[0] = 0xFF
	if _, err := f.admin.Exec(ctx, fmt.Sprintf(
		"SELECT public.syzy_coord_install(%s, %s, %s, ARRAY[%s]::text[])",
		quoteString(`public."acct"`), quoteString(hexID(f.tableID[:])),
		quoteString(hexID(bogus[:])), quoteString("email"))); err != nil {
		t.Fatal(err)
	}
	_, err := f.app.Exec(ctx, `INSERT INTO public.acct VALUES (1,'unknown@example.com',null)`)
	if err == nil {
		t.Fatal("write committed against a key the node could not resolve")
	}
	if code := sqlState(err); code != "40001" {
		t.Fatalf("SQLSTATE = %s (%v), want 40001 — an unresolvable key is retryable, not a duplicate", code, err)
	}
}

func assertRowCount(t *testing.T, conn *pgx.Conn, want int) {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM public.acct`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != want {
		t.Fatalf("table has %d rows, want %d", n, want)
	}
}

func claimShape(batches [][]unique.Claim) []int {
	out := make([]int, len(batches))
	for i, b := range batches {
		out[i] = len(b)
	}
	return out
}

// sqlState returns the SQLSTATE of a Postgres error, or "" if err is not one.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
