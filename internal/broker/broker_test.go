package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport"
)

// applierFixture is a broker-side stand-in for unit tests: app.db with
// no producer hooks installed, metadata seeded with cluster/node
// identity, catalog built off the schema, and a fresh Cache so the
// broker runs on the production apply path. MirrorJournals is left nil
// — recovery-replay scenarios live in the testcluster integration
// tests.
type applierFixture struct {
	app   *sqlitebridge.Conn
	sc    *metadata.Store
	cat   *catalog.Catalog
	cache *nodestate.Cache
	br    *Broker
	tab   *catalog.Table
}

// applierOpt mutates the broker.Config newApplier passes to broker.New.
// Tests that need to wire optional dependencies (GapFiller, TipSource,
// SchemaLog, etc.) supply one of these; default callers pass none.
type applierOpt func(*Config)

func withGapFiller(g transport.GapFiller) applierOpt {
	return func(c *Config) { c.GapFiller = g }
}

func withTipSource(ts transport.TipSource) applierOpt {
	return func(c *Config) { c.TipSource = ts }
}

func newApplier(t testing.TB, origin crdt.Origin, tx transport.Transport, opts ...applierOpt) *applierFixture {
	return newApplierSchema(t, origin, tx,
		`CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`, opts...)
}

func newApplierSchema(t testing.TB, origin crdt.Origin, tx transport.Transport, schema string, opts ...applierOpt) *applierFixture {
	t.Helper()
	dir := t.TempDir()

	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; ` + schema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	if err := sc.SetClusterID(testCluster); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(origin); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("catalog.SeedFromSchema: %v", err)
	}
	cache := nodestate.New(origin)
	cfg := Config{
		AppApply:  app,
		Meta:      sc,
		Catalog:   cat,
		Transport: tx,
		Cache:     cache,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	br, err := New(cfg)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	tab, _ := cat.Table("event")
	return &applierFixture{app: app, sc: sc, cat: cat, cache: cache, br: br, tab: tab}
}

var testCluster = crdt.ClusterID{0xCC, 0xCC, 0xCC, 0xCC}

func intCol(id crdt.ColumnID, v int64) crdt.ColValue {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return crdt.ColValue{Column: id, TypeTag: crdt.ColInt, Bytes: b[:]}
}

func textCol(id crdt.ColumnID, s string) crdt.ColValue {
	return crdt.ColValue{Column: id, TypeTag: crdt.ColText, Bytes: []byte(s)}
}

func blobCol(id crdt.ColumnID, b []byte) crdt.ColValue {
	return crdt.ColValue{Column: id, TypeTag: crdt.ColBlob, Bytes: b}
}

func buildInsert(t testing.TB, tab *catalog.Table, dot crdt.Dot, stamp crdt.Stamp, cl uint64, idVal []byte, name string) *crdt.Changeset {
	t.Helper()
	idCol := tab.PK[0].ID
	pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{
		idCol: blobCol(idCol, idVal),
	})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	nCol, _ := tab.Column("n")
	rec := crdt.Insert{
		Table: tab.ID, PK: pk, CL: cl,
		Image: []crdt.ColValue{textCol(nCol.ID, name)},
	}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	return cs
}

func TestApplyInsertWritesAppRowAndCache(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)

	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp, 1, []byte{0x01}, "hello")

	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("applyPayload: %v", err)
	}

	if got := readNCol(t, f.app, []byte{0x01}); got != "hello" {
		t.Errorf("event.n = %q; want hello", got)
	}
	if !f.cache.IsAppliedRemote(src, 1) {
		t.Errorf("cache: src/1 not marked applied")
	}
	front, ok := f.cache.FrontierFor(src)
	if !ok || front.LastSeq != 1 {
		t.Errorf("FrontierFor(%d) = %+v ok=%v; want LastSeq=1", src, front, ok)
	}
	rs := f.cache.RowState(f.tab.ID, cs.Records[0].(crdt.Insert).PK)
	if rs.CL != 1 {
		t.Errorf("row_clock CL = %d; want 1", rs.CL)
	}
	if rs.Base != stamp {
		t.Errorf("row_clock Base = %v; want %v", rs.Base, stamp)
	}
}

func TestApplyIdempotentReDelivery(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)

	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp, 1, []byte{0x01}, "hello")
	payload := cs.Encoded()

	if err := f.br.applyPayload(context.Background(), payload); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), payload); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	if !f.cache.IsAppliedRemote(src, 1) {
		t.Fatalf("cache: src/1 not applied after first apply")
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "hello" {
		t.Errorf("event.n = %q; want hello", got)
	}
}

func TestApplyClusterMismatchRejected(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)

	wrong := crdt.ClusterID{0xAA}
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}

	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})
	nCol, _ := f.tab.Column("n")
	rec := crdt.Insert{Table: f.tab.ID, PK: pk, CL: 1, Image: []crdt.ColValue{textCol(nCol.ID, "x")}}
	cs, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stamp, nil, wrong, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err == nil {
		t.Fatalf("applyPayload accepted wrong cluster_id")
	}
	if f.cache.IsAppliedRemote(src, 1) {
		t.Errorf("cache marks src/1 applied after rejected apply")
	}
}

func TestApplyLWWHigherCLWins(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)

	src := crdt.Origin(7)

	// First: INSERT at CL=1.
	stamp1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs1 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp1, 1, []byte{0x01}, "first")
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply1: %v", err)
	}

	// Concurrent: another INSERT on the same PK at CL=1 from a different
	// origin with an earlier wall time. Lower (CL, Stamp) loses; row_clock
	// stays at the first stamp.
	other := crdt.Origin(3)
	stamp2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 500}, Origin: other}
	cs2 := buildInsert(t, f.tab, crdt.Dot{Origin: other, Seq: 1}, stamp2, 1, []byte{0x01}, "loser")
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply2: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "first" {
		t.Errorf("after losing apply, n = %q; want first", got)
	}
	rs := f.cache.RowState(f.tab.ID, cs1.Records[0].(crdt.Insert).PK)
	if rs.Base != stamp1 {
		t.Errorf("row_clock base = %v; want %v (loser must not overwrite)", rs.Base, stamp1)
	}

	// Now a tombstone (CL=2) wins on parity even though its stamp is older.
	stamp3 := crdt.Stamp{Clock: crdt.Clock{WallTime: 200}, Origin: other}
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})
	del := crdt.Delete{Table: f.tab.ID, PK: pk, CL: 2}
	cs3, err := crdt.Build(crdt.Dot{Origin: other, Seq: 2}, stamp3, nil, testCluster, []crdt.Record{del})
	if err != nil {
		t.Fatalf("Build delete: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs3.Encoded()); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if rowExists(t, f.app, []byte{0x01}) {
		t.Errorf("row still present after winning DELETE")
	}
	rs = f.cache.RowState(f.tab.ID, pk)
	if rs.CL != 2 {
		t.Errorf("post-DELETE CL = %d; want 2", rs.CL)
	}
}

func TestApplyUpdate(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)
	src := crdt.Origin(7)

	stamp1 := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs1 := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp1, 1, []byte{0x01}, "first")
	if err := f.br.applyPayload(context.Background(), cs1.Encoded()); err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	// UPDATE: CL stays at 1 (UPDATE preserves liveness), stamp advances.
	stamp2 := crdt.Stamp{Clock: crdt.Clock{WallTime: 2000}, Origin: src}
	nCol, _ := f.tab.Column("n")
	idCol := f.tab.PK[0].ID
	pk, _ := f.tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, []byte{0x01})})
	upd := crdt.Update{Table: f.tab.ID, PK: pk, CL: 1, Changed: []crdt.ColValue{textCol(nCol.ID, "second")}}
	cs2, err := crdt.Build(crdt.Dot{Origin: src, Seq: 2}, stamp2, nil, testCluster, []crdt.Record{upd})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs2.Encoded()); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	if got := readNCol(t, f.app, []byte{0x01}); got != "second" {
		t.Errorf("n = %q; want second", got)
	}
}

func TestApplyUnknownTableSchemaGated(t *testing.T) {
	t.Parallel()
	f := newApplier(t, 1, nil)

	// A TableID for a table that doesn't exist locally.
	bogus := crdt.TableID{0xDE, 0xAD}
	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	rec := crdt.Insert{Table: bogus, PK: crdt.PKBlob{0x00}, CL: 1, Image: nil}
	cs, err := crdt.Build(crdt.Dot{Origin: src, Seq: 1}, stamp, nil, testCluster, []crdt.Record{rec})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); !errors.Is(err, errSchemaBehind) {
		t.Fatalf("apply err = %v; want errSchemaBehind", err)
	}
	if f.cache.IsAppliedRemote(src, 1) {
		t.Errorf("cache: src/1 applied despite missing catalog table")
	}
}

func TestSubscribeLoopRetriesAfterApplyError(t *testing.T) {
	t.Parallel()
	tr := &retrySubscribeTransport{ready: make(chan struct{})}
	f := newApplier(t, 1, tr)
	f.br.retryBackoff = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.br.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer f.br.Close()

	select {
	case <-tr.ready:
	case <-time.After(time.Second):
		t.Fatalf("subscribe loop did not retry after first error; calls=%d", tr.calls.Load())
	}
	if tr.calls.Load() < 2 {
		t.Fatalf("Subscribe calls = %d; want at least 2", tr.calls.Load())
	}
	if !errors.Is(f.br.LastSubscribeError(), errFakeSubscribe) {
		t.Fatalf("LastSubscribeError = %v; want %v", f.br.LastSubscribeError(), errFakeSubscribe)
	}
}

// TestOnAppliedFiresAfterApplyTx verifies the apply-side callback fires
// exactly once after the app.db apply commits, with the changeset's dot.
func TestOnAppliedFiresAfterApplyTx(t *testing.T) {
	f := newApplier(t, 1, nil)

	type ev struct {
		origin crdt.Origin
		seq    crdt.Seq
	}
	ch := make(chan ev, 4)
	f.br.OnApplied(func(o crdt.Origin, s crdt.Seq) { ch <- ev{o, s} })

	src := crdt.Origin(7)
	stamp := crdt.Stamp{Clock: crdt.Clock{WallTime: 1000}, Origin: src}
	cs := buildInsert(t, f.tab, crdt.Dot{Origin: src, Seq: 1}, stamp, 1, []byte{0x01}, "x")
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("applyPayload: %v", err)
	}
	select {
	case got := <-ch:
		if got.origin != src || got.seq != 1 {
			t.Errorf("OnApplied got (%d,%d); want (%d,1)", got.origin, got.seq, src)
		}
	case <-time.After(time.Second):
		t.Fatal("OnApplied did not fire")
	}

	// Idempotent re-delivery: applyPayload returns nil but the callback
	// should NOT fire again — the apply tx didn't run.
	if err := f.br.applyPayload(context.Background(), cs.Encoded()); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	select {
	case got := <-ch:
		t.Errorf("OnApplied fired on idempotent re-delivery: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

type retrySubscribeTransport struct {
	calls atomic.Int32
	once  sync.Once
	ready chan struct{}
}

func (t *retrySubscribeTransport) Broadcast(context.Context, []byte) error {
	return nil
}

func (t *retrySubscribeTransport) Subscribe(ctx context.Context, _ transport.ApplyFunc) error {
	if t.calls.Add(1) == 1 {
		return errFakeSubscribe
	}
	t.once.Do(func() { close(t.ready) })
	<-ctx.Done()
	return ctx.Err()
}

func (t *retrySubscribeTransport) Fetch(context.Context, []transport.Range, transport.ApplyFunc) error {
	return nil
}

func (t *retrySubscribeTransport) Inject(context.Context, []byte) error { return nil }

var errFakeSubscribe = errors.New("subscribe: synthetic apply failure")

// readNCol returns the value of event.n for id=blob, or the empty string
// if the row does not exist. Test helper, not for production use.
func readNCol(t *testing.T, app *sqlitebridge.Conn, id []byte) string {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT n FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !hasRow {
		return ""
	}
	return stmt.ColumnText(0)
}

func rowExists(t *testing.T, app *sqlitebridge.Conn, id []byte) bool {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT 1 FROM event WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return hasRow
}
