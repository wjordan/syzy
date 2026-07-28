package producer

import (
	"bytes"
	"context"
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
)

type fixture struct {
	app   *sqlitebridge.Conn
	sc    *metadata.Store
	cat   *catalog.Catalog
	cache *nodestate.Cache
	p     *Producer

	// captured collects every encoded payload via OnEncoded —
	// "what did the producer emit in commit order" for assertions.
	capturedMu sync.Mutex
	captured   [][]byte
}

func setup(t *testing.T) *fixture { return setupTB(t) }

func setupTB(t testing.TB) *fixture {
	return setupTBWithConfig(t, Config{JournalDir: filepath.Join(t.TempDir(), "jrn")})
}

// setupTBWithConfig is setupTB but lets the caller customize the
// producer Config. JournalDir, if empty, defaults to a per-test
// temp dir (caller-supplied dir wins). Cache is auto-attached when
// the caller did not supply one.
func setupTBWithConfig(t testing.TB, cfg Config) *fixture {
	t.Helper()
	dir := t.TempDir()

	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		t.Fatalf("init app schema: %v", err)
	}

	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	if err := sc.SetClusterID(crdt.ClusterID{1, 2, 3}); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(42)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}

	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		t.Fatalf("catalog.SeedFromSchema: %v", err)
	}
	if cfg.JournalDir == "" {
		cfg.JournalDir = filepath.Join(dir, "jrn")
	}
	cache := cfg.Cache
	if cache == nil {
		cache = nodestate.New(crdt.Origin(42))
		cfg.Cache = cache
	}
	p, err := New(app, sc, cat, cfg)
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	f := &fixture{app: app, sc: sc, cat: cat, cache: cache, p: p}
	p.OnEncoded(func(payload []byte) {
		cp := append([]byte(nil), payload...)
		f.capturedMu.Lock()
		f.captured = append(f.captured, cp)
		f.capturedMu.Unlock()
	})
	return f
}

// emitted snapshots the captured encoded payloads in commit order.
func (f *fixture) emitted() [][]byte {
	f.capturedMu.Lock()
	defer f.capturedMu.Unlock()
	out := make([][]byte, len(f.captured))
	for i, p := range f.captured {
		out[i] = append([]byte(nil), p...)
	}
	return out
}

// waitDrain blocks until the producer's drainer has caught up to the
// journal head — every committed record has flowed through the sink
// and fired OnEncoded.
func (f *fixture) waitDrain(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.p.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain: %v", err)
	}
}

func TestProducerCapturesInsert(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 1 {
		t.Fatalf("emitted = %d; want 1", len(payloads))
	}

	cs, err := crdt.Decode(payloads[0])
	if err != nil {
		t.Fatalf("Decode payload: %v", err)
	}
	if cs.Dot.Origin != 42 || cs.Dot.Seq != 1 {
		t.Errorf("Dot = %+v; want {42,1}", cs.Dot)
	}
	if len(cs.Records) != 1 {
		t.Fatalf("records = %d; want 1", len(cs.Records))
	}
	ins, ok := cs.Records[0].(crdt.Insert)
	if !ok {
		t.Fatalf("record type = %T; want crdt.Insert", cs.Records[0])
	}
	if ins.CL != 1 {
		t.Errorf("CL = %d; want 1", ins.CL)
	}
	tab, _ := f.cat.Table("event")
	if ins.Table != tab.ID {
		t.Errorf("Insert.Table = %x; want %x", ins.Table, tab.ID)
	}
	if len(ins.Image) != 2 {
		t.Fatalf("Image cols = %d; want 2", len(ins.Image))
	}
	// Each image column must carry its catalog ColumnID (regression
	// guard: parseJournal alone leaves Column zero — buildRecordEvidence
	// must populate it).
	for i, c := range tab.Columns {
		if ins.Image[i].Column != c.ID {
			t.Errorf("Image[%d].Column = %x; want %x", i, ins.Image[i].Column, c.ID)
		}
	}
	// Spot-check the n column carries the SQL-bound text "hello".
	for _, v := range ins.Image {
		if v.Column == tab.Columns[1].ID && string(v.Bytes) != "hello" {
			t.Errorf("Image n = %q; want hello", v.Bytes)
		}
	}

	if got := f.cache.SenderNextSeq(crdt.Origin(42)); got != 2 {
		t.Errorf("cache.SenderNextSeq = %d; want 2", got)
	}

	pkBlob := decodePK(t, tab, ins.PK)
	idCol := tab.PK[0].ID
	if !bytes.Equal(pkBlob[idCol].Bytes, []byte{0x01}) {
		t.Errorf("decoded PK id = %v; want [01]", pkBlob[idCol].Bytes)
	}

	rs := f.cache.RowState(tab.ID, ins.PK)
	if rs.CL != 1 {
		t.Errorf("cache.RowState CL = %d; want 1", rs.CL)
	}
}

func TestProducerCapturesUpdate(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}
	if err := f.app.Exec(`UPDATE event SET n = 'goodbye' WHERE id = x'01'`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 2 {
		t.Fatalf("emitted = %d; want 2", len(payloads))
	}
	cs, err := crdt.Decode(payloads[1])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	upd, ok := cs.Records[0].(crdt.Update)
	if !ok {
		t.Fatalf("record type = %T; want Update", cs.Records[0])
	}
	if upd.CL != 1 {
		t.Errorf("UPDATE CL = %d; want 1 (UPDATE preserves liveness)", upd.CL)
	}
	if len(upd.Changed) != 1 {
		t.Fatalf("changed cols = %d; want 1 (full non-PK image: n)", len(upd.Changed))
	}
}

func TestProducerCapturesPrimaryKeyUpdateAsDeleteInsert(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}
	if err := f.app.Exec(`UPDATE event SET id = x'02', n = 'moved' WHERE id = x'01'`); err != nil {
		t.Fatalf("PK UPDATE: %v", err)
	}
	f.waitDrain(t)

	payloads := f.emitted()
	if len(payloads) != 2 {
		t.Fatalf("emitted = %d; want 2", len(payloads))
	}
	cs, err := crdt.Decode(payloads[1])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(cs.Records) != 2 {
		t.Fatalf("records = %d; want DELETE old PK + INSERT new PK", len(cs.Records))
	}

	del, ok := cs.Records[0].(crdt.Delete)
	if !ok {
		t.Fatalf("record[0] = %T; want Delete", cs.Records[0])
	}
	ins, ok := cs.Records[1].(crdt.Insert)
	if !ok {
		t.Fatalf("record[1] = %T; want Insert", cs.Records[1])
	}

	tab, _ := f.cat.Table("event")
	idCol := tab.PK[0].ID
	oldPK := decodePK(t, tab, del.PK)
	if !bytes.Equal(oldPK[idCol].Bytes, []byte{0x01}) {
		t.Errorf("delete PK = %x; want 01", oldPK[idCol].Bytes)
	}
	newPK := decodePK(t, tab, ins.PK)
	if !bytes.Equal(newPK[idCol].Bytes, []byte{0x02}) {
		t.Errorf("insert PK = %x; want 02", newPK[idCol].Bytes)
	}
	if del.CL != 2 {
		t.Errorf("delete CL = %d; want 2", del.CL)
	}
	if ins.CL != 1 {
		t.Errorf("insert CL = %d; want 1", ins.CL)
	}

	if rs := f.cache.RowState(tab.ID, del.PK); rs.CL != 2 {
		t.Errorf("old row_clock CL = %d; want 2", rs.CL)
	}
	if rs := f.cache.RowState(tab.ID, ins.PK); rs.CL != 1 {
		t.Errorf("new row_clock CL = %d; want 1", rs.CL)
	}
}

func TestProducerCapturesDelete(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := f.app.Exec(`DELETE FROM event WHERE id = x'01'`); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 2 {
		t.Fatalf("emitted = %d; want 2", len(payloads))
	}
	cs, _ := crdt.Decode(payloads[1])
	del, ok := cs.Records[0].(crdt.Delete)
	if !ok {
		t.Fatalf("record type = %T; want Delete", cs.Records[0])
	}
	if del.CL != 2 {
		t.Errorf("DELETE CL = %d; want 2 (post-INSERT live=1, post-DELETE tomb=2)", del.CL)
	}
	tab, _ := f.cat.Table("event")
	if rs := f.cache.RowState(tab.ID, del.PK); rs.CL != 2 {
		t.Errorf("row_clock = %v; want CL=2", rs)
	}
}

func TestProducerDoesNotAdvanceSeqForCollapsedTransaction(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}
	if err := f.app.Exec(`BEGIN; INSERT INTO event (id, n) VALUES (x'02', 'temp'); DELETE FROM event WHERE id = x'02'; COMMIT`); err != nil {
		t.Fatalf("collapsed txn: %v", err)
	}
	f.waitDrain(t)

	payloads := f.emitted()
	if len(payloads) != 1 {
		t.Fatalf("emitted = %d; want only the seed INSERT", len(payloads))
	}
	if got := f.cache.SenderNextSeq(crdt.Origin(42)); got != 2 {
		t.Errorf("cache.SenderNextSeq = %d; want 2", got)
	}
}

func TestProducerSkipsNoOpUpdate(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}
	f.waitDrain(t) // ensure seed has flushed before listener registers
	var fired atomic.Int32
	f.p.OnCommit(func() { fired.Add(1) })

	if err := f.app.Exec(`UPDATE event SET n = 'hello' WHERE id = x'01'`); err != nil {
		t.Fatalf("no-op UPDATE: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 1 {
		t.Fatalf("emitted = %d; want only the seed INSERT", len(payloads))
	}
	if got := fired.Load(); got != 0 {
		t.Errorf("listener fired %d times for no-op UPDATE; want 0", got)
	}
	if got := f.cache.SenderNextSeq(crdt.Origin(42)); got != 2 {
		t.Errorf("cache.SenderNextSeq = %d; want 2", got)
	}
}

func TestOnCommitFiresAfterMaterialize(t *testing.T) {
	f := setup(t)
	var fired atomic.Int32
	f.p.OnCommit(func() { fired.Add(1) })

	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'a')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	f.waitDrain(t)
	if got := fired.Load(); got != 1 {
		t.Fatalf("listener fired %d times after one INSERT; want 1", got)
	}

	if err := f.app.Exec(`UPDATE event SET n = 'b' WHERE id = x'01'`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	f.waitDrain(t)
	if got := fired.Load(); got != 2 {
		t.Fatalf("listener fired %d after INSERT+UPDATE; want 2", got)
	}

	// Non-replicated DML (sqlite_* internal table touch via PRAGMA) must
	// not fire — only commits with touch-journal entries materialize.
	// Regression guard: a no-op transaction should be silent.
	if err := f.app.Exec(`BEGIN; COMMIT`); err != nil {
		t.Fatalf("empty txn: %v", err)
	}
	f.waitDrain(t)
	if got := fired.Load(); got != 2 {
		t.Errorf("listener fired %d after empty txn; want still 2", got)
	}
}

// Coalesces INSERT-then-UPDATE in one txn into a single Insert with the
// final NEW values. Locks in that the in-memory last-NEW capture matches
// what the old post-commit SELECT would have read.
func TestProducerCoalescesInsertThenUpdateInSameTxn(t *testing.T) {
	f := setup(t)
	if err := f.app.Exec(`BEGIN; INSERT INTO event (id, n) VALUES (x'01', 'first'); UPDATE event SET n = 'final' WHERE id = x'01'; COMMIT`); err != nil {
		t.Fatalf("INSERT+UPDATE txn: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 1 {
		t.Fatalf("emitted = %d; want 1 (coalesced)", len(payloads))
	}
	cs, err := crdt.Decode(payloads[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(cs.Records) != 1 {
		t.Fatalf("records = %d; want 1 Insert", len(cs.Records))
	}
	ins, ok := cs.Records[0].(crdt.Insert)
	if !ok {
		t.Fatalf("record = %T; want Insert", cs.Records[0])
	}
	tab, _ := f.cat.Table("event")
	var nCol crdt.ColumnID
	for _, c := range tab.Columns {
		if c.Name == "n" {
			nCol = c.ID
		}
	}
	for _, v := range ins.Image {
		if v.Column == nCol {
			if string(v.Bytes) != "final" {
				t.Errorf("Insert.Image n = %q; want %q (final NEW values, not first)", v.Bytes, "final")
			}
			return
		}
	}
	t.Fatalf("Insert.Image missing column n")
}

func decodePK(t *testing.T, tab *catalog.Table, pk crdt.PKBlob) map[crdt.ColumnID]crdt.ColValue {
	t.Helper()
	m, err := tab.DecodePK(pk)
	if err != nil {
		t.Fatalf("DecodePK: %v", err)
	}
	return m
}
