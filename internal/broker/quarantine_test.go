package broker

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// tSchema has a NOT NULL non-PK column (req) so a partial Insert omitting it,
// applied to an absent row, raises a NOT NULL constraint failure — the exact
// poison shape (a cross-origin dependent write materializing a row before its
// INSERT lands).
const tSchema = `CREATE TABLE t (id BLOB PRIMARY KEY NOT NULL, req TEXT NOT NULL, opt TEXT)`

// buildTRecord builds an Insert for table t carrying only the named non-PK
// columns (req/opt). Omitting "req" yields the partial, NOT-NULL-violating
// shape when applied to an absent row.
func buildTRecord(t testing.TB, tab *catalog.Table, dot crdt.Dot, stamp crdt.Stamp, cl uint64, id []byte, cols map[string]string) *crdt.Changeset {
	t.Helper()
	idCol := tab.PK[0].ID
	pk, err := tab.EncodePK(map[crdt.ColumnID]crdt.ColValue{idCol: blobCol(idCol, id)})
	if err != nil {
		t.Fatalf("EncodePK: %v", err)
	}
	var image []crdt.ColValue
	for name, val := range cols {
		c, ok := tab.Column(name)
		if !ok {
			t.Fatalf("no column %q", name)
		}
		image = append(image, textCol(c.ID, val))
	}
	cs, err := crdt.Build(dot, stamp, nil, testCluster, []crdt.Record{
		crdt.Insert{Table: tab.ID, PK: pk, CL: cl, Image: image},
	})
	if err != nil {
		t.Fatalf("crdt.Build: %v", err)
	}
	return cs
}

func stampAt(wall int64, origin crdt.Origin) crdt.Stamp {
	return crdt.Stamp{Clock: crdt.Clock{WallTime: wall}, Origin: origin}
}

func withQuarantineCap(n int) applierOpt {
	return func(c *Config) { c.QuarantineCap = n }
}

func readTReq(t *testing.T, app *sqlitebridge.Conn, id []byte) string {
	t.Helper()
	stmt, _, err := app.Prepare(`SELECT req FROM t WHERE id = ?`)
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

// TestQuarantine_AdvancesPastPoisonAndReapplies is the core regression for the
// apps.name poison: a partial cross-origin record that can't materialize its
// row must NOT permanently pin the origin's frontier. It is quarantined (frontier
// advances so later seqs flow), then re-applied once the missing INSERT lands.
func TestQuarantine_AdvancesPastPoisonAndReapplies(t *testing.T) {
	t.Parallel()
	f := newApplierSchema(t, 1, nil, tSchema)
	tab, ok := f.cat.Table("t")
	if !ok {
		t.Fatal("no table t")
	}
	const B = crdt.Origin(9) // origin issuing the dependent partial write
	const A = crdt.Origin(7) // origin that created the row (delivered late)
	id := []byte{0xAB}

	// B/seq=1: partial Insert (omits req) on the absent row -> NOT NULL -> quarantine.
	poison := buildTRecord(t, tab, crdt.Dot{Origin: B, Seq: 1}, stampAt(100, B), 1, id, map[string]string{"opt": "from-B"})
	if err := f.br.applyPayload(context.Background(), poison.Encoded()); err != nil {
		t.Fatalf("poison apply should be quarantined (nil), got: %v", err)
	}
	// Frontier for B advanced past the poison (the whole point — not pinned at 0).
	if fr, _ := f.cache.FrontierFor(B); fr.LastSeq != 1 {
		t.Fatalf("B frontier = %d, want 1 (quarantine must advance past the poison)", fr.LastSeq)
	}
	// The entry is durably quarantined.
	q, err := f.sc.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine: %v", err)
	}
	if len(q) != 1 || q[0].Origin != B || q[0].Seq != 1 {
		t.Fatalf("quarantine = %+v, want one entry for B/1", q)
	}
	// The resident entry is visible on the health surface.
	if h := f.br.InboundHealth(); h.QuarantineResident != 1 || h.QuarantineOldest.IsZero() {
		t.Fatalf("InboundHealth quarantine = %d/%v, want 1 resident with non-zero oldest",
			h.QuarantineResident, h.QuarantineOldest)
	}
	// A later contiguous seq from B applies cleanly (a full row) — proving the
	// origin is not starved behind the poison.
	ok2 := buildTRecord(t, tab, crdt.Dot{Origin: B, Seq: 2}, stampAt(101, B), 1, []byte{0xCD}, map[string]string{"req": "r2", "opt": "o2"})
	if err := f.br.applyPayload(context.Background(), ok2.Encoded()); err != nil {
		t.Fatalf("later B seq should apply: %v", err)
	}
	if fr, _ := f.cache.FrontierFor(B); fr.LastSeq != 2 {
		t.Fatalf("B frontier = %d, want 2 (later seq must flow past the quarantined one)", fr.LastSeq)
	}

	// The missing dependency lands: A creates the row (full image).
	create := buildTRecord(t, tab, crdt.Dot{Origin: A, Seq: 1}, stampAt(102, A), 1, id, map[string]string{"req": "from-A", "opt": "a-opt"})
	if err := f.br.applyPayload(context.Background(), create.Encoded()); err != nil {
		t.Fatalf("create apply: %v", err)
	}
	if got := readTReq(t, f.app, id); got != "from-A" {
		t.Fatalf("row req after create = %q, want from-A", got)
	}

	// Re-apply drain: the quarantined partial now applies (DO UPDATE), clears.
	f.br.RetryQuarantined(context.Background())
	q, err = f.sc.ListQuarantine()
	if err != nil {
		t.Fatalf("ListQuarantine after retry: %v", err)
	}
	if len(q) != 0 {
		t.Fatalf("quarantine after retry = %+v, want empty (re-apply should clear it)", q)
	}
	if h := f.br.InboundHealth(); h.QuarantineResident != 0 {
		t.Fatalf("InboundHealth quarantine after drain = %d, want 0", h.QuarantineResident)
	}
	// req preserved (partial omitted it), opt overwritten by B's deferred write.
	if got := readTReq(t, f.app, id); got != "from-A" {
		t.Errorf("row req after re-apply = %q, want from-A (NOT NULL col preserved)", got)
	}
}

// TestQuarantine_CapHardBlocks verifies the data-safety backstop: above the
// per-origin cap the broker stops advancing and hard-blocks (a flood of
// constraint failures likely signals real corruption, not an isolated gap).
func TestQuarantine_CapHardBlocks(t *testing.T) {
	t.Parallel()
	f := newApplierSchema(t, 1, nil, tSchema, withQuarantineCap(2)) // cap = 2
	tab, _ := f.cat.Table("t")
	const B = crdt.Origin(9)

	for seq := uint64(1); seq <= 2; seq++ {
		rec := buildTRecord(t, tab, crdt.Dot{Origin: B, Seq: crdt.Seq(seq)}, stampAt(int64(100+seq), B), 1, []byte{byte(seq)}, map[string]string{"opt": "x"})
		if err := f.br.applyPayload(context.Background(), rec.Encoded()); err != nil {
			t.Fatalf("seq %d under cap should quarantine (nil): %v", seq, err)
		}
	}
	// 3rd distinct poison for B is at the cap -> hard-block (error returned).
	rec := buildTRecord(t, tab, crdt.Dot{Origin: B, Seq: 3}, stampAt(200, B), 1, []byte{0x03}, map[string]string{"opt": "x"})
	err := f.br.applyPayload(context.Background(), rec.Encoded())
	if !isConstraintError(err) {
		t.Fatalf("seq 3 at cap should hard-block with the constraint error, got: %v", err)
	}
}
