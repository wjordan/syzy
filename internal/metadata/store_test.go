package metadata

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "syzy.db")
	sc, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc, path
}

func TestOpenSeedsSchemaVersion(t *testing.T) {
	sc, _ := openTemp(t)
	v, ok, err := sc.GetSchemaVersion()
	if err != nil {
		t.Fatalf("GetSchemaVersion: %v", err)
	}
	if !ok {
		t.Fatal("schema_version absent after Open")
	}
	if v != schemaVersion {
		t.Errorf("schema_version = %d; want %d", v, schemaVersion)
	}
}

func TestOpenRetainsReservedColumnLayout(t *testing.T) {
	sc, _ := openTemp(t)
	for _, column := range []string{"declared_type", "auto_pk"} {
		stmt, _, err := sc.conn.Prepare(`SELECT 1 FROM pragma_table_info('syzy_column') WHERE name = ?`)
		if err != nil {
			t.Fatalf("prepare column probe: %v", err)
		}
		if err := stmt.BindText(1, column); err != nil {
			t.Fatalf("bind column probe: %v", err)
		}
		has, err := stmt.Step()
		_ = stmt.Finalize()
		if err != nil {
			t.Fatalf("probe %s: %v", column, err)
		}
		if !has {
			t.Fatalf("reserved column %s missing", column)
		}
	}
}

func TestQuarantineMethodsAfterClose(t *testing.T) {
	sc, _ := openTemp(t)
	if err := sc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"stats", func() error { _, err := sc.QuarantineStats(); return err }},
		{"put", func() error { return sc.PutQuarantine(1, 1, nil, "test", 1) }},
		{"delete", func() error { return sc.DeleteQuarantine(1, 1) }},
		{"bump", func() error { return sc.BumpQuarantineAttempt(1, 1) }},
		{"count", func() error { _, err := sc.CountQuarantineByOrigin(1); return err }},
		{"list", func() error { _, err := sc.ListQuarantine(); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); !errors.Is(err, ErrClosed) {
				t.Fatalf("error = %v, want ErrClosed", err)
			}
		})
	}
}

func TestOpenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "syzy.db")
	sc, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	id := crdt.ClusterID{0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}
	if err := sc.SetClusterID(id); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sc2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sc2.Close()
	got, ok, err := sc2.GetClusterID()
	if err != nil {
		t.Fatalf("GetClusterID: %v", err)
	}
	if !ok || got != id {
		t.Errorf("GetClusterID after reopen = (%v, %v); want (%v, true)", got, ok, id)
	}
}

func TestOpenRejectsSchemaMismatch(t *testing.T) {
	sc, path := openTemp(t)
	if err := sc.SetSchemaVersion(99); err != nil {
		t.Fatalf("SetSchemaVersion: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := Open(path)
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open after version bump = %v; want ErrSchemaMismatch", err)
	}
}

func TestMetaRoundtrip(t *testing.T) {
	sc, _ := openTemp(t)

	if err := sc.SetMeta("custom", []byte{1, 2, 3}); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	got, ok, err := sc.GetMeta("custom")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !ok || string(got) != "\x01\x02\x03" {
		t.Errorf("GetMeta(custom) = (%v, %v); want ([1 2 3], true)", got, ok)
	}

	if err := sc.SetMeta("custom", []byte{9}); err != nil {
		t.Fatalf("SetMeta update: %v", err)
	}
	got, _, _ = sc.GetMeta("custom")
	if len(got) != 1 || got[0] != 9 {
		t.Errorf("after update GetMeta = %v; want [9]", got)
	}

	if err := sc.DeleteMeta("custom"); err != nil {
		t.Fatalf("DeleteMeta: %v", err)
	}
	if _, ok, err := sc.GetMeta("custom"); err != nil || ok {
		t.Errorf("after delete GetMeta = (_, %v, %v); want (_, false, nil)", ok, err)
	}
}

func TestSchemaHealthFirstFailureWins(t *testing.T) {
	t.Parallel()
	sc, _ := openTemp(t)

	if _, ok, err := sc.GetSchemaHealth(); err != nil || ok {
		t.Fatalf("initial GetSchemaHealth = (_, %v, %v); want (_, false, nil)", ok, err)
	}

	want := SchemaHealth{Seq: 7, Reason: "decode schema event: bad kind"}
	got, err := sc.MarkSchemaUnhealthy(want.Seq, want.Reason)
	if err != nil {
		t.Fatalf("MarkSchemaUnhealthy: %v", err)
	}
	if got != want {
		t.Fatalf("MarkSchemaUnhealthy = %#v; want %#v", got, want)
	}

	got, err = sc.MarkSchemaUnhealthy(8, "later failure")
	if err != nil {
		t.Fatalf("second MarkSchemaUnhealthy: %v", err)
	}
	if got != want {
		t.Fatalf("second MarkSchemaUnhealthy = %#v; want first failure %#v", got, want)
	}

	got, ok, err := sc.GetSchemaHealth()
	if err != nil || !ok || got != want {
		t.Fatalf("GetSchemaHealth = (%#v, %v, %v); want (%#v, true, nil)", got, ok, err, want)
	}
}

func TestSchemaHealthRejectsInvalidRecord(t *testing.T) {
	t.Parallel()
	sc, _ := openTemp(t)

	for _, tc := range []struct {
		name   string
		seq    uint64
		reason string
	}{
		{name: "zero sequence", reason: "failure"},
		{name: "empty reason", seq: 1},
		{name: "invalid UTF-8", seq: 1, reason: string([]byte{0xff})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sc.MarkSchemaUnhealthy(tc.seq, tc.reason); err == nil {
				t.Fatal("MarkSchemaUnhealthy returned nil error")
			}
		})
	}
}

func TestTypedAccessorsRoundtrip(t *testing.T) {
	sc, _ := openTemp(t)

	id := crdt.ClusterID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if err := sc.SetClusterID(id); err != nil {
		t.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(1234567890)); err != nil {
		t.Fatalf("SetNodeID: %v", err)
	}
	if err := sc.WithTx(func(tx *Tx) error {
		return tx.PutSenderSeq(crdt.Origin(7), crdt.Seq(42))
	}); err != nil {
		t.Fatalf("PutSenderSeq: %v", err)
	}
	clk := crdt.Clock{WallTime: 1_700_000_000_000, Logical: 7}
	if err := sc.SetHLCLast(clk); err != nil {
		t.Fatalf("SetHLCLast: %v", err)
	}
	if err := sc.SetSchemaSeq(15); err != nil {
		t.Fatalf("SetSchemaSeq: %v", err)
	}
	if err := sc.SetCleanShutdown(true); err != nil {
		t.Fatalf("SetCleanShutdown: %v", err)
	}

	if got, ok, err := sc.GetClusterID(); err != nil || !ok || got != id {
		t.Errorf("ClusterID round-trip = (%v, %v, %v); want (%v, true, nil)", got, ok, err, id)
	}
	if got, ok, err := sc.GetNodeID(); err != nil || !ok || got != 1234567890 {
		t.Errorf("NodeID round-trip = (%v, %v, %v); want (1234567890, true, nil)", got, ok, err)
	}
	if seqs, err := sc.SenderSeqs(); err != nil {
		t.Errorf("SenderSeqs: %v", err)
	} else if got := seqs[crdt.Origin(7)]; got != crdt.Seq(42) {
		t.Errorf("SenderSeqs[7] = %d; want 42", got)
	}
	if got, ok, err := sc.GetHLCLast(); err != nil || !ok || !got.Equal(clk) {
		t.Errorf("HLCLast round-trip = (%v, %v, %v); want (%v, true, nil)", got, ok, err, clk)
	}
	if got, ok, err := sc.GetSchemaSeq(); err != nil || !ok || got != 15 {
		t.Errorf("SchemaSeq round-trip = (%v, %v, %v); want (15, true, nil)", got, ok, err)
	}
	if got, ok, err := sc.GetCleanShutdown(); err != nil || !ok || !got {
		t.Errorf("CleanShutdown round-trip = (%v, %v, %v); want (true, true, nil)", got, ok, err)
	}
}

func TestAccessorsAbsentBeforeSet(t *testing.T) {
	sc, _ := openTemp(t)
	if _, ok, err := sc.GetClusterID(); err != nil || ok {
		t.Errorf("ClusterID before set = (_, %v, %v); want (_, false, nil)", ok, err)
	}
	if _, ok, err := sc.GetNodeID(); err != nil || ok {
		t.Errorf("NodeID before set = (_, %v, %v); want (_, false, nil)", ok, err)
	}
	if _, ok, err := sc.GetCleanShutdown(); err != nil || ok {
		t.Errorf("CleanShutdown before set = (_, %v, %v); want (_, false, nil)", ok, err)
	}
}

func TestWithTxCommits(t *testing.T) {
	sc, _ := openTemp(t)
	err := sc.WithTx(func(tx *Tx) error {
		return tx.SetMeta("k1", []byte{0xff})
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	got, ok, _ := sc.GetMeta("k1")
	if !ok || got[0] != 0xff {
		t.Errorf("after commit GetMeta = (%v, %v); want ([255], true)", got, ok)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	sc, _ := openTemp(t)
	if err := sc.SetMeta("k1", []byte{0x01}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	boom := errors.New("boom")
	err := sc.WithTx(func(tx *Tx) error {
		if err := tx.SetMeta("k1", []byte{0x02}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("WithTx = %v; want boom", err)
	}
	got, _, _ := sc.GetMeta("k1")
	if len(got) != 1 || got[0] != 0x01 {
		t.Errorf("after rollback GetMeta = %v; want [1]", got)
	}
	// Subsequent writes must succeed — i.e., rollback released the txn.
	if err := sc.SetMeta("k2", []byte{0x33}); err != nil {
		t.Fatalf("post-rollback SetMeta: %v", err)
	}
}

func tableExists(t *testing.T, sc *Store, name string) bool {
	t.Helper()
	stmt, _, err := sc.conn.Prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, name); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return hasRow
}

func TestSchemaTablesPresent(t *testing.T) {
	sc, _ := openTemp(t)
	for _, table := range []string{"meta", "frontier", "row_clock"} {
		if !tableExists(t, sc, table) {
			t.Errorf("table %s missing", table)
		}
	}
}

func TestWALPragmaApplied(t *testing.T) {
	sc, _ := openTemp(t)
	stmt, _, err := sc.conn.Prepare(`PRAGMA journal_mode`)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("PRAGMA journal_mode: hasRow=%v err=%v", hasRow, err)
	}
	if got := stmt.ColumnText(0); got != "wal" {
		t.Errorf("journal_mode = %q; want wal", got)
	}
}
