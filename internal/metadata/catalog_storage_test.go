package metadata

import (
	"bytes"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func tableID(b byte) crdt.TableID {
	var id crdt.TableID
	id[0] = b
	return id
}

func columnID(b byte) crdt.ColumnID {
	var id crdt.ColumnID
	id[0] = b
	return id
}

// TestCatalogEmpty verifies LoadCatalogSnapshot on a fresh metadata
// returns an empty CatalogSnapshot rather than nil.
func TestCatalogEmpty(t *testing.T) {
	sc, _ := openTemp(t)
	snap, err := sc.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("LoadCatalogSnapshot: %v", err)
	}
	if len(snap.Tables) != 0 || len(snap.Columns) != 0 || len(snap.Keys) != 0 {
		t.Errorf("empty snapshot got %+v", snap)
	}
}

func TestCatalogUpsertAndLoad(t *testing.T) {
	sc, _ := openTemp(t)

	tab := tableID(1)
	colA := columnID(0xA)
	colB := columnID(0xB)

	if err := sc.WithTx(func(tx *Tx) error {
		if err := tx.UpsertTable(TableEntry{
			ID:                tab,
			Name:              "users",
			State:             StateActive,
			DefaultClockGroup: ClockGroupRow,
			CreateSeq:         1,
		}); err != nil {
			return err
		}
		if err := tx.UpsertColumn(ColumnEntry{
			TableID:    tab,
			ColumnID:   colA,
			Name:       "id",
			Ordinal:    0,
			State:      StateActive,
			ClockGroup: ClockGroupRow,
			CreateSeq:  1,
		}); err != nil {
			return err
		}
		if err := tx.UpsertColumn(ColumnEntry{
			TableID:    tab,
			ColumnID:   colB,
			Name:       "email",
			Ordinal:    1,
			State:      StateActive,
			ClockGroup: ClockGroupRow,
			CreateSeq:  1,
		}); err != nil {
			return err
		}
		if err := tx.UpsertKey(KeyEntry{
			TableID:   tab,
			KeyID:     PKKeyID,
			ColumnID:  colA,
			Ordinal:   0,
			State:     StateActive,
			CreateSeq: 1,
		}); err != nil {
			return err
		}
		return tx.AppendSchemaEvent(SchemaEventEntry{
			SchemaSeq:   1,
			ParentSeq:   0,
			CatalogOp:   []byte{0x01, 0x02, 0x03},
			RawSQL:      "CREATE TABLE users (...)",
			AppliedAtUs: 1234,
			ApplyState:  ApplyStateApplied,
		})
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	snap, err := sc.LoadCatalogSnapshot()
	if err != nil {
		t.Fatalf("LoadCatalogSnapshot: %v", err)
	}
	if len(snap.Tables) != 1 {
		t.Fatalf("Tables = %d; want 1", len(snap.Tables))
	}
	if got := snap.Tables[0]; got.Name != "users" || got.State != StateActive ||
		got.DefaultClockGroup != ClockGroupRow || got.CreateSeq != 1 || got.DropSeq != 0 ||
		got.ID != tab {
		t.Errorf("Table = %+v", got)
	}
	if len(snap.Columns) != 2 {
		t.Fatalf("Columns = %d; want 2", len(snap.Columns))
	}
	if len(snap.Keys) != 1 || snap.Keys[0].KeyID != PKKeyID || snap.Keys[0].ColumnID != colA {
		t.Errorf("Keys = %+v", snap.Keys)
	}
}

func TestCatalogUpsertOverwrites(t *testing.T) {
	sc, _ := openTemp(t)
	tab := tableID(1)

	if err := sc.WithTx(func(tx *Tx) error {
		return tx.UpsertTable(TableEntry{
			ID: tab, Name: "users", State: StateActive,
			DefaultClockGroup: ClockGroupRow, CreateSeq: 1,
		})
	}); err != nil {
		t.Fatalf("WithTx insert: %v", err)
	}

	if err := sc.WithTx(func(tx *Tx) error {
		return tx.UpsertTable(TableEntry{
			ID: tab, Name: "people", State: StateActive,
			DefaultClockGroup: ClockGroupRow, CreateSeq: 1,
		})
	}); err != nil {
		t.Fatalf("WithTx update: %v", err)
	}

	snap, _ := sc.LoadCatalogSnapshot()
	if len(snap.Tables) != 1 || snap.Tables[0].Name != "people" {
		t.Errorf("after rename: %+v", snap.Tables)
	}
}

func TestCatalogDropSeqRoundTrip(t *testing.T) {
	sc, _ := openTemp(t)
	tab := tableID(1)
	col := columnID(0xA)

	if err := sc.WithTx(func(tx *Tx) error {
		if err := tx.UpsertTable(TableEntry{
			ID: tab, Name: "old", State: StateDropped,
			DefaultClockGroup: ClockGroupRow, CreateSeq: 1, DropSeq: 5,
		}); err != nil {
			return err
		}
		return tx.UpsertColumn(ColumnEntry{
			TableID: tab, ColumnID: col, Name: "x", Ordinal: 0,
			State: StateDropped, ClockGroup: ClockGroupRow, CreateSeq: 1, DropSeq: 5,
		})
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	snap, _ := sc.LoadCatalogSnapshot()
	if snap.Tables[0].DropSeq != 5 {
		t.Errorf("Table.DropSeq = %d; want 5", snap.Tables[0].DropSeq)
	}
	if snap.Columns[0].DropSeq != 5 {
		t.Errorf("Column.DropSeq = %d; want 5", snap.Columns[0].DropSeq)
	}
}

func TestSchemaEventAppend(t *testing.T) {
	sc, _ := openTemp(t)
	op := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := sc.WithTx(func(tx *Tx) error {
		if err := tx.AppendSchemaEvent(SchemaEventEntry{
			SchemaSeq: 1, ParentSeq: 0, CatalogOp: op,
			RawSQL: "CREATE TABLE t (id INT PRIMARY KEY)", AppliedAtUs: 100,
			ApplyState: ApplyStateApplied,
		}); err != nil {
			return err
		}
		return tx.AppendSchemaEvent(SchemaEventEntry{
			SchemaSeq: 2, ParentSeq: 1, CatalogOp: op,
			RawSQL: "", AppliedAtUs: 200, ApplyState: ApplyStateFailedLocal,
		})
	}); err != nil {
		t.Fatalf("WithTx: %v", err)
	}

	// Verify rows landed by reading via a one-shot query.
	stmt, _, err := sc.conn.Prepare(`SELECT schema_seq, parent_seq, catalog_op, raw_sql, apply_state FROM syzy_schema_event ORDER BY schema_seq`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()

	type row struct {
		seq, parent uint64
		op          []byte
		sql, state  string
		sqlNull     bool
	}
	var got []row
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if !hasRow {
			break
		}
		r := row{
			seq:    uint64(stmt.ColumnInt64(0)),
			parent: uint64(stmt.ColumnInt64(1)),
			op:     stmt.ColumnBlob(2),
			state:  stmt.ColumnText(4),
		}
		if stmt.ColumnIsNull(3) {
			r.sqlNull = true
		} else {
			r.sql = stmt.ColumnText(3)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %d; want 2", len(got))
	}
	if got[0].seq != 1 || got[0].parent != 0 || !bytes.Equal(got[0].op, op) ||
		got[0].sql != "CREATE TABLE t (id INT PRIMARY KEY)" || got[0].state != ApplyStateApplied {
		t.Errorf("row 0 = %+v", got[0])
	}
	if got[1].seq != 2 || got[1].parent != 1 || !got[1].sqlNull || got[1].state != ApplyStateFailedLocal {
		t.Errorf("row 1 = %+v", got[1])
	}
}
