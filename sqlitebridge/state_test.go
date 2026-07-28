package sqlitebridge

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// journalValue holds one captured column value decoded from the journal.
type journalValue struct {
	tag    byte
	intVal int64
	flt    float64
	bytes  []byte
}

// journalRecord is one preupdate fire's decoded contents.
type journalRecord struct {
	op       PreupdateOp
	oldRowID int64
	newRowID int64
	dbName   string
	table    string
	values   []journalValue // INSERT=NEW, UPDATE=OLD, DELETE=OLD
	newVals  []journalValue // UPDATE NEW values; nil otherwise
}

// parseJournal decodes the bridge's journal byte format into records. Mirrors
// the format documented on Conn.EnableTouchJournal.
func parseJournal(buf []byte) ([]journalRecord, error) {
	var out []journalRecord
	off := 0
	for off < len(buf) {
		if off+1+8+8+2 > len(buf) {
			return nil, fmt.Errorf("truncated record header at %d", off)
		}
		var rec journalRecord
		rec.op = PreupdateOp(buf[off])
		off++
		rec.oldRowID = int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		rec.newRowID = int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8

		dbN := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+dbN > len(buf) {
			return nil, fmt.Errorf("truncated db_name at %d", off)
		}
		rec.dbName = string(buf[off : off+dbN])
		off += dbN

		if off+2 > len(buf) {
			return nil, fmt.Errorf("truncated table_name length at %d", off)
		}
		tblN := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		if off+tblN > len(buf) {
			return nil, fmt.Errorf("truncated table_name at %d", off)
		}
		rec.table = string(buf[off : off+tblN])
		off += tblN

		if off+2 > len(buf) {
			return nil, fmt.Errorf("truncated col count at %d", off)
		}
		ncol := int(binary.BigEndian.Uint16(buf[off:]))
		off += 2
		var err error
		rec.values, off, err = readJournalValues(buf, off, ncol)
		if err != nil {
			return nil, err
		}
		if rec.op == PreupdateUpdate {
			rec.newVals, off, err = readJournalValues(buf, off, ncol)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, rec)
	}
	return out, nil
}

func readJournalValues(buf []byte, off, ncol int) ([]journalValue, int, error) {
	out := make([]journalValue, ncol)
	for i := 0; i < ncol; i++ {
		if off >= len(buf) {
			return nil, 0, fmt.Errorf("truncated value tag at column %d", i)
		}
		tag := buf[off]
		off++
		out[i].tag = tag
		switch tag {
		case 0:
		case 1:
			if off+8 > len(buf) {
				return nil, 0, fmt.Errorf("truncated int at column %d", i)
			}
			out[i].intVal = int64(binary.BigEndian.Uint64(buf[off:]))
			off += 8
		case 2:
			if off+8 > len(buf) {
				return nil, 0, fmt.Errorf("truncated real at column %d", i)
			}
			out[i].flt = math.Float64frombits(binary.BigEndian.Uint64(buf[off:]))
			off += 8
		case 3, 4:
			if off+4 > len(buf) {
				return nil, 0, fmt.Errorf("truncated bytes length at column %d", i)
			}
			n := int(binary.BigEndian.Uint32(buf[off:]))
			off += 4
			if off+n > len(buf) {
				return nil, 0, fmt.Errorf("truncated bytes payload at column %d", i)
			}
			out[i].bytes = append([]byte(nil), buf[off:off+n]...)
			off += n
		default:
			return nil, 0, fmt.Errorf("unknown tag %d at column %d", tag, i)
		}
	}
	return out, off, nil
}

func TestTouchJournalDisabledByDefault(t *testing.T) {
	c := memDB(t)
	if c.TouchJournalEnabled() {
		t.Error("TouchJournalEnabled() = true; want false by default")
	}
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT); INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("CREATE+INSERT: %v", err)
	}
	if got := c.TouchJournalLen(); got != 0 {
		t.Errorf("TouchJournalLen = %d; want 0 (journal disabled)", got)
	}
}

func TestTouchJournalCapturesInsert(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT, blob BLOB, fp REAL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if !c.TouchJournalEnabled() {
		t.Fatal("TouchJournalEnabled() = false after enable")
	}
	if err := c.Exec(`INSERT INTO t VALUES (1, 'alpha', x'01020304', 1.5)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records = %d; want 1", len(recs))
	}
	r := recs[0]
	if r.op != PreupdateInsert {
		t.Errorf("op = %d; want %d (insert)", r.op, PreupdateInsert)
	}
	if r.dbName != "main" {
		t.Errorf("dbName = %q; want main", r.dbName)
	}
	if r.table != "t" {
		t.Errorf("table = %q; want t", r.table)
	}
	if len(r.values) != 4 {
		t.Fatalf("values = %d; want 4", len(r.values))
	}
	if r.values[0].tag != 1 || r.values[0].intVal != 1 {
		t.Errorf("col 0: tag=%d val=%d; want tag=1 val=1", r.values[0].tag, r.values[0].intVal)
	}
	if r.values[1].tag != 3 || string(r.values[1].bytes) != "alpha" {
		t.Errorf("col 1: tag=%d bytes=%q; want tag=3 bytes=alpha", r.values[1].tag, r.values[1].bytes)
	}
	if r.values[2].tag != 4 || !bytes.Equal(r.values[2].bytes, []byte{1, 2, 3, 4}) {
		t.Errorf("col 2: tag=%d bytes=%v; want tag=4 bytes=[1 2 3 4]", r.values[2].tag, r.values[2].bytes)
	}
	if r.values[3].tag != 2 || r.values[3].flt != 1.5 {
		t.Errorf("col 3: tag=%d val=%v; want tag=2 val=1.5", r.values[3].tag, r.values[3].flt)
	}
}

func TestTouchJournalCapturesUpdateOldRow(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t VALUES (1, 'old')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`UPDATE t SET n='new' WHERE id=1`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records = %d; want 1", len(recs))
	}
	r := recs[0]
	if r.op != PreupdateUpdate {
		t.Errorf("op = %d; want %d (update)", r.op, PreupdateUpdate)
	}
	if string(r.values[1].bytes) != "old" {
		t.Errorf("UPDATE captured old value = %q; want old", r.values[1].bytes)
	}
}

func TestTouchJournalCapturesDeleteOldRow(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(`INSERT INTO t VALUES (7, 'gone')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`DELETE FROM t WHERE id=7`); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records = %d; want 1", len(recs))
	}
	r := recs[0]
	if r.op != PreupdateDelete {
		t.Errorf("op = %d; want %d (delete)", r.op, PreupdateDelete)
	}
	if r.oldRowID != 7 {
		t.Errorf("oldRowID = %d; want 7", r.oldRowID)
	}
	if r.values[0].intVal != 7 {
		t.Errorf("col 0 intVal = %d; want 7", r.values[0].intVal)
	}
	if string(r.values[1].bytes) != "gone" {
		t.Errorf("col 1 bytes = %q; want gone", r.values[1].bytes)
	}
}

func TestTouchJournalCapturesNullValue(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`INSERT INTO t (id, n) VALUES (1, NULL)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if recs[0].values[1].tag != 0 {
		t.Errorf("NULL col tag = %d; want 0", recs[0].values[1].tag)
	}
}

func TestTouchJournalAccumulates(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)`); err != nil {
		t.Fatalf("INSERTs: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("records = %d; want 3", len(recs))
	}
	for i, want := range []int64{1, 2, 3} {
		if recs[i].values[0].intVal != want {
			t.Errorf("rec %d id = %d; want %d", i, recs[i].values[0].intVal, want)
		}
	}
}

func TestTouchJournalRollbackClears(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); ROLLBACK;`); err != nil {
		t.Fatalf("BEGIN/ROLLBACK: %v", err)
	}
	if got := c.TouchJournalLen(); got != 0 {
		t.Errorf("after ROLLBACK, len = %d; want 0", got)
	}
}

func TestTouchJournalClearKeepsCapacity(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := c.TouchJournalLen(); got == 0 {
		t.Fatal("len = 0 after INSERT; want > 0")
	}
	c.ClearTouchJournal()
	if got := c.TouchJournalLen(); got != 0 {
		t.Errorf("len after clear = %d; want 0", got)
	}
	if err := c.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 1 || recs[0].values[0].intVal != 2 {
		t.Errorf("after clear+INSERT 2, recs=%v; want one record id=2", recs)
	}
}

func TestTouchJournalDisableStopsCapture(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	c.EnableTouchJournal()
	if err := c.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	first := c.TouchJournalLen()
	c.DisableTouchJournal()
	if err := c.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	if got := c.TouchJournalLen(); got != first {
		t.Errorf("len after disabled INSERT = %d; want %d (unchanged)", got, first)
	}
}

func TestTouchJournalCoexistsWithGoPreupdateHook(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	goFires := 0
	c.SetPreupdateHook(func(*PreupdateEvent) {
		goFires++
	})
	c.EnableTouchJournal()
	if err := c.Exec(`INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("INSERTs: %v", err)
	}
	if goFires != 2 {
		t.Errorf("Go preupdate fires = %d; want 2", goFires)
	}
	recs, err := parseJournal(c.TouchJournal())
	if err != nil {
		t.Fatalf("parseJournal: %v", err)
	}
	if len(recs) != 2 {
		t.Errorf("journal records = %d; want 2", len(recs))
	}
}
