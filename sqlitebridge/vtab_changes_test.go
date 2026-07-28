package sqlitebridge

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/notify"
)

// newVtabFixture spins up a notify.Writer + a sqlitebridge.Conn with
// the syzy_changes vtab registered against that feed. Caller drives
// the writer; vtab consumers read via SELECT.
func newVtabFixture(t *testing.T) (*Conn, *notify.Writer) {
	t.Helper()
	dir := t.TempDir()
	feedPath := filepath.Join(dir, "notify.feed")
	w, err := notify.NewWriter(notify.WriterConfig{Path: feedPath, NumSlots: 64})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	dbPath := filepath.Join(dir, "test.db")
	c, err := Open(dbPath, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := RegisterChangesVTab(c, feedPath, nil); err != nil {
		t.Fatalf("RegisterChangesVTab: %v", err)
	}
	return c, w
}

// runQuery prepares + steps + finalizes one SELECT, returning all rows
// as [][]any (origin, seq, table_name, op, pk).
func runQuery(t *testing.T, c *Conn, sql string) [][]any {
	t.Helper()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("Prepare(%q): %v", sql, err)
	}
	defer stmt.Finalize()
	var rows [][]any
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatalf("Step: %v", err)
		}
		if !hasRow {
			break
		}
		row := make([]any, stmt.ColumnCount())
		for i := range row {
			switch stmt.ColumnType(i) {
			case ColumnInt:
				row[i] = stmt.ColumnInt64(i)
			case ColumnText:
				row[i] = stmt.ColumnText(i)
			case ColumnBlob:
				row[i] = stmt.ColumnBlob(i)
			case ColumnNull:
				row[i] = nil
			default:
				row[i] = stmt.ColumnText(i)
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func TestVTabPeekEmpty(t *testing.T) {
	c, _ := newVtabFixture(t)
	rows := runQuery(t, c, "SELECT origin, seq, table_name, op, pk FROM syzy_changes WHERE timeout_ms = 0")
	if len(rows) != 0 {
		t.Errorf("got %d rows; want 0 on empty feed", len(rows))
	}
}

func TestVTabPeekDrains(t *testing.T) {
	c, w := newVtabFixture(t)
	if err := w.Append([]notify.Change{{
		Origin: 11, Seq: 22, Table: "users", Op: notify.OpInsert, PK: []byte{0xde, 0xad},
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	rows := runQuery(t, c,
		"SELECT origin, seq, table_name, op, pk FROM syzy_changes WHERE timeout_ms = 0")
	if len(rows) != 1 {
		t.Fatalf("got %d rows; want 1", len(rows))
	}
	r := rows[0]
	if r[0].(int64) != 11 || r[1].(int64) != 22 || r[2].(string) != "users" || r[3].(string) != "insert" {
		t.Errorf("row = %+v; want (11, 22, users, insert, deadbeef-ish)", r)
	}
	if pk, _ := r[4].([]byte); string(pk) != string([]byte{0xde, 0xad}) {
		t.Errorf("pk = %x; want dead", r[4])
	}
}

func TestVTabFilterByTable(t *testing.T) {
	c, w := newVtabFixture(t)
	if err := w.Append([]notify.Change{
		{Origin: 1, Seq: 1, Table: "users", Op: notify.OpInsert, PK: []byte{1}},
		{Origin: 1, Seq: 1, Table: "orders", Op: notify.OpInsert, PK: []byte{2}},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	rows := runQuery(t, c,
		"SELECT table_name FROM syzy_changes WHERE table_name = 'users' AND timeout_ms = 0")
	if len(rows) != 1 {
		t.Fatalf("got %d rows; want 1 (filtered to users)", len(rows))
	}
	if rows[0][0].(string) != "users" {
		t.Errorf("table_name = %q; want users", rows[0][0])
	}
}

func TestVTabBlocksUntilEvent(t *testing.T) {
	c, w := newVtabFixture(t)

	// In a goroutine, append after a small delay; the SELECT should
	// see the row when it returns.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = w.Append([]notify.Change{{
			Origin: 7, Seq: 1, Table: "t", Op: notify.OpUpdate, PK: []byte{9},
		}})
	}()

	start := time.Now()
	rows := runQuery(t, c,
		"SELECT origin, op FROM syzy_changes WHERE table_name = 't' AND timeout_ms = 5000")
	elapsed := time.Since(start)

	if len(rows) != 1 {
		t.Fatalf("got %d rows; want 1", len(rows))
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("returned in %v; expected to actually block for the writer", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("returned in %v; took too long, look for missed wake", elapsed)
	}
	if rows[0][0].(int64) != 7 || rows[0][1].(string) != "update" {
		t.Errorf("row = %+v; want (7, update)", rows[0])
	}
}

func TestVTabTimeoutReturnsEmpty(t *testing.T) {
	c, _ := newVtabFixture(t)
	start := time.Now()
	rows := runQuery(t, c,
		"SELECT * FROM syzy_changes WHERE timeout_ms = 200")
	elapsed := time.Since(start)
	if len(rows) != 0 {
		t.Errorf("got %d rows; want 0 on timeout", len(rows))
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned in %v; expected ≥200ms wait", elapsed)
	}
}

func TestVTabLossyRow(t *testing.T) {
	dir := t.TempDir()
	feedPath := filepath.Join(dir, "notify.feed")
	// Tiny ring so we can overrun.
	w, err := notify.NewWriter(notify.WriterConfig{Path: feedPath, NumSlots: 4})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	dbPath := filepath.Join(dir, "test.db")
	c, err := Open(dbPath, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := RegisterChangesVTab(c, feedPath, nil); err != nil {
		t.Fatalf("RegisterChangesVTab: %v", err)
	}

	// Issue a peek so the vtab opens its Reader and snapshots
	// lastSeen at the current head. Ring overrun is detected
	// relative to that bookmark.
	_ = runQuery(t, c, "SELECT * FROM syzy_changes WHERE timeout_ms = 0")

	// Now overrun the 4-slot ring without draining.
	for i := 0; i < 10; i++ {
		if err := w.Append([]notify.Change{{
			Origin: 1, Seq: uint64(i + 1), Table: "x", Op: notify.OpInsert, PK: []byte{byte(i)},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	rows := runQuery(t, c,
		"SELECT origin, seq, table_name, op FROM syzy_changes WHERE timeout_ms = 0")
	// Expect one synthetic Lossy row: op='lossy', other columns NULL.
	if len(rows) == 0 {
		t.Fatal("got 0 rows; want at least one Lossy row")
	}
	found := false
	for _, r := range rows {
		if op, ok := r[3].(string); ok && op == "lossy" {
			found = true
			if r[0] != nil || r[1] != nil || r[2] != nil {
				t.Errorf("lossy row should have NULL origin/seq/table; got %+v", r)
			}
		}
	}
	if !found {
		t.Errorf("no lossy row in %+v", rows)
	}
}

func TestVTabLimitPreservesResidual(t *testing.T) {
	c, w := newVtabFixture(t)
	for i := 0; i < 5; i++ {
		if err := w.Append([]notify.Change{{
			Origin: 1, Seq: uint64(i + 1), Table: "t", Op: notify.OpInsert, PK: []byte{byte(i)},
		}}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	// Peek-mode + LIMIT 1 should consume exactly one row.
	rows := runQuery(t, c,
		"SELECT seq FROM syzy_changes WHERE timeout_ms = 0 LIMIT 1")
	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Fatalf("first batch = %+v; want [seq=1]", rows)
	}
	// The other 4 should still be there for the next peek.
	rows = runQuery(t, c,
		"SELECT seq FROM syzy_changes WHERE timeout_ms = 0")
	if len(rows) != 4 {
		t.Fatalf("second batch len=%d; want 4 residual rows", len(rows))
	}
	for i, r := range rows {
		if r[0].(int64) != int64(i+2) {
			t.Errorf("rows[%d] seq = %d; want %d", i, r[0], i+2)
		}
	}
}

func TestVTabPKTruncation(t *testing.T) {
	c, w := newVtabFixture(t)
	long := make([]byte, notify.PKMaxBytes+8)
	for i := range long {
		long[i] = byte(i)
	}
	if err := w.Append([]notify.Change{{
		Origin: 1, Seq: 1, Table: "t", Op: notify.OpInsert, PK: long,
	}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	rows := runQuery(t, c,
		"SELECT pk_truncated, table_truncated FROM syzy_changes WHERE timeout_ms = 0")
	if len(rows) != 1 {
		t.Fatalf("got %d rows; want 1", len(rows))
	}
	if rows[0][0].(int64) != 1 {
		t.Errorf("pk_truncated = %v; want 1", rows[0][0])
	}
	if rows[0][1].(int64) != 0 {
		t.Errorf("table_truncated = %v; want 0", rows[0][1])
	}
}
