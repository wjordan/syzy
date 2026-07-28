package sqlitebridge

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func selectBlob(t *testing.T, c *Conn, sql string) []byte {
	t.Helper()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step %q: %v", sql, err)
	}
	if !hasRow {
		t.Fatalf("step %q: no row", sql)
	}
	return stmt.ColumnBlob(0)
}

func selectInt64(t *testing.T, c *Conn, sql string) int64 {
	t.Helper()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("step %q: %v", sql, err)
	}
	if !hasRow {
		t.Fatalf("step %q: no row", sql)
	}
	return stmt.ColumnInt64(0)
}

func collectInt64s(t *testing.T, c *Conn, sql string) []int64 {
	t.Helper()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	defer stmt.Finalize()
	var out []int64
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			t.Fatalf("step %q: %v", sql, err)
		}
		if !hasRow {
			return out
		}
		out = append(out, stmt.ColumnInt64(0))
	}
}

func TestUUIDv7_ShapeAndVersion(t *testing.T) {
	c := memDB(t)
	id := selectBlob(t, c, "SELECT uuidv7()")
	if len(id) != 16 {
		t.Fatalf("uuidv7 length = %d, want 16", len(id))
	}
	if got := id[6] >> 4; got != 0x7 {
		t.Errorf("uuidv7 version nibble = %x, want 7", got)
	}
	if got := id[8] >> 6; got != 0x2 {
		t.Errorf("uuidv7 variant top-2 = %x, want 0b10 (2)", got)
	}
}

func TestUUIDv7_TimestampIsRecent(t *testing.T) {
	c := memDB(t)
	before := time.Now().UnixMilli()
	id := selectBlob(t, c, "SELECT uuidv7()")
	after := time.Now().UnixMilli()
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	// The "borrow from the future" overflow path can advance ms past
	// wall-clock by up to a few ticks under prior bursts; allow slack.
	if ms < before-2 || ms > after+8 {
		t.Errorf("uuidv7 ms %d outside [%d, %d]", ms, before, after)
	}
}

func TestUUIDv7_Monotonic(t *testing.T) {
	c := memDB(t)
	const n = 200
	ids := make([][]byte, n)
	for i := 0; i < n; i++ {
		ids[i] = selectBlob(t, c, "SELECT uuidv7()")
	}
	for i := 1; i < n; i++ {
		if bytes.Compare(ids[i-1], ids[i]) >= 0 {
			t.Fatalf("uuidv7 not strictly increasing at i=%d:\n  prev=%x\n  curr=%x",
				i, ids[i-1], ids[i])
		}
	}
}

func TestUUIDv7_Unique(t *testing.T) {
	c := memDB(t)
	const n = 1000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := selectBlob(t, c, "SELECT uuidv7()")
		k := string(id)
		if _, dup := seen[k]; dup {
			t.Fatalf("duplicate uuidv7 at i=%d: %x", i, id)
		}
		seen[k] = struct{}{}
	}
}

func partitionOf(id int64) int64 { return id >> genIDCounterBits }
func counterOf(id int64) int64   { return id & int64(genIDCounterMax) }

func TestGenID_BasicSequence(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE t (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('t')), v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := c.Exec(`INSERT INTO t(v) VALUES ('x')`); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	ids := collectInt64s(t, c, `SELECT id FROM t ORDER BY id`)
	if len(ids) != 4 {
		t.Fatalf("got %d rows, want 4", len(ids))
	}
	if ids[0] <= 0 {
		t.Fatalf("id[0] = %d, want > 0", ids[0])
	}
	partition := partitionOf(ids[0])
	for i, id := range ids {
		if partitionOf(id) != partition {
			t.Errorf("id[%d]=%#x in partition %d, want %d", i, id, partitionOf(id), partition)
		}
		if counterOf(id) != int64(i+1) {
			t.Errorf("id[%d] counter = %d, want %d", i, counterOf(id), i+1)
		}
	}
}

func TestGenID_INTEGERPrimaryKey(t *testing.T) {
	// INTEGER PRIMARY KEY (rowid alias) must work as well as INT PK.
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE u (id INTEGER PRIMARY KEY DEFAULT (gen_id('u'))) WITHOUT ROWID`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Exec(`INSERT INTO u DEFAULT VALUES`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	id := selectInt64(t, c, `SELECT id FROM u`)
	if id <= 0 {
		t.Fatalf("id = %d, want > 0", id)
	}
}

func TestGenID_PartitionAvoidsOccupied(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE p (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('p')))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Manually insert at a known partition so gen_id must avoid it.
	occupied := int64(5) << genIDCounterBits
	stmt, _, err := c.Prepare(`INSERT INTO p(id) VALUES (?)`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := stmt.BindInt64(1, occupied); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := stmt.Step(); err != nil {
		t.Fatalf("step: %v", err)
	}
	stmt.Finalize()
	if err := c.Exec(`INSERT INTO p DEFAULT VALUES`); err != nil {
		t.Fatalf("insert default: %v", err)
	}
	ids := collectInt64s(t, c, `SELECT id FROM p ORDER BY id`)
	if len(ids) != 2 {
		t.Fatalf("got %d ids, want 2: %v", len(ids), ids)
	}
	var other int64
	for _, id := range ids {
		if id != occupied {
			other = id
		}
	}
	if partitionOf(other) == 5 {
		t.Errorf("gen_id picked the occupied partition 5 (id=%#x)", other)
	}
	if counterOf(other) != 1 {
		t.Errorf("gen_id counter = %d, want 1", counterOf(other))
	}
}

func TestGenID_NoPrimaryKey(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE n (v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	stmt, _, err := c.Prepare(`SELECT gen_id('n')`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err == nil {
		t.Fatal("expected error for table without primary key")
	}
}

func TestGenID_CompositePrimaryKey(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE k (a INT, b INT, PRIMARY KEY(a, b))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	stmt, _, err := c.Prepare(`SELECT gen_id('k')`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	if _, err := stmt.Step(); err == nil {
		t.Fatal("expected error for composite primary key")
	}
}

func TestGenID_PartitionStableWithinConn(t *testing.T) {
	c := memDB(t)
	if err := c.Exec(`CREATE TABLE s (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('s')))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := c.Exec(`INSERT INTO s DEFAULT VALUES`); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	ids := collectInt64s(t, c, `SELECT id FROM s ORDER BY id`)
	if len(ids) != 20 {
		t.Fatalf("got %d ids, want 20", len(ids))
	}
	partition := partitionOf(ids[0])
	for i, id := range ids {
		if partitionOf(id) != partition {
			t.Fatalf("row %d: partition %d != stable partition %d", i, partitionOf(id), partition)
		}
	}
}

func TestGenID_FreshConnPicksFreshPartitionAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.db")

	c1, err := Open(path, 0)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	if err := c1.Exec(`CREATE TABLE f (id INT PRIMARY KEY NOT NULL DEFAULT (gen_id('f')))`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c1.Exec(`INSERT INTO f DEFAULT VALUES`); err != nil {
		t.Fatalf("insert1: %v", err)
	}
	id1 := selectInt64(t, c1, `SELECT id FROM f`)
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path, 0)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	t.Cleanup(func() { _ = c2.Close() })
	if err := c2.Exec(`INSERT INTO f DEFAULT VALUES`); err != nil {
		t.Fatalf("insert2: %v", err)
	}
	ids := collectInt64s(t, c2, `SELECT id FROM f ORDER BY id`)
	if len(ids) != 2 {
		t.Fatalf("got %d ids, want 2", len(ids))
	}
	if ids[0] != id1 && ids[1] != id1 {
		t.Errorf("c1's id %#x missing from c2 view %v", id1, ids)
	}
	if partitionOf(ids[0]) == partitionOf(ids[1]) {
		t.Fatalf("partitions overlap: id1=%#x id2=%#x", ids[0], ids[1])
	}
}
