package testcluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport/memtransport"
)

// blobSchema declares a row keyed by integer id with a BLOB body
// column. Rows can be mutated via INSERT/UPDATE or via
// sqlite3_blob_write — the latter is what this test exercises.
const blobSchema = `CREATE TABLE blobrow (
  id   INTEGER PRIMARY KEY,
  body BLOB
)`

// TestBlobPatchConvergence exercises the end-to-end blob_patch path:
// node A INSERTs a row with a 16-byte blob, then mutates a 4-byte
// sub-range via sqlite3_blob_write. Node B should converge to the
// post-write content via the broker apply op=4 branch — proving
// capture (preupdate_blobwrite + diff), encode (BlobPatch wire
// format), and apply (ensureRow + ensureBlobLen + IntervalMap.Apply
// + sqlite3_blob_write).
func TestBlobPatchConvergence(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, blobSchema, 0)
	b := NewWithCache(t, hub, 2, blobSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	// Seed: insert a row with a 16-byte zero-padded blob.
	body0 := bytes.Repeat([]byte{0xAA}, 16)
	if err := execBindBlob(a.AppWrite, `INSERT INTO blobrow (id, body) VALUES (1, ?)`, body0); err != nil {
		t.Fatalf("A INSERT: %v", err)
	}
	b.WaitApplied(t, a.Origin, 1, time.Second)
	if got := readBlob(t, b.Read, 1); !bytes.Equal(got, body0) {
		t.Fatalf("after INSERT: B blob = %x; want %x", got, body0)
	}

	// Mutate bytes [4..8) on A via sqlite3_blob_write. Wrap in an
	// explicit transaction: wal_hook fires only on commit, and
	// sqlite3_blob_close does not auto-commit the implicit write
	// transaction it begins (a long-standing SQLite quirk that
	// applies to incremental BLOB I/O). Callers who want their
	// blob_writes to replicate must commit them explicitly.
	patch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := a.AppWrite.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	bh, err := a.AppWrite.OpenBlob("main", "blobrow", "body", 1, true)
	if err != nil {
		t.Fatalf("OpenBlob A: %v", err)
	}
	if err := bh.Write(patch, 4); err != nil {
		bh.Close()
		t.Fatalf("Write blob A: %v", err)
	}
	if err := bh.Close(); err != nil {
		t.Fatalf("Close blob A: %v", err)
	}
	if err := a.AppWrite.Exec(`COMMIT`); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	// Verify the writer's app.db reflects the patch.
	want := append([]byte{}, body0...)
	copy(want[4:], patch)
	if got := readBlob(t, a.AppWrite, 1); !bytes.Equal(got, want) {
		t.Fatalf("A post-blob_write: %x; want %x", got, want)
	}

	// Wait for the patch to drain into the changeset and apply on B.
	b.WaitApplied(t, a.Origin, 2, 2*time.Second)
	if got := readBlob(t, b.Read, 1); !bytes.Equal(got, want) {
		t.Fatalf("after blob_write: B blob = %x; want %x", got, want)
	}
}

// TestBlobPatchConcurrentDisjointConverges proves that two nodes
// patching disjoint byte ranges of the same row converge to a merged
// state under per-range LWW. After A patches [0..4) and B patches
// [12..16), each side sees both writes.
func TestBlobPatchConcurrentDisjointConverges(t *testing.T) {
	hub := memtransport.NewHub()
	defer hub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, blobSchema, 0)
	b := NewWithCache(t, hub, 2, blobSchema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	// Seed identical row on A.
	body0 := bytes.Repeat([]byte{0x55}, 16)
	if err := execBindBlob(a.AppWrite, `INSERT INTO blobrow (id, body) VALUES (1, ?)`, body0); err != nil {
		t.Fatalf("A INSERT: %v", err)
	}
	b.WaitApplied(t, a.Origin, 1, time.Second)

	// A patches the prefix; B patches the suffix. Issued back-to-back
	// — B's patch sees A's INSERT but not A's patch, and vice versa
	// (the test does not synchronize the two writers).
	patchA := []byte{0xA1, 0xA2, 0xA3, 0xA4}
	if err := writeBlobInTxn(a.AppWrite, "blobrow", "body", 1, 0, patchA); err != nil {
		t.Fatalf("A blob_write: %v", err)
	}
	patchB := []byte{0xB1, 0xB2, 0xB3, 0xB4}
	if err := writeBlobInTxn(b.AppWrite, "blobrow", "body", 1, 12, patchB); err != nil {
		t.Fatalf("B blob_write: %v", err)
	}

	// Wait for cross-replication.
	a.WaitApplied(t, b.Origin, 1, 2*time.Second)
	b.WaitApplied(t, a.Origin, 2, 2*time.Second)

	want := append([]byte{}, body0...)
	copy(want[0:], patchA)
	copy(want[12:], patchB)

	if got := readBlob(t, a.Read, 1); !bytes.Equal(got, want) {
		t.Errorf("A converged blob = %x; want %x", got, want)
	}
	if got := readBlob(t, b.Read, 1); !bytes.Equal(got, want) {
		t.Errorf("B converged blob = %x; want %x", got, want)
	}
}

// writeBlobInTxn wraps an incremental blob write in BEGIN IMMEDIATE /
// COMMIT — sqlite3_blob_close does not auto-commit the implicit txn
// it begins, so explicit commit is required for wal_hook (and thus
// replication) to fire.
func writeBlobInTxn(c *sqlitebridge.Conn, table, column string, rowid int64, offset int, p []byte) error {
	if err := c.Exec(`BEGIN IMMEDIATE`); err != nil {
		return err
	}
	bh, err := c.OpenBlob("main", table, column, rowid, true)
	if err != nil {
		_ = c.Exec(`ROLLBACK`)
		return err
	}
	if err := bh.Write(p, offset); err != nil {
		_ = bh.Close()
		_ = c.Exec(`ROLLBACK`)
		return err
	}
	if err := bh.Close(); err != nil {
		_ = c.Exec(`ROLLBACK`)
		return err
	}
	return c.Exec(`COMMIT`)
}

func execBindBlob(c *sqlitebridge.Conn, sql string, blob []byte) error {
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	if err := stmt.BindBlob(1, blob); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

func readBlob(t testing.TB, c *sqlitebridge.Conn, id int64) []byte {
	t.Helper()
	stmt, _, err := c.Prepare(`SELECT body FROM blobrow WHERE id = ?`)
	if err != nil {
		t.Fatalf("Prepare readBlob: %v", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, id); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if !hasRow {
		return nil
	}
	return stmt.ColumnBlob(0)
}
