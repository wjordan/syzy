package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	syzy "github.com/wjordan/syzy/sqlite"
)

func TestDriverCRUD(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, weight REAL, payload BLOB)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO t (id, name, weight, payload) VALUES (?, ?, ?, ?)`,
		int64(1), "alice", 1.5, []byte{0x01, 0x02, 0x03},
	); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO t (id, name, weight, payload) VALUES (?, ?, ?, ?)`,
		int64(2), "bob", 2.5, nil,
	); err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	rows, err := db.Query(`SELECT id, name, weight, payload FROM t ORDER BY id`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	type row struct {
		id     int64
		name   string
		weight float64
		blob   []byte
	}
	var got []row
	for rows.Next() {
		var r row
		var blobHolder sql.RawBytes
		if err := rows.Scan(&r.id, &r.name, &r.weight, &blobHolder); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		r.blob = append([]byte(nil), blobHolder...)
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}

	want := []row{
		{1, "alice", 1.5, []byte{0x01, 0x02, 0x03}},
		{2, "bob", 2.5, nil},
	}
	if len(got) != len(want) {
		t.Fatalf("rows: got %d, want %d", len(got), len(want))
	}
	for i, r := range got {
		w := want[i]
		if r.id != w.id || r.name != w.name || r.weight != w.weight {
			t.Errorf("row %d: got %+v want %+v", i, r, w)
		}
		if (r.blob == nil) != (w.blob == nil) || string(r.blob) != string(w.blob) {
			t.Errorf("row %d blob: got %v want %v", i, r.blob, w.blob)
		}
	}
}

func TestDriverTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (1, 100)`); err != nil {
		t.Fatalf("INSERT in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var n int64
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if n != 0 {
		t.Fatalf("after rollback: count = %d, want 0", n)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (2, 200)`); err != nil {
		t.Fatalf("INSERT in tx 2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count after commit: %v", err)
	}
	if n != 1 {
		t.Fatalf("after commit: count = %d, want 1", n)
	}
}

func TestDBFacadesShareOneWriterPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	a := syzy.NewDB(node)
	b := syzy.NewDB(node)
	defer a.Close()
	defer b.Close()

	if _, err := a.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	tx, err := a.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (1, 100)`); err != nil {
		t.Fatalf("tx insert: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := b.Exec(`INSERT INTO t VALUES (2, 200)`)
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("second facade wrote while first facade held writer tx: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second facade insert: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second facade did not resume after commit")
	}

	var n int64
	if err := a.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
}

func TestDBFacadesConcurrentUse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for g := 0; g < 4; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := syzy.NewDB(node)
			for i := 0; i < 25; i++ {
				id := int64(g*100 + i)
				if _, err := local.Exec(`INSERT INTO t (id, v) VALUES (?, ?)`, id, id*10); err != nil {
					errCh <- err
					return
				}
				var n int64
				if err := local.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent facade use: %v", err)
		}
	}

	var n int64
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("final count: %v", err)
	}
	if n != 100 {
		t.Fatalf("final count = %d, want 100", n)
	}
}

func TestDBBlobWriteAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE blobrow (id INTEGER PRIMARY KEY, body BLOB NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO blobrow (id, body) VALUES (?, zeroblob(4))`, int64(1)); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.BlobWriteAt("blobrow", "body", 1, 1, []byte{0xAA, 0xBB}); err != nil {
		t.Fatalf("BlobWriteAt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var got []byte
	if err := db.QueryRow(`SELECT body FROM blobrow WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("Query body: %v", err)
	}
	want := []byte{0x00, 0xAA, 0xBB, 0x00}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = % x; want % x", got, want)
	}
}

// TestBlobWriteAtIntentJournalCompact verifies that tx.BlobWriteAt
// against a row with a multi-megabyte blob column produces a journal
// payload proportional to the write size, not the column size. This
// is the path that turns the BLOB_WRITE preupdate's full-OLD capture
// (a panic-inducing ~MB on the SyzyFS heartbeat in production) into a
// compact SYZY_OP_BLOB_INTENT record.
func TestBlobWriteAtIntentJournalCompact(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE blobrow (id INTEGER PRIMARY KEY, body BLOB NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	const colSize = 1 << 21 // 2 MiB — well past the default 1 MiB segment.
	if _, err := db.Exec(`INSERT INTO blobrow (id, body) VALUES (?, zeroblob(?))`, int64(1), colSize); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	patch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := tx.BlobWriteAt("blobrow", "body", 1, 100, patch); err != nil {
		t.Fatalf("BlobWriteAt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Sanity-check the bytes landed.
	var got []byte
	if err := db.QueryRow(`SELECT body FROM blobrow WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("Query body: %v", err)
	}
	if len(got) != colSize {
		t.Fatalf("body len = %d; want %d", len(got), colSize)
	}
	if !bytes.Equal(got[100:104], patch) {
		t.Fatalf("body[100:104] = % x; want % x", got[100:104], patch)
	}

	// The producer's self-journal records the encoded changeset, not the
	// touch buffer. The relevant size assertion is on that record: it
	// should be small enough to fit comfortably in a default segment,
	// nowhere near the 2 MiB column.
	last := lastJournalPayloadLen(t, dbPath)
	if last >= 1<<19 { // 512 KiB ceiling — far above intent record (~64B) but well below column.
		t.Fatalf("last journal payload = %d bytes; want compact (< 512 KiB) — OLD-image likely leaked through", last)
	}
}

// TestSyzyBlobWriteSQLFunc verifies the SQL function surface — same
// intent path as the Go API but reachable from any caller that can run
// SQL on the producer connection (e.g., loadable-extension consumers
// who don't go through the Go wrapper).
func TestSyzyBlobWriteSQLFunc(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	node, err := syzy.Open(ctx, syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	db := syzy.NewDB(node)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE blobrow (id INTEGER PRIMARY KEY, body BLOB NOT NULL)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	const colSize = 1 << 21 // 2 MiB
	if _, err := db.Exec(`INSERT INTO blobrow (id, body) VALUES (?, zeroblob(?))`, int64(1), colSize); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	patch := []byte{0x11, 0x22, 0x33, 0x44, 0x55}
	if _, err := db.Exec(`BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec(`SELECT syzy_blob_write(?, ?, ?, ?, ?)`,
		"blobrow", int64(1), "body", int64(50), patch); err != nil {
		t.Fatalf("syzy_blob_write: %v", err)
	}
	if _, err := db.Exec(`COMMIT`); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	var got []byte
	if err := db.QueryRow(`SELECT body FROM blobrow WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("Query body: %v", err)
	}
	if len(got) != colSize {
		t.Fatalf("body len = %d; want %d", len(got), colSize)
	}
	if !bytes.Equal(got[50:55], patch) {
		t.Fatalf("body[50:55] = % x; want % x", got[50:55], patch)
	}

	last := lastJournalPayloadLen(t, dbPath)
	if last >= 1<<19 {
		t.Fatalf("last journal payload = %d bytes; want compact (< 512 KiB) — OLD-image likely leaked through", last)
	}
}

func lastJournalPayloadLen(t *testing.T, dbPath string) int {
	t.Helper()
	root := layout.OriginsRoot(dbPath)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read origins: %v", err)
	}
	var lastLen int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		j, err := journal.Open(filepath.Join(root, e.Name(), "journal"), 0, journal.SyncOff)
		if err != nil {
			t.Fatalf("open journal %s: %v", e.Name(), err)
		}
		it := j.Iterate(0)
		for {
			rec, _, err := it.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
				break
			}
			if err != nil {
				_ = j.Close()
				t.Fatalf("iterate journal %s: %v", e.Name(), err)
			}
			if rec.Kind == journal.KindLocalDML {
				lastLen = len(rec.Payload)
			}
		}
		if err := j.Close(); err != nil {
			t.Fatalf("close journal %s: %v", e.Name(), err)
		}
	}
	if lastLen == 0 {
		t.Fatal("no journal payloads found")
	}
	return lastLen
}
