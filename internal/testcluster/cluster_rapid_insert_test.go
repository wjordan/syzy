package testcluster

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/wjordan/syzy/transport/memtransport"
)

func hexBytes(b []byte) string { return hex.EncodeToString(b) }

// TestRapidConsecutiveInsertsConverge regression-tests an apply-time
// row-loss bug: three rapid INSERTs into a (PK + BLOB-value) table
// from one writer materialized only the last record's row on
// followers — the first two records' value/PK pairs both got
// rewritten to the third record's column data.
//
// Root cause: the producer's syncer reused the journalRecs parser
// scratch across records in one drain batch, and recordEvidence.image
// held the slice header from rec.Values. The next parseJournal call
// overwrote that backing array with the next record's columns, so by
// the time buildRecords / applyRecord ran, every evidence.image
// pointed to the last-parsed record's column data. The applyInsert
// statement is cached and binds slots positionally; once Image
// rewrote the PK column slot to the last record's key, all three
// applies effectively upserted the same target row.
//
// Fix lives in internal/syncer/materialize.go: clone the ColValue
// slice header into evidenceForTouched's returned recordEvidence. The
// per-ColValue Bytes still alias into the stable journal mmap so no
// deep-copy is needed for the bytes themselves.
func TestRapidConsecutiveInsertsConverge(t *testing.T) {
	const schema = `CREATE TABLE cert_store (
		key   TEXT PRIMARY KEY NOT NULL,
		value BLOB NOT NULL
	)`

	hub := memtransport.NewHub()
	defer hub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := NewWithCache(t, hub, 1, schema, 0)
	b := NewWithCache(t, hub, 2, schema, 0)
	a.Start(t, ctx)
	b.Start(t, ctx)

	rows := []struct {
		key   string
		value []byte
	}{
		{"certs/wildcard.crt", []byte("CERT-PEM-BYTES")},
		{"certs/wildcard.json", []byte(`{"sans":["*.example.dev"]}`)},
		{"certs/wildcard.key", []byte("KEY-PEM-BYTES")},
	}
	for _, r := range rows {
		if err := a.AppWrite.Exec(`INSERT INTO cert_store (key, value) VALUES ('` + r.key + `', x'` + hexBytes(r.value) + `')`); err != nil {
			t.Fatalf("exec %q: %v", r.key, err)
		}
	}

	b.WaitApplied(t, a.Origin, 3, 2*time.Second)

	for _, r := range rows {
		stmt, _, err := b.Read.Prepare(`SELECT value FROM cert_store WHERE key = ?`)
		if err != nil {
			t.Fatalf("prepare select: %v", err)
		}
		if err := stmt.BindText(1, r.key); err != nil {
			t.Fatalf("bind: %v", err)
		}
		ok, err := stmt.Step()
		if err != nil {
			t.Fatalf("step %q: %v", r.key, err)
		}
		if !ok {
			t.Errorf("row %q missing on B", r.key)
			stmt.Finalize()
			continue
		}
		got := stmt.ColumnBlob(0)
		if string(got) != string(r.value) {
			t.Errorf("key=%q: got value %q, want %q", r.key, got, r.value)
		}
		stmt.Finalize()
	}
}
