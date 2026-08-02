package ltxstream_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/syzy/internal/ltxstream"
)

const ckPageSize = 512

// writeFakeDB writes a database-shaped file (page size stamped at the
// SQLite header offset) and returns the page contents.
func writeFakeDB(t *testing.T, path string, pages int) [][]byte {
	t.Helper()
	content := make([][]byte, pages)
	var buf bytes.Buffer
	for i := range content {
		page := bytes.Repeat([]byte{byte(0x11 * (i + 1))}, ckPageSize)
		if i == 0 {
			binary.BigEndian.PutUint16(page[16:18], ckPageSize)
		}
		content[i] = page
		buf.Write(page)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write fake db: %v", err)
	}
	return content
}

// rollingChecksum computes the LTX rolling checksum of a page set the
// way the format defines it (XOR of per-page checksums, lock page
// excluded).
func rollingChecksum(pages map[uint32][]byte) ltx.Checksum {
	sum := ltx.ChecksumFlag
	for pgno, data := range pages {
		sum = ltx.ChecksumFlag | (sum ^ ltx.ChecksumPage(pgno, data))
	}
	return sum
}

// TestChecksumStateTracksGroundTruth drives a seeded state through
// growth, overwrite, and shrink batches and checks every staged
// pre/post value against a from-scratch rolling checksum of the
// simulated database.
func TestChecksumStateTracksGroundTruth(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "src.db")
	content := writeFakeDB(t, dbPath, 3)

	var buf bytes.Buffer
	_, state, err := ltxstream.EncodeBaseline(context.Background(), &buf, dbPath, 1)
	if err != nil {
		t.Fatalf("EncodeBaseline: %v", err)
	}

	db := map[uint32][]byte{1: content[0], 2: content[1], 3: content[2]}
	if got, want := state.Checksum(), rollingChecksum(db); got != want {
		t.Fatalf("seeded checksum %s != ground truth %s", got, want)
	}

	apply := func(batch map[uint32][]byte, commit uint32) {
		t.Helper()
		st := state.Stage(batch, commit)
		if st.Pre != rollingChecksum(db) {
			t.Fatalf("stage pre %s != ground truth %s", st.Pre, rollingChecksum(db))
		}
		for pgno, data := range batch {
			db[pgno] = data
		}
		for pgno := range db {
			if pgno > commit {
				delete(db, pgno)
			}
		}
		if st.Post != rollingChecksum(db) {
			t.Fatalf("stage post %s != ground truth %s", st.Post, rollingChecksum(db))
		}
		st.Commit()
		if state.Checksum() != st.Post {
			t.Fatalf("committed state %s != staged post %s", state.Checksum(), st.Post)
		}
	}

	p := func(marker byte) []byte { return bytes.Repeat([]byte{marker}, ckPageSize) }
	apply(map[uint32][]byte{2: p(0xA1)}, 3)               // overwrite
	apply(map[uint32][]byte{4: p(0xA2), 5: p(0xA3)}, 5)   // growth
	apply(map[uint32][]byte{1: p(0xA4)}, 2)               // shrink to 2 pages
	apply(map[uint32][]byte{2: p(0xA5), 3: p(0xA6)}, 3)   // regrow

	// An abandoned stage must not move the state.
	before := state.Checksum()
	_ = state.Stage(map[uint32][]byte{1: p(0xEE)}, 3)
	if state.Checksum() != before {
		t.Fatalf("abandoned stage mutated state")
	}
}

// TestEncodeBaselineCarriesChecksums: the snapshot must not set
// HeaderFlagNoChecksum and its PostApplyChecksum must equal the seeded
// state, verified end-to-end by the LTX decoder.
func TestEncodeBaselineCarriesChecksums(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "src.db")
	writeFakeDB(t, dbPath, 3)

	var buf bytes.Buffer
	_, state, err := ltxstream.EncodeBaseline(context.Background(), &buf, dbPath, 7)
	if err != nil {
		t.Fatalf("EncodeBaseline: %v", err)
	}
	dec := ltx.NewDecoder(bytes.NewReader(buf.Bytes()))
	if err := dec.Verify(); err != nil {
		t.Fatalf("decoder verify (includes snapshot post-apply check): %v", err)
	}
	if dec.Header().NoChecksum() {
		t.Fatalf("baseline encoded with HeaderFlagNoChecksum")
	}
	if got := dec.Trailer().PostApplyChecksum; got != state.Checksum() {
		t.Fatalf("trailer post-apply %s != state %s", got, state.Checksum())
	}
}

// TestEncodeIncrementalFlagConsistency: attested headers require a
// post-apply checksum and unattested headers refuse one.
func TestEncodeIncrementalFlagConsistency(t *testing.T) {
	t.Parallel()
	pageMap := map[uint32][]byte{1: bytes.Repeat([]byte{1}, ckPageSize)}
	hdr := ltx.Header{
		Version: ltx.Version, PageSize: ckPageSize, Commit: 1,
		MinTXID: 2, MaxTXID: 2, Timestamp: time.Now().UnixMilli(),
		PreApplyChecksum: ltx.ChecksumFlag | 1,
	}
	var buf bytes.Buffer
	if err := ltxstream.EncodeIncremental(context.Background(), &buf, pageMap, hdr, 0); err == nil {
		t.Fatalf("attested header without post-apply checksum accepted")
	}
	hdr.Flags = ltx.HeaderFlagNoChecksum
	hdr.PreApplyChecksum = 0
	if err := ltxstream.EncodeIncremental(context.Background(), &buf, pageMap, hdr, ltx.ChecksumFlag|1); err == nil {
		t.Fatalf("NoChecksum header with post-apply checksum accepted")
	}
}

// TestTailer_EmitsAttestedLTX drives a real SQLite WAL: baseline the
// checkpointed database, install the seeded state, commit more
// transactions, and verify the emitted LTX carries checksums that
// match the database state materialized by applying its pages onto
// the baseline copy.
func TestTailer_EmitsAttestedLTX(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	walPath := dbPath + "-wal"

	conn := openWAL(t, dbPath)
	defer conn.Close()
	mustExec(t, conn, `CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`)
	mustExec(t, conn, `INSERT INTO t VALUES (1, 'one')`)
	mustExec(t, conn, `PRAGMA wal_checkpoint(TRUNCATE)`)

	// Baseline the quiesced database and keep a copy to apply onto.
	basePath := filepath.Join(dir, "base.db")
	raw, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	if err := os.WriteFile(basePath, raw, 0o644); err != nil {
		t.Fatalf("copy db: %v", err)
	}
	var baseBuf bytes.Buffer
	_, state, err := ltxstream.EncodeBaseline(context.Background(), &baseBuf, basePath, 1)
	if err != nil {
		t.Fatalf("EncodeBaseline: %v", err)
	}
	baseChecksum := state.Checksum()

	mustExec(t, conn, `INSERT INTO t VALUES (2, 'two')`)
	mustExec(t, conn, `INSERT INTO t VALUES (3, 'three')`)

	var caught []struct {
		hdr  ltx.Header
		body []byte
	}
	var nextTXID atomic.Uint64
	nextTXID.Store(1) // baseline took txid 1
	tailer := ltxstream.New(ltxstream.Config{
		WALPath:  walPath,
		NextTXID: func() uint64 { return nextTXID.Add(1) },
		OnLTX: func(_ context.Context, hdr ltx.Header, body []byte) error {
			caught = append(caught, struct {
				hdr  ltx.Header
				body []byte
			}{hdr, append([]byte(nil), body...)})
			return nil
		},
		SyncInterval: time.Hour,
	}, ltxstream.Position{})
	tailer.SetChecksumState(state)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tailer.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if len(caught) != 1 {
		t.Fatalf("expected 1 LTX, got %d", len(caught))
	}
	c := caught[0]
	if c.hdr.NoChecksum() {
		t.Fatalf("emitted LTX carries HeaderFlagNoChecksum despite installed state")
	}
	if c.hdr.PreApplyChecksum != baseChecksum {
		t.Fatalf("pre-apply %s != baseline state %s", c.hdr.PreApplyChecksum, baseChecksum)
	}

	// Apply the LTX pages onto the baseline copy and compare the
	// resulting rolling checksum with the trailer's attestation.
	dec := ltx.NewDecoder(bytes.NewReader(c.body))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode header: %v", err)
	}
	pageSize := int(dec.Header().PageSize)
	base, err := os.OpenFile(basePath, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open base copy: %v", err)
	}
	defer base.Close()
	page := make([]byte, pageSize)
	var ph ltx.PageHeader
	for {
		if err := dec.DecodePage(&ph, page); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode page: %v", err)
		}
		if _, err := base.WriteAt(page, int64(ph.Pgno-1)*int64(pageSize)); err != nil {
			t.Fatalf("apply page %d: %v", ph.Pgno, err)
		}
	}
	if err := dec.Close(); err != nil {
		t.Fatalf("decoder close: %v", err)
	}
	commit := dec.Header().Commit
	materialized := make(map[uint32][]byte, commit)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		buf := make([]byte, pageSize)
		if _, err := base.ReadAt(buf, int64(pgno-1)*int64(pageSize)); err != nil {
			t.Fatalf("read materialized page %d: %v", pgno, err)
		}
		materialized[pgno] = buf
	}
	if got, want := rollingChecksum(materialized), dec.Trailer().PostApplyChecksum; got != want {
		t.Fatalf("materialized state %s != attested post-apply %s", got, want)
	}
	if state.Checksum() != dec.Trailer().PostApplyChecksum {
		t.Fatalf("committed state %s != emitted post-apply %s", state.Checksum(), dec.Trailer().PostApplyChecksum)
	}
}
