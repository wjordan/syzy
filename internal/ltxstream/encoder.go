package ltxstream

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/superfly/ltx"
)

// EncodeIncremental writes an L0-style LTX file: only the pages listed
// in pageMap, sourced from the WAL. The LTX file's bytes go to w.
//
// hdr is the exact header to encode (the caller owns TXID range,
// commit, flags, and PreApplyChecksum). When hdr tracks checksums,
// postApply must carry the database's rolling checksum after the batch
// applies; when hdr carries HeaderFlagNoChecksum (no ChecksumState
// available), postApply must be zero. The trailer's FileChecksum is
// always present either way.
//
// pageMap contains page bytes copied while their WAL frames were
// checksum-verified. Encoding must not reread live WAL offsets: an
// uncoordinated checkpoint can recycle those offsets between the scan
// and encode phases.
func EncodeIncremental(
	ctx context.Context,
	w io.Writer,
	pageMap map[uint32][]byte,
	hdr ltx.Header,
	postApply ltx.Checksum,
) error {
	if len(pageMap) == 0 {
		return fmt.Errorf("ltxstream: empty page map")
	}
	if hdr.NoChecksum() != (postApply == 0) {
		return fmt.Errorf("ltxstream: post-apply checksum %s inconsistent with header flags %#x", postApply, hdr.Flags)
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return fmt.Errorf("ltx encoder: %w", err)
	}

	if err := enc.EncodeHeader(hdr); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	pgnos := make([]uint32, 0, len(pageMap))
	for p := range pageMap {
		pgnos = append(pgnos, p)
	}
	slices.Sort(pgnos)

	for _, pgno := range pgnos {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		data := pageMap[pgno]
		if len(data) != int(hdr.PageSize) {
			return fmt.Errorf("page %d has %d bytes, want %d", pgno, len(data), hdr.PageSize)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}
	}

	if postApply != 0 {
		enc.SetPostApplyChecksum(postApply)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder: %w", err)
	}
	return nil
}

// EncodeBaseline writes a snapshot LTX (MinTXID=1, every page present)
// for the database at dbPath. Used by the publisher to lay down the
// foundation that L0 deltas apply on top of. Reads pages directly from
// the file; caller is responsible for ensuring the file is consistent
// (typically a sqlite3_backup pinned copy).
//
// The snapshot carries the database's rolling checksum as its
// PostApplyChecksum, and the returned ChecksumState is seeded from the
// same pass: install it into the stream's tailer so subsequent L0s
// carry pre/post-apply checksums chained from this baseline.
func EncodeBaseline(ctx context.Context, w io.Writer, dbPath string, txid uint64) (commit uint32, state *ChecksumState, err error) {
	dbFile, err := os.Open(dbPath)
	if err != nil {
		return 0, nil, err
	}
	defer dbFile.Close()

	hdr := make([]byte, 100)
	if _, err := dbFile.ReadAt(hdr, 0); err != nil {
		return 0, nil, fmt.Errorf("read sqlite header: %w", err)
	}
	pageSize := uint32(hdr[16])<<8 | uint32(hdr[17])
	if pageSize == 1 {
		pageSize = 65536 // SQLite encodes 65536 as 1
	}
	if !ltx.IsValidPageSize(pageSize) {
		return 0, nil, fmt.Errorf("invalid page size %d in sqlite header", pageSize)
	}
	stat, err := dbFile.Stat()
	if err != nil {
		return 0, nil, err
	}
	commit = uint32(stat.Size() / int64(pageSize))
	if commit == 0 {
		return 0, nil, fmt.Errorf("empty database file")
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return 0, nil, fmt.Errorf("ltx encoder: %w", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   1,
		MaxTXID:   ltx.TXID(txid),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		return 0, nil, fmt.Errorf("encode header: %w", err)
	}

	state = &ChecksumState{
		pages:    make([]ltx.Checksum, commit),
		pageSize: pageSize,
		chksum:   ltx.ChecksumFlag,
	}
	lockPgno := ltx.LockPgno(pageSize)
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		default:
		}
		off := int64(pgno-1) * int64(pageSize)
		if _, err := dbFile.ReadAt(page, off); err != nil {
			return 0, nil, fmt.Errorf("read page %d: %w", pgno, err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			return 0, nil, fmt.Errorf("encode page %d: %w", pgno, err)
		}
		c := ltx.ChecksumPage(pgno, page)
		state.pages[pgno-1] = c
		state.chksum = ltx.ChecksumFlag | (state.chksum ^ c)
	}
	enc.SetPostApplyChecksum(state.chksum)
	if err := enc.Close(); err != nil {
		return 0, nil, fmt.Errorf("close encoder: %w", err)
	}
	return commit, state, nil
}
