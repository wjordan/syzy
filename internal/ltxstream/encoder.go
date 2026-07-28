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
// LTX headers carry HeaderFlagNoChecksum (matching Litestream); per-LTX
// integrity is provided by the trailer's FileChecksum, and per-WAL-frame
// integrity by SQLite's WAL checksum chain (validated by WALReader).
//
// pageSize and commit are the WAL's reported page size and post-commit
// DB size in pages. walFile must be open (read-only); pageMap[pgno]
// points at the start of the WAL frame (frame-header included; the
// page payload starts at offset+WALFrameHeaderSize).
func EncodeIncremental(
	ctx context.Context,
	w io.Writer,
	walFile *os.File,
	pageMap map[uint32]int64,
	pageSize uint32,
	commit uint32,
	minTXID, maxTXID uint64,
) error {
	if len(pageMap) == 0 {
		return fmt.Errorf("ltxstream: empty page map")
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return fmt.Errorf("ltx encoder: %w", err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   ltx.TXID(minTXID),
		MaxTXID:   ltx.TXID(maxTXID),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	pgnos := make([]uint32, 0, len(pageMap))
	for p := range pageMap {
		pgnos = append(pgnos, p)
	}
	slices.Sort(pgnos)

	page := make([]byte, pageSize)
	for _, pgno := range pgnos {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		off := pageMap[pgno]
		if n, err := walFile.ReadAt(page, off+WALFrameHeaderSize); err != nil {
			return fmt.Errorf("read page %d @ %d: %w", pgno, off, err)
		} else if n != len(page) {
			return fmt.Errorf("short read page %d @ %d", pgno, off)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			return fmt.Errorf("encode page %d: %w", pgno, err)
		}
	}

	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder: %w", err)
	}
	return nil
}

// EncodeBaseline writes a snapshot LTX (MinTXID=1, every page present,
// HeaderFlagNoChecksum) for the database at dbPath. Used by the
// publisher to lay down the foundation that L0 deltas apply on top of.
// Reads pages directly from the file; caller is responsible for
// ensuring the file is consistent (typically a sqlite3_backup pinned
// copy).
func EncodeBaseline(ctx context.Context, w io.Writer, dbPath string, txid uint64) (commit uint32, err error) {
	dbFile, err := os.Open(dbPath)
	if err != nil {
		return 0, err
	}
	defer dbFile.Close()

	hdr := make([]byte, 100)
	if _, err := dbFile.ReadAt(hdr, 0); err != nil {
		return 0, fmt.Errorf("read sqlite header: %w", err)
	}
	pageSize := uint32(hdr[16])<<8 | uint32(hdr[17])
	if pageSize == 1 {
		pageSize = 65536 // SQLite encodes 65536 as 1
	}
	if !ltx.IsValidPageSize(pageSize) {
		return 0, fmt.Errorf("invalid page size %d in sqlite header", pageSize)
	}
	stat, err := dbFile.Stat()
	if err != nil {
		return 0, err
	}
	commit = uint32(stat.Size() / int64(pageSize))
	if commit == 0 {
		return 0, fmt.Errorf("empty database file")
	}

	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return 0, fmt.Errorf("ltx encoder: %w", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   1,
		MaxTXID:   ltx.TXID(txid),
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		return 0, fmt.Errorf("encode header: %w", err)
	}

	lockPgno := ltx.LockPgno(pageSize)
	page := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		off := int64(pgno-1) * int64(pageSize)
		if _, err := dbFile.ReadAt(page, off); err != nil {
			return 0, fmt.Errorf("read page %d: %w", pgno, err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			return 0, fmt.Errorf("encode page %d: %w", pgno, err)
		}
	}
	if err := enc.Close(); err != nil {
		return 0, fmt.Errorf("close encoder: %w", err)
	}
	return commit, nil
}
