package sqlite

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// sqliteMagic is the 16-byte header prefix of every valid SQLite database.
var sqliteMagic = []byte("SQLite format 3\x00")

// verifyDBHeader fails when path holds a materialized database whose first
// page no longer begins with the SQLite magic — the signature of the page-1
// corruption class where a checkpoint backfills zeros (or foreign bytes)
// over the header. Absent files and files smaller than one page pass: a
// WAL-mode database legitimately stays empty until its first checkpoint.
//
// Callers gate two boundaries with this check, both fail-closed:
//   - checkpoint fences, so a corrupted header halts WAL recycling loudly
//     instead of publishing over it;
//   - staged baseline files, so a corrupted page 1 can never be encoded
//     into a published baseline.
func verifyDBHeader(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < 4096 {
		return nil
	}
	var hdr [16]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return err
	}
	if !bytes.Equal(hdr[:], sqliteMagic) {
		return fmt.Errorf("syzy: %s: database page 1 header invalid (first 16 bytes %x); refusing to proceed", path, hdr[:])
	}
	return nil
}
