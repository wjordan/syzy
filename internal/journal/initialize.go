package journal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type segmentInitOps struct {
	syncFile func(*os.File) error
	link     func(string, string) error
	syncDir  func(string) error
}

// initializeSegment publishes only a complete, durable header, including under
// SyncOff. The final name never identifies an initialization in progress and a
// competing creator cannot replace an existing segment. Record appends retain
// their configured sync policy.
func initializeSegment(path string, size uint32, ops segmentInitOps) (*os.File, error) {
	if size < minSegmentSize {
		return nil, fmt.Errorf("journal: segmentSize %d too small (min %d)", size, minSegmentSize)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".segment-init-*")
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
		}
		_ = os.Remove(f.Name())
	}()
	var salt [16]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(size)); err != nil {
		return nil, err
	}
	header := encodeFileHeader(fileHeader{
		Magic: magic, Version: formatVersion, SegmentSize: size,
		CreatedUs: uint64(time.Now().UnixMicro()),
		Salt0:     binary.LittleEndian.Uint64(salt[:8]), Salt1: binary.LittleEndian.Uint64(salt[8:]),
	})
	if n, err := f.WriteAt(header[:], 0); err != nil {
		return nil, err
	} else if n != len(header) {
		return nil, io.ErrShortWrite
	}
	if err := ops.syncFile(f); err != nil {
		return nil, fmt.Errorf("journal: initialize segment sync: %w", err)
	}
	if err := ops.link(f.Name(), path); errors.Is(err, os.ErrExist) {
		if err := ops.syncDir(filepath.Dir(path)); err != nil {
			return nil, fmt.Errorf("journal: competing publication directory sync: %w", err)
		}
		return os.OpenFile(path, os.O_RDWR, 0)
	} else if err != nil {
		return nil, fmt.Errorf("journal: publish segment: %w", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		return nil, err
	}
	if err := ops.syncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("journal: publish directory sync: %w", err)
	}
	keep = true
	return f, nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncJournalDirectories(path string) error {
	return syncDirectoryAncestors(path, syncDirectory)
}

// A parent created by the caller (or a competing opener) can already exist
// without being durable. Sync the full path, not just directories created by
// this call, so the initialized segment remains reachable after a crash.
func syncDirectoryAncestors(path string, syncDir func(string) error) error {
	dir, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, start := range []string{real, dir} {
		for dir := start; !seen[dir]; dir = filepath.Dir(dir) {
			if err := syncDir(dir); err != nil {
				return err
			}
			seen[dir] = true
		}
	}
	return nil
}
