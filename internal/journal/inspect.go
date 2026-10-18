package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// InspectZeroSegment reads raw bytes without invoking journal recovery, mmap,
// or any file creation. Only a full-size, entirely uninitialized segment is
// eligible; a zero header with any nonzero payload is not an empty segment.
func InspectZeroSegment(path string) (os.FileInfo, string, error) {
	f, err := openInspectionFile(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	if !before.Mode().IsRegular() || before.Size() < minSegmentSize || before.Size() > int64(^uint32(0)) {
		return nil, "", fmt.Errorf("not a full-size regular segment: %d bytes, %s", before.Size(), before.Mode())
	}
	hash := sha256.New()
	var buf [32768]byte
	var read int64
	reader := io.LimitReader(f, before.Size()+1)
	for {
		n, err := reader.Read(buf[:])
		for _, b := range buf[:n] {
			if b != 0 {
				return nil, "", fmt.Errorf("segment contains nonzero bytes; refusing empty-segment repair")
			}
		}
		_, _ = hash.Write(buf[:n])
		read += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
	}
	after, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	if read != before.Size() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, "", fmt.Errorf("segment changed during inspection")
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, current) {
		return nil, "", fmt.Errorf("segment path changed during inspection")
	}
	return before, hex.EncodeToString(hash.Sum(nil)), nil
}
