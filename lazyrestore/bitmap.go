//go:build linux

// Package lazyrestore prepares sparse Syzy databases and exposes them through
// a FUSE mount that fetches missing SQLite pages from object storage. Other
// files beside the database retain loopback behavior.
package lazyrestore

import (
	"fmt"
	"math/bits"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// pageBitmap tracks per-page presence in a sparse backing file. One bit
// per SQLite page; 0 = hole (must be fetched on demand), 1 = on disk.
// All mutations are atomic so the FUSE handler can race a concurrent
// reader/writer touching the same page without a per-page lock.
type pageBitmap struct {
	bits []atomic.Uint64
	pgs  uint32
}

// newPageBitmap allocates a pageBitmap for commitPages pages, all bits zero.
func newPageBitmap(commitPages uint32) *pageBitmap {
	words := (commitPages + 63) / 64
	return &pageBitmap{
		bits: make([]atomic.Uint64, words),
		pgs:  commitPages,
	}
}

// newPageBitmapFromFile allocates a pageBitmap for commitPages pages and
// seeds presence by walking the backing fd with lseek(SEEK_DATA) /
// lseek(SEEK_HOLE). Any page wholly inside a data extent is marked
// present. Used at remount so pages from a prior run don't have to
// re-fetch.
//
// Requires page_size >= fs_block_size for accuracy; the caller is
// responsible for enforcing that invariant (Prepare does so
// before producing the manifest). Without it, SEEK_DATA's block-
// granularity boundaries would falsely advertise neighboring
// pages as present.
func newPageBitmapFromFile(fd int, commitPages uint32, pageSize uint32) (*pageBitmap, error) {
	b := newPageBitmap(commitPages)
	if commitPages == 0 {
		return b, nil
	}
	totalBytes := int64(commitPages) * int64(pageSize)
	var off int64
	for off < totalBytes {
		dataOff, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			// ENXIO = no more data past off; the rest is hole.
			if err == unix.ENXIO {
				break
			}
			return nil, fmt.Errorf("lazyrestore: SEEK_DATA from %d: %w", off, err)
		}
		if dataOff >= totalBytes {
			break
		}
		holeOff, err := unix.Seek(fd, dataOff, unix.SEEK_HOLE)
		if err != nil {
			return nil, fmt.Errorf("lazyrestore: SEEK_HOLE from %d: %w", dataOff, err)
		}
		if holeOff > totalBytes {
			holeOff = totalBytes
		}
		// Mark pages wholly inside [dataOff, holeOff). A page
		// partially overlapping the data extent (only possible
		// when page < block, which we reject upstream) would be
		// unsafe to mark; we only set bits for pages fully covered.
		ps := int64(pageSize)
		firstPage := uint32(dataOff/ps) + 1
		lastPage := uint32((holeOff-1)/ps) + 1
		for pgno := firstPage; pgno <= lastPage; pgno++ {
			// Non-atomic OR is safe here: newPageBitmapFromFile runs
			// before any FUSE handler can race the bits.
			if idx, mask, ok := pageBit(pgno, commitPages); ok {
				b.bits[idx].Store(b.bits[idx].Load() | mask)
			}
		}
		off = holeOff
	}
	return b, nil
}

// isSet reports whether pgno is present (1-based). Returns false for
// out-of-range pgno.
func (b *pageBitmap) isSet(pgno uint32) bool {
	idx, mask, ok := pageBit(pgno, b.pgs)
	if !ok {
		return false
	}
	return b.bits[idx].Load()&mask != 0
}

// trySet atomically transitions pgno from 0 to 1. Returns true if
// this caller flipped the bit, false if it was already 1. Out-of-
// range pgno returns false.
func (b *pageBitmap) trySet(pgno uint32) bool {
	idx, mask, ok := pageBit(pgno, b.pgs)
	if !ok {
		return false
	}
	for {
		cur := b.bits[idx].Load()
		if cur&mask != 0 {
			return false
		}
		if b.bits[idx].CompareAndSwap(cur, cur|mask) {
			return true
		}
	}
}

// set unconditionally marks pgno present. Out-of-range no-op.
func (b *pageBitmap) set(pgno uint32) { b.trySet(pgno) }

// clear atomically transitions pgno from 1 to 0. Returns true if this
// caller cleared the bit, false if it was already 0. Out-of-range
// pgno returns false. Used by the cleanBitmap path: a local write
// invalidates "this page still matches its manifest entry," so the
// clean bit gets cleared before the pwrite lands.
func (b *pageBitmap) clear(pgno uint32) bool {
	idx, mask, ok := pageBit(pgno, b.pgs)
	if !ok {
		return false
	}
	for {
		cur := b.bits[idx].Load()
		if cur&mask == 0 {
			return false
		}
		if b.bits[idx].CompareAndSwap(cur, cur&^mask) {
			return true
		}
	}
}

// presentCount returns the number of bits set. O(n_words).
func (b *pageBitmap) presentCount() uint32 {
	var c uint32
	for i := range b.bits {
		c += uint32(bits.OnesCount64(b.bits[i].Load()))
	}
	return c
}

// pageBit translates 1-based pgno to (word index, bitmask) plus an
// in-range flag. Centralizes the bounds check used by every accessor.
func pageBit(pgno, commitPages uint32) (idx uint32, mask uint64, ok bool) {
	if pgno < 1 || pgno > commitPages {
		return 0, 0, false
	}
	zeroBased := pgno - 1
	return zeroBased / 64, uint64(1) << (zeroBased % 64), true
}
