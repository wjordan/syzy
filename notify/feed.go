// Package notify implements Syzy's cross-process change notification
// feed. A single mmap'd file with a fixed-size slot ring acts as a
// shared-memory broadcast channel; the writer atomically advances a
// uint32 head index and uses a futex wake to notify waiting readers.
//
// One writer (typically the daemon's dispatcher), N readers (in-process
// Subscribe consumers, extension processes, polyglot clients via a
// future SQL surface). Slow readers degrade to a Lossy notification
// rather than blocking the writer.
//
// File layout (all little-endian, 64-byte aligned header):
//
//	off  size  field
//	  0     4  magic = "SYNF"
//	  4     2  version = 1
//	  6     2  headerSize = 64
//	  8     4  numSlots
//	 12     4  slotSize = 128
//	 16     8  generation (bumped on each writer Open)
//	 24     4  head (slot index, futex word)
//	 28     4  reserved
//	 32-63   reserved
//
// Slot (128 bytes, fixed):
//
//	off  size  field
//	  0     8  origin
//	  8     8  seq
//	 16     1  op
//	 17     1  flags (bit 0: pkTruncated, bit 1: tableTruncated)
//	 18     1  tableLen (effective; 0..TableNameMaxBytes)
//	 19     1  reserved
//	 20     2  pkLen (effective; 0..PKMaxBytes)
//	 22     2  reserved
//	 24    32  table (zero-padded; truncated past TableNameMaxBytes)
//	 56    72  pk (zero-padded; truncated past PKMaxBytes)
package notify

import (
	"errors"
	"path/filepath"
)

// FeedPath returns the canonical shared-memory notification-feed path for an
// application database. Callers should use this helper rather than reproduce
// Syzy's on-disk layout.
func FeedPath(databasePath string) string {
	return filepath.Join(databasePath+"-syzy", "notify.feed")
}

const (
	MagicBytes           = "SYNF"
	FormatVersion uint16 = 1

	HeaderSize = 64
	SlotSize   = 128

	// PKMaxBytes / TableNameMaxBytes are inline budgets per slot.
	// Longer values are truncated and the corresponding flag bit is
	// set; consumers that need the full value fall back to a wildcard
	// invalidation for that record.
	PKMaxBytes        = 72
	TableNameMaxBytes = 32

	slotOriginOff   = 0
	slotSeqOff      = 8
	slotOpOff       = 16
	slotFlagsOff    = 17
	slotTableLenOff = 18
	slotPKLenOff    = 20
	slotTableOff    = 24
	slotPKOff       = 56

	hdrMagicOff      = 0
	hdrVersionOff    = 4
	hdrHeaderSizeOff = 6
	hdrNumSlotsOff   = 8
	hdrSlotSizeOff   = 12
	hdrGenerationOff = 16
	hdrHeadOff       = 24

	DefaultNumSlots = 64 * 1024
)

const (
	flagPKTruncated    uint8 = 1 << 0
	flagTableTruncated uint8 = 1 << 1
)

// Op is the change kind, mirroring the wire-level crdt record kinds.
type Op uint8

const (
	OpInsert    Op = 1
	OpUpdate    Op = 2
	OpDelete    Op = 3
	OpBlobPatch Op = 4
)

// String returns the wire-stable lower-case op name used by the
// syzy_changes SQL surface. Unknown values render as "unknown".
func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpBlobPatch:
		return "blob_patch"
	}
	return "unknown"
}

// Change is one decoded slot. Table and PK alias reader-owned scratch
// when returned by Reader.Read; copy before retaining past the next
// call.
type Change struct {
	Origin         uint64
	Seq            uint64
	Table          string
	Op             Op
	PK             []byte
	TableTruncated bool
	PKTruncated    bool
}

// Notification groups one Changeset's records (consecutive slots
// sharing the same Origin + Seq). When Lossy is true, one or more
// prior records were overwritten in the ring before the reader
// drained them; Changes is empty and the consumer should treat all
// of its subscribed tables as dirty.
type Notification struct {
	Origin  uint64
	Seq     uint64
	Changes []Change
	Lossy   bool
}

var (
	// ErrFormatMismatch indicates the feed file's header has the wrong
	// magic or an unsupported version. Writer.Open recreates the file;
	// Reader.Open returns this error so the caller can decide.
	ErrFormatMismatch = errors.New("notify: feed format mismatch")

	// ErrClosed is returned by Read/Append on a closed handle.
	ErrClosed = errors.New("notify: feed closed")
)
