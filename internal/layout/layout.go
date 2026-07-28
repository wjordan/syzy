// Package layout owns the on-disk path conventions for syzy state and
// the flock-based origin-claim protocol.
//
// Layout under an app database "app.db":
//
//	app.db                                user's SQLite database
//	app.db-syzy/
//	├── daemon.lock                       held by the daemon-role process
//	├── metadata.db                        frontier, applied_gaps, row_clock,
//	│                                     catalog (daemon-role-only writer)
//	└── origins/
//	    └── <origin-hex>/                 flock target; one per active node
//	        └── journal/                  mmap'd append-only segments
//
// One process per box holds daemon.lock. Any number of processes hold
// origin claims; sequential CLI invocations recycle, concurrent ones
// each take a distinct origin.
package layout

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/wjordan/syzy/crdt"
)

// ErrOriginLocked is returned when ClaimOrigin or Recycle finds the
// directory's lock held by another live process.
var ErrOriginLocked = errors.New("layout: origin locked")

// ErrDaemonLocked is returned when ClaimDaemon finds the daemon-role
// lock already held by another process.
var ErrDaemonLocked = errors.New("layout: daemon role locked")

// MetaDir returns "<appDB>-syzy" — the syzy state directory adjacent
// to the user's database.
func MetaDir(appDB string) string {
	return appDB + "-syzy"
}

// OriginsRoot returns "<metadata>/origins" where per-origin directories live.
func OriginsRoot(appDB string) string {
	return filepath.Join(MetaDir(appDB), "origins")
}

// OriginDir returns the per-origin directory; the flock target.
func OriginDir(appDB string, origin crdt.Origin) string {
	return filepath.Join(OriginsRoot(appDB), OriginHex(origin))
}

// JournalDir returns the journal directory inside an origin's dir.
func JournalDir(appDB string, origin crdt.Origin) string {
	return filepath.Join(OriginDir(appDB, origin), "journal")
}

// OriginJournalDirIn returns the journal directory for origin inside an
// already-resolved metadata dir. Useful when staging a metadata at a
// non-canonical path (e.g. a ".tmp" dir during clone Adopt) where
// JournalDir's appDB-based derivation doesn't apply.
func OriginJournalDirIn(metaDir string, origin crdt.Origin) string {
	return filepath.Join(metaDir, "origins", OriginHex(origin), "journal")
}

// MetaDB returns the path to the metadata SQLite database (CRDT
// frontiers, row clocks, schema catalog) that lives next to appDB.
func MetaDB(appDB string) string {
	return filepath.Join(MetaDir(appDB), "metadata.db")
}

// DaemonLock returns the path to the daemon-role coordination lock.
func DaemonLock(appDB string) string {
	return filepath.Join(MetaDir(appDB), "daemon.lock")
}

// OriginHex renders an origin as a fixed-width 16-character hex string.
// Used as the directory name under origins/.
func OriginHex(o crdt.Origin) string {
	return fmt.Sprintf("%016x", uint64(o))
}

// OriginClaim is an exclusive lock on one origin's on-disk directory.
// Hold for the lifetime of the process owning the origin; Release drops
// the flock and lets recycling pick the same origin up next time.
type OriginClaim struct {
	Origin crdt.Origin
	Dir    string
	file   *os.File
}

// Release drops the flock. Idempotent.
func (c *OriginClaim) Release() error {
	if c == nil || c.file == nil {
		return nil
	}
	f := c.file
	c.file = nil
	return f.Close()
}

// File returns the claim's locked directory handle so it can be passed to a
// successor process (the flock rides the open file description, so a passed FD
// keeps the lock held). The claim retains ownership; don't close it directly.
func (c *OriginClaim) File() *os.File { return c.file }

// AdoptOrigin wraps an already-locked origin-dir file description (handed off
// from a predecessor) as an OriginClaim WITHOUT taking a fresh flock — the lock
// is already held via the shared open file description.
func AdoptOrigin(appDB string, origin crdt.Origin, f *os.File) *OriginClaim {
	setCloexec(f)
	return &OriginClaim{Origin: origin, Dir: OriginDir(appDB, origin), file: f}
}

// setCloexec marks a held-lock FD close-on-exec. Fresh ClaimDaemon/ClaimOrigin
// open via os.OpenFile/os.Open, which are O_CLOEXEC by default; only an adopted
// FD — handed off from a predecessor over fork/exec via cmd.ExtraFiles — arrives
// with CLOEXEC cleared (that is how it survives the exec). Re-set it here so the
// lock cannot leak into unrelated child processes the adopting process later
// spawns: such a child would inherit the open file description and keep the flock
// held, blocking the next ClaimDaemon/ClaimOrigin on a clean (re)start. Safe
// across a subsequent handoff — ExtraFiles re-clears CLOEXEC on the successor's
// own inherited copy independently of this FD's flag.
func setCloexec(f *os.File) {
	if f != nil {
		syscall.CloseOnExec(int(f.Fd()))
	}
}

// ClaimOrigin tries to acquire LOCK_EX|LOCK_NB on the directory for the
// given origin. Creates the directory (and its journal subdir) if
// absent. Returns ErrOriginLocked if another live process holds the
// lock; closing the returned claim releases it.
func ClaimOrigin(appDB string, origin crdt.Origin) (*OriginClaim, error) {
	dir := OriginDir(appDB, origin)
	if err := os.MkdirAll(filepath.Join(dir, "journal"), 0o755); err != nil {
		return nil, fmt.Errorf("layout: mkdir origin: %w", err)
	}
	f, err := os.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("layout: open origin dir: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrOriginLocked
		}
		return nil, fmt.Errorf("layout: flock origin dir: %w", err)
	}
	return &OriginClaim{Origin: origin, Dir: dir, file: f}, nil
}

// Acquire returns an OriginClaim using this priority:
//  1. If pinned != 0, claim that specific origin (errors if locked).
//  2. Otherwise recycle: walk origins/ and take the first unlocked one,
//     skipping exclude (when non-zero).
//  3. If nothing recyclable exists, mint a fresh random origin (never
//     exclude).
//
// This is the entry point producers use at startup. Pinned mode comes
// from Config.NodeID (the host daemon claiming its stable origin);
// recycle handles sequential CLI reuse; mint handles the first-ever
// invocation and the concurrent-CLI case. exclude lets an in-guest
// writer avoid the host daemon's origin — its dir flock is invisible to
// the guest across the pmem/virtiofs boundary — which the guest reads
// from the metadata node_id.
func Acquire(appDB string, pinned, exclude crdt.Origin) (*OriginClaim, error) {
	if pinned != 0 {
		return ClaimOrigin(appDB, pinned)
	}
	claim, err := Recycle(appDB, exclude)
	if err != nil || claim != nil {
		return claim, err
	}
	return mintAndClaim(appDB, exclude)
}

// MintAndClaim allocates a brand-new random origin and claims its
// directory. Skips Recycle entirely — used by callers that explicitly
// want a fresh origin even when unclaimed dirs exist (e.g. producer
// startup after an unclean shutdown rotates the local origin).
func MintAndClaim(appDB string) (*OriginClaim, error) {
	return mintAndClaim(appDB, 0)
}

func mintAndClaim(appDB string, exclude crdt.Origin) (*OriginClaim, error) {
	// 64-bit random with bit 63 zeroed (Origin reserves it). Collision
	// against an existing locked origin is astronomically unlikely; a
	// few retries cover the case that two concurrent mints pick the same
	// value (or, with overwhelming improbability, the excluded one).
	for i := 0; i < 8; i++ {
		v, err := MintOrigin()
		if err != nil {
			return nil, err
		}
		if exclude != 0 && v == exclude {
			continue
		}
		claim, err := ClaimOrigin(appDB, v)
		if errors.Is(err, ErrOriginLocked) {
			continue
		}
		return claim, err
	}
	return nil, errors.New("layout: mint origin: too many collisions")
}

// MintOrigin returns a fresh random Origin: 64 random bits with bit 63
// cleared (Origin reserves it for wire encoding) and zero promoted to
// one. Collision probability against any existing origin is ~2^-63 per
// call. Exported so callers that mint an origin without immediately
// claiming it (clone Adopt) share the same generation rules as Acquire.
func MintOrigin() (crdt.Origin, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("layout: random origin: %w", err)
	}
	v := binary.BigEndian.Uint64(buf[:]) &^ (uint64(1) << 63)
	if v == 0 {
		v = 1
	}
	return crdt.Origin(v), nil
}

// Recycle walks origins/ and returns the first unlocked origin's claim,
// skipping exclude when non-zero (the host daemon's origin, which a guest
// writer must not steal). Returns (nil, nil) when every existing origin
// is held or excluded — caller should mint a fresh origin in that case.
func Recycle(appDB string, exclude crdt.Origin) (*OriginClaim, error) {
	entries, err := os.ReadDir(OriginsRoot(appDB))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("layout: read origins: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var v uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x", &v); err != nil {
			continue
		}
		if exclude != 0 && crdt.Origin(v) == exclude {
			continue
		}
		claim, err := ClaimOrigin(appDB, crdt.Origin(v))
		if errors.Is(err, ErrOriginLocked) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return claim, nil
	}
	return nil, nil
}

// DaemonClaim holds the per-box daemon-role lock. Whichever process
// holds this is responsible for the syncer pipeline (drainer, broker,
// snapshotter, gossip, transport). Other processes that open the same
// app database run as producer-only.
type DaemonClaim struct {
	file *os.File
}

// Release drops the daemon-role lock. Idempotent.
func (c *DaemonClaim) Release() error {
	if c == nil || c.file == nil {
		return nil
	}
	f := c.file
	c.file = nil
	return f.Close()
}

// File returns the daemon lock's handle for handoff to a successor process. The
// claim retains ownership.
func (c *DaemonClaim) File() *os.File { return c.file }

// AdoptDaemon wraps an already-locked daemon.lock file description (handed off
// from a predecessor) as a DaemonClaim WITHOUT a fresh flock.
func AdoptDaemon(f *os.File) *DaemonClaim {
	setCloexec(f)
	return &DaemonClaim{file: f}
}

// ClaimDaemon tries to acquire the daemon-role lock non-blocking.
// Returns ErrDaemonLocked if another live process holds it. Creates
// the metadata dir + lock file as needed.
func ClaimDaemon(appDB string) (*DaemonClaim, error) {
	if err := os.MkdirAll(MetaDir(appDB), 0o755); err != nil {
		return nil, fmt.Errorf("layout: mkdir metadata: %w", err)
	}
	f, err := os.OpenFile(DaemonLock(appDB), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("layout: open daemon.lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrDaemonLocked
		}
		return nil, fmt.Errorf("layout: flock daemon.lock: %w", err)
	}
	return &DaemonClaim{file: f}, nil
}

// WaitForDaemon blocks until ClaimDaemon succeeds or ctx is canceled.
// Polls at retry interval (default 2s when zero). The `syzy daemon`
// CLI uses this so an operator starting the daemon while an embedded
// library still holds the role waits cleanly instead of failing.
func WaitForDaemon(ctx context.Context, appDB string, retry time.Duration) (*DaemonClaim, error) {
	if retry <= 0 {
		retry = 2 * time.Second
	}
	for {
		claim, err := ClaimDaemon(appDB)
		if err == nil {
			return claim, nil
		}
		if !errors.Is(err, ErrDaemonLocked) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
}
