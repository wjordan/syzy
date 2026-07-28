package layout

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
	"time"

	"github.com/wjordan/syzy/crdt"
)

func TestPathHelpers(t *testing.T) {
	const db = "/var/lib/foo/app.db"
	if got, want := MetaDir(db), "/var/lib/foo/app.db-syzy"; got != want {
		t.Errorf("MetaDir = %q, want %q", got, want)
	}
	if got, want := OriginsRoot(db), "/var/lib/foo/app.db-syzy/origins"; got != want {
		t.Errorf("OriginsRoot = %q, want %q", got, want)
	}
	if got, want := MetaDB(db), "/var/lib/foo/app.db-syzy/metadata.db"; got != want {
		t.Errorf("MetaDB = %q, want %q", got, want)
	}
	if got, want := DaemonLock(db), "/var/lib/foo/app.db-syzy/daemon.lock"; got != want {
		t.Errorf("DaemonLock = %q, want %q", got, want)
	}
	o := crdt.Origin(0xdeadbeefcafebabe)
	if got, want := OriginHex(o), "deadbeefcafebabe"; got != want {
		t.Errorf("OriginHex = %q, want %q", got, want)
	}
	if got, want := JournalDir(db, o), "/var/lib/foo/app.db-syzy/origins/deadbeefcafebabe/journal"; got != want {
		t.Errorf("JournalDir = %q, want %q", got, want)
	}
}

func TestClaimOriginCreatesDirs(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	o := crdt.Origin(42)

	claim, err := ClaimOrigin(db, o)
	if err != nil {
		t.Fatalf("ClaimOrigin: %v", err)
	}
	defer claim.Release()

	if _, err := os.Stat(JournalDir(db, o)); err != nil {
		t.Fatalf("journal dir not created: %v", err)
	}
	if claim.Origin != o {
		t.Errorf("claim.Origin = %d, want %d", claim.Origin, o)
	}
}

func TestClaimOriginConflict(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	o := crdt.Origin(1)

	first, err := ClaimOrigin(db, o)
	if err != nil {
		t.Fatalf("first ClaimOrigin: %v", err)
	}
	defer first.Release()

	if _, err := ClaimOrigin(db, o); !errors.Is(err, ErrOriginLocked) {
		t.Fatalf("second ClaimOrigin err = %v, want ErrOriginLocked", err)
	}
}

func TestReleaseAllowsReclaim(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	o := crdt.Origin(7)

	first, err := ClaimOrigin(db, o)
	if err != nil {
		t.Fatalf("first ClaimOrigin: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := ClaimOrigin(db, o)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	defer second.Release()
}

func TestRecycleFirstUnlocked(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")

	// Pre-create three origins; hold the first two locked.
	held1, err := ClaimOrigin(db, crdt.Origin(0x10))
	if err != nil {
		t.Fatalf("ClaimOrigin 0x10: %v", err)
	}
	defer held1.Release()
	held2, err := ClaimOrigin(db, crdt.Origin(0x20))
	if err != nil {
		t.Fatalf("ClaimOrigin 0x20: %v", err)
	}
	defer held2.Release()
	free3, err := ClaimOrigin(db, crdt.Origin(0x30))
	if err != nil {
		t.Fatalf("ClaimOrigin 0x30: %v", err)
	}
	if err := free3.Release(); err != nil {
		t.Fatalf("Release 0x30: %v", err)
	}

	got, err := Recycle(db, 0)
	if err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if got == nil {
		t.Fatalf("Recycle returned nil; expected origin 0x30")
	}
	defer got.Release()
	if got.Origin != crdt.Origin(0x30) {
		t.Errorf("Recycle.Origin = %x, want 30", got.Origin)
	}
}

func TestRecycleAllLocked(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")

	held, err := ClaimOrigin(db, crdt.Origin(1))
	if err != nil {
		t.Fatalf("ClaimOrigin: %v", err)
	}
	defer held.Release()

	got, err := Recycle(db, 0)
	if err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if got != nil {
		got.Release()
		t.Fatalf("Recycle = %v, want nil (all held)", got.Origin)
	}
}

func TestRecycleEmptyTree(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	got, err := Recycle(db, 0)
	if err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if got != nil {
		got.Release()
		t.Fatalf("Recycle on empty tree = %v, want nil", got.Origin)
	}
}

func TestAcquirePinned(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	want := crdt.Origin(0xabc)
	got, err := Acquire(db, want, 0)
	if err != nil {
		t.Fatalf("Acquire pinned: %v", err)
	}
	defer got.Release()
	if got.Origin != want {
		t.Errorf("Acquire pinned origin = %x, want %x", got.Origin, want)
	}
}

func TestAcquireMintsWhenEmpty(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	got, err := Acquire(db, 0, 0)
	if err != nil {
		t.Fatalf("Acquire mint: %v", err)
	}
	defer got.Release()
	if got.Origin == 0 {
		t.Errorf("minted origin must be nonzero")
	}
	if uint64(got.Origin)&(uint64(1)<<63) != 0 {
		t.Errorf("minted origin %x has reserved bit 63 set", got.Origin)
	}
}

func TestAcquireRecyclesAfterRelease(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	first, err := Acquire(db, 0, 0)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	original := first.Origin
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := Acquire(db, 0, 0)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	defer second.Release()
	if second.Origin != original {
		t.Errorf("second Acquire = %x, want recycled %x", second.Origin, original)
	}
}

func TestRecycleExcludes(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")

	// Two unlocked origins; 0x10 sorts first but is excluded, so Recycle
	// must skip it and return 0x20.
	a, err := ClaimOrigin(db, crdt.Origin(0x10))
	if err != nil {
		t.Fatalf("ClaimOrigin 0x10: %v", err)
	}
	if err := a.Release(); err != nil {
		t.Fatalf("Release 0x10: %v", err)
	}
	b, err := ClaimOrigin(db, crdt.Origin(0x20))
	if err != nil {
		t.Fatalf("ClaimOrigin 0x20: %v", err)
	}
	if err := b.Release(); err != nil {
		t.Fatalf("Release 0x20: %v", err)
	}

	got, err := Recycle(db, crdt.Origin(0x10))
	if err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if got == nil {
		t.Fatalf("Recycle returned nil; expected 0x20")
	}
	defer got.Release()
	if got.Origin != crdt.Origin(0x20) {
		t.Errorf("Recycle excluding 0x10 = %x, want 20", got.Origin)
	}
}

// TestPinExcludeInteraction models the host/guest split: the host pins
// its reserved origin, then a guest acquiring with that origin excluded
// must never land on it (even though it is the only existing dir).
func TestPinExcludeInteraction(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	host := crdt.Origin(0xabc)

	hostClaim, err := Acquire(db, host, 0) // host pins
	if err != nil {
		t.Fatalf("host Acquire pinned: %v", err)
	}
	if hostClaim.Origin != host {
		t.Fatalf("host origin = %x, want %x", hostClaim.Origin, host)
	}
	defer hostClaim.Release()

	guest, err := Acquire(db, 0, host) // guest recycles, excluding host
	if err != nil {
		t.Fatalf("guest Acquire excluding host: %v", err)
	}
	defer guest.Release()
	if guest.Origin == host {
		t.Fatalf("guest claimed the host's pinned origin %x", host)
	}
}

func TestClaimDaemonCreatesLock(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	claim, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("ClaimDaemon: %v", err)
	}
	defer claim.Release()
	if _, err := os.Stat(DaemonLock(db)); err != nil {
		t.Errorf("daemon.lock not created: %v", err)
	}
}

// An adopted (handed-off) lock FD arrives with close-on-exec cleared — that is
// how it survives the predecessor's fork/exec. AdoptDaemon/AdoptOrigin must
// re-set it so the held lock cannot leak into child processes the adopter later
// spawns (which would inherit the open file description and block the next
// Claim on a clean restart).
func TestAdoptSetsCloexec(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")

	// daemon.lock: claim fresh (CLOEXEC on), then clear it to simulate an
	// inherited FD, and verify AdoptDaemon re-sets it.
	dc, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("ClaimDaemon: %v", err)
	}
	clearCloexec(t, dc.File())
	if isCloexec(t, dc.File()) {
		t.Fatal("precondition: CLOEXEC should be cleared to simulate an inherited FD")
	}
	if ad := AdoptDaemon(dc.File()); !isCloexec(t, ad.File()) {
		t.Error("AdoptDaemon left FD_CLOEXEC clear on the adopted daemon.lock FD")
	}
	_ = dc.Release()

	// origin dir: same.
	oc, err := ClaimOrigin(db, crdt.Origin(7))
	if err != nil {
		t.Fatalf("ClaimOrigin: %v", err)
	}
	clearCloexec(t, oc.File())
	if ao := AdoptOrigin(db, crdt.Origin(7), oc.File()); !isCloexec(t, ao.File()) {
		t.Error("AdoptOrigin left FD_CLOEXEC clear on the adopted origin-dir FD")
	}
	_ = oc.Release()
}

func isCloexec(t *testing.T, f *os.File) bool {
	t.Helper()
	// unix.FcntlInt rather than a raw syscall: darwin has no exported
	// fcntl syscall number.
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("F_GETFD: %v", err)
	}
	return flags&unix.FD_CLOEXEC != 0
}

func clearCloexec(t *testing.T, f *os.File) {
	t.Helper()
	if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("F_SETFD clear: %v", err)
	}
}

func TestClaimDaemonConflict(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	first, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("first ClaimDaemon: %v", err)
	}
	defer first.Release()
	if _, err := ClaimDaemon(db); !errors.Is(err, ErrDaemonLocked) {
		t.Fatalf("second ClaimDaemon err = %v, want ErrDaemonLocked", err)
	}
}

func TestClaimDaemonReleaseAllowsReclaim(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	first, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("first ClaimDaemon: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	defer second.Release()
}

func TestWaitForDaemonReturnsWhenAvailable(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	holder, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("ClaimDaemon: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	released := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = holder.Release()
		close(released)
	}()

	got, err := WaitForDaemon(ctx, db, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForDaemon: %v", err)
	}
	defer got.Release()
	<-released
}

func TestWaitForDaemonHonorsContext(t *testing.T) {
	db := filepath.Join(t.TempDir(), "app.db")
	holder, err := ClaimDaemon(db)
	if err != nil {
		t.Fatalf("ClaimDaemon: %v", err)
	}
	defer holder.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()

	if _, err := WaitForDaemon(ctx, db, 25*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForDaemon err = %v, want DeadlineExceeded", err)
	}
}
