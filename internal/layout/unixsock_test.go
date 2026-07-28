package layout

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The limit is a kernel ABI constant, so assert it against the real
// thing: bind the longest path we claim is legal, and one byte more.
func TestMaxUnixSocketPath_MatchesKernel(t *testing.T) {
	dir, err := os.MkdirTemp("", "syzy-sun")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	max := maxUnixSocketPath()
	// filepath.Join(dir, name) is len(dir)+1+len(name).
	nameLen := max - len(dir) - 1
	if nameLen < 1 {
		t.Skipf("temp dir %q already exceeds the %d-byte limit", dir, max)
	}

	atLimit := filepath.Join(dir, strings.Repeat("a", nameLen))
	if err := CheckUnixSocketPath(atLimit); err != nil {
		t.Fatalf("CheckUnixSocketPath rejected a %d-byte path: %v", len(atLimit), err)
	}
	ln, err := net.Listen("unix", atLimit)
	if err != nil {
		t.Fatalf("kernel rejected a %d-byte path we accept: %v", len(atLimit), err)
	}
	ln.Close()

	overLimit := atLimit + "a"
	if err := CheckUnixSocketPath(overLimit); err == nil {
		t.Fatalf("CheckUnixSocketPath accepted a %d-byte path", len(overLimit))
	}
	if ln, err := net.Listen("unix", overLimit); err == nil {
		ln.Close()
		t.Fatalf("kernel accepted a %d-byte path we reject; limit is too low", len(overLimit))
	}
}

func TestCheckUnixSocketPath_ErrorNamesTheOverage(t *testing.T) {
	long := "/" + strings.Repeat("x", maxUnixSocketPath()+10)
	err := CheckUnixSocketPath(long)
	if err == nil {
		t.Fatal("want error for an overlong path")
	}
	// The whole point is that the operator can see what went wrong,
	// unlike bind(2)'s bare "invalid argument".
	for _, want := range []string{"unix socket path", "limit", long} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestShortSocketPath_FitsAndIsStable(t *testing.T) {
	deep := "/" + strings.Repeat("deep/", 40) + "app.db"

	a, err := ShortSocketPath(deep, "-peer")
	if err != nil {
		t.Fatalf("ShortSocketPath: %v", err)
	}
	if err := CheckUnixSocketPath(a); err != nil {
		t.Errorf("ShortSocketPath returned an unusable path: %v", err)
	}
	b, err := ShortSocketPath(deep, "-peer")
	if err != nil {
		t.Fatalf("ShortSocketPath (second call): %v", err)
	}
	if a != b {
		t.Errorf("not stable across calls: %q vs %q", a, b)
	}
	other, err := ShortSocketPath("/other/app.db", "-peer")
	if err != nil {
		t.Fatalf("ShortSocketPath (other db): %v", err)
	}
	if a == other {
		t.Errorf("distinct databases collided on %q", a)
	}
}

func TestSocketDir_IsPrivate(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	dir, err := socketDir()
	if err != nil {
		t.Fatalf("socketDir: %v", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}
	// Other users must not be able to connect to or unlink our sockets.
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("socket dir mode = %o, want 700", perm)
	}
}
