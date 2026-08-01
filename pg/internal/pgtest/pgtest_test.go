package pgtest

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestNameNamespacesPerRun: the whole point of Name is that two live runs
// cannot produce the same server-side object name, and that applying it at
// several boundaries in one call path is harmless.
func TestNameNamespacesPerRun(t *testing.T) {
	got := Name("syzy_fixture")
	if got == "syzy_fixture" {
		t.Fatal("Name did not namespace the fixture")
	}
	if !strings.HasSuffix(got, "_r"+strconv.Itoa(os.Getpid())) {
		t.Errorf("Name(%q) = %q, want a suffix identifying this process", "syzy_fixture", got)
	}
	if again := Name(got); again != got {
		t.Errorf("Name is not idempotent: %q -> %q", got, again)
	}
	if a, b := Name("syzy_a"), Name("syzy_b"); a == b {
		t.Errorf("distinct fixtures collided: %q", a)
	}
	// Postgres truncates identifiers at 63 bytes, which would defeat the
	// namespace on a long fixture name by cutting the suffix off.
	if n := len(Name(strings.Repeat("x", 40))); n > 63 {
		t.Errorf("namespaced name is %d bytes, over the 63-byte identifier limit", n)
	}
}
