package sqlitebridge

import (
	"os"
	"strings"
	"testing"
)

func TestLibVersion(t *testing.T) {
	got := LibVersion()
	want, err := os.ReadFile("../third_party/sqlite/VERSION")
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	wantStr := strings.TrimSpace(string(want))
	if got != wantStr {
		t.Fatalf("linked sqlite version %q does not match vendored VERSION %q",
			got, wantStr)
	}
}

func TestLibVersionNumber(t *testing.T) {
	n := LibVersionNumber()
	// 3.53.0 = 3_053_000. Sanity-check we're at or above this floor; bumping
	// the vendored amalgamation up should not break the test, but a downgrade
	// to a version missing preupdate-hook features should.
	if n < 3_053_000 {
		t.Fatalf("linked sqlite version_number=%d is below the 3.53.0 floor", n)
	}
}
