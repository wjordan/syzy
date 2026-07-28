package crdt

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestSpecSync_InvariantsMatchCode keeps CRDT.md and the package
// in sync.
//
// CRDT.md is the authoritative index of numbered invariants. Each
// invariant should appear in at least one doc comment in this package
// (cross-reference from code → spec) so a reader of the code can find
// the spec text. Conversely, every invariant number cited in code must
// exist in CRDT.md (so the reverse navigation never dead-ends).
//
// If this test fails, either:
//   - You added/removed an invariant in CRDT.md without updating the
//     corresponding doc comment, or
//   - You cited an invariant number in a Go comment that doesn't exist
//     in the spec.
//
// Fix the drift; do not silence this test.
func TestSpecSync_InvariantsMatchCode(t *testing.T) {
	repoRoot := findRepoRoot(t)

	specBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "CRDT.md"))
	if err != nil {
		t.Fatalf("read CRDT.md: %v", err)
	}
	spec := parseSpecInvariants(string(specBytes))
	if len(spec) == 0 {
		t.Fatal("no numbered invariants found in CRDT.md ## Invariants section")
	}

	code := collectCodeInvariantRefs(t, filepath.Join(repoRoot, "crdt"))

	if missing := slices.DeleteFunc(slices.Clone(spec), func(n int) bool { return code[n] }); len(missing) > 0 {
		t.Errorf("invariants in CRDT.md with no code reference (add `invariant (N)` to a doc comment): %v", missing)
	}
	if extra := slices.DeleteFunc(slices.Sorted(maps.Keys(code)), func(n int) bool { return slices.Contains(spec, n) }); len(extra) > 0 {
		t.Errorf("invariant numbers cited in code but missing from CRDT.md ## Invariants: %v", extra)
	}
}

// invariantNumber matches "(N)" — a parenthesised positive integer.
var invariantNumber = regexp.MustCompile(`\(([0-9]+)\)`)

// parseSpecInvariants extracts invariant numbers from the
// "## Invariants" section of CRDT.md.
func parseSpecInvariants(spec string) []int {
	const header = "## Invariants"
	start := strings.Index(spec, header)
	if start < 0 {
		return nil
	}
	body := spec[start+len(header):]
	if next := strings.Index(body, "\n## "); next >= 0 {
		body = body[:next]
	}
	// Match list items like "1. **Dot uniqueness.**" at the start of a line.
	re := regexp.MustCompile(`(?m)^([0-9]+)\.\s+\*\*`)
	seen := make(map[int]bool)
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil {
			seen[n] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// collectCodeInvariantRefs scans every Go file (excluding this test)
// for comment lines containing "invariant" — case-insensitive — and
// records every "(N)" mention on those lines.
func collectCodeInvariantRefs(t *testing.T, dir string) map[int]bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read crdt dir: %v", err)
	}
	out := make(map[int]bool)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || name == "specsync_test.go" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(strings.ToLower(line), "invariant") {
				continue
			}
			for _, m := range invariantNumber.FindAllStringSubmatch(line, -1) {
				if n, err := strconv.Atoi(m[1]); err == nil {
					out[n] = true
				}
			}
		}
	}
	return out
}

// findRepoRoot walks up from the test file's directory until it finds
// go.mod, so the test runs from any cwd.
func findRepoRoot(t *testing.T) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from test file")
		}
		dir = parent
	}
}
