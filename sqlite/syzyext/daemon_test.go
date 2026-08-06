package syzyext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An absent CLI is a partial install, not a startup race: it must
// report itself as permanent so AttachWithRetry gives up immediately
// instead of burying the cause under four retries.
func TestSpawnDaemonWithoutBinary(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("SYZY_BIN", "")
	err := spawnDaemon(filepath.Join(t.TempDir(), "app.db"), nil)
	if err == nil {
		t.Fatal("spawnDaemon succeeded with no syzy on PATH")
	}
	if !permanent(err) {
		t.Errorf("permanent(%v) = false, want true", err)
	}
	if !strings.Contains(err.Error(), "SYZY_BIN") {
		t.Errorf("error does not name the escape hatch: %v", err)
	}
}

// lookPath resolves against $PATH the same way exec.LookPath does; the
// difference is which environment it reads (see internal/syzyenv).
func TestLookPath(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "syzy")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "syzy-not-executable"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	if got, err := lookPath("syzy"); err != nil || got != exe {
		t.Errorf("lookPath(syzy) = %q, %v; want %q, nil", got, err, exe)
	}
	if _, err := lookPath("syzy-not-executable"); err == nil {
		t.Error("lookPath resolved a non-executable file")
	}
	if _, err := lookPath("syzy-absent"); err == nil {
		t.Error("lookPath resolved a name that is not on PATH")
	}
	// An explicit path bypasses $PATH entirely.
	if got, err := lookPath(exe); err != nil || got != exe {
		t.Errorf("lookPath(%q) = %q, %v; want %q, nil", exe, got, err, exe)
	}
}
