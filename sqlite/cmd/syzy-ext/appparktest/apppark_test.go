// Tests for the "syzy-app" parkable wrapper VFS. app_vfs.c is plain C
// with no Go or extension-API dependencies, so it is exercised
// directly: compile testdata/apppark_harness.c + ../app_vfs.c against
// the host libsqlite3 and run each scenario as a subprocess. (A
// separate package: the parent directory's loose .c files are only
// valid under the syzy_extension cgo build.)
package appparktest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func buildHarness(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("cc"); err != nil {
		t.Skip("cc not available")
	}
	dir := t.TempDir()
	// Probe the environment separately from the harness build: only a
	// missing toolchain/library may skip. A compile error in app_vfs.c
	// itself must FAIL, not skip — otherwise a broken change silently
	// disables the whole test.
	probe := filepath.Join(dir, "probe.c")
	if err := os.WriteFile(probe,
		[]byte("#include <sqlite3.h>\nint main(void){return 0;}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cc", "-o", filepath.Join(dir, "probe"), probe,
		"-lsqlite3").CombinedOutput(); err != nil {
		t.Skipf("libsqlite3-dev not available: %v\n%s", err, out)
	}
	bin := filepath.Join(dir, "apppark_harness")
	cmd := exec.Command("cc", "-O1", "-Wall", "-I", "../../../../third_party/sqlite",
		"-I", "..", "-o", bin, "testdata/apppark_harness.c", "../app_vfs.c",
		"-lsqlite3", "-lpthread", "-ldl")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("harness build failed: %v\n%s", err, out)
	}
	return bin
}

func TestAppPark(t *testing.T) {
	bin := buildHarness(t)
	for _, mode := range []string{"roundtrip", "nack", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(bin, mode, dir)
			done := make(chan error, 1)
			var out []byte
			go func() {
				var err error
				out, err = cmd.CombinedOutput()
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s: %v\n%s", mode, err, out)
				}
				t.Logf("%s: %s", mode, out)
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("%s: harness timed out (gate deadlock?)", mode)
			}
		})
	}
}
