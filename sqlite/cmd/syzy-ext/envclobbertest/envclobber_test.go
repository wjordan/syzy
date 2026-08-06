// Package envclobbertest exercises the built extension against a host
// process that has taken its environment block away.
//
// The engine is a c-shared object, so the Go runtime inside it reads
// the environment once, off the initial process stack, whenever the
// shim dlopen's it. A host that implements setproctitle by reusing the
// argv+environ region has already overwritten that block by then, and
// every knob the engine resolves through Go comes back empty while
// getenv still answers: SYZY_AUTOSPAWN stops suppressing the daemon
// spawn, SYZY_SCHEMA_LOG_DIAL stops naming a schema authority, and
// SYZY_WAKE_VSOCK stops naming a wake transport. internal/syzyenv is
// the fix; this is the end-to-end proof, which needs the real .so.
package envclobbertest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/sqlitebridge"
)

const repoRoot = "../../../.."

func TestExtensionSurvivesStolenEnvBlock(t *testing.T) {
	engine := artifact(t, "ext/syzy.so")
	shim := artifact(t, "ext/syzy-shim.so")
	harness := buildHarness(t)

	// The control run proves the harness is measuring something, and
	// that the extension works at all.
	clean := runHarness(t, harness, engine, shim, scenario{})
	if clean.stackEntries == 0 {
		t.Fatalf("unclobbered run already lost its stack environment; harness is not measuring anything:\n%s", clean.output)
	}
	if !clean.ok {
		t.Fatalf("extension failed on an untouched environment:\n%s", clean.output)
	}

	// The env file is the channel for a process whose environment was
	// fixed before anyone knew which database it would serve; in that
	// run nothing but the bootstrap knobs arrives in the environment.
	for name, sc := range map[string]scenario{
		"knobs in the environment": {clobber: true},
		"knobs in the env file":    {clobber: true, configInFile: true},
	} {
		t.Run(name, func(t *testing.T) {
			res := runHarness(t, harness, engine, shim, sc)
			if res.stackEntries != 0 {
				t.Fatalf("harness did not actually steal the env block (%d SYZY_* entries left); "+
					"the test would pass whether or not the extension reads libc:\n%s",
					res.stackEntries, res.output)
			}
			if !res.ok {
				t.Fatalf("extension failed after the host stole its env block; it is reading Go's "+
					"stack snapshot instead of libc (see internal/syzyenv):\n%s", res.output)
			}
		})
	}
}

// scenario selects how one harness run is set up: whether the host
// steals the environment block, and whether the attach-time knobs
// arrive in the environment or in the database's env file.
type scenario struct {
	clobber      bool
	configInFile bool
}

type result struct {
	ok           bool
	stackEntries int
	output       string
}

// runHarness opens a freshly initialized DB from a subprocess, with the
// shim preloaded exactly as a deployment does. Each run gets its own DB
// so an origin claim from one cannot influence the other.
func runHarness(t *testing.T, harness, engine, shim string, sc scenario) result {
	t.Helper()
	dbPath := prepareDB(t)
	args := []string{}
	if sc.clobber {
		args = append(args, "clobber")
	}
	cmd := exec.Command(harness, args...)
	// $PATH holds no syzy, so any run that decides to spawn a daemon
	// fails loudly instead of quietly succeeding on a developer box
	// that happens to have one installed.
	cmd.Env = []string{
		"PATH=" + t.TempDir(),
		"LD_PRELOAD=" + shim,
		"SYZY_ENGINE=" + engine,
		"SYZY_AUTOLOAD=1",
		"SYZY_DB=" + dbPath,
	}
	if sc.configInFile {
		if err := os.WriteFile(layout.EnvFile(dbPath), []byte("SYZY_AUTOSPAWN=0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	} else {
		cmd.Env = append(cmd.Env, "SYZY_AUTOSPAWN=0")
	}
	out, err := cmd.CombinedOutput()
	res := result{ok: err == nil, output: string(out), stackEntries: -1}
	for _, line := range strings.Split(res.output, "\n") {
		if rest, found := strings.CutPrefix(line, "stack_syzy_entries="); found {
			if n, err := strconv.Atoi(strings.TrimSpace(rest)); err == nil {
				res.stackEntries = n
			}
		}
	}
	if res.stackEntries < 0 {
		t.Fatalf("harness did not report its stack environment:\n%s", res.output)
	}
	return res
}

// prepareDB stands in for the daemon that normally owns the database:
// it lays out the metadata the producer requires and creates the table
// the harness writes to, so the harness exercises attach and nothing
// else.
func prepareDB(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	conn, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		t.Fatal(err)
	}
	meta, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.SetClusterID(crdt.ClusterID{1}); err != nil {
		t.Fatal(err)
	}
	if err := meta.SetNodeID(crdt.Origin(1)); err != nil {
		t.Fatal(err)
	}
	if err := meta.Close(); err != nil {
		t.Fatal(err)
	}
	return dbPath
}

// artifact resolves a built extension artifact. Building it here would
// cost a c-shared link on every run, so the artifacts are a
// prerequisite: `make test-ext` builds them and then runs these tests.
func artifact(t *testing.T, rel string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join(repoRoot, rel))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("%s not built; run `make test-ext`", rel)
	}
	return path
}

func buildHarness(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	// Probe the toolchain separately from the harness build: only a
	// missing compiler or library may skip. A compile error in the
	// harness itself must FAIL, not skip.
	probe := filepath.Join(dir, "probe.c")
	if err := os.WriteFile(probe,
		[]byte("#include <sqlite3.h>\nint main(void){return 0;}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cc", "-o", filepath.Join(dir, "probe"), probe,
		"-lsqlite3").CombinedOutput(); err != nil {
		t.Skipf("no cc with libsqlite3 headers: %v\n%s", err, out)
	}
	bin := filepath.Join(dir, "clobber_harness")
	out, err := exec.Command("cc", "-O1", "-Wall", "-o", bin,
		"testdata/clobber_harness.c", "-lsqlite3").CombinedOutput()
	if err != nil {
		t.Fatalf("build harness: %v\n%s", err, out)
	}
	return bin
}
