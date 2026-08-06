package syzyext

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syzyenv"
)

// newDB returns a database path whose env file holds body, or a bare
// path when body is empty.
func newDB(t *testing.T, body string) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "app.db")
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if body != "" {
		if err := os.WriteFile(layout.EnvFile(dbPath), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dbPath
}

func TestLoadVars(t *testing.T) {
	// A database with no file is the common case.
	if vars, err := LoadVars(newDB(t, "")); err != nil || vars.Get("SYZY_AUTOSPAWN") != "" {
		t.Fatalf("LoadVars without a file = %v, %v; want an empty view", vars, err)
	}

	dbPath := newDB(t, "SYZY_AUTOSPAWN=0\n")
	vars, err := LoadVars(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := vars.Get("SYZY_AUTOSPAWN"); got != "0" {
		t.Errorf("Get(SYZY_AUTOSPAWN) = %q, want the database file's value", got)
	}

	// SYZY_ENV_FILE names the file outright, over the database's own.
	named := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(named, []byte("SYZY_AUTOSPAWN=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SYZY_ENV_FILE", named)
	if vars, err = LoadVars(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := vars.Get("SYZY_AUTOSPAWN"); got != "1" {
		t.Errorf("Get(SYZY_AUTOSPAWN) = %q, want the named file's value", got)
	}

	// An operator-named file that is not there is a mistake, not a default.
	t.Setenv("SYZY_ENV_FILE", filepath.Join(t.TempDir(), "absent"))
	if _, err := LoadVars(dbPath); err == nil {
		t.Error("LoadVars accepted a SYZY_ENV_FILE that does not exist")
	}
}

// Attach must not retry an unparseable file: the next read says the
// same thing, and the retries bury the line number.
func TestMalformedFileIsPermanent(t *testing.T) {
	_, err := LoadVars(newDB(t, "SYZY_AUTOSPAWN 0\n"))
	if !errors.Is(err, syzyenv.ErrMalformed) {
		t.Fatalf("LoadVars = %v, want ErrMalformed", err)
	}
	if !permanent(err) {
		t.Errorf("permanent(%v) = false, want true", err)
	}
}

// The schema log is the knob a deployment most often has to set after
// the process it configures already exists.
func TestSchemaLogFromFile(t *testing.T) {
	dbPath := newDB(t, "SYZY_SCHEMA_LOG_DIAL=unix:"+filepath.Join(t.TempDir(), "schema.sock")+"\n")
	for _, key := range []string{"SYZY_SCHEMA_LOG", "SYZY_SCHEMA_LOG_DIAL",
		"SYZY_SCHEMA_LOG_S3", "SYZY_CLUSTER", "SYZY_OBJECT_BACKEND"} {
		t.Setenv(key, "")
	}
	vars, err := LoadVars(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// The socket deliberately has no listener: resolution must produce
	// the lazy client for the file's endpoint, not the default file log.
	log, closer, err := OpenSchemaLog(dbPath, vars)
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		t.Cleanup(func() { _ = closer.Close() })
	}
	if log == nil {
		t.Fatal("OpenSchemaLog ignored the file's SYZY_SCHEMA_LOG_DIAL and returned no log")
	}
	// The default backend is a file schema log under the state dir;
	// its absence is what proves the file's dial endpoint won.
	if _, err := os.Stat(filepath.Join(layout.MetaDir(dbPath), "schema.db")); err == nil {
		t.Error("OpenSchemaLog fell back to the default file schema log")
	}
}
