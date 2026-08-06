package syzyenv

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeEnvFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadParsesFile(t *testing.T) {
	path := writeEnvFile(t, `# a comment

SYZY_AUTOSPAWN=0
export SYZY_CLUSTER=s3://bucket/app
  SYZY_SCHEMA_LOG_DIAL = vsock:2:7850
SYZY_BIN="/opt/syzy/bin/syzy"
SYZY_OBJECT_BACKEND='s3://other'
SYZY_LISTEN=
`)
	vars, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ key, want string }{
		{"SYZY_AUTOSPAWN", "0"},
		{"SYZY_CLUSTER", "s3://bucket/app"},
		{"SYZY_SCHEMA_LOG_DIAL", "vsock:2:7850"},
		{"SYZY_BIN", "/opt/syzy/bin/syzy"},
		{"SYZY_OBJECT_BACKEND", "s3://other"},
		{"SYZY_LISTEN", ""},
		{"SYZY_ABSENT", ""},
	} {
		if got := vars.Get(tc.key); got != tc.want {
			t.Errorf("Get(%s) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

// A knob silently dropped because of a typo is the failure this file
// exists to prevent, so every parse problem is loud.
func TestLoadRejectsMalformed(t *testing.T) {
	for name, body := range map[string]string{
		"no equals":     "SYZY_AUTOSPAWN 0\n",
		"foreign key":   "PATH=/usr/bin\n",
		"bare SYZY_":    "SYZY_=1\n",
		"lowercase key": "syzy_autospawn=0\n",
		"duplicate":     "SYZY_AUTOSPAWN=0\nSYZY_AUTOSPAWN=1\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeEnvFile(t, body))
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("Load = %v, want ErrMalformed", err)
			}
			if !strings.Contains(err.Error(), ":1:") && name != "duplicate" {
				t.Errorf("error does not locate the line: %v", err)
			}
		})
	}
}

func TestGetPrecedence(t *testing.T) {
	vars, err := Load(writeEnvFile(t, "SYZY_CLUSTER=from-file\nSYZY_BIN=from-file\n"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SYZY_CLUSTER", "from-env")
	t.Setenv("SYZY_BIN", "") // exported but empty is not a value
	if got := vars.Get("SYZY_CLUSTER"); got != "from-env" {
		t.Errorf("Get(SYZY_CLUSTER) = %q, want the environment's value", got)
	}
	if got := vars.Get("SYZY_BIN"); got != "from-file" {
		t.Errorf("Get(SYZY_BIN) = %q, want the file's value", got)
	}
	// Databases without a file are the common case, and the view they
	// get still has to answer from the environment.
	var absent *Vars
	if got := absent.Get("SYZY_CLUSTER"); got != "from-env" {
		t.Errorf("nil Vars Get = %q, want the environment's value", got)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Load of a missing file = %v, want os.ErrNotExist", err)
	}
}
