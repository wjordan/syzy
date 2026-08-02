package sqlite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyDBHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	t.Run("missing file passes", func(t *testing.T) {
		if err := verifyDBHeader(filepath.Join(dir, "absent.db")); err != nil {
			t.Fatalf("missing file: %v", err)
		}
	})

	t.Run("sub-page file passes", func(t *testing.T) {
		p := filepath.Join(dir, "tiny.db")
		if err := os.WriteFile(p, make([]byte, 100), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyDBHeader(p); err != nil {
			t.Fatalf("sub-page file: %v", err)
		}
	})

	t.Run("valid header passes", func(t *testing.T) {
		p := filepath.Join(dir, "valid.db")
		buf := make([]byte, 4096)
		copy(buf, sqliteMagic)
		if err := os.WriteFile(p, buf, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyDBHeader(p); err != nil {
			t.Fatalf("valid header: %v", err)
		}
	})

	t.Run("zeroed header fails", func(t *testing.T) {
		p := filepath.Join(dir, "zeroed.db")
		if err := os.WriteFile(p, make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}
		err := verifyDBHeader(p)
		if err == nil || !strings.Contains(err.Error(), "page 1 header invalid") {
			t.Fatalf("zeroed header: want page-1 error, got %v", err)
		}
	})

	t.Run("garbage header fails", func(t *testing.T) {
		p := filepath.Join(dir, "garbage.db")
		buf := make([]byte, 4096)
		copy(buf, "not a database!!")
		if err := os.WriteFile(p, buf, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyDBHeader(p); err == nil {
			t.Fatal("garbage header: want error, got nil")
		}
	})
}
