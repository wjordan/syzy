package clone

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Pure-codec tests. Integration tests that round-trip through a real
// syzy node live in syzy_test.go at the module root to avoid an import
// cycle (the syzy package imports this one).

func TestAdopt_RejectsMalformedBundle(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"truncated magic", []byte("SY")},
		{"bad magic", []byte("XXXXX")},
		{"wrong version", append(bundleMagic[:], 0xFE)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "x.db")
			_, err := Adopt(bytes.NewReader(tt.body), dst)
			if err == nil {
				t.Fatalf("expected error")
			}
			if _, statErr := os.Stat(dst); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("dst leaked: %v", statErr)
			}
		})
	}
}
