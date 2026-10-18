package mirror_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/mirror"
)

func emptyMirror(t *testing.T) (root, dir, segment string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(base, "mirror")
	dir = filepath.Join(root, "origin_123")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	segment = filepath.Join(dir, "seg-00000000.bin")
	if err := os.WriteFile(segment, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	return
}

func TestEmptyMirrorInspectionRefusesAmbiguousDamage(t *testing.T) {
	for _, scenario := range []string{"zero length", "short", "nonzero header", "nonzero tail", "second segment", "extra file", "segment symlink", "directory symlink", "root symlink", "segment directory"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			root, dir, path := emptyMirror(t)
			switch scenario {
			case "zero length":
				if err := os.Truncate(path, 0); err != nil {
					t.Fatal(err)
				}
			case "short":
				if err := os.Truncate(path, 64); err != nil {
					t.Fatal(err)
				}
			case "nonzero header", "nonzero tail":
				f, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				off := int64(0)
				if scenario == "nonzero tail" {
					off = (1 << 20) - 1
				}
				_, err = f.WriteAt([]byte{1}, off)
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "second segment", "extra file":
				name := "evidence.txt"
				if scenario == "second segment" {
					name = "seg-00000001.bin"
				}
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "segment symlink":
				if err := os.Rename(path, filepath.Join(root, "original")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "original"), path); err != nil {
					t.Fatal(err)
				}
			case "directory symlink", "root symlink":
				old := dir
				if scenario == "root symlink" {
					old = root
				}
				if err := os.Rename(old, old+"-original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(old+"-original", old); err != nil {
					t.Fatal(err)
				}
			case "segment directory":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := mirror.InspectEmptyMirror(root, 123); err == nil {
				t.Fatal("accepted unsafe repair")
			}
			entries, err := os.ReadDir(filepath.Dir(root))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if e.Name() != "mirror" && e.Name() != "mirror-original" {
					t.Fatalf("inspection created %s", e.Name())
				}
			}
		})
	}
}

func TestEmptyMirrorQuarantinePreservesEvidenceAndOtherState(t *testing.T) {
	t.Parallel()
	root, dir, path := emptyMirror(t)
	metadata := filepath.Join(filepath.Dir(root), "metadata.db")
	state := []byte("frontiers, gaps, row clocks and markers are not edited")
	if err := os.WriteFile(metadata, state, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := mirror.InspectEmptyMirror(root, 123)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(make([]byte, 1<<20))
	if plan.Size != 1<<20 || plan.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("incorrect plan: %+v", plan)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("inspection moved original: %v", err)
	}
	recovery, err := mirror.QuarantineEmptyMirror(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("origin still scanned: %v", err)
	}
	preserved := filepath.Join(recovery, "original", "seg-00000000.bin")
	after, err := os.Stat(preserved)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("original identity or modification time changed")
	}
	b, err := os.ReadFile(preserved)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, make([]byte, 1<<20)) {
		t.Fatal("original bytes changed")
	}
	manifest, err := os.ReadFile(filepath.Join(recovery, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var audit struct {
		Inspection mirror.EmptyMirrorPlan `json:"inspection"`
		Evidence   string                 `json:"evidence"`
	}
	if err := json.Unmarshal(manifest, &audit); err != nil {
		t.Fatal(err)
	}
	if audit.Inspection.SHA256 != plan.SHA256 || audit.Evidence != filepath.Join(recovery, "original") {
		t.Fatalf("incorrect audit: %s", manifest)
	}
	b, err = os.ReadFile(metadata)
	if err != nil || !bytes.Equal(b, state) {
		t.Fatalf("metadata changed: %v", err)
	}
	if _, err := mirror.QuarantineEmptyMirror(plan); err == nil {
		t.Fatal("stale plan applied twice")
	}
}

func TestEmptyMirrorQuarantineRevalidatesPlan(t *testing.T) {
	for _, change := range []string{"bytes", "inode", "directory", "mtime", "extra entry", "forged digest"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			root, dir, path := emptyMirror(t)
			plan, err := mirror.InspectEmptyMirror(root, 123)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "bytes":
				if err := os.WriteFile(path, []byte{1}, 0o600); err != nil {
					t.Fatal(err)
				}
			case "inode":
				if err := os.Rename(path, filepath.Join(root, "old")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, make([]byte, 1<<20), 0o600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Rename(dir, dir+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(dir+"-old", "seg-00000000.bin"), path); err != nil {
					t.Fatal(err)
				}
			case "mtime":
				when := time.Unix(1, 0)
				if err := os.Chtimes(path, when, when); err != nil {
					t.Fatal(err)
				}
			case "extra entry":
				if err := os.WriteFile(filepath.Join(dir, "other"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "forged digest":
				plan.SHA256 = "wrong"
			}
			if _, err := mirror.QuarantineEmptyMirror(plan); err == nil {
				t.Fatal("accepted changed repair plan")
			}
			if _, err := os.Stat(dir); err != nil {
				t.Fatalf("moved changed original: %v", err)
			}
			matches, err := filepath.Glob(filepath.Join(filepath.Dir(root), "mirror-recovery-*"))
			if err != nil || len(matches) != 0 {
				t.Fatalf("created recovery before revalidation: %v %v", matches, err)
			}
		})
	}
}
