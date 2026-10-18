package mirror

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEmptyMirrorQuarantineFailureRetainsOriginalAndAudit(t *testing.T) {
	// Every directory sync and the rename can fail independently. Failures
	// before the rename must leave the origin in place; failures after it
	// must report the evidence location without deleting or moving it back.
	for _, failAt := range []int{1, 2, 3, 4, 5} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			t.Parallel()
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(base, "mirror")
			dir := filepath.Join(root, "origin_123")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "seg-00000000.bin")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(65536); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := InspectEmptyMirror(root, 123)
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected durability failure")
			step := 0
			advance := func() error {
				step++
				if step == failAt {
					return failure
				}
				return nil
			}
			recovery, err := quarantineEmptyMirror(plan,
				func(from, to string) error {
					if err := advance(); err != nil {
						return err
					}
					return os.Rename(from, to)
				},
				func(path string) error {
					if err := advance(); err != nil {
						return err
					}
					return syncRepairDir(path)
				})
			if !errors.Is(err, failure) || recovery == "" {
				t.Fatalf("evidence=%q error=%v", recovery, err)
			}
			if _, err := os.Stat(filepath.Join(recovery, "manifest.json")); err != nil {
				t.Fatalf("audit missing: %v", err)
			}
			preserved := path
			if failAt > 3 {
				preserved = filepath.Join(recovery, "original", "seg-00000000.bin")
			}
			after, err := os.Stat(preserved)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) || before.Size() != after.Size() {
				t.Fatal("original file lost or replaced")
			}
		})
	}
}
