package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/mirror"
)

func repairFixture(t *testing.T) (db, segment string, plan mirror.EmptyMirrorPlan) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	db = filepath.Join(base, "app.db")
	if err := os.WriteFile(db, []byte("application bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	segment = filepath.Join(layout.MetaDir(db), "mirror", "origin_123", "seg-00000000.bin")
	if err := os.MkdirAll(filepath.Dir(segment), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segment, make([]byte, 65536), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(layout.OriginDir(db, 456), 0o700); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := repairMirrorCmdTo([]string{"--db", db, "--origin", "123"}, &out); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	return
}

func TestRepairMirrorDryRunDoesNotCreateFiles(t *testing.T) {
	t.Parallel()
	db, segment, _ := repairFixture(t)
	if _, err := os.Stat(layout.DaemonLock(db)); !os.IsNotExist(err) {
		t.Fatalf("dry run created lock: %v", err)
	}
	if _, err := os.Stat(layout.MetaDB(db)); !os.IsNotExist(err) {
		t.Fatalf("dry run created metadata: %v", err)
	}
	if _, err := os.Stat(segment); err != nil {
		t.Fatalf("dry run moved segment: %v", err)
	}
	var out bytes.Buffer
	if err := repairMirrorCmdTo([]string{"--db", db + "-missing", "--origin", "123"}, &out); err == nil {
		t.Fatal("accepted missing database")
	}
	if _, err := os.Stat(db + "-missing"); !os.IsNotExist(err) {
		t.Fatal("created missing database")
	}
}

func TestRepairMirrorApplyRequiresReviewedDigestAndStoppedReplica(t *testing.T) {
	for _, scenario := range []string{"missing digest", "wrong digest", "missing lock", "live daemon", "live writer", "lock symlink", "metadata symlink", "origin symlink"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			db, segment, plan := repairFixture(t)
			args := []string{"--db", db, "--origin", "123", "--apply", "--expected-sha256", plan.SHA256}
			if scenario != "missing lock" {
				claim, err := layout.ClaimDaemon(db)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "live daemon" {
					defer claim.Release()
				} else if err := claim.Release(); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "missing digest":
				args = args[:len(args)-2]
			case "wrong digest":
				args[len(args)-1] = strings.Repeat("0", 64)
			case "live writer":
				claim, err := layout.ClaimOrigin(db, 456)
				if err != nil {
					t.Fatal(err)
				}
				defer claim.Release()
			case "lock symlink":
				if err := os.Remove(layout.DaemonLock(db)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(db, layout.DaemonLock(db)); err != nil {
					t.Fatal(err)
				}
			case "metadata symlink":
				old := layout.MetaDir(db)
				if err := os.Rename(old, old+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(old+"-old", old); err != nil {
					t.Fatal(err)
				}
			case "origin symlink":
				old := layout.OriginDir(db, 456)
				if err := os.Rename(old, old+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(old+"-old", old); err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			if err := repairMirrorCmdTo(args, &out); err == nil {
				t.Fatal("unsafe repair succeeded")
			}
			if _, err := os.Stat(segment); err != nil {
				t.Fatalf("moved original on refusal: %v", err)
			}
			if scenario != "live daemon" && scenario != "lock symlink" && scenario != "metadata symlink" && scenario != "missing lock" {
				claim, err := layout.ClaimDaemon(db)
				if err != nil {
					t.Fatalf("leaked daemon lock: %v", err)
				}
				_ = claim.Release()
			}
		})
	}
}

func TestRepairMirrorApplyPreservesDatabaseBytes(t *testing.T) {
	t.Parallel()
	db, segment, plan := repairFixture(t)
	claim, err := layout.ClaimDaemon(db)
	if err != nil {
		t.Fatal(err)
	}
	_ = claim.Release()
	meta := []byte("metadata frontiers and clocks")
	if err := os.WriteFile(layout.MetaDB(db), meta, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := repairMirrorCmdTo([]string{"--db", db, "--origin", "123", "--apply", "--expected-sha256", plan.SHA256}, &out); err != nil {
		t.Fatal(err)
	}
	var result map[string]string
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(result["evidence"], "original", "seg-00000000.bin")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segment); !os.IsNotExist(err) {
		t.Fatal("origin still present")
	}
	b, err := os.ReadFile(db)
	if err != nil || string(b) != "application bytes" {
		t.Fatalf("database changed: %v", err)
	}
	b, err = os.ReadFile(layout.MetaDB(db))
	if err != nil || !bytes.Equal(b, meta) {
		t.Fatalf("metadata changed: %v", err)
	}
	claim, err = layout.ClaimDaemon(db)
	if err != nil {
		t.Fatalf("lock leaked: %v", err)
	}
	_ = claim.Release()
}
