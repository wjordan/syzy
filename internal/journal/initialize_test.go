package journal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestSegmentPublicationOrdersDurabilityBeforeVisibility(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := segmentPath(dir, 0)
	var events []string
	f, err := initializeSegment(path, testSegmentSize, segmentInitOps{
		syncFile: func(f *os.File) error {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("final name became visible before header sync")
			}
			b := make([]byte, fileHeaderSize)
			if _, err := f.ReadAt(b, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := decodeFileHeader(b); err != nil {
				t.Fatalf("syncing incomplete header: %v", err)
			}
			events = append(events, "file sync")
			return fdatasyncFile(f)
		},
		link: func(from, to string) error {
			events = append(events, "publish")
			return os.Link(from, to)
		},
		syncDir: func(dir string) error {
			events = append(events, "directory sync")
			return syncDirectory(dir)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if !reflect.DeepEqual(events, []string{"file sync", "publish", "directory sync"}) {
		t.Fatal(events)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("temporary files remain: %v", entries)
	}
}

func TestSegmentPublicationFailurePreservesRecoverableState(t *testing.T) {
	for _, stage := range []string{"file sync", "publish", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := segmentPath(dir, 0)
			failure := errors.New("injected failure")
			ops := segmentInitOps{syncFile: fdatasyncFile, link: os.Link, syncDir: syncDirectory}
			switch stage {
			case "file sync":
				ops.syncFile = func(*os.File) error { return failure }
			case "publish":
				ops.link = func(string, string) error { return failure }
			case "directory sync":
				ops.syncDir = func(string) error { return failure }
			}
			if f, err := initializeSegment(path, testSegmentSize, ops); !errors.Is(err, failure) || f != nil {
				t.Fatalf("file=%v error=%v", f, err)
			}
			entries, _ := os.ReadDir(dir)
			if stage != "directory sync" {
				if len(entries) != 0 {
					t.Fatalf("failed initialization published files: %v", entries)
				}
				return
			}
			j, err := Open(dir, testSegmentSize, SyncOff)
			if err != nil {
				t.Fatalf("published header is not recoverable: %v", err)
			}
			j.Close()
		})
	}
}

func TestSegmentPublicationDoesNotReplaceWinner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := segmentPath(dir, 0)
	winner := []byte("existing segment must not be replaced")
	if err := os.WriteFile(path, winner, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := initializeSegment(path, testSegmentSize, segmentInitOps{fdatasyncFile, os.Link, syncDirectory})
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	b, _ := os.ReadFile(path)
	if !bytes.Equal(b, winner) {
		t.Fatal("replaced existing bytes")
	}
}

func TestExistingUninitializedSegmentsAreNeverRewritten(t *testing.T) {
	for _, n := range []int{0, 1, fileHeaderSize - 1, int(testSegmentSize)} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := segmentPath(dir, 0)
			before := make([]byte, n)
			if err := os.WriteFile(path, before, 0o600); err != nil {
				t.Fatal(err)
			}
			if j, err := Open(dir, testSegmentSize, SyncOff); err == nil || j != nil {
				t.Fatal("accepted an uninitialized existing segment")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("rewrote existing segment")
			}
		})
	}
}

func TestConcurrentSegmentCreationAndAbandonedInitialization(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".segment-init-interrupted")
	if err := os.WriteFile(orphan, make([]byte, testSegmentSize), 0o600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := Open(dir, testSegmentSize, SyncOff)
			if err != nil {
				t.Error(err)
				return
			}
			j.Close()
		}()
	}
	wg.Wait()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatalf("unexpected files: %v", entries)
	}
	if b, _ := os.ReadFile(orphan); !bytes.Equal(b, make([]byte, testSegmentSize)) {
		t.Fatal("altered abandoned initialization evidence")
	}
}

func TestSegmentDirectoryDurabilityIncludesExistingAncestors(t *testing.T) {
	t.Parallel()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, "already-created", "journal")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := syncDirectoryAncestors(path, func(dir string) error {
		got = append(got, dir)
		return syncDirectory(dir)
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for dir := path; ; dir = filepath.Dir(dir) {
		want = append(want, dir)
		if filepath.Dir(dir) == dir {
			break
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory syncs %v, want %v", got, want)
	}
	failure := errors.New("ancestor sync failed")
	if err := syncDirectoryAncestors(path, func(dir string) error {
		if dir == filepath.Dir(path) {
			return failure
		}
		return nil
	}); !errors.Is(err, failure) {
		t.Fatalf("ignored directory sync failure: %v", err)
	}
}

func TestSegmentDirectoryDurabilityFollowsSymlinkTargetAndLinkParents(t *testing.T) {
	t.Parallel()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(base, "target", "journal")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(base, "links")
	if err := os.Mkdir(linkDir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(linkDir, "journal")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	if err := syncDirectoryAncestors(link, func(path string) error { seen[path] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{target, filepath.Dir(target), link, linkDir, base} {
		if !seen[path] {
			t.Errorf("did not sync %s", path)
		}
	}
}
