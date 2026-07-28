package main

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
)

func TestResolveListenAddr_FileClusterPicksUnixSocket(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	root := "file://" + dir

	listen, err := resolveListenAddr("", root, dbPath)
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	if !strings.HasPrefix(listen, "unix:") {
		t.Errorf("listen = %q, want unix: prefix", listen)
	}
	// The socket must live under <cluster_root>/peers/.
	if !strings.Contains(listen, filepath.Join(dir, "peers")+string(filepath.Separator)) {
		t.Errorf("listen = %q, expected under %s/peers/", listen, dir)
	}
}

func TestResolveListenAddr_S3ClusterPicksTCP(t *testing.T) {
	listen, err := resolveListenAddr("", "s3://bkt/myapp", "/tmp/app.db")
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	if listen != defaultMeshPort {
		t.Errorf("listen = %q, want %q", listen, defaultMeshPort)
	}
}

func TestResolveListenAddr_ExplicitOverride(t *testing.T) {
	listen, err := resolveListenAddr(":8000", "file:///srv", "/tmp/app.db")
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	if listen != ":8000" {
		t.Errorf("explicit value not honored: listen=%q", listen)
	}
}

func TestResolveListenAddr_OffDisables(t *testing.T) {
	listen, err := resolveListenAddr("off", "file:///srv", "/tmp/app.db")
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	if listen != "" {
		t.Errorf("off should disable: listen=%q", listen)
	}
}

func TestResolveListenAddr_NoCluster(t *testing.T) {
	// Producer-only mode: cluster_root is empty, no listener.
	listen, err := resolveListenAddr("", "", "/tmp/app.db")
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	if listen != "" {
		t.Errorf("empty cluster should yield no listener: listen=%q", listen)
	}
}

func TestDBPathHash_StableAndShort(t *testing.T) {
	a := layout.PathHash("/srv/myapp/a.db")
	b := layout.PathHash("/srv/myapp/a.db")
	c := layout.PathHash("/srv/myapp/b.db")
	if a != b {
		t.Errorf("hash not stable: %q vs %q", a, b)
	}
	if a == c {
		t.Errorf("hash collided across distinct paths: %q == %q", a, c)
	}
	if len(a) != 16 {
		t.Errorf("hash len = %d, want 16 (8 bytes hex)", len(a))
	}
}

func TestResolveListenAddr_DeepPathFallsBackToShortSocket(t *testing.T) {
	// A database deep enough that <cluster>/peers/<hash>.sock overflows
	// sockaddr_un.sun_path. Binding it would fail with a bare EINVAL, so
	// the default relocates to the short per-user socket directory.
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t))
	dir := filepath.Join(t.TempDir(), strings.Repeat("nested/", 12))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dbPath := filepath.Join(dir, "app.db")

	listen, err := resolveListenAddr("", "file://"+dir, dbPath)
	if err != nil {
		t.Fatalf("resolveListenAddr: %v", err)
	}
	path := strings.TrimPrefix(listen, "unix:")
	if path == listen {
		t.Fatalf("listen = %q, want a unix: address", listen)
	}
	if err := layout.CheckUnixSocketPath(path); err != nil {
		t.Errorf("fallback path is still unusable: %v", err)
	}
	// It must actually bind — the whole point of the fallback.
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("fallback path did not bind: %v", err)
	}
	ln.Close()
}

func TestResolveListenAddr_DeepPathSocketsAreDistinctPerDatabase(t *testing.T) {
	// Every database in a cluster shares one cluster root, so a
	// fallback socket keyed on the root would collide: the second
	// daemon unlinks and steals the first's listener, and both sides
	// silently lose their peer.
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t))
	root := filepath.Join(t.TempDir(), strings.Repeat("nested/", 12))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	a, err := resolveListenAddr("", "file://"+root, filepath.Join(root, "a.db"))
	if err != nil {
		t.Fatalf("resolveListenAddr(a): %v", err)
	}
	b, err := resolveListenAddr("", "file://"+root, filepath.Join(root, "b.db"))
	if err != nil {
		t.Fatalf("resolveListenAddr(b): %v", err)
	}
	if a == b {
		t.Fatalf("two databases in one cluster collided on %q", a)
	}
}

// shortTempDir is t.TempDir() without the test name in the path. The
// fallback socket directory has to leave room under sun_path, and
// t.TempDir() embeds the (long) test function name.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "syzy")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
