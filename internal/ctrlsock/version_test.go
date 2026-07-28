package ctrlsock_test

import (
	"bufio"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/buildinfo"
	"github.com/wjordan/syzy/internal/ctrlsock"
)

func TestCheckVersion(t *testing.T) {
	self := buildinfo.Version()

	if err := ctrlsock.CheckVersion(self); err != nil {
		t.Errorf("same build rejected: %v", err)
	}

	err := ctrlsock.CheckVersion("v0.0.0-something-else")
	if err == nil {
		t.Fatal("mismatched build accepted")
	}
	for _, want := range []string{self, "v0.0.0-something-else", "reinstall both"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}

	// A daemon predating the handshake sends no version at all. That is
	// still a skew, and the message must not read as an empty string.
	err = ctrlsock.CheckVersion("")
	if err == nil {
		t.Fatal("unversioned peer accepted")
	}
	if !strings.Contains(err.Error(), "unversioned") {
		t.Errorf("unversioned peer error is unclear: %v", err)
	}
}

func TestHello_RefusesVersionSkew(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	db := filepath.Join(t.TempDir(), "app.db")
	srv, err := ctrlsock.Listen(db, "origin", "cluster", "")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Close()

	path, err := ctrlsock.SocketPath(db)
	if err != nil {
		t.Fatalf("SocketPath: %v", err)
	}
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	b, _ := json.Marshal(ctrlsock.HelloMsg{Type: "hello", DBPath: db, Version: "v9.9.9-stale"})
	if _, err := conn.Write(append(b, '\n')); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	var ack ctrlsock.HelloAck
	if err := json.Unmarshal(line, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Type != "error" {
		t.Fatalf("ack.Type = %q, want error for a skewed client", ack.Type)
	}
	if !strings.Contains(ack.Msg, "version mismatch") {
		t.Errorf("ack.Msg = %q, want a version mismatch", ack.Msg)
	}
	// A refused client must not count as attached, or it would hold the
	// daemon's idle-exit timer open.
	if n := srv.Clients(); n != 0 {
		t.Errorf("skewed client counted as attached: clients=%d", n)
	}
}
