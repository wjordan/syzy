//go:build syzy_extension

// Park control: a per-process socket over which a supervisor asks the
// engine to park (drop every fd/mmap/lock its $SYZY_DB attachments
// hold, ahead of the backing share going away) and later unpark
// (reopen the files and re-attach with a fresh origin). The wrapper
// VFS half lives in app_vfs.c; this file owns the socket protocol and
// the engine (Go) half: tearing down and rebuilding the syzyext
// attachments around the VFS park.
//
// The C half is driven through the sx_park_ops table published in the
// registered VFS's pAppData, reached via the app libsqlite3's
// sqlite3_vfs_find — NOT via linked references or dlsym of the sx_app_*
// symbols. Two copies of app_vfs.c coexist under the lazy shim
// (preloaded shim + this dlopen'd engine), and this .so is linked
// -Bsymbolic, so symbol lookups from here bind to its own DORMANT
// copy; the VFS list is the only authoritative route to the active one.
package main

/*
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sqlite3.h"
#include "syzy_app_vfs.h"

// sx_park_ops_find returns the active wrapper VFS's park ops, or NULL
// (with a reason in err) when no wrapper is registered in this process.
static const sx_park_ops *sx_park_ops_find(char *err, int errlen) {
	typedef sqlite3_vfs *(*xVfsFind)(const char *);
	xVfsFind vfs_find = (xVfsFind)dlsym(RTLD_DEFAULT, "sqlite3_vfs_find");
	if (vfs_find == NULL) {
		snprintf(err, errlen, "sqlite3_vfs_find not resolvable");
		return NULL;
	}
	sqlite3_vfs *vfs = vfs_find("syzy-app");
	if (vfs == NULL || vfs->pAppData == NULL) {
		snprintf(err, errlen, "syzy-app vfs not registered");
		return NULL;
	}
	return (const sx_park_ops *)vfs->pAppData;
}

// sx_park_step_call runs one park op by index (order of sx_park_ops).
static int sx_park_step_call(int step, char *err, int errlen) {
	const sx_park_ops *ops = sx_park_ops_find(err, errlen);
	if (ops == NULL) return 1;
	switch (step) {
	case 0: return ops->park_begin(err, errlen);
	case 1: return ops->park_commit(err, errlen);
	case 2: return ops->unpark_files(err, errlen);
	case 3: return ops->unpark_open(err, errlen);
	}
	snprintf(err, errlen, "bad park step %d", step);
	return 1;
}

static void sx_bypass_call(int on) {
	char err[64];
	const sx_park_ops *ops = sx_park_ops_find(err, sizeof(err));
	if (ops != NULL) ops->gate_bypass(on);
}
*/
import "C"

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/sqlite/syzyext"
	"github.com/wjordan/syzy/sqlitebridge"
)

var parkOnce sync.Once

// startParkControl starts the park control listener on the abstract
// unix socket @syzy-park.<pid>, once per process. Called after the
// first successful attach; SYZY_APP_PARK=0 disables it (matching the
// shim's wrapper-VFS gate, so both halves switch off together).
func startParkControl() {
	parkOnce.Do(func() {
		if os.Getenv("SYZY_APP_PARK") == "0" {
			return
		}
		ln, err := net.Listen("unix", fmt.Sprintf("@syzy-park.%d", os.Getpid()))
		if err != nil {
			syzylog.Default().Warn("syzy: park control listen failed", "error", err)
			return
		}
		go serveParkControl(ln)
	})
}

// serveParkControl accepts one command per connection: "park" or
// "unpark", answered with "ok" or "err <reason>". The goroutine is
// locked to its OS thread so the gate-bypass registration in app_vfs.c
// (thread-keyed) covers every cgo call made while handling a command.
func serveParkControl(ln net.Listener) {
	runtime.LockOSThread()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handleParkConn(conn)
	}
}

func handleParkConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	var reply string
	switch strings.TrimSpace(line) {
	case "park":
		reply = replyFor(parkAttachments())
	case "unpark":
		reply = replyFor(unparkAttachments())
	default:
		reply = "err unknown command"
	}
	_, _ = conn.Write([]byte(reply + "\n"))
}

func replyFor(err error) string {
	if err != nil {
		return "err " + strings.ReplaceAll(err.Error(), "\n", " ")
	}
	return "ok"
}

// Step indices match sx_park_step_call's dispatch over sx_park_ops.
const (
	stepParkBegin = iota
	stepParkCommit
	stepUnparkFiles
	stepUnparkOpen
)

var stepNames = []string{"park_begin", "park_commit", "unpark_files", "unpark_open"}

// parkStep invokes one wrapper-VFS park op, translating its
// error-buffer convention.
func parkStep(step int) error {
	buf := make([]byte, 256)
	rc := C.sx_park_step_call(C.int(step), (*C.char)(unsafe.Pointer(&buf[0])), C.int(len(buf)))
	if rc != 0 {
		msg := string(buf)
		if i := strings.IndexByte(msg, 0); i >= 0 {
			msg = msg[:i]
		}
		return fmt.Errorf("%s: %s", stepNames[step], msg)
	}
	return nil
}

// targetEntries returns the extMap entries attached to $SYZY_DB.
func targetEntries() []*extState {
	target := os.Getenv("SYZY_DB")
	if target != "" {
		if r, err := filepath.EvalSymlinks(target); err == nil {
			target = r
		}
	}
	var out []*extState
	extMu.Lock()
	for _, st := range extMap {
		if target != "" && st.dbPath == target {
			out = append(out, st)
		}
	}
	extMu.Unlock()
	return out
}

// parkAttachments: latch+drain the wrapper VFS (nacks on an open
// transaction), tear down the engine attachments while the app's
// connections are still open (hook removal needs them) — Close plus
// writer Release, the exact inverse of initExtension, so per-conn
// state like the notify-feed reader goes too — then release the
// connections' files. The gate stays latched until unpark.
func parkAttachments() error {
	C.sx_bypass_call(1)
	defer C.sx_bypass_call(0)
	if err := parkStep(stepParkBegin); err != nil {
		return err
	}
	for _, st := range targetEntries() {
		if st.attached != nil {
			if err := st.attached.Close(); err != nil {
				syzylog.Default().Warn("syzy: park detach", "db", st.dbPath, "error", err)
			}
			st.attached = nil
		}
		if st.writer != nil {
			_ = st.writer.Release()
			st.writer = nil
		}
	}
	if err := parkStep(stepParkCommit); err != nil {
		return err
	}
	syzylog.Default().Info("syzy: parked", "pid", os.Getpid())
	return nil
}

// unparkAttachments: reopen the files (same paths, fresh backing),
// re-run the attach flow on each parked entry — a fresh Attach mints
// a NEW origin, required for restored clones — and only then reopen
// the gate.
func unparkAttachments() error {
	C.sx_bypass_call(1)
	defer C.sx_bypass_call(0)
	if err := parkStep(stepUnparkFiles); err != nil {
		return err
	}
	for _, st := range targetEntries() {
		if st.attached != nil {
			continue
		}
		writer, err := sqlitebridge.WrapHandle(st.handle)
		if err != nil {
			return fmt.Errorf("rewrap %s: %w", st.dbPath, err)
		}
		attached, err := syzyext.AttachWithRetry(writer, syzyext.Config{
			DBPath:    st.dbPath,
			AutoSpawn: os.Getenv("SYZY_AUTOSPAWN") != "0",
		})
		if err != nil {
			_ = writer.Release()
			return fmt.Errorf("re-attach %s: %w", st.dbPath, err)
		}
		st.writer = writer
		st.attached = attached
	}
	if err := parkStep(stepUnparkOpen); err != nil {
		return err
	}
	syzylog.Default().Info("syzy: unparked", "pid", os.Getpid())
	return nil
}
