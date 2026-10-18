package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/mirror"
)

func repairMirrorCmd(args []string) error { return repairMirrorCmdTo(args, os.Stdout) }

func repairMirrorCmdTo(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("repair-mirror", flag.ContinueOnError)
	db := fs.String("db", "", "existing application database path")
	originText := fs.String("origin", "", "affected origin in decimal")
	apply := fs.Bool("apply", false, "quarantine the inspected empty mirror; requires all writers stopped")
	expected := fs.String("expected-sha256", "", "reviewed segment digest, required with --apply")
	if err := fs.Parse(args); err != nil {
		return err
	}
	origin, err := strconv.ParseUint(*originText, 10, 64)
	if *db == "" || err != nil || origin == 0 || len(fs.Args()) != 0 {
		return errors.New("--db and a nonzero decimal --origin are required, without positional arguments")
	}
	info, err := os.Lstat(*db)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("--db must name an existing regular file")
	}
	abs, err := filepath.Abs(*db)
	if err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil || real != abs {
		return errors.New("database path must have no symlink components")
	}
	real, err = filepath.EvalSymlinks(layout.MetaDir(abs))
	if err != nil || real != layout.MetaDir(abs) {
		return errors.New("metadata path must exist without symlink components")
	}
	if *apply {
		digest, err := hex.DecodeString(*expected)
		if err != nil || len(digest) != 32 {
			return errors.New("--apply requires the reviewed --expected-sha256")
		}
		// Do not create a lock or initialize an unknown metadata directory. The
		// existing daemon lock must identify the same stopped replica.
		locks, err := lockOfflineReplica(abs)
		if err != nil {
			return err
		}
		defer func() {
			for _, lock := range locks {
				_ = lock.Close()
			}
		}()
	}
	plan, err := mirror.InspectEmptyMirror(filepath.Join(layout.MetaDir(abs), "mirror"), crdt.Origin(origin))
	if err != nil {
		return err
	}
	if !*apply {
		return json.NewEncoder(out).Encode(plan)
	}
	if plan.SHA256 != *expected {
		return errors.New("segment digest differs from reviewed inspection")
	}
	recovery, err := mirror.QuarantineEmptyMirror(plan)
	if err != nil {
		if recovery == "" {
			return fmt.Errorf("repair refused before quarantine: %w", err)
		}
		return fmt.Errorf("repair stopped; inspect evidence directory %q before retrying: %w", recovery, err)
	}
	return json.NewEncoder(out).Encode(map[string]string{"evidence": recovery, "result": "quarantined; database and metadata unchanged"})
}

func lockOfflineReplica(db string) (locks []*os.File, err error) {
	defer func() {
		if err != nil {
			for _, lock := range locks {
				_ = lock.Close()
			}
			locks = nil
		}
	}()
	claim := func(path string, directory bool) error {
		flags := os.O_RDONLY | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
		if directory {
			flags |= syscall.O_DIRECTORY
		}
		f, err := os.OpenFile(path, flags, 0)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil || (!directory && !info.Mode().IsRegular()) {
			_ = f.Close()
			return fmt.Errorf("invalid replica lock: %s", path)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			_ = f.Close()
			return fmt.Errorf("all daemons and writers must be stopped (%s): %w", path, err)
		}
		locks = append(locks, f)
		return nil
	}
	if err = claim(layout.DaemonLock(db), false); err != nil {
		return nil, err
	}
	root := layout.OriginsRoot(db)
	info, err := os.Lstat(root)
	if err != nil {
		return locks, err
	}
	if !info.IsDir() {
		return locks, fmt.Errorf("origin root must be a real directory: %s", root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return locks, err
	}
	for _, entry := range entries {
		if len(entry.Name()) != 16 {
			return locks, fmt.Errorf("unexpected origin entry: %s", entry.Name())
		}
		origin, parseErr := strconv.ParseUint(entry.Name(), 16, 64)
		if parseErr != nil || origin == 0 {
			return locks, fmt.Errorf("invalid origin entry: %s", entry.Name())
		}
		if err = claim(filepath.Join(root, entry.Name()), true); err != nil {
			return locks, err
		}
	}
	return locks, nil
}
