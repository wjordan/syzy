package syzyext

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/wjordan/syzy/internal/ctrlsock"
	"github.com/wjordan/syzy/internal/layout"
)

// attachOrSpawnDaemon returns a held-open client connection to the syzy
// daemon serving dbPath. If no daemon is running and autoSpawn is
// true, it fork+execs `syzy daemon` (or the binary named by
// SYZY_BIN) detached and polls the control socket until it comes up.
// Returns (nil, nil) when no daemon is reachable and autoSpawn is
// false; the producer still works in that mode, journal writes just
// don't replicate until a daemon starts.
//
// Daemon spawn forwards env knobs to the child:
//
//	SYZY_BIN              path to the syzy CLI binary (default: syzy on PATH)
//	SYZY_LISTEN           --listen
//	SYZY_BUNDLE_LISTEN    --bundle-listen
//	SYZY_SEEDS            --seeds
//	SYZY_CLUSTER          --cluster
//	SYZY_OBJECT_BACKEND   --object-backend
//	SYZY_SCHEMA_LOG       --schema-log
//	SYZY_SCHEMA_LOG_DIAL  --schema-log-dial
//	SYZY_SCHEMA_LOG_S3    --schema-log-s3
//
// The forked daemon detaches (Setsid) so it survives the caller
// exiting; stderr lands at <metadata>/daemon.log.
func attachOrSpawnDaemon(dbPath string, autoSpawn bool) (*ctrlsock.Client, error) {
	if c, err := ctrlsock.Dial(dbPath); err == nil {
		return c, nil
	} else if !errors.Is(err, ctrlsock.ErrNoDaemon) {
		return nil, fmt.Errorf("ctrlsock dial: %w", err)
	}
	if !autoSpawn {
		return nil, nil
	}
	if err := spawnDaemon(dbPath); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := ctrlsock.Dial(dbPath)
		if err == nil {
			return c, nil
		}
		if !errors.Is(err, ctrlsock.ErrNoDaemon) {
			return nil, fmt.Errorf("ctrlsock dial after spawn: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("daemon did not bind control socket within 10s; see %s/daemon.log",
				layout.MetaDir(dbPath))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func spawnDaemon(dbPath string) error {
	bin := os.Getenv("SYZY_BIN")
	if bin == "" {
		bin = "syzy"
	}
	resolved, err := exec.LookPath(bin)
	if err != nil {
		// The extension and the CLI install together, so a loadable
		// extension with no CLI beside it means a partial install.
		// Say that instead of leaking an exec errno.
		return fmt.Errorf("cannot find the %q command on $PATH: the loadable extension "+
			"spawns it to run replication, and the two install together. Add its "+
			"directory to $PATH, or set SYZY_BIN to its full path.", bin)
	}
	bin = resolved
	args := []string{"daemon", "-db", dbPath}
	for _, fwd := range []struct{ env, flag string }{
		{"SYZY_LISTEN", "-listen"},
		{"SYZY_BUNDLE_LISTEN", "-bundle-listen"},
		{"SYZY_SEEDS", "-seeds"},
		{"SYZY_CLUSTER", "-cluster"},
		{"SYZY_OBJECT_BACKEND", "-object-backend"},
		{"SYZY_SCHEMA_LOG", "-schema-log"},
		{"SYZY_SCHEMA_LOG_DIAL", "-schema-log-dial"},
		{"SYZY_SCHEMA_LOG_S3", "-schema-log-s3"},
	} {
		if v := strings.TrimSpace(os.Getenv(fwd.env)); v != "" {
			args = append(args, fwd.flag, v)
		}
	}
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		return fmt.Errorf("mkdir metadata dir: %w", err)
	}
	logPath := layout.MetaDir(dbPath) + "/daemon.log"
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open daemon log: %w", err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = nil
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("spawn syzy daemon (%s %v): %w", bin, args, err)
	}
	// Child inherited the fd; the parent no longer needs it. Closing
	// immediately avoids holding daemon.log open for the daemon's full
	// lifetime (which can be days for long-running clients).
	_ = logFile.Close()
	go func() { _ = cmd.Wait() }()
	return nil
}
