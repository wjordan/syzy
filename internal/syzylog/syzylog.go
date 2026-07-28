// Package syzylog is the process-wide diagnostic logger.
//
// Most of this module runs as a library inside somebody else's process:
// the loadable extension is dlopen'd into sqlite3, Python, Node, Ruby.
// Writing to stderr uninvited there corrupts the host's output — an
// interactive `sqlite3` session should not print replication timings
// between the prompt and the query result. So the default sink is
// silent, and diagnostics are opted into with SYZY_LOG.
//
// The daemon is the exception: it owns its process and its stderr goes
// to <db>-syzy/daemon.log, so it installs its own configured logger via
// SetDefault at startup.
package syzylog

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

var current atomic.Pointer[slog.Logger]

// Default returns the diagnostic logger. Until SetDefault is called it
// reflects SYZY_LOG: unset, "off", or "silent" discards; "debug",
// "info", "warn", or "error" writes at that level to stderr.
func Default() *slog.Logger {
	if l := current.Load(); l != nil {
		return l
	}
	l := fromEnv(os.Getenv("SYZY_LOG"))
	// Losing the race is fine: both racers built an equivalent logger.
	current.CompareAndSwap(nil, l)
	return current.Load()
}

// SetDefault installs logger as the process diagnostic logger,
// overriding SYZY_LOG. Pass nil to fall back to the environment.
func SetDefault(logger *slog.Logger) { current.Store(logger) }

func fromEnv(v string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		// Unset, "off", "silent", or anything unrecognized. An
		// unrecognized value must not turn logging on — a typo in a
		// production env var should not start writing into a host
		// process's stderr.
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

// Printf writes one operational diagnostic line at info level.
func Printf(format string, args ...any) { logf(slog.LevelInfo, format, args...) }

// Debugf writes one line at debug level, for high-frequency detail
// (per-open timings and similar) that should stay out of the daemon's
// log at its default level.
func Debugf(format string, args ...any) { logf(slog.LevelDebug, format, args...) }

// logf checks Enabled before formatting: the default sink discards, and
// these sit on hot paths where the Sprintf would be the whole cost.
func logf(lvl slog.Level, format string, args ...any) {
	log := Default()
	if !log.Enabled(nil, lvl) {
		return
	}
	log.Log(nil, lvl, fmt.Sprintf(format, args...))
}
