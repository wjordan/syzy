// Package buildinfo reports the build identity of the running binary.
//
// The CLI and the loadable extension are two artifacts of one build
// that talk to each other over the control socket, and the extension
// auto-spawns the CLI's daemon from $PATH. A half-upgraded pair is
// therefore easy to produce and hard to diagnose, so both stamp their
// version here and the control-socket handshake refuses a mismatch.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

// version is set at link time by the release build:
//
//	-ldflags "-X github.com/wjordan/syzy/internal/buildinfo.version=v0.1.0"
//
// Unstamped builds fall back to the VCS data the Go toolchain embeds.
var version string

// Version returns the build's version string. Release builds report
// their tag ("v0.1.0"); everything else reports "devel+<revision>", or
// plain "devel" when no VCS data is available.
var Version = sync.OnceValue(func() string {
	if version != "" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	// Current toolchains synthesize a VCS pseudo-version here even for a
	// plain `go build`, and a `go install module@version` carries the
	// real tag. Both artifacts derive it from the same tree, so an
	// unstamped dev pair still agrees at the handshake.
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	// Fallback for build modes that leave Main.Version empty but still
	// record the raw VCS settings.
	var rev, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				// Two builds from one dirty tree are indistinguishable.
				// A limit of the data, not something to paper over.
				dirty = ".dirty"
			}
		}
	}
	if rev == "" {
		return "devel"
	}
	return "devel+" + rev[:min(12, len(rev))] + dirty
})

// Full returns the multi-line form printed by `syzy version`.
func Full() string {
	return fmt.Sprintf("syzy %s\n%s %s/%s\n",
		Version(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
