// Package syzyenv reads configuration out of the process environment
// on behalf of code that can end up inside the loadable extension.
//
// It exists because Go and libc disagree about what "the environment"
// is inside a c-shared object. Go snapshots it once, during runtime
// init, by walking the pointer array the kernel left on the initial
// process stack (runtime.goenvs_unix); libc getenv follows __environ,
// which the host process is free to relocate at any time.
//
// Runtimes that implement setproctitle the BSD way copy environ onto
// the heap, repoint __environ at the copy, and reuse the original
// argv+environ stack region as scratch space for the title. Ruby does
// this, as do libuv (Node's process.title) and Python's setproctitle
// package. Once a title outgrows the argv region it overwrites the
// environment strings still sitting on the stack, so a Go runtime that
// starts afterwards, which is exactly what a lazily dlopen'd engine
// does, reads a truncated environment: getenv resolves a variable that
// os.Getenv reports as unset.
//
// Under the extension build every knob therefore resolves through
// libc. Other builds read the stack snapshot, which is equivalent in a
// process whose argv region nobody has scribbled on.
//
// The environment is not the only channel. A process's environment is
// fixed at exec, so a host that starts a process before it knows which
// database that process will serve cannot configure it that way. Vars
// resolves the environment first and a per-database file second; see
// the SQLite architecture doc's Configuration section for the split
// between attach-time knobs and the bootstrap ones that must arrive in
// the environment.
//
// Every SYZY_* knob is read through this package, and so is any other
// variable extension-reachable code consults; `make vet` fails on a
// direct os.Getenv of a SYZY_* name.
package syzyenv

// Getenv returns the value of the named environment variable, or ""
// when it is unset.
func Getenv(key string) string { return getenv(key) }

// Environ returns the environment as "key=value" strings, in the form
// exec.Cmd.Env expects. Callers that hand an environment to a child
// process must use this rather than os.Environ, or the child inherits
// the same truncated snapshot.
func Environ() []string { return environ() }
