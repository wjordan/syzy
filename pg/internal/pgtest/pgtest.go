// Package pgtest holds the live-Postgres test contract shared by every
// package in the pg module, so the connection recipe lives in one place
// rather than as a constant repeated per package.
//
// Contract: SYZY_PG_TEST_URL selects the server.
//
//   - set   → the tests MUST connect, and FAIL if they cannot. A broken
//     or missing container is a build failure, never a silent pass. CI
//     always sets it.
//   - unset → PG tests skip, so `go test ./...` stays useful on a
//     machine with no Docker.
//
// scripts/pg-test-container.sh is the canonical server recipe and prints
// the URL to set.
package pgtest

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// URLEnv names the environment variable holding the base URL (a server
// URL ending in "/", to which a database name is appended).
const URLEnv = "SYZY_PG_TEST_URL"

// SocketDirEnv names an optional directory that both this process and
// the Postgres server can see, used for the coordinated-uniqueness
// endpoint's unix socket. When Postgres runs in a container it is a bind
// mount, which is why the tests cannot simply use t.TempDir().
const SocketDirEnv = "SYZY_PG_TEST_SOCKDIR"

// defaultTimeout bounds the reachability probe. Generous enough for a
// cold container, short enough that a wrong URL fails fast.
const defaultTimeout = 5 * time.Second

// probe runs once per process: the reachability check is about the
// server, not about any one test, and the suite calls BaseURL from every
// helper that needs a connection string.
var probe struct {
	once sync.Once
	url  string
	err  error
}

// BaseURL returns the server URL per the contract above, skipping the
// test when SYZY_PG_TEST_URL is unset and failing it when the server is
// unreachable. The returned URL ends in "/".
func BaseURL(t testing.TB) string {
	t.Helper()
	url := os.Getenv(URLEnv)
	if url == "" {
		t.Skipf("%s not set; skipping live Postgres test (see scripts/pg-test-container.sh)", URLEnv)
	}
	if url[len(url)-1] != '/' {
		url += "/"
	}
	probe.once.Do(func() {
		probe.url = url
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		c, err := pgx.Connect(ctx, url+"postgres")
		if err != nil {
			probe.err = err
			return
		}
		_ = c.Close(ctx)
	})
	if probe.err != nil {
		// Deliberately fatal, not a skip: the operator asked for a live
		// run by setting the variable, so an unreachable server is a
		// broken environment and must be visible as a failure.
		t.Fatalf("%s=%s is set but Postgres is unreachable: %v", URLEnv, url, probe.err)
	}
	return probe.url
}

// URL returns the configured server URL without gating the test, for
// building connection strings once BaseURL has already applied the
// skip-or-fail contract. It is empty when no server is configured, which
// only a test that skipped could observe.
func URL() string {
	url := os.Getenv(URLEnv)
	if url != "" && url[len(url)-1] != '/' {
		url += "/"
	}
	return url
}

// SocketDir returns a directory visible to both this process and the
// Postgres server, skipping the test when none is configured. Only tests
// that need Postgres to dial back into this process require it.
func SocketDir(t testing.TB) string {
	t.Helper()
	dir := os.Getenv(SocketDirEnv)
	if dir == "" {
		t.Skipf("%s not set; skipping test that needs Postgres to reach this process", SocketDirEnv)
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatalf("socket dir %s: %v", dir, err)
	}
	return dir
}
