package sqlitebridge

import (
	"errors"
	"strings"
	"testing"
)

// A commit-hook veto must surface with Extended =
// SQLITE_CONSTRAINT_COMMITHOOK so embedders can distinguish a commit-hook
// rejection from an ordinary UNIQUE/NOT NULL violation (same primary
// SQLITE_CONSTRAINT code). Higher layers decide whether the rejection is
// retryable from its attached Go cause.
func TestError_CommitHookVetoCarriesExtendedCode(t *testing.T) {
	c, err := Open(":memory:", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	if err := c.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	c.SetCommitHook(func() int { return 1 })
	err = c.Exec(`INSERT INTO t VALUES (1)`)
	if err == nil {
		t.Fatal("vetoed insert succeeded")
	}
	var se Error
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want sqlitebridge.Error", err)
	}
	if se.Code != ResultConstraint {
		t.Fatalf("Code = %d, want %d", se.Code, ResultConstraint)
	}
	if se.Extended != ResultConstraintCommitHook {
		t.Fatalf("Extended = %d, want %d (COMMITHOOK)", se.Extended, ResultConstraintCommitHook)
	}
	if !IsCode(err, ResultConstraintCommitHook) {
		t.Fatal("IsCode(COMMITHOOK) = false")
	}

	// An ordinary UNIQUE violation must NOT read as a commit-hook veto.
	c.SetCommitHook(nil)
	if err := c.Exec(`CREATE TABLE u (x UNIQUE); INSERT INTO u VALUES (1)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err = c.Exec(`INSERT INTO u VALUES (1)`)
	if err == nil || !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("want UNIQUE violation, got %v", err)
	}
	if IsCode(err, ResultConstraintCommitHook) {
		t.Fatalf("UNIQUE violation misread as commit-hook veto: %v", err)
	}
}

func TestError_CommitHookCausePreservesSQLiteCode(t *testing.T) {
	c, err := Open(":memory:", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	if err := c.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	cause := errors.New("reservation unavailable")
	c.SetCommitHook(func() int {
		c.SetCommitHookCause(cause)
		return 1
	})
	err = c.Exec(`INSERT INTO t VALUES (1)`)
	if !errors.Is(err, cause) {
		t.Fatalf("error lost commit-hook cause: %v", err)
	}
	if !IsCode(err, ResultConstraintCommitHook) {
		t.Fatalf("error lost SQLite commit-hook code: %v", err)
	}
	c.SetCommitHook(func() int { return 1 })
	err = c.Exec(`INSERT INTO t VALUES (2)`)
	if errors.Is(err, cause) {
		t.Fatalf("commit-hook cause leaked into the next rejection: %v", err)
	}
}
