package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/layout"
	syzy "github.com/wjordan/syzy/sqlite"
)

// TestOpen_DaemonClaimRetriesPastTransientHold: the daemon flock can look
// held for a moment after its holder released it — O_CLOEXEC closes
// fork-inherited FDs at exec, not at fork, so any concurrently forking
// thread briefly keeps a just-released lock alive through the child's
// inherited descriptor. Open must retry past such a transient hold instead
// of failing a legitimate single-process reopen; a genuine long-lived
// holder still fails.
func TestOpen_DaemonClaimRetriesPastTransientHold(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir() + "/app.db"

	claim, err := layout.ClaimDaemon(dbPath)
	if err != nil {
		t.Fatalf("pre-hold daemon claim: %v", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = claim.Release()
	}()

	node, err := syzy.Open(context.Background(), syzy.Config{Path: dbPath})
	if err != nil {
		t.Fatalf("Open under transient daemon hold: %v", err)
	}
	node.Close()
}
