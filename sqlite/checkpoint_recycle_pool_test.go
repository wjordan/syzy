package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/ltxstream"
)

// TestAppRecycleHoldsWriterPoolConn: the coordinated WAL-recycle bracket must
// run with writerDB's single pooled connection checked out. database/sql reads
// serialize only on that checkout (not on writeMu), so a bracket that runs
// directly on appWrite races them on the NOMUTEX connection. A read attempted
// inside the bracket must therefore block until the bracket commits, not
// execute concurrently.
func TestAppRecycleHoldsWriterPoolConn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	node, err := Open(context.Background(), Config{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()
	db := node.WriterDB()

	fence := func(hooks ltxstream.CheckpointHooks) error {
		_, err := hooks.Recycle(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			var one int
			err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
			switch {
			case err == nil:
				t.Error("read on the writer pool executed inside the recycle bracket; the bracket does not hold the pooled connection")
			case !errors.Is(err, context.DeadlineExceeded):
				t.Errorf("read inside recycle bracket failed with %v, want context.DeadlineExceeded", err)
			}
			return nil
		})
		return err
	}
	if err := node.appCheckpoint(context.Background(), "PASSIVE", fence); err != nil {
		t.Fatalf("appCheckpoint: %v", err)
	}

	// The bracket released the pooled connection: reads flow again.
	var one int
	if err := db.QueryRow(`SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("read after recycle: %v", err)
	}
}
