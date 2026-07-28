package syncer

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wjordan/syzy/internal/journal"
)

// A secondary drainer whose Run loop dies (sink apply failure) must
// fail WaitForDrain instead of letting callers poll a DrainedOffset
// that can never advance. Regression: a dead drainer left the
// publisher's takeover baseline spinning in waitAllDrained forever
// while holding the node writeMu, deadlocking Node.Close.
func TestSecondaryWaitForDrainFailsWhenDrainerDies(t *testing.T) {
	jdir := filepath.Join(t.TempDir(), "jrn")
	j, err := journal.Open(jdir, 1<<20, journal.SyncOff)
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	sink := &mockSink{failNth: 1}
	dr, err := NewDrainer(j, sink)
	if err != nil {
		t.Fatalf("NewDrainer: %v", err)
	}
	sd := &SecondaryDrainer{Origin: 7, Journal: j, Drainer: dr}
	if _, _, err := j.Append(journal.KindLocalDML, 1, 7, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	sd.Start(context.Background())
	t.Cleanup(func() { _ = sd.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitErr := sd.WaitForDrain(ctx)
	if ctx.Err() != nil {
		t.Fatal("WaitForDrain hung on a dead drainer until ctx timeout")
	}
	if waitErr == nil || !strings.Contains(waitErr.Error(), "drainer dead") {
		t.Fatalf("want drainer-dead error, got %v", waitErr)
	}
}
