// Command jprobe replays drainer-vs-Refresh semantics over origin journals to
// diagnose a WaitForDrain that never converges (DrainedOffset < Head).
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/wjordan/syzy/internal/journal"
)

func main() {
	for _, dir := range os.Args[1:] {
		probe(dir)
	}
}

func probe(dir string) {
	origin := filepath.Base(filepath.Dir(dir))
	j, err := journal.Open(dir, 0, journal.SyncOff)
	if err != nil {
		fmt.Printf("%s: open: %v\n", origin, err)
		return
	}
	defer j.Close()

	refreshed := j.Refresh()
	head := j.Head()

	// Walk like a drainer from 0: stop at EOF/ErrPending like collectBatch.
	it := j.Iterate(0)
	var n, sealed, aborted int
	var stopErr error
	for {
		rec, _, err := it.Next()
		if err != nil {
			stopErr = err
			break
		}
		n++
		if rec.Kind == journal.KindSeal {
			sealed++
		}
		if rec.Aborted() {
			aborted++
		}
	}
	stop := it.Offset()
	verdict := "CONVERGES"
	if uint64(stop) < uint64(head) {
		verdict = "STUCK (iterator stop < head)"
	}
	errName := "nil"
	switch {
	case errors.Is(stopErr, io.EOF):
		errName = "EOF"
	case errors.Is(stopErr, journal.ErrPending):
		errName = "PENDING(torn)"
	case stopErr != nil:
		errName = stopErr.Error()
	}
	fmt.Printf("%s: refreshed=%d head=%d records=%d seals=%d aborted=%d iterStop=%d stopErr=%s -> %s\n",
		origin, refreshed, uint64(head), n, sealed, aborted, uint64(stop), errName, verdict)
}
