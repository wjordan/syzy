package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/wjordan/syzy/internal/journal"
)

// resumeProbe mimics a drainer resuming from a persisted marker:
// Iterate(marker) + Next, printing exactly what the drainer sees.
func resumeProbe(dir string, marker uint64) {
	j, err := journal.Open(dir, 0, journal.SyncOff)
	if err != nil {
		fmt.Printf("open: %v\n", err)
		return
	}
	defer j.Close()
	fmt.Printf("head=%d refreshed=%d\n", uint64(j.Head()), j.Refresh())
	aligned := j.AlignResume(journal.Offset(marker))
	fmt.Printf("AlignResume(%d) = %d\n", marker, uint64(aligned))
	it := j.Iterate(aligned)
	for i := 0; i < 5; i++ {
		rec, off, err := it.Next()
		fmt.Printf("next[%d]: off=%d itOff=%d kind=%d seq=%d aborted=%v err=%v\n",
			i, uint64(off), uint64(it.Offset()), rec.Kind, rec.Seq, rec.Aborted(), err)
		if err != nil {
			return
		}
	}
}

func init() {
	if len(os.Args) == 4 && os.Args[1] == "resume" {
		marker, _ := strconv.ParseUint(os.Args[3], 10, 64)
		resumeProbe(os.Args[2], marker)
		os.Exit(0)
	}
}
