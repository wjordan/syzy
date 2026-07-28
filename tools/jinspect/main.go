// Command jinspect is a scratch debugging tool: it reports the origin-seq floor/tip of a mirror
// journal dir by parsing the wire-format (origin, seq) prefix of each
// KindMirror record — no full changeset decode.
// Usage: jinspect <journal-dir>
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/wjordan/syzy/internal/journal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jinspect <journal-dir>")
		os.Exit(2)
	}
	j, err := journal.Open(os.Args[1], 0, journal.SyncOff)
	if err != nil {
		panic(err)
	}
	it := j.Iterate(0)
	var minSeq, maxSeq uint64
	var n, mirror int
	var firstSeqs []uint64
	// HLC==0 identifies records from the pre-self-log writer. Counting both
	// forms makes retained legacy prefixes and the durable-capture roll point
	// visible without interpreting their payloads.
	var legacy, current int
	var legacyMax, currentFloor uint64
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			fmt.Println("ITER ERR:", err)
			break
		}
		n++
		if rec.Kind != journal.KindMirror || rec.Aborted() || len(rec.Payload) < 17 {
			continue
		}
		seq := binary.BigEndian.Uint64(rec.Payload[9:17])
		mirror++
		if mirror == 1 || seq < minSeq {
			minSeq = seq
		}
		if seq > maxSeq {
			maxSeq = seq
		}
		if rec.HLC == 0 {
			legacy++
			if seq > legacyMax {
				legacyMax = seq
			}
		} else {
			current++
			if current == 1 || seq < currentFloor {
				currentFloor = seq
			}
		}
		if len(firstSeqs) < 5 {
			firstSeqs = append(firstSeqs, seq)
		}
	}
	fmt.Printf("records=%d mirror=%d floor_seq=%d tip_seq=%d first=%v\n",
		n, mirror, minSeq, maxSeq, firstSeqs)
	fmt.Printf("legacy=%d legacy_max=%d current=%d current_floor=%d\n",
		legacy, legacyMax, current, currentFloor)
	// Second pass: detect holes in [minSeq,maxSeq] and report presence of a
	// target seq (arg 2). Cheap for the segment-sized ranges we inspect.
	if maxSeq == 0 {
		return
	}
	present := map[uint64]bool{}
	it2 := j.Iterate(0)
	for {
		rec, _, err := it2.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			break
		}
		if rec.Kind == journal.KindMirror && len(rec.Payload) >= 17 {
			present[binary.BigEndian.Uint64(rec.Payload[9:17])] = true
		}
	}
	var holes []uint64
	for s := minSeq; s <= maxSeq; s++ {
		if !present[s] {
			holes = append(holes, s)
			if len(holes) > 5000000 {
				break
			}
		}
	}
	fmt.Printf("holes_in[%d,%d]=%d first_holes=%v\n", minSeq, maxSeq, len(holes), holes)
	if len(os.Args) > 2 {
		var t uint64
		fmt.Sscanf(os.Args[2], "%d", &t)
		fmt.Printf("target_seq %d present=%v\n", t, present[t])
	}
}
