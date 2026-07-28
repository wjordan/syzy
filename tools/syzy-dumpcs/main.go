// Command syzy-dumpcs is a scratch debugging tool: it decodes wire-format changesets from a journal dir.
// Usage: syzy-dumpcs <journal-dir> [pk-substr]
package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

func colStr(v crdt.ColValue) string {
	id := hex.EncodeToString(v.Column[:])[:8]
	switch v.TypeTag {
	case crdt.ColInt:
		return fmt.Sprintf("%s=int:%d", id, int64(binary.BigEndian.Uint64(v.Bytes)))
	case crdt.ColText:
		return fmt.Sprintf("%s=text:%q", id, string(v.Bytes))
	case crdt.ColNull:
		return id + "=null"
	default:
		s := hex.EncodeToString(v.Bytes)
		if len(s) > 64 {
			s = s[:64] + "..."
		}
		return fmt.Sprintf("%s=t%d:%s", id, v.TypeTag, s)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: syzy-dumpcs <journal-dir> [pk-substr]")
		os.Exit(2)
	}
	dir := os.Args[1]
	var filter string
	if len(os.Args) > 2 {
		filter = os.Args[2]
	}
	j, err := journal.Open(dir, 0, journal.SyncOff)
	if err != nil {
		panic(err)
	}
	it := j.Iterate(0)
	for {
		rec, _, err := it.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
			break
		}
		if err != nil {
			fmt.Println("ITER ERR:", err)
			break
		}
		cs, err := crdt.Decode(rec.Payload)
		if err != nil {
			fmt.Println("DECODE ERR:", err)
			continue
		}
		var lines []string
		for _, r := range cs.Records {
			h := r.Header()
			pk := fmt.Sprintf("%x", []byte(h.PK))
			pkPrint := strings.Map(func(c rune) rune {
				if c >= 32 && c < 127 {
					return c
				}
				return '.'
			}, string(h.PK))
			if filter != "" && !strings.Contains(pkPrint, filter) && !strings.Contains(pk, filter) {
				continue
			}
			line := fmt.Sprintf("  op=%T table=%x pk=%q cl=%d", r, h.Table[:4], pkPrint, h.CL)
			switch rr := r.(type) {
			case crdt.Insert:
				for _, v := range rr.Image {
					line += " " + colStr(v)
				}
			case crdt.Update:
				for _, v := range rr.Changed {
					line += " " + colStr(v)
				}
			}
			lines = append(lines, line)
		}
		if len(lines) > 0 {
			fmt.Printf("seq=%d stamp={wall=%d logical=%d origin=%d} deps=%v nrec=%d\n",
				cs.Dot.Seq, cs.Stamp.WallTime, cs.Stamp.Logical, cs.Stamp.Origin, cs.Deps, len(cs.Records))
			for _, l := range lines {
				fmt.Println(l)
			}
		}
	}
}
