package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// rchain lists L0/L1 chain entries around a TXID:
// jprobe rchain <uri> <prefix> <aroundTXID>
func rchain() {
	if len(os.Args) != 5 || os.Args[1] != "rchain" {
		return
	}
	ctx := context.Background()
	b, err := objectstore.Open(ctx, os.Args[2])
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	b = objectstore.Prefixed(b, os.Args[3]+"/")
	around, _ := strconv.ParseUint(os.Args[4], 10, 64)
	for _, lvl := range []int{objstore.L0Level, objstore.L1Level} {
		files, err := objstore.ListLTX(ctx, b, streamPrefix(), lvl)
		if err != nil {
			fmt.Printf("list L%d: %v\n", lvl, err)
			os.Exit(1)
		}
		fmt.Printf("== level %d: %d files\n", lvl, len(files))
		for _, f := range files {
			// Print entries near or past the pivot, and any range
			// that STRADDLES it (the corruption suspect).
			straddle := f.MinTXID <= around && f.MaxTXID >= around
			if f.MaxTXID >= around-20 || straddle {
				marker := ""
				if straddle {
					marker = "  <-- STRADDLES BASELINE"
				}
				fmt.Printf("  %s min=%d max=%d%s\n", f.Key, f.MinTXID, f.MaxTXID, marker)
			}
		}
	}
	os.Exit(0)
}

func init() { rchain() }

// streamPrefix selects db vs metadata via JPROBE_STREAM (default db).
func streamPrefix() string {
	if os.Getenv("JPROBE_STREAM") == "metadata" {
		return objstore.MetadataPrefix
	}
	return objstore.DBPrefix
}
