package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
)

// rapplyone applies a single LTX object's pages onto an existing db
// file (delta apply): jprobe rapplyone <uri> <key> <dstPath>
func rapplyone() {
	if len(os.Args) != 5 || os.Args[1] != "rapplyone" {
		return
	}
	ctx := context.Background()
	b, err := objectstore.Open(ctx, os.Args[2])
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	rc, _, err := b.Get(ctx, os.Args[3])
	if err != nil {
		fmt.Printf("get: %v\n", err)
		os.Exit(1)
	}
	defer rc.Close()
	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		fmt.Printf("decode header: %v\n", err)
		os.Exit(1)
	}
	hdr := dec.Header()
	ps := int64(hdr.PageSize)
	dst, err := os.OpenFile(os.Args[4], os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		fmt.Printf("open dst: %v\n", err)
		os.Exit(1)
	}
	defer dst.Close()
	page := make([]byte, ps)
	var ph ltx.PageHeader
	n := 0
	for {
		if err := dec.DecodePage(&ph, page); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("decode page: %v\n", err)
			os.Exit(1)
		}
		if _, err := dst.WriteAt(page, int64(ph.Pgno-1)*ps); err != nil {
			fmt.Printf("write: %v\n", err)
			os.Exit(1)
		}
		n++
	}
	fmt.Printf("applied key=%s pages=%d commit=%d min=%d max=%d\n", os.Args[3], n, hdr.Commit, hdr.MinTXID, hdr.MaxTXID)
	os.Exit(0)
}

func init() { rapplyone() }
