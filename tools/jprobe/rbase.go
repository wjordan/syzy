package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

// rbase restores ONLY the app baseline (no L0/L1 chain) from a topic
// prefix, to bisect baseline-encode corruption from chain-replay
// corruption: jprobe rbase <uri> <prefix> <dstPath>
func rbase() {
	if len(os.Args) != 5 || os.Args[1] != "rbase" {
		return
	}
	ctx := context.Background()
	b, err := objectstore.Open(ctx, os.Args[2])
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	b = objectstore.Prefixed(b, os.Args[3]+"/")
	head, _, err := objstore.LoadHEAD(ctx, b)
	if err != nil {
		fmt.Printf("LoadHEAD: %v\n", err)
		os.Exit(1)
	}
	bl := head.Baseline
	if os.Getenv("JPROBE_STREAM") == "metadata" {
		bl = head.MetaBaseline
	}
	fmt.Printf("baseline: txid=%d key=%s sha=%.16s built=%d\n",
		bl.TXID, bl.LTXRef.Key, bl.LTXRef.Sha256, bl.BuiltAtUS)
	rc, _, err := b.Get(ctx, bl.LTXRef.Key)
	if err != nil {
		fmt.Printf("get baseline: %v\n", err)
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
	dst, err := os.OpenFile(os.Args[4], os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
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
	if err := dst.Truncate(int64(hdr.Commit) * ps); err != nil {
		fmt.Printf("truncate: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("decoded pages=%d commit=%d pageSize=%d minTX=%d maxTX=%d\n", n, hdr.Commit, hdr.PageSize, hdr.MinTXID, hdr.MaxTXID)
	os.Exit(0)
}

func init() { rbase() }
