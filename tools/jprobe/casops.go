package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wjordan/objectstore"
)

// Individual objectstore ops for cross-node CAS linearizability probing:
//
//	jprobe bput <uri> <key> <val>          → etag
//	jprobe bcas <uri> <key> <val> <etag>   → etag | error
//	jprobe bget <uri> <key>                → etag + body
func casOps() {
	if len(os.Args) < 4 {
		return
	}
	op := os.Args[1]
	if op != "bput" && op != "bcas" && op != "bget" {
		return
	}
	ctx := context.Background()
	b, err := objectstore.Open(ctx, os.Args[2])
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	key := os.Args[3]
	switch op {
	case "bput":
		etag, err := b.Put(ctx, key, bytes.NewReader([]byte(os.Args[4])), int64(len(os.Args[4])), nil)
		fmt.Printf("etag=%s err=%v\n", etag, err)
	case "bcas":
		etag, err := b.Put(ctx, key, bytes.NewReader([]byte(os.Args[4])), int64(len(os.Args[4])), &os.Args[5])
		fmt.Printf("etag=%s err=%v\n", etag, err)
	case "bget":
		rc, etag, err := getForCAS(ctx, b, key)
		if err != nil {
			fmt.Printf("err=%v\n", err)
			os.Exit(1)
		}
		body, _ := io.ReadAll(rc)
		rc.Close()
		fmt.Printf("etag=%s body=%s\n", etag, body)
	}
	os.Exit(0)
}

func getForCAS(ctx context.Context, b objectstore.Bucket, key string) (io.ReadCloser, string, error) {
	return b.Get(objectstore.WithConsistentRead(ctx), key)
}

func init() { casOps() }
