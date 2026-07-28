package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/wjordan/objectstore"
)

// casProbe verifies the backing store actually enforces conditional
// writes (If-Match / If-None-Match). A provider that ignores them
// silently turns every CAS in the publisher lease protocol into a
// blind overwrite.
func casProbe(uri string) {
	ctx := context.Background()
	b, err := objectstore.Open(ctx, uri)
	if err != nil {
		fmt.Printf("open: %v\n", err)
		os.Exit(1)
	}
	key := "diag/cas-probe"
	put := func(val string, ifMatch *string) (string, error) {
		return b.Put(ctx, key, bytes.NewReader([]byte(val)), int64(len(val)), ifMatch)
	}
	etag1, err := put("v1", nil)
	report("unconditional put", err == nil, err)
	_, err = put("v2", objectstore.IfAbsent())
	report("if-none-match on existing key REJECTED", errors.Is(err, objectstore.ErrPreconditionFailed), err)
	etag2, err := put("v2", &etag1)
	report("if-match current etag accepted", err == nil, err)
	_, err = put("v3", &etag1)
	report("if-match STALE etag REJECTED", errors.Is(err, objectstore.ErrPreconditionFailed), err)

	// Concurrency: N racers CAS from the same etag; exactly one must win.
	var wg sync.WaitGroup
	wins := 0
	var mu sync.Mutex
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := put(fmt.Sprintf("racer-%d", i), &etag2); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	report(fmt.Sprintf("concurrent CAS: exactly one winner (got %d)", wins), wins == 1, nil)
	_ = b.Delete(ctx, key)
}

func report(name string, ok bool, err error) {
	status := "PASS"
	if !ok {
		status = "FAIL"
	}
	if err != nil && ok {
		fmt.Printf("%s: %s\n", status, name)
		return
	}
	fmt.Printf("%s: %s (err=%v)\n", status, name, err)
}

func init() {
	if len(os.Args) == 3 && os.Args[1] == "casprobe" {
		casProbe(os.Args[2])
		os.Exit(0)
	}
}
