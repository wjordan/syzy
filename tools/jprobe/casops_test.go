package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
)

type consistentGetBucket struct {
	objectstore.Bucket
	consistent bool
}

func (b *consistentGetBucket) Get(ctx context.Context, _ string) (io.ReadCloser, string, error) {
	b.consistent = objectstore.IsConsistentRead(ctx)
	return io.NopCloser(strings.NewReader("head")), "etag", nil
}

func TestGetForCASUsesConsistentRead(t *testing.T) {
	b := &consistentGetBucket{}
	rc, _, err := getForCAS(context.Background(), b, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if !b.consistent {
		t.Fatal("CAS probe read did not request global consistency")
	}
}
