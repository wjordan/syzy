package objstore

import (
	"context"
	"io"

	"github.com/wjordan/objectstore"
)

// GuardedBucket wraps a Bucket so every mutation re-checks a local fence
// immediately before it is handed to the backend. This is the last locally
// controlled point: a request already accepted by the backend is irrevocable.
// Reads pass through unguarded.
type GuardedBucket struct {
	objectstore.Bucket
	Check func() error
}

func (b *GuardedBucket) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	if err := b.Check(); err != nil {
		return "", err
	}
	return b.Bucket.Put(ctx, key, body, length, ifMatch)
}

func (b *GuardedBucket) PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (string, error) {
	if err := b.Check(); err != nil {
		// Bucket.PutStream promises to consume the body even when it rejects a
		// conditional write. Preserve that contract so an io.Pipe producer can
		// always finish instead of deadlocking behind the fence.
		_, _ = io.Copy(io.Discard, body)
		return "", err
	}
	return b.Bucket.PutStream(ctx, key, body, ifMatch)
}

func (b *GuardedBucket) Delete(ctx context.Context, key string) error {
	if err := b.Check(); err != nil {
		return err
	}
	return b.Bucket.Delete(ctx, key)
}
