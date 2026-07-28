package objstore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/wjordan/objectstore"
)

// A rejected PutStream must still consume the body: Bucket.PutStream promises
// that, and an io.Pipe producer would otherwise deadlock behind the fence.
func TestGuardedBucketConsumesRejectedStream(t *testing.T) {
	t.Parallel()
	want := errors.New("fence closed")
	body := bytes.NewReader([]byte("compacted LTX"))
	b := &GuardedBucket{Check: func() error { return want }}
	if _, err := b.PutStream(context.Background(), "db/0001/test.ltx", body, objectstore.IfAbsent()); !errors.Is(err, want) {
		t.Fatalf("PutStream error = %v, want fence error", err)
	}
	if body.Len() != 0 {
		t.Fatalf("rejected PutStream left %d body bytes unread", body.Len())
	}
}

func TestGuardedBucketRejectsPutAndDelete(t *testing.T) {
	t.Parallel()
	want := errors.New("fence closed")
	b := &GuardedBucket{Check: func() error { return want }}
	if _, err := b.Put(context.Background(), "k", bytes.NewReader(nil), 0, nil); !errors.Is(err, want) {
		t.Fatalf("Put error = %v, want fence error", err)
	}
	if err := b.Delete(context.Background(), "k"); !errors.Is(err, want) {
		t.Fatalf("Delete error = %v, want fence error", err)
	}
}
