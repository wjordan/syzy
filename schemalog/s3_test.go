package schemalog

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wjordan/objectstore"
)

// newS3OnFile returns an S3 schema log backed by a FileBackend rooted
// in t.TempDir(). This exercises the same Append/Read/Head/encode
// paths the production AWSBackend uses without a real bucket.
func newS3OnFile(t *testing.T) *S3 {
	t.Helper()
	root := t.TempDir()
	be, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	return NewS3WithBackend(be)
}

func TestS3_OpenValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  S3Config
		want string
	}{
		{"endpoint", S3Config{}, "Endpoint required"},
		{"region", S3Config{Endpoint: "http://x/b"}, "Region required"},
		{"prefix", S3Config{Endpoint: "http://x/b", Region: "r"}, "Prefix required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenS3(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v; want %q substring", err, tc.want)
			}
		})
	}
}

func TestS3_AppendIdempotenceOnExistingKey(t *testing.T) {
	s := newS3OnFile(t)
	ctx := context.Background()

	if _, err := s.Append(ctx, 0, []byte("op1"), "raw1"); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	// Re-issuing with the same parentSeq attempts to PUT the same key
	// with If-None-Match:* — backend returns ErrPreconditionFailed →
	// ErrHeadMoved.
	if _, err := s.Append(ctx, 0, []byte("op1-dup"), "raw1-dup"); !errors.Is(err, ErrHeadMoved) {
		t.Fatalf("dup Append err = %v; want ErrHeadMoved", err)
	}
}

func TestS3_HeadGrowsWithAppends(t *testing.T) {
	s := newS3OnFile(t)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		seq, err := s.Append(ctx, uint64(i), []byte("op"), "")
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq = %d; want %d", seq, i+1)
		}
	}
	head, err := s.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head != 50 {
		t.Fatalf("head = %d; want 50", head)
	}
}

func TestS3_ReadCatchUp(t *testing.T) {
	s := newS3OnFile(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := s.Append(ctx, uint64(i), []byte("op"), ""); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	evs, err := s.Read(ctx, 2, 100)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(evs) != 3 {
		t.Fatalf("Read returned %d events; want 3", len(evs))
	}
	for i, ev := range evs {
		if ev.SchemaSeq != uint64(i+3) {
			t.Fatalf("evs[%d].SchemaSeq = %d; want %d", i, ev.SchemaSeq, i+3)
		}
	}
}

func TestS3_ReadUsesDirectEventKeys(t *testing.T) {
	root := t.TempDir()
	be, err := objectstore.OpenFS(root)
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}
	noList := &noListBucket{Bucket: be}
	s := NewS3WithBackend(noList)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := s.Append(ctx, uint64(i), []byte("op"), ""); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	evs, err := s.Read(ctx, 2, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(evs) != 2 || evs[0].SchemaSeq != 3 || evs[1].SchemaSeq != 4 {
		t.Fatalf("Read returned %#v; want seqs 3,4", evs)
	}
	evs, err = s.Read(ctx, 5, 64)
	if err != nil {
		t.Fatalf("Read at head: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("Read at head returned %d events; want 0", len(evs))
	}
	if noList.calls != 0 {
		t.Fatalf("Read called List %d times; want 0", noList.calls)
	}
}

type noListBucket struct {
	objectstore.Bucket
	calls int
}

func (b *noListBucket) List(ctx context.Context, prefix, startAfter string) ([]objectstore.ObjectInfo, error) {
	b.calls++
	return nil, errors.New("unexpected List")
}

func TestS3_SeqFromKey(t *testing.T) {
	for _, tc := range []struct {
		key string
		ok  bool
	}{
		{"events/0000000000000001.bin", true},
		{"events/00000000000000a1.bin", true},
		{"events/short.bin", false},
		{"events/0000000000000001.txt", false},
		{"other/0000000000000001.bin", false},
	} {
		got, ok := seqFromKey(tc.key)
		if ok != tc.ok {
			t.Errorf("seqFromKey(%q) ok=%v; want %v", tc.key, ok, tc.ok)
		}
		if ok && got == 0 {
			t.Errorf("seqFromKey(%q) → 0", tc.key)
		}
	}
}

func TestS3_ParseBucketEndpoint(t *testing.T) {
	for _, tc := range []struct {
		raw       string
		bucket    string
		base      string
		pathStyle bool
		wantErr   bool
	}{
		{"https://my-bucket.s3.us-east-1.amazonaws.com", "my-bucket", "", false, false},
		{"https://my-bucket.s3.amazonaws.com", "my-bucket", "", false, false},
		{"https://s3.us-east-1.amazonaws.com/my-bucket", "my-bucket", "https://s3.us-east-1.amazonaws.com", true, false},
		{"http://localhost:9000/my-bucket", "my-bucket", "http://localhost:9000", true, false},
		{"ftp://x", "", "", false, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			b, base, ps, err := parseBucketEndpoint(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (b=%q base=%q ps=%v)", b, base, ps)
				}
				return
			}
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if b != tc.bucket || base != tc.base || ps != tc.pathStyle {
				t.Fatalf("got (%q, %q, %v); want (%q, %q, %v)", b, base, ps, tc.bucket, tc.base, tc.pathStyle)
			}
		})
	}
}

// TestS3_RoundTrip exercises the same shape the daemon uses: append a
// few events, then read them back via the standard Log surface.
func TestS3_RoundTrip(t *testing.T) {
	s := newS3OnFile(t)
	ctx := context.Background()
	seq, err := s.Append(ctx, 0, []byte("hello"), "raw")
	if err != nil || seq != 1 {
		t.Fatalf("Append: (%d, %v)", seq, err)
	}
	head, err := s.Head(ctx)
	if err != nil || head != 1 {
		t.Fatalf("Head: (%d, %v)", head, err)
	}
	evs, err := s.Read(ctx, 0, 10)
	if err != nil || len(evs) != 1 || evs[0].SchemaSeq != 1 || evs[0].RawSQL != "raw" {
		t.Fatalf("Read: (%d evs, %v) %+v", len(evs), err, evs)
	}
}

// Compile-time check that S3 satisfies the interface.
var _ Log = (*S3)(nil)
