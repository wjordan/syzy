package sqlite

import (
	"context"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/transport"
)

type captureTransport struct {
	sent chan []byte
}

func (t *captureTransport) Broadcast(_ context.Context, payload []byte) error {
	t.sent <- append([]byte(nil), payload...)
	return nil
}

func (*captureTransport) Subscribe(ctx context.Context, _ transport.ApplyFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestSecondaryPublishRetainsPeerCatchupCopy(t *testing.T) {
	t.Parallel()
	const origin crdt.Origin = 7
	mgr, err := mirror.New(mirror.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	tx := &captureTransport{sent: make(chan []byte, 1)}
	n := &Node{
		transport: tx,
		mirror:    mgr,
		log:       slog.New(slog.DiscardHandler),
	}
	payload := make([]byte, 17)
	payload[0] = 1
	binary.BigEndian.PutUint64(payload[1:9], uint64(origin))
	binary.BigEndian.PutUint64(payload[9:17], 1)

	n.secondaryPublishFn(origin)(payload)
	if got := <-tx.sent; string(got) != string(payload) {
		t.Fatalf("broadcast payload = %x, want %x", got, payload)
	}

	var got [][]byte
	err = mgr.Serve(context.Background(), transport.CatchupRequest{
		Ranges: []transport.Range{{Origin: origin, Lo: 1, Hi: 1}},
	}, func(p []byte) error {
		got = append(got, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("catchup returned %d records, want 1", len(got))
	}
	if string(got[0]) != string(payload) {
		t.Fatalf("catchup payload = %x, want %x", got[0], payload)
	}
}
