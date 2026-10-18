package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/transport"
)

// A donor journal is not a recovery log for the receiving database: only
// ordinary inbound apply may install donor changes and mark them applied.
func TestEmptyMirrorRepairFetchesMissingRowsThroughApply(t *testing.T) {
	t.Parallel()
	const remote crdt.Origin = 9
	f := newApplier(t, 1, nil)
	payloads := make([][]byte, 3)
	for i := range payloads {
		seq := crdt.Seq(i + 1)
		cs := buildInsert(t, f.tab, crdt.Dot{Origin: remote, Seq: seq},
			crdt.Stamp{Clock: crdt.Clock{WallTime: int64(1000 + seq)}, Origin: remote},
			1, []byte{byte(seq)}, "received")
		payloads[i] = cs.Encoded()
	}
	// Already applied state has a gap; the repair must preserve both the
	// frontier and the out-of-order receipt while leaving seq 2 fetchable.
	for _, i := range []int{0, 2} {
		if err := f.br.applyPayload(context.Background(), payloads[i]); err != nil {
			t.Fatal(err)
		}
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "mirror")
	originDir := filepath.Join(root, "origin_9")
	if err := os.MkdirAll(originDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(originDir, "seg-00000000.bin"), make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	broken, err := mirror.New(mirror.Config{Root: root, Self: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.LoadExisting(); err == nil {
		t.Fatal("uninitialized journal must remain fatal before explicit repair")
	}
	_ = broken.Close()
	plan, err := mirror.InspectEmptyMirror(root, remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.QuarantineEmptyMirror(plan); err != nil {
		t.Fatal(err)
	}
	repaired, err := mirror.New(mirror.Config{Root: root, Self: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Close()
	if err := repaired.LoadExisting(); err != nil {
		t.Fatal(err)
	}
	if _, err := nodestate.RecoverMirror(f.cache, repaired, f.cat); err != nil {
		t.Fatal(err)
	}
	front, ok := f.cache.FrontierFor(remote)
	if !ok || front.LastSeq != 1 || f.cache.IsAppliedRemote(remote, 2) || !f.cache.IsAppliedRemote(remote, 3) {
		t.Fatalf("repair altered applied state: %+v", front)
	}
	f.br.cfg.MirrorJournals = repaired
	donor, err := mirror.New(mirror.Config{Root: t.TempDir(), Self: remote})
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	for i, payload := range payloads {
		if err := donor.AppendSelf(payload, journal.Offset(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	delivered := 0
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := donor.Serve(ctx, transport.CatchupRequest{Ranges: []transport.Range{{Origin: remote, Lo: 2, Hi: 3}}}, func(payload []byte) error {
		delivered++
		return f.br.applyPayload(ctx, payload)
	}); err != nil {
		t.Fatal(err)
	}
	if delivered != 2 {
		t.Fatalf("delivered %d changes", delivered)
	}
	if got := readNCol(t, f.app, []byte{2}); got != "received" {
		t.Fatalf("missing row after fetch: %q", got)
	}
	front, _ = f.cache.FrontierFor(remote)
	if front.LastSeq != 3 {
		t.Fatalf("gap did not close: %+v", front)
	}
	if got := readNCol(t, f.app, []byte{3}); got != "received" {
		t.Fatalf("redelivery damaged existing row: %q", got)
	}
	if err := repaired.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nodestate.NewSnapshotter(f.cache, f.sc, nodestate.SnapshotterConfig{}).SnapshotOnce(); err != nil {
		t.Fatal(err)
	}
	reloaded := nodestate.New(1)
	if err := reloaded.LoadFromMeta(f.sc); err != nil {
		t.Fatal(err)
	}
	reopened, err := mirror.New(mirror.Config{Root: root, Self: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.LoadExisting(); err != nil {
		t.Fatal(err)
	}
	if _, err := nodestate.RecoverMirror(reloaded, reopened, f.cat); err != nil {
		t.Fatal(err)
	}
	front, _ = reloaded.FrontierFor(remote)
	if front.LastSeq != 3 {
		t.Fatalf("recovered frontier did not survive another restart: %+v", front)
	}
}
