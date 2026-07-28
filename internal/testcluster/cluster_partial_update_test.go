package testcluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/transport/memtransport"
)

// TestConcurrentUpdateConvergesAcrossDeliveryOrders reproduces a
// production divergence: two writers race on one row, and a third
// replica that saw the writes in a different order must still end
// byte-identical.
//
// The failing shape (before Update records carried the full row
// image): writer H emits u1 (status=deploying) then u2 (status=active,
// svc=new). Writer L applies only u1, then issues a touch-style UPDATE
// that changes just one column — its record diffs down to that single
// column but carries the highest stamp, so it wins whole-row LWW
// everywhere. Replicas that had applied u2 keep u2's values for the
// columns L's record didn't carry; L itself rejects u2 (stamp loses to
// its own write) and keeps u1's. Same changeset set, different
// delivery order, permanently divergent rows.
func TestConcurrentUpdateConvergesAcrossDeliveryOrders(t *testing.T) {
	const schema = `CREATE TABLE app (id BLOB PRIMARY KEY NOT NULL, status TEXT, svc TEXT, upd TEXT)`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Each node gets a private hub: live broadcasts go nowhere, and the
	// test delivers payloads across nodes explicitly via injector peers
	// so the interleaving is deterministic.
	type isoNode struct {
		n   *Node
		hub *memtransport.Hub
	}
	mk := func(origin crdt.Origin) isoNode {
		hub := memtransport.NewHub()
		t.Cleanup(hub.Close)
		n := NewWithCache(t, hub, origin, schema, 0)
		n.Start(t, ctx)
		return isoNode{n: n, hub: hub}
	}
	h, l, f := mk(1), mk(2), mk(3)

	// Capture each writer's encoded changesets.
	capture := func(in isoNode) func(int) []byte {
		var mu sync.Mutex
		var payloads [][]byte
		in.n.Producer.OnEncoded(func(p []byte) {
			cp := append([]byte(nil), p...)
			mu.Lock()
			payloads = append(payloads, cp)
			mu.Unlock()
		})
		return func(i int) []byte {
			drainCtx, dc := context.WithTimeout(context.Background(), 5*time.Second)
			defer dc()
			if err := in.n.Producer.WaitForDrain(drainCtx); err != nil {
				t.Fatalf("WaitForDrain: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if i >= len(payloads) {
				t.Fatalf("payload %d not captured (have %d)", i, len(payloads))
			}
			return payloads[i]
		}
	}
	hPayload := capture(h)
	lPayload := capture(l)

	deliver := func(to isoNode, payload []byte, origin crdt.Origin, seq crdt.Seq) {
		t.Helper()
		inj := to.hub.Peer()
		if err := inj.Broadcast(context.Background(), payload); err != nil {
			t.Fatalf("inject broadcast: %v", err)
		}
		to.n.WaitApplied(t, origin, seq, 5*time.Second)
	}

	exec := func(in isoNode, sql string) {
		t.Helper()
		if err := in.n.AppWrite.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Baseline row from H, delivered everywhere.
	exec(h, `INSERT INTO app (id, status, svc, upd) VALUES (x'01', 'active', 'old', 't0')`)
	deliver(l, hPayload(0), h.n.Origin, 1)
	deliver(f, hPayload(0), h.n.Origin, 1)

	// H's deploy sequence: mid-deploy state u1, then final state u2.
	exec(h, `UPDATE app SET status = 'deploying', upd = 't1' WHERE id = x'01'`)
	exec(h, `UPDATE app SET status = 'active', svc = 'new', upd = 't2' WHERE id = x'01'`)

	// L sees only the mid-deploy write; F sees both.
	deliver(l, hPayload(1), h.n.Origin, 2)
	deliver(f, hPayload(1), h.n.Origin, 2)
	deliver(f, hPayload(2), h.n.Origin, 3)

	// L's concurrent touch write: only `upd` differs from L's local row,
	// and it must carry the highest stamp so it wins whole-row LWW. The
	// HLC wall clock is millisecond-granular; step past u2's stamp so
	// the win doesn't depend on a logical-counter tie-break.
	time.Sleep(3 * time.Millisecond)
	exec(l, `UPDATE app SET upd = 't3' WHERE id = x'01'`)

	// Force L's drain to finish (row clock advanced to the touch's
	// stamp) before H's final write arrives — the production ordering:
	// u2 then loses the LWW gate on L and its content never lands
	// there. Without full-image updates L keeps u1's values for the
	// columns its touch didn't carry while H and F keep u2's.
	_ = lPayload(0)

	// Cross-deliver the stragglers: H's final write reaches L late and
	// loses LWW to L's touch; L's touch reaches H and F.
	deliver(l, hPayload(2), h.n.Origin, 3)
	deliver(h, lPayload(0), l.n.Origin, 1)
	deliver(f, lPayload(0), l.n.Origin, 1)

	// SEC: all three replicas hold the same set of changesets and must
	// be byte-identical, regardless of which content won.
	row := func(in isoNode) string { return readAppRow(t, in.n.Read) }
	hRow, lRow, fRow := row(h), row(l), row(f)
	if hRow != lRow || hRow != fRow {
		t.Fatalf("replicas diverged:\n  H: %s\n  L: %s\n  F: %s", hRow, lRow, fRow)
	}
}

func readAppRow(t *testing.T, conn *sqlitebridge.Conn) string {
	t.Helper()
	stmt, _, err := conn.Prepare(`SELECT status, svc, upd FROM app WHERE id = x'01'`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Finalize()
	ok, err := stmt.Step()
	if err != nil {
		t.Fatalf("step: %v", err)
	}
	if !ok {
		return "<no row>"
	}
	return fmt.Sprintf("status=%s svc=%s upd=%s", stmt.ColumnText(0), stmt.ColumnText(1), stmt.ColumnText(2))
}
