package sqlite

import (
	"context"
	"log/slog"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/notify"
)

// publishRecords converts a freshly committed/applied changeset's
// records into notify.Change slots and appends them to the shared-
// memory feed. Wired as both producer.OnRecords (self + secondaries)
// and broker.OnApplyRecords (remote applies). Runs on the firing
// goroutine; never blocks (notify.Append takes a single mutex around
// memcpy and atomic head store).
//
// Skips records whose Table is dropped or unknown (catalog drift).
func (n *Node) publishRecords(dot crdt.Dot, records []crdt.Record) {
	if n.notifier == nil || len(records) == 0 {
		return
	}
	changes := make([]notify.Change, 0, len(records))
	for _, r := range records {
		h := r.Header()
		tab, ok := n.catalog.TableByID(h.Table)
		if !ok || tab.Dropped() {
			continue
		}
		var op notify.Op
		switch r.(type) {
		case crdt.Insert:
			op = notify.OpInsert
		case crdt.Update:
			op = notify.OpUpdate
		case crdt.Delete:
			op = notify.OpDelete
		case crdt.BlobPatch:
			op = notify.OpBlobPatch
		default:
			continue
		}
		changes = append(changes, notify.Change{
			Origin: uint64(dot.Origin),
			Seq:    uint64(dot.Seq),
			Table:  tab.Name,
			Op:     op,
			PK:     []byte(h.PK),
		})
	}
	if len(changes) == 0 {
		return
	}
	_ = n.notifier.Append(changes)
}

// reassertFn returns the per-sink re-assert hook (primary and
// secondary alike): broker.ReassertLocal with errors logged rather
// than propagated — a failed re-assert leaves this node's app.db
// lagging its own winning write until the row is written again, which
// beats wedging the drain. Returns nil when there is no broker.
func (n *Node) reassertFn(log *slog.Logger) func([]crdt.Record, crdt.Stamp) error {
	if n.broker == nil {
		return nil
	}
	br := n.broker
	return func(records []crdt.Record, stamp crdt.Stamp) error {
		if err := br.ReassertLocal(records, stamp); err != nil {
			log.Warn("syzy: reassert local commit", "err", err)
			return err
		}
		return nil
	}
}

// secondaryPublishFn retains a locally-drained secondary changeset for
// immediate peer catch-up, then broadcasts it. The secondary's raw producer
// journal remains the durable source; this mirror is the encoded serving copy.
func (n *Node) secondaryPublishFn(origin crdt.Origin) func([]byte) {
	if n.transport == nil {
		return nil
	}
	tx := n.transport
	return func(payload []byte) {
		if n.mirror != nil {
			if err := n.mirror.AppendWait(origin, payload); err != nil {
				n.log.Warn("syzy: retain secondary changeset", "origin", origin, "err", err)
			}
		}
		cp := append([]byte(nil), payload...)
		_ = tx.Broadcast(context.Background(), cp)
	}
}
