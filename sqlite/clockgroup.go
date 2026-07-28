package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
)

// Clock groups selectable via SetClockGroup. See CRDT.md#layers:
// 'row' treats each row as one LWW register (concurrent updates to
// the same row resolve whole-row, last writer wins); 'cell'
// arbitrates updates per column, so concurrent updates to disjoint
// columns of one row merge. A third, column-only group — 'counter',
// declared via the INTEGER COUNTER type at CREATE TABLE / ADD COLUMN
// (sqlite/docs/DDL.md#counter-columns) — is not selectable here.
const (
	ClockGroupRow  = metadata.ClockGroupRow
	ClockGroupCell = metadata.ClockGroupCell
)

// SetClockGroup sets a replicated table's clock group cluster-wide.
// The change rides the schema log as a replicated catalog event, so
// every node switches arbitration rules at the same point in the
// schema chain — changesets produced after the flip carry a schema
// dep at or past it. Idempotent: a no-op when the table already uses
// the requested group.
//
// Safe on a live cluster: records produced before the flip carry the
// full row image (the row-group payload rule), and applying a
// full-image update under either rule yields identical per-column
// effective stamps, so records straddling the flip converge on every
// node regardless of which side applied them.
func (n *Node) SetClockGroup(ctx context.Context, table, group string) error {
	if group != ClockGroupRow && group != ClockGroupCell {
		return fmt.Errorf("syzy: invalid clock group %q (want %q or %q)",
			group, ClockGroupRow, ClockGroupCell)
	}
	resolve := func() (*catalog.Table, error) {
		tab, ok := n.catalog.Table(table)
		if !ok {
			return nil, fmt.Errorf("syzy: table %q not in replicated catalog", table)
		}
		if group == ClockGroupRow && tab.HasCounters() {
			return nil, fmt.Errorf("syzy: table %q has COUNTER columns; counter contributions are per-column payloads and require the cell clock group", table)
		}
		if group == ClockGroupCell {
			for _, uk := range tab.UniqueKeys {
				if uk.Coordinated && (len(uk.Columns) > 1 || uk.Predicate.Root != nil) {
					return nil, fmt.Errorf("syzy: table %q has a composite or partial coordinated (NOT NULL UNIQUE) key; per-cell merge could assemble a row from writes that were never reserved together — drop the key before switching to 'cell'", table)
				}
			}
		}
		return tab, nil
	}

	// Single-node mode (no schema log): a direct metadata update is
	// the whole story.
	if n.schemaLog == nil {
		tab, err := resolve()
		if err != nil {
			return err
		}
		if tab.ClockGroup() == group {
			return nil
		}
		if err := n.meta.WithTx(func(tx *metadata.Tx) error {
			return tx.SetDefaultClockGroup(tab.ID, group)
		}); err != nil {
			return fmt.Errorf("syzy: set clock group: %w", err)
		}
		return n.catalog.Reload()
	}

	// Fast path: if the local catalog already shows the target group, we're
	// done — skip the schema-log round-trip. A boot-time "flip every replicated
	// table" loop hits this for every already-flipped table; on a node far from
	// the bucket that otherwise costs one schema-log Read (an S3 GET) per table,
	// serially (~60s observed from a distant region on a hot-restart where every
	// table was already flipped). A stale-catalog false-negative (a peer flipped
	// it but we have not caught up) just falls through to the catch-up + CAS loop
	// below and converges via ErrHeadMoved — local-says-set is never a false
	// positive, since we only show a group we actually applied.
	if tab, err := resolve(); err == nil && tab.ClockGroup() == group {
		return nil
	}

	// Replicated path: catch up, append at head (CAS), apply locally.
	for attempt := 0; attempt < 8; attempt++ {
		if n.broker != nil {
			if err := n.broker.RunSchemaCatchupOnce(ctx); err != nil {
				return fmt.Errorf("syzy: set clock group: catch up: %w", err)
			}
		}
		tab, err := resolve()
		if err != nil {
			return err
		}
		if tab.ClockGroup() == group {
			return nil
		}
		buf, err := crdt.EncodeCatalogOp(crdt.CatalogOp{
			Kind:       crdt.OpSetClockGroup,
			TableID:    tab.ID,
			ClockGroup: group,
		})
		if err != nil {
			return fmt.Errorf("syzy: set clock group: encode: %w", err)
		}
		head, _, err := n.meta.GetSchemaSeq()
		if err != nil {
			return fmt.Errorf("syzy: set clock group: read schema_seq: %w", err)
		}
		raw := fmt.Sprintf("/* syzy: set clock group %s = %s */", table, group)
		if _, err := n.schemaLog.Append(ctx, head, buf, raw); err != nil {
			if errors.Is(err, schemalog.ErrHeadMoved) {
				continue // someone else appended; catch up and retry
			}
			return fmt.Errorf("syzy: set clock group: append: %w", err)
		}
		if n.broker != nil {
			if err := n.broker.RunSchemaCatchupOnce(ctx); err != nil {
				return fmt.Errorf("syzy: set clock group: apply: %w", err)
			}
		}
		return nil
	}
	return fmt.Errorf("syzy: set clock group %s = %s: schema log contention", table, group)
}

// ClockGroup returns table's clock group per this node's current
// catalog view.
func (n *Node) ClockGroup(table string) (string, error) {
	tab, ok := n.catalog.Table(table)
	if !ok {
		return "", fmt.Errorf("syzy: table %q not in replicated catalog", table)
	}
	return tab.ClockGroup(), nil
}

// ReplicatedTables returns the names of every active replicated table
// in this node's catalog, sorted. Consumers use it to apply
// cluster-wide policy (e.g. flipping every table's clock group at
// boot) without hardcoding a schema list.
func (n *Node) ReplicatedTables() []string {
	tabs := n.catalog.Tables()
	names := make([]string, 0, len(tabs))
	for _, t := range tabs {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return names
}
