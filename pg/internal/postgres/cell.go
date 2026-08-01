package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// Cell clock group + counter columns (docs/postgres.md §8).
//
// A table in the cell group arbitrates UPDATEs per column instead of per row,
// so concurrent writes to disjoint columns of one row merge. A counter column
// goes further: its cell is the SUM of every node's contributions, so
// concurrent increments accumulate instead of one overwriting the other.
//
// Both ride Postgres's own schema surface rather than a syzy-specific API:
//
//   - REPLICA IDENTITY FULL is the cell-group opt-in. It is exactly the
//     capability per-column merge needs — capture diffs the WAL's old tuple
//     against the new one to learn which columns a transaction really changed —
//     so the physical setting and the merge rule cannot drift apart.
//   - The syzy_counter domain (a bigint underneath, sql/counter.sql) declares a
//     counter column, the way "INTEGER COUNTER" does in a SQLite schema. It
//     implies the cell group: the DDL admission gate sets REPLICA IDENTITY FULL
//     in the same transaction that declares the first counter column.
//
// Row group stays the default. REPLICA IDENTITY FULL logs the whole old row on
// every UPDATE and DELETE, so it is a WAL-volume tradeoff worth making on
// narrow, contended tables and worth avoiding on wide or TOAST-heavy ones.

// counterTypeName is the canonical name of the counter domain, schema-qualified
// so it resolves identically no matter what search_path a session carries.
const counterTypeName = "public.syzy_counter"

// replIdentByte narrows pg_class.relreplident (read as ::text so the driver
// hands back a plain string) to its single character; an empty read is the
// default identity.
func replIdentByte(s string) byte {
	if s == "" {
		return 'd'
	}
	return s[0]
}

// tableReplIdent reads one relation's current REPLICA IDENTITY setting.
func tableReplIdent(ctx context.Context, conn *pgx.Conn, oid uint32) (byte, error) {
	var s string
	if err := conn.QueryRow(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = $1`, oid).Scan(&s); err != nil {
		return 0, fmt.Errorf("postgres: read replica identity of oid %d: %w", oid, err)
	}
	return replIdentByte(s), nil
}

// clockGroupForReplIdent maps pg_class.relreplident to the table's clock group.
func clockGroupForReplIdent(ri byte) string {
	if ri == 'f' {
		return metadata.ClockGroupCell
	}
	return metadata.ClockGroupRow
}

// replIdentClause renders the REPLICA IDENTITY a clock group runs on.
func replIdentClause(group string) string {
	if group == metadata.ClockGroupCell {
		return "FULL"
	}
	return "DEFAULT"
}

// clockGroupForColumn is the per-column clock group a CatalogColumn carries:
// 'counter' marks the summation cells, and the cross-engine catalog derives the
// table's group from it (internal/metadata catApplyCreateTable).
func clockGroupForColumn(counter bool) string {
	if counter {
		return metadata.ClockGroupCounter
	}
	return metadata.ClockGroupRow
}

func (ti *tableInfo) cellGroup() bool { return ti.clockGroup == metadata.ClockGroupCell }

func (ti *tableInfo) hasCounters() bool {
	for _, c := range ti.cols {
		if c.counter {
			return true
		}
	}
	return false
}

// ColumnRole and NonPKColumns implement crdt.CellTable, the shape the core's
// cell-group normalization consults (crdt.AsCellUpdate / crdt.CoversAllNonPK) —
// the same interface the SQLite catalog implements, so both engines arbitrate
// per column through one implementation.
func (ti *tableInfo) ColumnRole(id crdt.ColumnID) (pk, counter bool) {
	if c := ti.colByID(id); c != nil {
		return c.isPK, c.counter
	}
	return false, false
}

func (ti *tableInfo) NonPKColumns() []crdt.ColumnID {
	out := make([]crdt.ColumnID, 0, len(ti.cols))
	for _, c := range ti.cols {
		if !c.isPK {
			out = append(out, c.cid)
		}
	}
	return out
}

// cellChanged diffs a cell-group row's first-touch OLD image against its
// last-touch NEW image and returns the columns the transaction actually
// changed — the payload unit a cell-group record must carry, since receivers
// arbitrate each carried column independently and an unchanged column would
// stomp a concurrent disjoint write at this record's stamp.
//
// Counter columns ship the signed adjustment NEW − OLD instead of the absolute
// value (CRDT.md F_counter). A column missing from old is treated as changed:
// pgoutput elides an unchanged-TOAST value from the old tuple, so equality
// cannot be proven and the safe answer is to carry it.
func cellChanged(ti *tableInfo, old, new []crdt.ColValue) ([]crdt.ColValue, error) {
	prev := make(map[crdt.ColumnID]crdt.ColValue, len(old))
	for _, v := range old {
		prev[v.Column] = v
	}
	out := make([]crdt.ColValue, 0, len(new))
	for _, v := range new {
		c := ti.colByID(v.Column)
		if c == nil || c.isPK {
			continue
		}
		o, hadOld := prev[v.Column]
		if hadOld && crdt.ColValueEqual(o, v) {
			continue
		}
		if c.counter {
			if !hadOld {
				return nil, fmt.Errorf("postgres: counter column %s.%s has no old value in the WAL tuple; a counter contribution is the difference against it", ti.name, c.name)
			}
			d, err := crdt.CounterDelta(o, v)
			if err != nil {
				return nil, fmt.Errorf("postgres: counter column %s.%s: %w", ti.name, c.name, err)
			}
			v = d
		}
		out = append(out, v)
	}
	return out, nil
}

// counterBearing reports whether records carry a non-idempotent counter
// contribution — a FormatDelta update value, or a counter column in an insert
// image (which sums when it lands on a live row at the same causal length).
// Drives the exactly-once applied marker (apply_cell.go).
func (a *applier) counterBearing(records []crdt.Record) bool {
	for _, rec := range records {
		ti := a.cat.table(rec.Header().Table)
		if ti == nil {
			continue // the apply loop errors on it independently
		}
		switch r := rec.(type) {
		case crdt.Update:
			for _, v := range r.Changed {
				if v.Format == crdt.FormatDelta {
					return true
				}
			}
		case crdt.Insert:
			if !ti.hasCounters() {
				continue
			}
			for _, v := range r.Image {
				if c := ti.colByID(v.Column); c != nil && c.counter {
					return true
				}
			}
		}
	}
	return false
}

// stripCounterContributions returns records with their counter contributions
// removed: FormatDelta update values dropped, counter columns omitted from an
// insert image (the partial-image UPSERT then leaves the committed cell
// untouched). Used when the applied marker certifies the contributions already
// landed, so only the idempotent remainder re-applies.
func (a *applier) stripCounterContributions(records []crdt.Record) []crdt.Record {
	out := make([]crdt.Record, len(records))
	for i, rec := range records {
		ti := a.cat.table(rec.Header().Table)
		if ti == nil {
			out[i] = rec
			continue
		}
		switch r := rec.(type) {
		case crdt.Update:
			kept := make([]crdt.ColValue, 0, len(r.Changed))
			for _, v := range r.Changed {
				if v.Format != crdt.FormatDelta {
					kept = append(kept, v)
				}
			}
			r.Changed = kept
			out[i] = r
		case crdt.Insert:
			if !ti.hasCounters() {
				out[i] = rec
				continue
			}
			kept := make([]crdt.ColValue, 0, len(r.Image))
			for _, v := range r.Image {
				if c := ti.colByID(v.Column); c != nil && c.counter {
					continue
				}
				kept = append(kept, v)
			}
			r.Image = kept
			out[i] = r
		default:
			out[i] = rec
		}
	}
	return out
}

// validateCounterValues rejects wire values that violate the counter contract
// before they can bypass stamp arbitration or run SQL arithmetic: FormatDelta
// is valid only on a declared counter column as an 8-byte integer, and a
// counter column in an update only ever carries FormatDelta. A violation means
// a buggy or hostile peer — fail deterministically (into quarantine) rather
// than diverge silently.
func validateCounterValues(ti *tableInfo, values []crdt.ColValue) error {
	for _, v := range values {
		c := ti.colByID(v.Column)
		if c == nil {
			continue // dropped column; skipped downstream
		}
		if v.Format == crdt.FormatDelta {
			if !c.counter || v.TypeTag != crdt.ColInt || len(v.Bytes) != 8 {
				return fmt.Errorf("%w: %s.%s carries a counter contribution but is not an integer counter cell (counter=%v, tag=%d, %d bytes)",
					errCounterApply, ti.name, c.name, c.counter, v.TypeTag, len(v.Bytes))
			}
		} else if c.counter {
			return fmt.Errorf("%w: %s.%s is a counter column but carries an absolute value (format=%d, tag=%d); counter updates ship contributions",
				errCounterApply, ti.name, c.name, v.Format, v.TypeTag)
		}
	}
	return nil
}

// validateInsertCounterImage rejects insert images whose counter columns are
// not plain 8-byte integers (or that carry a wire-level contribution, which no
// producer emits on an image — images are absolute).
func validateInsertCounterImage(ti *tableInfo, image []crdt.ColValue) error {
	if !ti.hasCounters() {
		return nil
	}
	for _, v := range image {
		c := ti.colByID(v.Column)
		if v.Format == crdt.FormatDelta {
			return fmt.Errorf("%w: %s insert image carries a counter contribution; images ship absolute values", errCounterApply, ti.name)
		}
		if c != nil && c.counter && (v.TypeTag != crdt.ColInt || len(v.Bytes) != 8) {
			return fmt.Errorf("%w: %s.%s counter image is not an 8-byte integer (tag=%d, %d bytes)",
				errCounterApply, ti.name, c.name, v.TypeTag, len(v.Bytes))
		}
	}
	return nil
}
