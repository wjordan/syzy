package postgres

import (
	"context"
	"fmt"
	"strings"

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

// hasCounters reports whether any of the table's columns merges by summation.
// Nil-receiver-safe: the fold consults it on rows whose table it may not have.
func (ti *tableInfo) hasCounters() bool {
	if ti == nil {
		return false
	}
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

// stripCounterContributions drops the FormatDelta values from Updates, leaving
// only the idempotent remainder to re-apply. Used when the applied marker
// certifies the contributions already landed.
//
// Insert images are left intact: their counter columns are the generation's
// opening value and are NOT NULL, so removing them would leave an image that
// cannot recreate a row that is no longer physically present (a local delete
// that has not folded yet is exactly that, and the marker only outlives the
// sidecar frontier in the crash window where the row clock still says live).
// Not re-counting them is the renderer's job instead — upsertSQLKeepingCounters
// inserts them if the row is gone and leaves them alone if it is not.
func (a *applier) stripCounterContributions(records []crdt.Record) []crdt.Record {
	out := make([]crdt.Record, len(records))
	for i, rec := range records {
		r, ok := rec.(crdt.Update)
		if !ok {
			out[i] = rec
			continue
		}
		kept := make([]crdt.ColValue, 0, len(r.Changed))
		for _, v := range r.Changed {
			if v.Format != crdt.FormatDelta {
				kept = append(kept, v)
			}
		}
		r.Changed = kept
		out[i] = r
	}
	return out
}

// rejectCounterShape is the bootstrap-table floor under sql/ddl.sql's
// syzy_ddl_admit_cells: a table listed in Config.Tables was created before this
// node had event triggers, so nothing has ever checked its counter columns.
// The rules are the gate's — a counter cell has to be a real number every peer
// can add into, on a table whose merge unit is per column — and violating them
// silently produces counters that overwrite instead of accumulating.
func rejectCounterShape(ctx context.Context, conn *pgx.Conn, schema, name string, oid uint32, ri byte, pgcols []pgColumn) error {
	qname := schema + "." + name
	var counters []string
	for _, pc := range pgcols {
		if !pc.counter {
			continue
		}
		counters = append(counters, pc.name)
		switch {
		case !pc.notNull:
			return unsupportedDDLf("postgres: %s: counter column %q must be NOT NULL — a NULL cell has no value to sum contributions into", qname, pc.name)
		case pc.pkpos > 0:
			return unsupportedDDLf("postgres: %s: counter column %q cannot be part of the PRIMARY KEY — row identity must not change when a contribution is summed in", qname, pc.name)
		case pc.generated || pc.identity != 0:
			return unsupportedDDLf("postgres: %s: counter column %q cannot be GENERATED or an IDENTITY column — its value is the sum of replicated contributions, not a node-local expression", qname, pc.name)
		}
	}
	if len(counters) == 0 {
		return nil
	}
	if clockGroupForReplIdent(ri) != metadata.ClockGroupCell {
		return unsupportedDDLf("postgres: %s: counter column(s) %s require the cell clock group; run ALTER TABLE %s REPLICA IDENTITY FULL",
			qname, strings.Join(counters, ", "), qname)
	}
	var inKey string
	if err := conn.QueryRow(ctx, `
		SELECT COALESCE(min(a.attname), '')
		FROM pg_index i, unnest(i.indkey) AS x(attnum)
		JOIN pg_attribute a ON a.attrelid = $1 AND a.attnum = x.attnum
		WHERE i.indrelid = $1 AND i.indisunique AND a.attname = ANY($2)`, oid, counters).Scan(&inKey); err != nil {
		return fmt.Errorf("postgres: check counter unique keys on %s: %w", qname, err)
	}
	if inKey != "" {
		return unsupportedDDLf("postgres: %s: counter column %q cannot be part of a UNIQUE key — concurrent contributions sum to a value no writer reserved", qname, inKey)
	}
	return nil
}

// counterMergeImage marks a generation-establishing Insert's counter columns as
// contributions so the UPSERT sums them onto whatever the row already holds
// instead of overwriting it. It applies when this node's row clock does not yet
// cover the row (an undrained local commit — capture folds a transaction well
// after Postgres committed it — or a row adopted from before replication
// started): the physical content is a same-generation contribution every peer
// sums, so an absolute image would erase it here while the cluster adds both.
//
// No probe for the row's existence is needed: on a row that is genuinely absent
// the INSERT arm lands the contribution verbatim as the generation's opening
// value, which is the same result.
func counterMergeImage(ti *tableInfo, image []crdt.ColValue, rs crdt.RowState, cl uint64) ([]crdt.ColValue, bool) {
	if !ti.hasCounters() || rs.IsLive() || cl != rs.NextLiveCL() {
		return nil, false
	}
	out := make([]crdt.ColValue, len(image))
	for i, v := range image {
		if c := ti.colByID(v.Column); c != nil && c.counter {
			v.Format = crdt.FormatDelta
		}
		out[i] = v
	}
	return out, true
}

// counterContributions extracts a row image's counter cells as the summable
// contributions they are on the wire — the shape crdt.AsCellUpdate gives a
// same-generation Insert, reused when a losing local fold has to keep its
// contributions alive after the rest of the record is dropped (capture.go).
func counterContributions(ti *tableInfo, image []crdt.ColValue) []crdt.ColValue {
	if !ti.hasCounters() {
		return nil
	}
	var out []crdt.ColValue
	for _, v := range image {
		c := ti.colByID(v.Column)
		if c == nil || !c.counter || v.TypeTag != crdt.ColInt || len(v.Bytes) != 8 {
			continue
		}
		v.Format = crdt.FormatDelta
		out = append(out, v)
	}
	return out
}

// recordImage returns the column values a record carries, or nil for a record
// shape that carries none (a Delete).
func recordImage(rec crdt.Record) []crdt.ColValue {
	switch r := rec.(type) {
	case crdt.Insert:
		return r.Image
	case crdt.Update:
		return r.Changed
	}
	return nil
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
