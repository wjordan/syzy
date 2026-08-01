package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// Conflict observability (§9). Every arbitration this engine runs is
// deterministic and convergent, but one side's committed values are still
// discarded — and an operator who cannot see WHICH values, and whether the two
// writes were genuinely concurrent, has to take "eventually consistent" on
// faith. Each loss is recorded to public.syzy_conflicts inside the transaction
// that performs it, so the audit row is exactly as durable as the overwrite.
//
// The causal length is what makes the classification meaningful: a loser from an
// older generation was superseded by a delete-and-recreate and could not have
// survived under any policy, while a loser at the SAME generation from a
// different origin is a real clobber of a write no ordering rules out. A
// timestamp-only conflict log cannot separate those two.

// conflictRetention bounds the table: the writer prunes everything older than
// the newest N rows, so the log never needs an operator's attention.
const conflictRetention = 10000

const conflictsTable = "public.syzy_conflicts"

// conflict is one recorded loss: the values discarded, whose write they came
// from, and the write that displaced them.
type conflict struct {
	ti         *tableInfo
	pk         crdt.PKBlob
	loserLocal bool   // the discarded values were committed on this node
	op         string // the losing write's shape: insert, update or delete
	lost       []crdt.ColValue
	winner     crdt.Stamp
	winnerCL   uint64
	loser      crdt.Stamp
	loserCL    uint64
}

// kind classifies the loss. Same-origin losses are never recorded (an origin
// overwriting its own earlier value is ordinary sequential history), so the
// only two cases left are a superseded generation and a same-generation clobber.
func (c conflict) kind() string {
	if c.loserCL != c.winnerCL {
		return "superseded"
	}
	return "concurrent"
}

// newConflict assembles a loss, or reports false when there is nothing worth
// recording: no discarded values, or both writes from the same origin.
func newConflict(ti *tableInfo, pk crdt.PKBlob, loserLocal bool, lost []crdt.ColValue,
	winner crdt.Stamp, winnerCL uint64, loser crdt.Stamp, loserCL uint64, op string) (conflict, bool) {
	// A delete carries no values, so it is the one loss recorded without them.
	if (len(lost) == 0 && op != "delete") || winner.Origin == loser.Origin ||
		loser.Origin == 0 || winner.Origin == 0 {
		return conflict{}, false
	}
	return conflict{ti: ti, pk: pk, loserLocal: loserLocal, op: op, lost: lost,
		winner: winner, winnerCL: winnerCL, loser: loser, loserCL: loserCL}, true
}

// lostColumns returns the pre-image columns a write actually changed: the
// values that were discarded, PK columns excluded (they identify the row rather
// than being lost with it). A nil post image means the row is gone (a Delete),
// so every non-PK value is lost.
func lostColumns(ti *tableInfo, pre, post []crdt.ColValue) []crdt.ColValue {
	byID := make(map[crdt.ColumnID]crdt.ColValue, len(post))
	for _, v := range post {
		byID[v.Column] = v
	}
	var out []crdt.ColValue
	for _, v := range pre {
		c := ti.colByID(v.Column)
		if c == nil || c.isPK {
			continue
		}
		if post != nil {
			after, ok := byID[v.Column]
			if !ok || crdt.ColValueEqual(v, after) {
				continue // untouched by the winning write
			}
		}
		out = append(out, v)
	}
	return out
}

// groupByCellStamp splits columns by the row stamp governing each one, so a
// cell-group row whose columns were last written by different peers yields one
// audit row per writer instead of attributing them all to the baseline.
func groupByCellStamp(rs crdt.RowState, lost []crdt.ColValue) map[crdt.Stamp][]crdt.ColValue {
	out := map[crdt.Stamp][]crdt.ColValue{}
	for _, v := range lost {
		s := rs.EffectiveStamp(v.Column, crdt.ByteRange{})
		out[s] = append(out[s], v)
	}
	return out
}

// localLosses records what an inbound write discarded: the pre-image columns it
// changed, split by the origin that last wrote each one. Nothing is recorded
// when the row was untouched by any other origin (pre is nil then, since the
// caller only reads a pre-image for contended rows).
func localLosses(ti *tableInfo, pk crdt.PKBlob, pre, post []crdt.ColValue,
	rs crdt.RowState, winner crdt.Stamp, winnerCL uint64, op string) []conflict {
	var out []conflict
	for stamp, lost := range groupByCellStamp(rs, lostColumns(ti, pre, post)) {
		if c, ok := newConflict(ti, pk, true, lost, winner, winnerCL, stamp, rs.CL, op); ok {
			out = append(out, c)
		}
	}
	return out
}

// inboundLosses records what an inbound write lost: the values it carried, split
// by the origin whose committed state outranked each column.
func inboundLosses(ti *tableInfo, r crdt.Record, rs crdt.RowState, loser crdt.Stamp, loserCL uint64) []conflict {
	var carried []crdt.ColValue
	op := "update"
	switch rec := r.(type) {
	case crdt.Insert:
		carried, op = rec.Image, "insert"
	case crdt.Update:
		carried = rec.Changed
	case crdt.Delete:
		// A losing delete carries no values; the discarded write is the removal
		// itself, which the op column states.
		if c, ok := newConflict(ti, rs2pk(r), false, nil, rs.Base, rs.CL, loser, loserCL, "delete"); ok {
			return []conflict{c}
		}
		return nil
	default:
		return nil
	}
	var out []conflict
	// Only columns the record would have changed are lost; PK columns identify
	// the row rather than being lost with it (lostColumns drops them).
	for stamp, lost := range groupByCellStamp(rs, lostColumns(ti, carried, nil)) {
		if c, ok := newConflict(ti, rs2pk(r), false, lost, stamp, rs.CL, loser, loserCL, op); ok {
			out = append(out, c)
		}
	}
	return out
}

// rs2pk is the record's primary key, which every Record header carries.
func rs2pk(r crdt.Record) crdt.PKBlob { return r.Header().PK }

// localValues is the values a local record carries — what a fold discards when
// winner-repair drops it whole.
func localValues(r crdt.Record) []crdt.ColValue {
	switch rec := r.(type) {
	case crdt.Insert:
		return rec.Image
	case crdt.Update:
		return rec.Changed
	}
	return nil
}

// writtenByOtherOrigin reports whether any part of the row's current state came
// from an origin other than s — the necessary condition for this write to
// discard someone else's values, and the gate that keeps the conflict log's
// pre-image read off uncontended rows.
func writtenByOtherOrigin(rs crdt.RowState, s crdt.Stamp) bool {
	if rs.CL == 0 {
		return false // row never existed here; nothing to lose
	}
	if rs.Base.Origin != 0 && rs.Base.Origin != s.Origin {
		return true
	}
	for _, cell := range rs.Cells {
		if cell.Origin != 0 && cell.Origin != s.Origin {
			return true
		}
	}
	return false
}

// writeConflicts appends the batch to syzy_conflicts and prunes to the
// retention bound. Called inside the transaction that discarded the values.
func writeConflicts(ctx context.Context, tx pgx.Tx, rows []conflict) error {
	if len(rows) == 0 {
		return nil
	}
	for _, c := range rows {
		cols := make([]string, 0, len(c.lost))
		values := make(map[string]any, len(c.lost))
		for _, v := range c.lost {
			ci := c.ti.colByID(v.Column)
			if ci == nil {
				continue
			}
			cols = append(cols, ci.name)
			if v.TypeTag == crdt.ColNull {
				values[ci.name] = nil
				continue
			}
			text, err := colValueText(v)
			if err != nil {
				return fmt.Errorf("postgres: conflict log: render %s.%s: %w", c.ti.name, ci.name, err)
			}
			values[ci.name] = text
		}
		if len(cols) == 0 && c.op != "delete" {
			continue
		}
		sort.Strings(cols)
		lostJSON, err := json.Marshal(values)
		if err != nil {
			return fmt.Errorf("postgres: conflict log: encode values: %w", err)
		}
		side := "inbound"
		if c.loserLocal {
			side = "local"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+conflictsTable+` (tbl, pk, kind, loser_side, op, cols, lost_values,
			    winner_origin, winner_wall, winner_logical, winner_cl,
			    loser_origin, loser_wall, loser_logical, loser_cl)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			c.ti.schema+"."+c.ti.name, pkText(c.ti, c.pk), c.kind(), side, c.op, cols, lostJSON,
			int64(c.winner.Origin), c.winner.WallTime, int32(c.winner.Logical), int64(c.winnerCL),
			int64(c.loser.Origin), c.loser.WallTime, int32(c.loser.Logical), int64(c.loserCL),
		); err != nil {
			return fmt.Errorf("postgres: conflict log: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+conflictsTable+
		` WHERE seq <= (SELECT max(seq) FROM `+conflictsTable+`) - $1`, conflictRetention); err != nil {
		return fmt.Errorf("postgres: conflict log prune: %w", err)
	}
	return nil
}

// pkText renders a row's primary key as "col=value" pairs — the log is read by
// humans, and a PKBlob's canonical bytes are not.
func pkText(ti *tableInfo, pk crdt.PKBlob) string {
	vals := decodePKBlobTyped(pk)
	out := ""
	for i, c := range ti.pk {
		if i > 0 {
			out += ", "
		}
		out += c.name + "="
		if i < len(vals) {
			t, err := colValueText(vals[i])
			if err != nil {
				t = "?"
			}
			out += t
		}
	}
	return out
}
