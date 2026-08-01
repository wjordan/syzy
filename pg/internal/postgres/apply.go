package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
)

// applier writes decoded peer Changesets. It mirrors internal/broker's apply
// path: idempotency + (CL, Stamp) LWW + frontier against the shared Cache, the
// DML in one transaction under session_replication_role=replica with the syzy
// replication origin (so applied writes are not re-captured).
//
// Apply is NOT internally synchronized: the orchestrator is its sole caller and
// runs it on one goroutine (interleaved with local folds), so it is the single
// Cache writer by construction. The capture-catch-up gate that used to live
// here is now the orchestrator's drainToWALTarget — it folds every pending
// local draft before this arbitrates.
type applier struct {
	cfg  Config
	cat  *catalog
	conn *pgx.Conn

	// winners is the engine's shared winner-repair stash (§9,
	// winners.go), populated here at apply and consumed by the fold path.
	winners *winnerStash

	// schemaSeq is the node's schema head (§6), shared with the engine. The gate
	// refuses a changeset whose Deps[SchemaChain] exceeds it; the orchestrator
	// catches the schema log up before calling Apply, so a met dep passes. nil
	// without a schema log — then any Deps>0 is unsatisfiable.
	schemaSeq *atomic.Uint64

	// skew caps how far a peer's clock can drag this node's HLC (skew.go).
	skew *skewGuard
}

func (a *applier) Apply(ctx context.Context, cs *crdt.Changeset) error {
	return a.apply(ctx, cs, false)
}

// apply is Apply with an idempotency override: force skips the applied-
// frontier short-circuit so a quarantined changeset (whose frontier was
// advanced at quarantine time) can be re-applied by retryQuarantined.
func (a *applier) apply(ctx context.Context, cs *crdt.Changeset, force bool) error {
	if cs.ClusterID != a.cfg.Cluster {
		return fmt.Errorf("postgres: cluster mismatch")
	}
	cache := a.cfg.Cache
	if !force && cache.IsAppliedRemote(cs.Dot.Origin, cs.Dot.Seq) {
		return nil // already applied — idempotent skip
	}
	// Schema-catchup gate (§6): refuse the DML unless the local catalog has
	// reached the changeset's Deps[SchemaChain]. The orchestrator catches the
	// schema log up (catchUpSchema) before calling Apply, so a met dep passes;
	// this is the defense-in-depth backstop. Fixed-schema mode ships seq 0.
	if reqSeq, ok := cs.Deps[crdt.SchemaChain]; ok && reqSeq > 0 {
		var local uint64
		if a.schemaSeq != nil {
			local = a.schemaSeq.Load()
		}
		if uint64(reqSeq) > local {
			return fmt.Errorf("postgres: schema dep %d exceeds local schema head %d", reqSeq, local)
		}
	}
	for _, r := range cs.Records {
		h := r.Header()
		ti := a.cat.table(h.Table)
		if ti == nil {
			return fmt.Errorf("postgres: changeset references unknown table %x", h.Table[:8])
		}
		if _, ok := r.(crdt.BlobPatch); ok {
			return fmt.Errorf("%w (table %x)", errBlobPatchUnsupported, h.Table[:8])
		}
		if err := validatePKBlob(ti, h.PK); err != nil {
			return err
		}
	}

	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Counter contributions are not idempotent, and the sidecar's frontier is
	// persisted outside this transaction — so a crash between the two would
	// re-deliver this changeset. The applied marker closes that window: it is
	// written in THIS transaction, and on a re-delivery it certifies the
	// contributions already landed, leaving only the idempotent remainder to
	// re-apply (§8).
	records := cs.Records
	var certified bool // this changeset's counter contributions already landed
	if a.counterBearing(records) {
		present, err := appliedMarkerPresent(ctx, tx, cs.Dot)
		if err != nil {
			return err
		}
		if present {
			certified = true
			records = a.stripCounterContributions(records)
		} else if err := writeAppliedMarker(ctx, tx, cache, cs.Dot); err != nil {
			return err
		}
	}
	var done []rowClockWrite
	// pendingWinners stashes each winning record's post-DML image to be pushed
	// to Cache.StashWinner after tx.Commit. For Insert we hold the post-arb
	// Image directly; for Update we read the full post-UPSERT row inside the
	// apply tx (the changeset carried only Changed columns). A later local fold
	// checks the stash and self-corrects when it loses LWW to this stamp —
	// winner repair in docs/postgres.md §9.
	type pendingWinner struct {
		tid   crdt.TableID
		pk    crdt.PKBlob
		cl    uint64
		image []crdt.ColValue
		cols  map[crdt.ColumnID]struct{} // cell group: the columns this record won
	}
	var pendingWinners []pendingWinner
	// cellStamps holds the §5 steal path's cell_clock writes (on loser rows),
	// applied to the Cache only after the apply tx commits, so a rolled-back
	// apply leaves no stamp the loser-null UPDATE didn't also undo.
	var cellStamps []cellStamp
	// conflicts accumulates the arbitrations that discarded committed values, in
	// either direction; they are written to the audit table (§9) in this same
	// transaction, so the record cannot outlive or precede the overwrite.
	var conflicts []conflict

	for _, r := range records {
		h := r.Header()
		ti := a.cat.table(h.Table)
		rs := cache.RowState(h.Table, h.PK)
		prevCL := rs.CL
		// Conflict audit (§9): only a row another origin has written can lose
		// anything to this changeset, so the pre-image read that tells us WHICH
		// values are about to be discarded is paid on contended rows alone.
		var preImage []crdt.ColValue
		if writtenByOtherOrigin(rs, cs.Stamp) {
			preImage, err = readRowImage(ctx, tx, ti, h.PK)
			if err != nil {
				return fmt.Errorf("conflict log: read pre-image %s: %w", ti.name, err)
			}
		}
		// Cell-group dispatch (§8): an Update — and a same-generation Insert,
		// which is an UPSERT-update — arbitrates per column. Delete and a
		// CL-bumping Insert stay on the row-level path. A record carrying counter
		// contributions must not be dropped on a Stamp it never arbitrates with,
		// so it gates on causal length alone and refines per column inside.
		if ti.cellGroup() {
			if upd, isCell := crdt.AsCellUpdate(ti, r, rs); isCell {
				if h.CL < rs.CL || (!updateHasCounter(upd) && !rs.DominatedBy(h.CL, cs.Stamp)) {
					conflicts = append(conflicts, inboundLosses(ti, r, rs, cs.Stamp, h.CL)...)
					continue
				}
				out, err := a.applyCellUpdate(ctx, tx, cache, ti, upd, rs, cs.Stamp,
					prevCL > 0 && h.CL > prevCL, certified)
				if err != nil {
					return err
				}
				conflicts = append(conflicts, inboundLosses(ti,
					crdt.Update{Table: h.Table, PK: h.PK, CL: h.CL, Changed: out.lost}, rs, cs.Stamp, h.CL)...)
				if !out.applied {
					continue
				}
				if out.rowUpdate != nil {
					done = append(done, *out.rowUpdate)
				}
				cellStamps = append(cellStamps, out.cellStamps...)
				if len(out.winnerCols) == 0 {
					continue // no DML ran; nothing this record can claim to have won
				}
				img, err := readRowImage(ctx, tx, ti, h.PK)
				if err != nil {
					return fmt.Errorf("winner-repair: read post-UPSERT row %s: %w", ti.name, err)
				}
				if img != nil {
					pendingWinners = append(pendingWinners, pendingWinner{h.Table, h.PK, h.CL, img, out.winnerCols})
				}
				conflicts = append(conflicts, localLosses(ti, h.PK, preImage, img, rs, cs.Stamp, h.CL, "update")...)
				continue
			}
		}
		if !rs.DominatedBy(h.CL, cs.Stamp) {
			conflicts = append(conflicts, inboundLosses(ti, r, rs, cs.Stamp, h.CL)...)
			continue // local/seen write dominates
		}
		switch rec := r.(type) {
		case crdt.Insert:
			if err := validateInsertCounterImage(ti, rec.Image); err != nil {
				return err
			}
		case crdt.Update:
			// The row-level path renders a FormatDelta value as SQL addition
			// (upsertSQL), so the wire contract has to be checked here too —
			// otherwise a payload naming a non-counter column would silently add
			// to it instead of quarantining.
			if err := validateCounterValues(ti, rec.Changed); err != nil {
				return err
			}
		}
		// genBumped: this record advances the row to a new generation (a recreate),
		// so any cell clock on the prior generation is stale and about to be cleared
		// by PutRowState's CL-bump — the §5 cell-LWW pass must not gate against it.
		genBumped := prevCL > 0 && h.CL > prevCL
		var w rowWrite
		// winnerInsertImg / winnerUpdate gate the winner-repair stash for this
		// record. Both are zeroed and assigned per iteration; the stash is pushed
		// only after actual DML succeeds, and an Update's full post-UPSERT image is
		// then read in the transaction because the changeset carries only Changed
		// columns.
		var winnerInsertImg []crdt.ColValue
		var winnerUpdate bool
		switch rec := r.(type) {
		case crdt.Insert:
			// §5 loser-null UNIQUE arbitration: may null R's own key columns
			// (cede), drop them (cell-LWW loss), or steal v from a losing owner
			// (staged on tx now; its cell clock applied to the Cache post-commit).
			arb, stolen, err := arbitrateUnique(ctx, tx, cache, ti, rec, cs.Stamp, genBumped)
			if err != nil {
				return err
			}
			cellStamps = append(cellStamps, stolen...)
			arbImg := arb.(crdt.Insert).Image
			if certified && ti.hasCounters() {
				// Redelivery the applied marker certifies: the contributions in
				// this image already landed, so it must not add them again —
				// which rules out the additive merge below as well as a plain
				// overwrite. It still has to be able to recreate the row, whose
				// counter columns are NOT NULL. The stash then needs the row read
				// back like the additive merge below: on a row that was still
				// present the write kept a total this image does not carry.
				w = upsertSQLKeepingCounters(ti, arbImg)
				winnerUpdate = true
			} else if merged, ok := counterMergeImage(ti, arbImg, rs, h.CL); ok {
				// Generation-establishing Insert on a row this node's clock does
				// not cover yet: its counter columns merge additively instead of
				// erasing physical content every peer is summing (§8). The stash
				// then needs the post-UPSERT row read back, since the merged row
				// is not the image.
				w = upsertSQL(ti, merged)
				winnerUpdate = true
			} else {
				w = upsertSQL(ti, arbImg)
				winnerInsertImg = arbImg
			}
		case crdt.Update:
			arb, stolen, err := arbitrateUnique(ctx, tx, cache, ti, rec, cs.Stamp, genBumped)
			if err != nil {
				return err
			}
			cellStamps = append(cellStamps, stolen...)
			w = upsertSQL(ti, arb.(crdt.Update).Changed)
			winnerUpdate = true
		case crdt.Delete:
			w = deleteSQL(ti, h.PK)
		default:
			continue
		}
		if w.sql == "" {
			continue // arbitration left no columns to write (cell-LWW loss); row unchanged
		}
		if err := execRowWrite(ctx, tx, w); err != nil {
			return fmt.Errorf("apply %T %s: %w", r, ti.name, err)
		}
		done = append(done, rowClockWrite{tid: h.Table, pk: h.PK, state: crdt.RowState{CL: h.CL, Base: cs.Stamp}})
		postImage := winnerInsertImg
		op := "insert"
		switch {
		case winnerInsertImg != nil:
			pendingWinners = append(pendingWinners, pendingWinner{h.Table, h.PK, h.CL, winnerInsertImg, nil})
		case winnerUpdate:
			op = "update"
			img, err := readRowImage(ctx, tx, ti, h.PK)
			if err != nil {
				return fmt.Errorf("winner-repair: read post-UPSERT row %s: %w", ti.name, err)
			}
			postImage = img
			if img != nil {
				pendingWinners = append(pendingWinners, pendingWinner{h.Table, h.PK, h.CL, img, nil})
			}
		default:
			// A winning Delete: the row is gone, so every value it held is lost.
			// Stashed with a nil image — there is nothing to repair TO, but the
			// fold still needs to know a peer removed this row, both to drop a
			// losing local write and to re-assert a winning one that this delete
			// may have erased before it was folded.
			op, postImage = "delete", nil
			pendingWinners = append(pendingWinners, pendingWinner{h.Table, h.PK, h.CL, nil, nil})
		}
		conflicts = append(conflicts, localLosses(ti, h.PK, preImage, postImage, rs, cs.Stamp, h.CL, op)...)
	}
	if err := writeConflicts(ctx, tx, conflicts); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, d := range done {
		if cache.PutRowState(d.tid, d.pk, d.state) && d.clearCells {
			cache.ClearCellsForRow(d.tid, d.pk)
		}
	}
	// Winner repair (§9): stash each winning record's post-DML image so
	// a later local fold that loses LWW to this stamp can self-correct rather
	// than ship a loser. Done AFTER PutRowState so the stash and the row state
	// it relates to land together.
	for _, w := range pendingWinners {
		a.winners.stash(w.tid, w.pk, winnerEntry{
			Dot: cs.Dot, CL: w.cl, Stamp: cs.Stamp, Image: w.image, Cols: w.cols,
		})
	}
	// Key-column cell clocks AFTER the row states. A steal records an override on
	// the loser (clear=false): it keeps its row baseline untouched and gains only a
	// per-key-column override at R.stamp, so a concurrent non-key write still
	// compares against its own baseline. A winning key write clears any prior
	// (stale) override on its own row (clear=true) so the column's effective stamp
	// follows the row's new baseline — else a later lower-stamp writer mis-arbitrates.
	for _, c := range cellStamps {
		if c.clear {
			cache.DeleteCellStamp(c.tid, c.pk, c.col)
		} else {
			cache.PutCellStamp(c.tid, c.pk, c.col, c.stamp)
		}
	}
	// The applied frontier and the local HLC take the changeset's clock only up
	// to the skew bound (skew.go): a peer's broken clock must not become ours.
	cache.MarkApplied(cs.Dot.Origin, cs.Dot.Seq, a.skew.admit(cs.Dot.Origin, cs.Stamp.Clock))
	return nil
}

// currentWALLSN reads this node's current WAL insert position — the catch-up
// target the orchestrator drains local drafts up to before a remote apply (see
// drainToWALTarget). Uses the apply connection, which the orchestrator owns on
// its single goroutine.
func (a *applier) currentWALLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var lsnStr string
	if err := a.conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()`).Scan(&lsnStr); err != nil {
		return 0, fmt.Errorf("current wal lsn: %w", err)
	}
	return pglogrepl.ParseLSN(lsnStr)
}

// applySelfCorrect runs the winner-repair writes the fold path deferred (§9):
// an UPSERT of the row's repaired image, or a DELETE when the row is
// not to survive. The apply conn is the same one the orchestrator owns, so this
// runs serialized with the rest of the actor and — being origin-tagged — is
// filtered out of the capture stream, so a repair never folds back as a fresh
// local write. One transaction, so a mid-batch error rolls back cleanly.
//
// An error here is FATAL to capture, deliberately, and is the one apply-side
// write with no quarantine behind it. Quarantine works for an inbound changeset
// because setting it aside is safe: the node keeps serving, and the frontier
// records that those bytes still owe. A repair cannot be set aside. It exists
// because the cluster's winner is already in this node's Cache while its value
// is no longer in this node's table, so skipping it leaves the table
// contradicting the clock that node publishes — silent, permanent divergence
// that no retry sweep would ever revisit. Halting is the fail-closed choice, and
// a transient failure recovers on restart: the local commit is re-decoded,
// re-folded, and the repair re-runs.
func (a *applier) applySelfCorrect(ctx context.Context, ops []selfCorrectOp) error {
	if len(ops) == 0 {
		return nil
	}
	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: self-correct begin: %w", err)
	}
	defer tx.Rollback(ctx)
	var conflicts []conflict
	for _, op := range ops {
		ti := a.cat.table(op.tid)
		if ti == nil {
			return fmt.Errorf("postgres: self-correct unknown table %x", op.tid[:8])
		}
		w := deleteSQL(ti, op.pk)
		if !op.del {
			w = upsertSQL(ti, op.image)
		}
		if w.sql == "" {
			continue // image had no columns to write (shouldn't happen for a stashed winner)
		}
		if err := execRowWrite(ctx, tx, w); err != nil {
			return fmt.Errorf("postgres: self-correct write: %w", err)
		}
		// This repair is the one place a LOCAL committed write is discarded
		// outright, so it is exactly what the audit log exists to show (§9).
		if c, ok := newConflict(ti, op.pk, true, lostColumns(ti, op.lost, nil),
			op.winner, op.winnerCL, op.loser, op.loserCL, "update"); ok {
			conflicts = append(conflicts, c)
		}
	}
	if err := writeConflicts(ctx, tx, conflicts); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// upsertTarget aliases the conflict target so a counter contribution can name
// the committed row it sums onto (`n = <alias>.n + excluded.n`).
const upsertTarget = "syzy_target"

// rowWrite is one row's rendered DML: the statement to run, plus the statement
// to fall back to when it turns out the row is not physically there.
type rowWrite struct {
	sql string
	// materialize is the INSERT … ON CONFLICT rendering of the SAME image, set
	// only when sql is the plain-UPDATE form of a partial image (updateSQL). That
	// UPDATE cannot create the row it names, so a zero-row result means the row
	// is absent — the record that creates it has not landed (cross-origin
	// delivery is not causally gated, and a quarantined Insert lets later seqs
	// flow past it). Falling back to the INSERT lets Postgres itself decide:
	// where the physical schema can fill the columns the image lacks the row
	// materializes, and where it cannot the constraint violation is deterministic
	// and routes to quarantine — retried once the row exists. Either way the
	// write is never counted as applied when nothing was written.
	materialize string
}

// execRowWrite runs one rendered write inside the apply transaction, falling
// back to the materializing INSERT when the partial UPDATE found no row.
func execRowWrite(ctx context.Context, tx pgx.Tx, w rowWrite) error {
	tag, err := tx.Exec(ctx, w.sql)
	if err != nil {
		return err
	}
	if w.materialize == "" || tag.RowsAffected() > 0 {
		return nil
	}
	_, err = tx.Exec(ctx, w.materialize)
	return err
}

// upsertSQL renders INSERT … ON CONFLICT (pk) DO UPDATE for a full-image row.
// Values are cast from text literals to each column's local type — exactly how
// the real adapter binds via local typinput (§5). Omitted columns (e.g.
// unchanged-TOAST) are left intact on update. A GENERATED ALWAYS AS IDENTITY
// column gets OVERRIDING SYSTEM VALUE so the replicated id is accepted verbatim.
//
// A counter contribution (FormatDelta, §8) is SUMMED onto the committed cell
// rather than overwriting it, so concurrent increments accumulate; on the INSERT
// side it lands verbatim as the generation's opening value. The arithmetic runs
// in Postgres, where bigint overflow raises rather than silently changing type.
func upsertSQL(ti *tableInfo, image []crdt.ColValue) rowWrite {
	return renderUpsert(ti, image, false)
}

// upsertSQLKeepingCounters renders an Insert whose counter contributions the
// applied marker already certifies (§8). The counter columns stay in the INSERT
// list, so a row that is no longer physically present is recreated with the
// generation's opening value instead of failing NOT NULL — but they are left out
// of the ON CONFLICT SET, so a row that IS present keeps the total it has
// accumulated rather than being re-opened at a value already counted.
func upsertSQLKeepingCounters(ti *tableInfo, image []crdt.ColValue) rowWrite {
	return renderUpsert(ti, image, true)
}

func renderUpsert(ti *tableInfo, image []crdt.ColValue, keepCounters bool) rowWrite {
	if len(image) == 0 {
		return rowWrite{} // arbitration dropped every column (cell-LWW loss); nothing to write
	}
	byID := make(map[crdt.ColumnID]crdt.ColValue, len(image))
	for _, cv := range image {
		byID[cv.Column] = cv
	}
	insert := insertSQL(ti, byID, keepCounters)
	// A partial image is written as an UPDATE — but only if it names the row.
	// Without every PK column there is nothing to target, so fall through and let
	// the INSERT fail loudly rather than issue an UPDATE that silently matches
	// nothing.
	if hasWholePK(ti, byID) && !coversWholeRow(ti, byID) {
		if upd := updateSQL(ti, byID, keepCounters); upd != "" {
			return rowWrite{sql: upd, materialize: insert}
		}
		// Nothing left to set once PK and certified counter columns are excluded.
		// The INSERT alone is then the whole write: it creates the row if it is
		// absent and does nothing if it is not.
	}
	return rowWrite{sql: insert}
}

// insertSQL renders the INSERT … ON CONFLICT (pk) DO UPDATE form of an image.
func insertSQL(ti *tableInfo, byID map[crdt.ColumnID]crdt.ColValue, keepCounters bool) string {
	var cols, vals, sets []string
	overriding := ""
	for _, c := range ti.cols {
		cv, ok := byID[c.cid]
		if !ok {
			continue
		}
		cols = append(cols, quoteIdent(c.name))
		vals = append(vals, literal(cv, c.typeName))
		if keepCounters && c.counter {
			continue // insert-if-absent only; a live row keeps its accumulated total
		}
		if cv.Format == crdt.FormatDelta {
			sets = append(sets, fmt.Sprintf("%s = %s.%s + excluded.%s",
				quoteIdent(c.name), quoteIdent(upsertTarget), quoteIdent(c.name), quoteIdent(c.name)))
			continue
		}
		// A GENERATED ALWAYS AS IDENTITY column rejects an explicit INSERT value
		// unless OVERRIDING SYSTEM VALUE is given, and cannot be UPDATEd at all —
		// so feed the replicated id with OVERRIDING but never put it in the SET
		// (its value is immutable, identical on every node anyway).
		if c.identity == 'a' {
			overriding = " OVERRIDING SYSTEM VALUE"
		}
		if !c.isPK && c.identity != 'a' {
			sets = append(sets, fmt.Sprintf("%s = excluded.%s", quoteIdent(c.name), quoteIdent(c.name)))
		}
	}
	conflict := "DO NOTHING"
	if len(sets) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(sets, ", ")
	}
	return fmt.Sprintf("INSERT INTO %s AS %s (%s)%s VALUES (%s) ON CONFLICT (%s) %s",
		tableRef(ti), quoteIdent(upsertTarget), strings.Join(cols, ", "), overriding,
		strings.Join(vals, ", "), strings.Join(pkIdents(ti), ", "), conflict)
}

// hasWholePK reports whether an image names the row — every PK column present.
func hasWholePK(ti *tableInfo, byID map[crdt.ColumnID]crdt.ColValue) bool {
	for _, c := range ti.pk {
		if _, ok := byID[c.cid]; !ok {
			return false
		}
	}
	return len(ti.pk) > 0
}

// coversWholeRow reports whether an image carries every column apply can write:
// all non-PK columns except generated ones, whose values Postgres computes.
//
// Deliberately a plain completeness test rather than a model of which columns
// are *required* (NOT NULL without a default). Postgres has more ways to make a
// column required than a column's own attributes show — a NOT NULL declared on a
// DOMAIN leaves attnotnull false, and a CHECK (col IS NOT NULL) is not a column
// attribute at all — and every one this side fails to predict routes a partial
// image back to the INSERT and reinstates the 23502 this all exists to avoid.
// The two errors are not symmetric: under-routing breaks ordinary updates, while
// over-routing costs at most one extra statement on a row that turns out to be
// absent, since the zero-row fallback materializes it. So the cheap direction is
// the safe one, and the schema is not modelled at all.
func coversWholeRow(ti *tableInfo, byID map[crdt.ColumnID]crdt.ColValue) bool {
	for _, c := range ti.cols {
		if c.isPK || c.generated {
			continue
		}
		if _, ok := byID[c.cid]; !ok {
			return false
		}
	}
	return true
}

// updateSQL writes a partial image — one that cannot construct a whole row — as
// a plain UPDATE.
//
// It cannot go through INSERT ... ON CONFLICT: Postgres builds and checks the
// proposed tuple BEFORE it detects the conflict, so an image missing a NOT NULL
// column that has no default raises 23502 even when the row already exists and
// that column is not being written. Every cell-group update is a partial image
// by construction (it carries only the columns the transaction changed), so on
// such a table an ordinary update to one column would otherwise fail on every
// receiver — deterministically, which quarantines it, which diverges the node
// that could not apply it.
//
// The trade is that a partial write no longer creates a row that is physically
// absent — while the row IS there. When it is not, execRowWrite falls back to
// the INSERT (rowWrite.materialize), which either materializes the row from the
// physical schema's defaults or raises the constraint violation that sends the
// changeset to quarantine to be retried. A zero-row UPDATE is never mistaken for
// an apply.
func updateSQL(ti *tableInfo, byID map[crdt.ColumnID]crdt.ColValue, keepCounters bool) string {
	var sets []string
	for _, c := range ti.cols {
		cv, ok := byID[c.cid]
		// A PK column identifies the row rather than being written to it, and an
		// identity column's value is immutable and identical on every node.
		if !ok || c.isPK || c.identity == 'a' {
			continue
		}
		if keepCounters && c.counter {
			continue // certified redelivery: the contribution already landed
		}
		if cv.Format == crdt.FormatDelta {
			sets = append(sets, fmt.Sprintf("%s = %s + %s",
				quoteIdent(c.name), quoteIdent(c.name), literal(cv, c.typeName)))
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = %s", quoteIdent(c.name), literal(cv, c.typeName)))
	}
	if len(sets) == 0 {
		return ""
	}
	preds := make([]string, len(ti.pk))
	for i, c := range ti.pk {
		preds[i] = fmt.Sprintf("%s = %s", quoteIdent(c.name), literal(byID[c.cid], c.typeName))
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableRef(ti), strings.Join(sets, ", "), strings.Join(preds, " AND "))
}

// deleteSQL removes the row. Matching nothing is a legitimate outcome — the row
// may already be gone — so it never carries a materializing fallback.
func deleteSQL(ti *tableInfo, pk crdt.PKBlob) rowWrite {
	return rowWrite{sql: fmt.Sprintf("DELETE FROM %s WHERE %s", tableRef(ti), pkWhere(ti, pk))}
}

// pkWhere renders the "pk1 = v1 AND pk2 = v2" predicate for a PKBlob, each
// value cast from its canonical text to the column type (shared by delete and
// the row-image/unique lookups).
func pkWhere(ti *tableInfo, pk crdt.PKBlob) string {
	vals := decodePKBlobTyped(pk)
	preds := make([]string, len(ti.pk))
	for i, c := range ti.pk {
		var cv crdt.ColValue
		if i < len(vals) {
			cv = vals[i]
		}
		preds[i] = fmt.Sprintf("%s = %s", quoteIdent(c.name), literal(cv, c.typeName))
	}
	return strings.Join(preds, " AND ")
}

func pkIdents(ti *tableInfo) []string {
	out := make([]string, len(ti.pk))
	for i, c := range ti.pk {
		out[i] = quoteIdent(c.name)
	}
	return out
}

// literal renders a canonical typed ColValue as a quoted-text-cast SQL
// literal. A malformed value renders as an always-false comparison target
// (invalid cast) so the statement fails loudly instead of writing garbage.
func literal(cv crdt.ColValue, typeName string) string {
	if cv.TypeTag == crdt.ColNull {
		return "NULL"
	}
	text, err := colValueText(cv)
	if err != nil {
		return "'syzy:malformed-value'::" + typeName
	}
	return quoteString(text) + "::" + typeName
}

func tableRef(ti *tableInfo) string { return quoteIdent(ti.schema) + "." + quoteIdent(ti.name) }
func quoteIdent(s string) string    { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// readRowImage SELECTs the row's columns as text and re-encodes them in the
// canonical typed form (value.go) inside the apply tx, so the winner-repair
// stash carries the full post-merge row a peer Update can't ship verbatim
// (the changeset's Changed columns alone would miss every unchanged column).
// Returns nil iff
// the row vanished between tx.Exec and this SELECT — shouldn't happen on the
// orchestrator goroutine, but treat it as "nothing to stash" rather than err.
func readRowImage(ctx context.Context, tx pgx.Tx, ti *tableInfo, pk crdt.PKBlob) ([]crdt.ColValue, error) {
	cols := make([]string, len(ti.cols))
	for i, c := range ti.cols {
		cols[i] = quoteIdent(c.name) + "::text"
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(cols, ", "), tableRef(ti), pkWhere(ti, pk))
	scanners := make([]any, len(ti.cols))
	nulls := make([]*string, len(ti.cols))
	for i := range scanners {
		scanners[i] = &nulls[i]
	}
	if err := tx.QueryRow(ctx, sql).Scan(scanners...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]crdt.ColValue, len(ti.cols))
	for i, c := range ti.cols {
		if nulls[i] == nil {
			out[i] = crdt.ColValue{Column: c.cid, TypeTag: crdt.ColNull}
			continue
		}
		cv, err := encodeColValue(c.cid, c.typeName, []byte(*nulls[i]))
		if err != nil {
			return nil, fmt.Errorf("read row image %s.%s: %w", ti.name, c.name, err)
		}
		out[i] = cv
	}
	return out, nil
}
func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
