package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

var errSchemaBehind = errors.New("broker: schema behind")

// errUpdateOutranInsert marks a row-group update whose UPDATE matched no
// physical row: the record outran the row's INSERT under non-causally-
// gated delivery. Deterministic and payload-specific, so applyPayload
// quarantines it; the retry converges once the Insert lands (row-group
// updates carry the full post-image). The cell-group path materializes
// the row instead — see applyCellUpdate.
var errUpdateOutranInsert = errors.New("broker: update outran its row's insert")

// postDMLState carries one applied payload's post-DML state from
// applyPayloadCache to advanceCacheState: enough to advance the cache,
// append the mirror journal, and fire listeners.
type postDMLState struct {
	origin      crdt.Origin
	seq         crdt.Seq
	hlc         crdt.Clock
	payload     []byte
	records     []crdt.Record
	rowUpdates  []rowClockUpdate
	cellUpdates []cellClockUpdate
}

// applyPayload is the entry point for both Subscribe-delivered bytes and
// future Fetch-delivered bytes. Returns nil iff the bytes have been
// durably accepted (mirror-journal append, idempotent skip, or
// schema-gated retry completion). Errors are returned to the transport
// only when this broker cannot accept the payload.
func (b *Broker) applyPayload(_ context.Context, payload []byte) error {
	b.fireApplyStart(payload)
	cs, err := crdt.Decode(payload)
	if err != nil {
		return fmt.Errorf("broker: decode: %w", err)
	}
	if cs.ClusterID != b.cluster {
		return fmt.Errorf("broker: cluster_id mismatch: got %x, want %x", cs.ClusterID, b.cluster)
	}
	err = b.applyPayloadCache(cs, payload, false)
	if err != nil && isDeterministicApplyFailure(err) {
		// A deterministic, payload-specific DML failure: a SQLite
		// constraint (e.g. a record forced to materialize a row whose
		// cross-origin INSERT has not yet been delivered, leaving a NOT
		// NULL column unsatisfiable), a counter apply failure (wire
		// contract violation, int64 overflow), or a row-group update
		// that outran its row's INSERT. Quarantine + advance past it so
		// it can't permanently, silently pin this origin's frontier and
		// starve every later seq. Returns false (keep the hard block)
		// when the per-origin quarantine cap is exceeded.
		if b.quarantineConstraintFailure(cs, payload, err) {
			return nil
		}
	}
	return err
}

// Apply writes one already-decoded peer Changeset through the same cache,
// app.db, and mirror-journal path as Subscribe-delivered bytes. The subscribe
// loop decodes raw bytes and funnels into applyPayloadCache; this entry takes
// the decoded Changeset directly. The apply path is synchronous.
func (b *Broker) Apply(_ context.Context, cs *crdt.Changeset) error {
	b.fireApplyStart(cs.Encoded())
	if cs.ClusterID != b.cluster {
		return fmt.Errorf("broker: cluster_id mismatch: got %x, want %x", cs.ClusterID, b.cluster)
	}
	return b.applyPayloadCache(cs, cs.Encoded(), false)
}

// applyPayloadCache is the apply path: idempotency check against
// nodestate.Cache (frontier + applied_gaps), per-record LWW against
// Cache.RowState, app.db DML inside one BEGIN IMMEDIATE / COMMIT,
// then in-memory state advance + mirror-journal append +
// fireApplied. No metadata reads or writes on the hot path; the
// snapshotter persists state asynchronously.
//
// Mirror-then-applied ordering is the spec's: app.db commit -> Cache
// state update -> mirror journal append -> fireApplied. A crash
// between app.db commit and mirror append leaves the row in app.db
// without a journal entry; the in-memory frontier (loaded from
// snapshot on restart) doesn't include this seq, so the peer's
// re-broadcast lands as an idempotent app.db UPSERT and the journal
// gets the entry on the second application. No corruption.
// force, when true, skips the applied-idempotency short-circuit so the
// quarantine re-apply drain can re-run the DML for a seq the frontier was
// already advanced past at quarantine time (see RetryQuarantined).
func (b *Broker) applyPayloadCache(cs *crdt.Changeset, payload []byte, force bool) (err error) {
	b.applyMu.Lock()
	defer func() {
		if err != nil {
			err = b.repairApplyFailure(err)
		}
		b.applyMu.Unlock()
	}()
	cache := b.cfg.Cache
	// Producer-side commits don't flow through the apply path; if one
	// somehow does (loopback transport bug) it would otherwise advance
	// senderNextSeq incorrectly via MarkApplied. Reject to be safe.
	if cs.Dot.Origin == cache.Self() {
		return nil
	}
	if !force && cache.IsAppliedRemote(cs.Dot.Origin, cs.Dot.Seq) {
		return nil
	}
	// Schema-chain Deps gating. The originator stamps required_schema_seq
	// onto every Changeset; if our local catalog is behind, return the
	// schema-behind sentinel so the subscribe loop holds this payload
	// until schema-chain catch-up advances meta.schema_seq. The
	// applied_gaps-extension version of gating is a follow-up (see
	// docs/ARCHITECTURE.md#distribution-and-anti-entropy).
	if b.cfg.Meta != nil && cs.Deps != nil {
		if reqSeq, ok := cs.Deps[crdt.SchemaChain]; ok && reqSeq > 0 {
			localSeq, _, err := b.cfg.Meta.GetSchemaSeq()
			if err != nil {
				return fmt.Errorf("broker: read schema_seq: %w", err)
			}
			if uint64(reqSeq) > localSeq {
				return fmt.Errorf("%w: changeset requires schema_seq=%d (local=%d)",
					errSchemaBehind, reqSeq, localSeq)
			}
		}
	}
	if b.applyTrace != nil {
		b.applyTrace("post-decode")
	}
	// Counter contributions (FormatDelta / counter-column images) are
	// not idempotent. If a prior apply of this seq committed but died
	// before the frontier survived (crash between COMMIT and journal
	// append), its applied marker is in app.db: strip the counter
	// effects and re-apply only the idempotent remainder. Otherwise
	// request the marker be written atomically with this apply's DML.
	records := cs.Records
	var marker *crdt.Dot
	if b.counterBearing(records) {
		present, merr := b.appliedMarkerPresent(cs.Dot.Origin, cs.Dot.Seq)
		if merr != nil {
			return fmt.Errorf("broker: applied marker probe: %w", merr)
		}
		if present {
			records = b.stripCounterContributions(records)
		} else {
			marker = &cs.Dot
		}
	}
	updates, cellUpdates, err := b.applyRecordsLWW(records, cs.Stamp, marker)
	if err != nil {
		return fmt.Errorf("broker: applyRecordsLWW: %w", err)
	}
	if b.applyTrace != nil {
		b.applyTrace("post-dml")
	}
	return b.advanceCacheState(postDMLState{
		origin:      cs.Dot.Origin,
		seq:         cs.Dot.Seq,
		hlc:         cs.Stamp.Clock,
		payload:     payload,
		records:     records,
		rowUpdates:  updates,
		cellUpdates: cellUpdates,
	})
}

// advanceCacheState runs the post-DML half of one applyPayload: cache
// row/cell stamp writes, frontier MarkApplied, mirror journal append,
// applied-listener fires, apply-records-listener fires. Idempotent at
// the cache layer (PutRowState/MarkApplied dedupe on equal state); the
// journal append is NOT idempotent and the caller must ensure each
// (origin, seq) is only advanced once.
func (b *Broker) advanceCacheState(p postDMLState) error {
	cache := b.cfg.Cache
	// In-memory state advance under cache mutex. Order: write rowClock
	// updates, then cellClock overrides, then MarkApplied (frontier +
	// gaps + hlcLast). MarkApplied also pulls hlcLast forward to
	// MAX(self, p.hlc).
	for _, u := range p.rowUpdates {
		if cache.PutRowState(u.table, u.pk, u.state) && u.clearCells {
			cache.ClearCellsForRow(u.table, u.pk)
		}
	}
	for _, u := range p.cellUpdates {
		cache.PutCellStamp(u.table, u.pk, u.col, u.stamp)
	}
	// MarkApplied returns the contiguous head as it was before this
	// call so we can wake the fetcher only when a *new* gap appears.
	priorHead := cache.MarkApplied(p.origin, p.seq, p.hlc)
	if p.seq > priorHead+1 {
		b.kickFetcher()
	}
	if b.cfg.MirrorJournals != nil {
		if err := b.cfg.MirrorJournals.Append(p.origin, p.payload); err != nil {
			return fmt.Errorf("broker: mirror journal append: %w", err)
		}
		// Advance the per-origin marker so the next snapshot can replay
		// from at least this point. We use the journal's current head
		// post-Append; recovery's iterator (from marker to head) covers
		// any races where another payload landed concurrently.
		if j, err := b.cfg.MirrorJournals.Journal(p.origin); err == nil {
			cache.SetSnapshotMarker(p.origin, j.Head())
		}
	}
	b.recordApplied(p.origin, p.seq, p.hlc)
	b.fireApplied(p.origin, p.seq)
	b.fireApplyRecords(p.origin, p.seq, p.records)
	return nil
}

// ReassertLocal closes the local-commit/inbound-apply race. A local
// transaction writes app.db at commit time, but its row-clock advance
// only lands when the drain materializes the journal record. An
// inbound changeset arriving in that window compares against the
// not-yet-advanced row clock, can pass the LWW gate, and overwrite the
// locally-committed (higher-stamped) content in app.db — the drain
// then advances the clock without re-running DML, leaving this node's
// content permanently behind its own winning write.
//
// The drain calls this with each materialized commit's records and
// stamp. Under the apply lock — atomically with inbound applies — any
// record whose row clock was last advanced by a remote write that the
// local commit dominates gets its DML re-applied and its row clock
// advanced. The common single-writer case (row clock still on this
// node's own chain, or already past the local stamp) skips without
// touching app.db.
func (b *Broker) ReassertLocal(records []crdt.Record, stamp crdt.Stamp) error {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	cache := b.cfg.Cache
	self := cache.Self()
	var todo []crdt.Record
	for _, rec := range records {
		if _, isBlob := rec.(crdt.BlobPatch); isBlob {
			// Per-byte ranges arbitrate independently of the row
			// clock; nothing to re-assert.
			continue
		}
		h := rec.Header()
		rs := cache.RowState(h.Table, h.PK)
		if rs.CL == 0 {
			continue
		}
		if tab, ok := b.cfg.Catalog.TableByID(h.Table); ok && tab.CellGroup() {
			// Per-column variant: re-assert when any carried column's
			// effective stamp was last advanced by a remote write the
			// local commit dominates. The cell apply path re-gates
			// per column, so over-inclusion is just an idempotent DML.
			// Counter contributions never participate: they commute
			// with any interleaved apply (no overwrite to repair) and
			// re-running one would double-count, so the record is
			// re-asserted with them stripped.
			if upd, isCell := crdt.AsCellUpdate(tab, rec, rs); isCell {
				for _, v := range upd.Changed {
					if v.Format == crdt.FormatDelta {
						continue
					}
					es := rs.EffectiveStamp(v.Column, crdt.ByteRange{})
					if es.Origin != self && stamp.Dominates(es) {
						todo = append(todo, b.stripCounterContribution(rec))
						break
					}
				}
				continue
			}
		}
		if rs.Base.Origin == self {
			// Row clock is still on this node's own write chain: no
			// inbound apply interleaved with the commit. The journal
			// record's effects are already in app.db.
			continue
		}
		if !rs.DominatedBy(h.CL, stamp) {
			// A remote write outranking the local commit applied after
			// it; that content legitimately stands.
			continue
		}
		todo = append(todo, rec)
	}
	if len(todo) == 0 {
		return nil
	}
	updates, cellUpdates, err := b.applyRecordsLWW(todo, stamp, nil)
	if err != nil {
		return fmt.Errorf("broker: reassert local: %w", err)
	}
	for _, u := range updates {
		if cache.PutRowState(u.table, u.pk, u.state) && u.clearCells {
			cache.ClearCellsForRow(u.table, u.pk)
		}
	}
	for _, u := range cellUpdates {
		cache.PutCellStamp(u.table, u.pk, u.col, u.stamp)
	}
	return nil
}

// applyRecordsLWW runs the apply DML for each LWW-winning record
// inside one BEGIN IMMEDIATE / COMMIT, reading row_clock from
// nodestate.Cache. Returns the row_clock updates and cell_clock
// updates the caller will write back to the cache after a successful
// app.db commit. A non-nil marker asks for the counter applied-marker
// row to be written inside the same transaction (apply_counter.go).
func (b *Broker) applyRecordsLWW(records []crdt.Record, stamp crdt.Stamp, marker *crdt.Dot) ([]rowClockUpdate, []cellClockUpdate, error) {
	if len(records) == 0 {
		return nil, nil, nil
	}
	cache := b.cfg.Cache
	if err := b.beginApply(); err != nil {
		return nil, nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = b.rollbackApply()
		}
	}()
	if marker != nil {
		if err := b.writeAppliedMarker(marker.Origin, marker.Seq); err != nil {
			return nil, nil, err
		}
	}
	var updates []rowClockUpdate
	var cellUpdates []cellClockUpdate
	// blobMutations collects the post-app.db blob_range_clock side
	// effects (UPSERT/DELETE per row). Persisted in a metadata txn
	// after the app.db commit succeeds — best-effort consistency for
	// v0 (idempotent replay handles the crash window).
	type blobMutation struct {
		table crdt.TableID
		pk    crdt.PKBlob
		cols  []metadata.BlobRangeClockEntry
		drop  bool
	}
	var blobMutations []blobMutation
	for _, rec := range records {
		h := rec.Header()
		tab, ok := b.cfg.Catalog.TableByID(h.Table)
		if !ok {
			return nil, nil, fmt.Errorf("%w: table_id=%x not in catalog", errSchemaBehind, h.Table)
		}
		if tab.Dropped() {
			continue
		}
		if bp, isBlob := rec.(crdt.BlobPatch); isBlob {
			rs := cache.RowState(h.Table, h.PK)
			// blob_patch carries the writer's view of the row's
			// current CL (must be odd). Drop on tombstoned receiver:
			// patches do not resurrect (BLOB_PATCH.md "tombstones are
			// terminal"). Per-byte LWW handles within-generation
			// arbitration via IntervalMap.Apply with baseline=parent.
			if rs.IsTombstoned() {
				continue
			}
			mut, err := b.applyBlobPatch(tab, bp, rs, stamp)
			if err != nil {
				return nil, nil, err
			}
			b.markTableHasBlobClock(h.Table)
			blobMutations = append(blobMutations, blobMutation{
				table: h.Table, pk: h.PK,
				cols: mut.cols, drop: mut.drop,
			})
			continue
		}
		rs := cache.RowState(h.Table, h.PK)
		// Cell-group dispatch: Updates (and same-generation Inserts,
		// which are UPSERT-updates) arbitrate per column. Delete and
		// CL-bumping Insert stay on the row-level path below.
		//
		// The row-level (CL, Stamp) gate runs first for register-only
		// records — at equal CL it is equivalent to the per-column
		// gates (EffectiveStamp never falls below Base), just cheaper.
		// A record carrying counter contributions must not be dropped
		// on a Stamp it never arbitrates with: it gates on CL alone
		// and refines per column inside applyCellUpdate (registers
		// stamp-gate there; counter contributions always land).
		if tab.CellGroup() {
			if upd, isCell := crdt.AsCellUpdate(tab, rec, rs); isCell {
				if h.CL < rs.CL ||
					(!cellUpdateHasCounter(upd) && !rs.DominatedBy(h.CL, stamp)) {
					continue
				}
				outcome, err := b.applyCellUpdate(tab, upd, rs, stamp)
				if err != nil {
					return nil, nil, err
				}
				if !outcome.applied {
					continue
				}
				updates = append(updates, outcome.rowUpdates...)
				cellUpdates = append(cellUpdates, outcome.cellUpdates...)
				switch {
				case outcome.blobReconciled:
					blobMutations = append(blobMutations, blobMutation{
						table: h.Table, pk: h.PK,
						cols: outcome.blobCols, drop: len(outcome.blobCols) == 0,
					})
				case b.cfg.Meta != nil && b.tableHasBlobClock(h.Table):
					blobMutations = append(blobMutations, blobMutation{
						table: h.Table, pk: h.PK, drop: true,
					})
				}
				continue
			}
		}
		// Row-level path: Delete, CL-bumping Insert, and every record
		// on a row-group table gate on (CL, Stamp) against the row base.
		if !rs.DominatedBy(h.CL, stamp) {
			continue
		}
		if ins, isIns := rec.(crdt.Insert); isIns {
			if err := validateInsertCounterImage(tab, ins); err != nil {
				return nil, nil, err
			}
		}
		arbRec, stolen, err := b.arbitrate(tab, rec, stamp)
		if err != nil {
			return nil, nil, fmt.Errorf("broker: unique arbitration: %w", err)
		}
		// A generation-establishing Insert on a counter table that finds
		// a physical row the row clock doesn't cover (the undrained
		// local-commit window, or an adopted pre-existing row) merges
		// counter columns additively instead of erasing them — the
		// physical content is a same-generation contribution that every
		// peer sums (CRDT.md F_counter). The converted update flows
		// through the ordinary DML routes below; row-clock bookkeeping
		// (CL, Base=stamp) is unchanged.
		if ins, isIns := arbRec.(crdt.Insert); isIns && tab.HasCounters() &&
			!rs.IsLive() && ins.CL == rs.NextLiveCL() {
			exists, perr := b.rowExists(tab, ins.PK)
			if perr != nil {
				return nil, nil, perr
			}
			if exists {
				arbRec = counterMergeUpdate(tab, ins)
			}
		}
		// Delete drops the range_clock entry — patches don't survive a
		// tombstone. Insert/Update against a row with active per-byte
		// overrides routes through reconciliation; otherwise the full
		// DML absorbs every dominated entry and we drop.
		//
		// Skip the entire blob_range_clock dance when we know the
		// table has no entries (no BLOB columns, or no blob_patches
		// have ever applied). The flag is sticky-true: once any
		// blob_patch lands, the table goes through the full path.
		_, isDelete := arbRec.(crdt.Delete)
		hasBlobClock := b.cfg.Meta != nil && b.tableHasBlobClock(h.Table)
		if !isDelete && hasBlobClock {
			existing, gerr := b.cfg.Meta.GetBlobRangeClock(h.Table, h.PK)
			if gerr != nil {
				return nil, nil, fmt.Errorf("broker: load blob_range_clock: %w", gerr)
			}
			if len(existing) > 0 {
				cols, rerr := b.applyDMLReconciled(tab, arbRec, rs, stamp, existing)
				if rerr != nil {
					return nil, nil, rerr
				}
				b.markTableHasBlobClock(h.Table)
				cellUpdates = append(cellUpdates, stolen...)
				updates = append(updates, rowClockUpdate{
					table: h.Table, pk: h.PK,
					state: crdt.RowState{CL: h.CL, Base: stamp},
				})
				blobMutations = append(blobMutations, blobMutation{
					table: h.Table, pk: h.PK,
					cols: cols, drop: len(cols) == 0,
				})
				continue
			}
		}
		switch r := arbRec.(type) {
		case crdt.Insert:
			if err := b.applyInsert(tab, r); err != nil {
				return nil, nil, err
			}
		case crdt.Update:
			matched, uerr := b.applyUpdate(tab, r)
			if uerr != nil {
				return nil, nil, uerr
			}
			if matched == 0 {
				// A row-group update outran its row's INSERT (delivery is
				// not causally gated): no physical row, yet the row clock
				// below would go live at this update's stamp and row-level
				// LWW would then drop the causally-earlier (lower-stamped)
				// Insert forever — permanent silent row loss. Unlike the
				// cell-group path, materializing from the update here would
				// diverge (no per-column merge lets the Insert fill in), so
				// fail deterministically: the whole changeset rolls back and
				// quarantines, and the retry converges once the Insert
				// lands — the update carries the full post-image.
				return nil, nil, fmt.Errorf("%w: %s row %x", errUpdateOutranInsert, tab.Name, h.PK)
			}
		case crdt.Delete:
			if err := b.applyDelete(tab, r); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("broker: unsupported record %T", arbRec)
		}
		cellUpdates = append(cellUpdates, stolen...)
		updates = append(updates, rowClockUpdate{
			table: h.Table,
			pk:    h.PK,
			state: crdt.RowState{CL: h.CL, Base: stamp},
		})
		// Only schedule a post-commit DELETE on blob_range_clock when
		// the table is known to have entries. Otherwise the DELETE is
		// a no-op that costs a metadata transaction per Insert.
		if hasBlobClock {
			blobMutations = append(blobMutations, blobMutation{
				table: h.Table, pk: h.PK, drop: true,
			})
		}
	}
	if err := b.commitApply(); err != nil {
		return nil, nil, err
	}
	committed = true
	if len(blobMutations) > 0 && b.cfg.Meta != nil {
		if err := b.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
			for _, m := range blobMutations {
				if m.drop {
					if err := tx.DeleteBlobRangeClock(m.table, m.pk); err != nil {
						return err
					}
					continue
				}
				if err := tx.PutBlobRangeClock(m.table, m.pk, m.cols); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("broker: persist blob_range_clock: %w", err)
		}
	}
	return updates, cellUpdates, nil
}

// blobPatchOutcome bundles the per-row blob_range_clock state to
// persist after a successful app.db commit. drop=true asks for a
// DELETE; otherwise cols is the full per-column entry set.
type blobPatchOutcome struct {
	cols []metadata.BlobRangeClockEntry
	drop bool
}

// applyBlobPatch writes won bytes to app.db via sqlite3_blob_write and
// returns the new blob_range_clock state for the row. Algorithm per
// BLOB_PATCH.md "Apply / blob_patch Records":
//
//   - ensureRow placeholder INSERT OR IGNORE if missing
//   - ensureBlobLen extends the column to max(range.end) with zeroblob
//   - load row's IntervalMap, run Apply per range with baseline =
//     rs.EffectiveStamp(col, empty range), write won bytes, persist
//     the coalesced map
func (b *Broker) applyBlobPatch(tab *catalog.Table, bp crdt.BlobPatch,
	rs crdt.RowState, stamp crdt.Stamp) (blobPatchOutcome, error) {
	col, ok := tab.ColumnByID(bp.Col)
	if !ok {
		// Unknown column id (DDL drift) — skip, keep row state as-is.
		return blobPatchOutcome{cols: nil}, nil
	}

	if err := b.ensureRowPlaceholder(tab, bp.PK); err != nil {
		return blobPatchOutcome{}, fmt.Errorf("ensureRow: %w", err)
	}
	rowid, ok, err := b.lookupRowid(tab, bp.PK)
	if err != nil {
		return blobPatchOutcome{}, fmt.Errorf("lookup rowid: %w", err)
	}
	if !ok {
		// Placeholder insert was filtered by a constraint we couldn't
		// honor (e.g., NOT NULL on a non-blob column with no default).
		// Documented as a DDL precondition (BLOB_PATCH.md sharp edges);
		// receivers drop the patch convergently.
		return blobPatchOutcome{cols: nil}, nil
	}

	maxEnd := uint64(0)
	for _, rg := range bp.Ranges {
		if e := rg.End(); e > maxEnd {
			maxEnd = e
		}
	}
	if maxEnd > 0 {
		if err := b.ensureBlobLen(tab, col, bp.PK, maxEnd); err != nil {
			return blobPatchOutcome{}, fmt.Errorf("ensureBlobLen: %w", err)
		}
	}

	existing, err := b.cfg.Meta.GetBlobRangeClock(tab.ID, bp.PK)
	if err != nil {
		return blobPatchOutcome{}, fmt.Errorf("load blob_range_clock: %w", err)
	}
	maps := metadata.LoadIntervalMaps(existing)
	curMap, ok := maps[bp.Col]
	if !ok {
		curMap = crdt.NewIntervalMap()
		maps[bp.Col] = curMap
	}

	bh, err := b.cfg.AppApply.OpenBlob("main", tab.Name, col.Name, rowid, true)
	if err != nil {
		return blobPatchOutcome{}, fmt.Errorf("open blob for write: %w", err)
	}
	defer bh.Close()

	// baseline = effective parent Stamp for this column. Falls
	// through cell_clock[col] then row_clock baseline per
	// CRDT.md#layer-composition; the cell-clock layer carries
	// UNIQUE-loser-null overrides and (in the future) explicit
	// cell-LWW writes.
	baseline := rs.EffectiveStamp(bp.Col, crdt.ByteRange{})
	for _, rg := range bp.Ranges {
		end := rg.End()
		won := curMap.Apply(rg.Offset, end, stamp, baseline)
		for _, w := range won {
			lo := w.Start - rg.Offset
			hi := w.End - rg.Offset
			if err := bh.Write(rg.Bytes[lo:hi], int(w.Start)); err != nil {
				return blobPatchOutcome{}, fmt.Errorf("blob_write %d..%d: %w", w.Start, w.End, err)
			}
		}
	}

	cols := metadata.EntriesFromMaps(maps)
	return blobPatchOutcome{cols: cols, drop: len(cols) == 0}, nil
}

type rowClockUpdate struct {
	table crdt.TableID
	pk    crdt.PKBlob
	state crdt.RowState
	// clearCells drops the row's cell_clock overrides alongside the
	// state write: set on cell-group opportunistic collapse (a full-
	// coverage write re-absorbs the row into its baseline) and on
	// same-state generation advances. CL bumps clear implicitly in
	// PutRowState; this covers the equal-CL collapse.
	clearCells bool
}

// cellClockUpdate is one (table, pk, col) → stamp override emitted by
// the UNIQUE arbitrator's loser-null path. Applied to nodestate.Cache
// after the app.db commit succeeds so the cache stays consistent with
// the on-disk row state.
type cellClockUpdate struct {
	table crdt.TableID
	pk    crdt.PKBlob
	col   crdt.ColumnID
	stamp crdt.Stamp
}

// Transaction control is deliberately uncached. sqlite3_reset returns the
// previous sqlite3_step result, so caching these statements can turn an earlier
// auto-rollback error into a later skipped ROLLBACK.
func (b *Broker) beginApply() error {
	return b.cfg.AppApply.Exec("BEGIN IMMEDIATE")
}
func (b *Broker) commitApply() error {
	return b.cfg.AppApply.Exec("COMMIT")
}
func (b *Broker) rollbackApply() error {
	if b.cfg.AppApply.InAutocommit() {
		return nil
	}
	return b.cfg.AppApply.Exec("ROLLBACK")
}

// repairApplyFailure clears any cached statement that retained a failed
// sqlite3_step result and restores the connection's autocommit invariant.
// Caller holds applyMu.
func (b *Broker) repairApplyFailure(applyErr error) error {
	var sqliteErr sqlitebridge.Error
	if b.cfg.AppApply.InAutocommit() && !errors.As(applyErr, &sqliteErr) {
		return applyErr
	}
	b.finalizeCachedStmts()
	b.selfHeals.Add(1)
	if !b.cfg.AppApply.InAutocommit() {
		if err := b.cfg.AppApply.Exec("ROLLBACK"); err != nil {
			applyErr = errors.Join(applyErr, fmt.Errorf("broker: rollback failed apply: %w", err))
		}
	}
	if !b.cfg.AppApply.InAutocommit() {
		applyErr = errors.Join(applyErr, errors.New("broker: apply connection left outside autocommit"))
	}
	return applyErr
}

func (b *Broker) applyInsert(tab *catalog.Table, r crdt.Insert) error {
	// A record missing a value for some column was journaled before an ADD
	// COLUMN migration added it. The cached all-columns statement can't apply
	// such a record: an unbound placeholder would keep a stale value from the
	// previous call. Route it to the partial path, which omits the absent
	// columns so SQLite fills their declared defaults — exactly what ALTER TABLE
	// ADD COLUMN does to pre-existing rows. The len check short-circuits the
	// common full-row Image (production) without scanning; only a short Image
	// (a non-PK Image that still covers every column, or a genuine pre-migration
	// record) pays for the precise coverage check.
	if len(r.Image) < len(tab.Columns) && !recordCoversAllColumns(tab, r) {
		return b.applyInsertPartial(tab, r)
	}

	stmt, err := b.applyInsertStmt(tab)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	// Each cached INSERT statement has one ? per table column in
	// Columns-slice order. Walk PK + Image and bind each value to its
	// slice-index slot directly — no map or intermediate slice. Slice
	// index, NOT Column.Ordinal: ordinals go sparse after a non-trailing
	// DROP COLUMN while the statement's placeholders stay dense. PK +
	// Image cover every column here (full Image, checked above), so
	// every slot is bound.
	if err := tab.RangePK(r.PK, func(id crdt.ColumnID, v crdt.ColValue) error {
		idx, ok := tab.ColumnIndexByID(id)
		if !ok {
			return nil
		}
		return bindColValue(stmt, idx+1, v)
	}); err != nil {
		return err
	}
	for _, v := range r.Image {
		idx, ok := tab.ColumnIndexByID(v.Column)
		if !ok {
			continue
		}
		if err := bindColValue(stmt, idx+1, v); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

// recordCoversAllColumns reports whether r's PK + Image together carry a value
// for every column of tab. It distinguishes a record that merely omits PK
// columns from its Image (full coverage — the test/spec convention) from one
// journaled before an ADD COLUMN migration (a column genuinely absent). Only
// consulted when the Image is shorter than the column count, so it is off the
// common full-row-Image path.
func recordCoversAllColumns(tab *catalog.Table, r crdt.Insert) bool {
	present := make([]bool, len(tab.Columns))
	covered := 0
	mark := func(id crdt.ColumnID) {
		if idx, ok := tab.ColumnIndexByID(id); ok && !present[idx] {
			present[idx] = true
			covered++
		}
	}
	_ = tab.RangePK(r.PK, func(id crdt.ColumnID, _ crdt.ColValue) error { mark(id); return nil })
	for _, v := range r.Image {
		mark(v.Column)
	}
	return covered == len(tab.Columns)
}

// applyInsertPartial upserts a record whose Image covers only a subset of the
// table's columns — a row journaled before an ADD COLUMN migration appended the
// trailing column(s). It builds an INSERT over just the columns the record
// carries (PK + Image), so SQLite applies each absent column's declared default,
// the same value ALTER TABLE ADD COLUMN gives pre-existing rows. The statement
// is built per record: partial images only arise from pre-migration journal
// records, which are rare and bounded, so the prepare cost is not on the hot
// path. Idempotency and conflict handling match applyInsertStmt's UPSERT.
func (b *Broker) applyInsertPartial(tab *catalog.Table, r crdt.Insert) error {
	// Indexed by Columns-slice position throughout (present, vals, and
	// the order slots below) — mixing in Column.Ordinal here mis-places
	// values once ordinals go sparse after a non-trailing DROP COLUMN.
	present := make([]bool, len(tab.Columns))
	vals := make([]crdt.ColValue, len(tab.Columns))
	if err := tab.RangePK(r.PK, func(id crdt.ColumnID, v crdt.ColValue) error {
		if idx, ok := tab.ColumnIndexByID(id); ok {
			present[idx] = true
			vals[idx] = v
		}
		return nil
	}); err != nil {
		return err
	}
	for _, v := range r.Image {
		if idx, ok := tab.ColumnIndexByID(v.Column); ok {
			present[idx] = true
			vals[idx] = v
		}
	}

	cols := make([]string, 0, len(tab.Columns))
	placeholders := make([]string, 0, len(tab.Columns))
	updates := make([]string, 0, len(tab.Columns))
	order := make([]int, 0, len(tab.Columns)) // bind slot -> Columns-slice index
	for i, col := range tab.Columns {
		if !present[i] {
			continue
		}
		quoted := sqlitebridge.QuoteIdent(col.Name)
		cols = append(cols, quoted)
		placeholders = append(placeholders, "?")
		order = append(order, i)
		if col.PKPos == 0 {
			updates = append(updates, fmt.Sprintf("%s = excluded.%s", quoted, quoted))
		}
	}
	pkNames := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		pkNames[i] = sqlitebridge.QuoteIdent(pk.Name)
	}
	var sql string
	if len(updates) == 0 {
		// All present columns are PK; UPSERT with no SET clause is malformed.
		sql = fmt.Sprintf(`INSERT OR IGNORE INTO %s (%s) VALUES (%s)`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	} else {
		sql = fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(pkNames, ", "),
			strings.Join(updates, ", "),
		)
	}
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	for slot, ord := range order {
		if err := bindColValue(stmt, slot+1, vals[ord]); err != nil {
			return err
		}
	}
	_, err = stmt.Step()
	return err
}

// applyInsertStmt returns the cached UPSERT for tab, building it on first
// use. v0 has no schema evolution, so a per-table key is sufficient — the
// column shape is fixed.
func (b *Broker) applyInsertStmt(tab *catalog.Table) (*sqlitebridge.Stmt, error) {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	if stmt, ok := b.applyInsertStmts[tab.ID]; ok {
		return stmt, nil
	}
	cols := make([]string, 0, len(tab.Columns))
	placeholders := make([]string, 0, len(tab.Columns))
	updates := make([]string, 0, len(tab.Columns))
	for _, col := range tab.Columns {
		quoted := sqlitebridge.QuoteIdent(col.Name)
		cols = append(cols, quoted)
		placeholders = append(placeholders, "?")
		if col.PKPos == 0 {
			updates = append(updates, fmt.Sprintf("%s = excluded.%s", quoted, quoted))
		}
	}
	pkNames := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		pkNames[i] = sqlitebridge.QuoteIdent(pk.Name)
	}
	var sql string
	if len(updates) == 0 {
		// All columns are PK; UPSERT with no SET clause is malformed.
		// INSERT OR IGNORE preserves idempotency.
		sql = fmt.Sprintf(`INSERT OR IGNORE INTO %s (%s) VALUES (%s)`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
	} else {
		sql = fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s`,
			sqlitebridge.QuoteIdent(tab.Name),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(pkNames, ", "),
			strings.Join(updates, ", "),
		)
	}
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return nil, err
	}
	if b.applyInsertStmts == nil {
		b.applyInsertStmts = map[crdt.TableID]*sqlitebridge.Stmt{}
	}
	b.applyInsertStmts[tab.ID] = stmt
	return stmt, nil
}

// applyUpdate runs the UPDATE DML for r and returns how many rows it
// matched (execRowUpdate's contract: -1 when no DML ran, 0 when the PK
// found no physical row — the cell-group path uses 0 to detect an
// update that outran its row's INSERT).
func (b *Broker) applyUpdate(tab *catalog.Table, r crdt.Update) (matched int64, err error) {
	return b.execRowUpdate(tab, r.PK, r.Changed)
}

func (b *Broker) applyDelete(tab *catalog.Table, r crdt.Delete) error {
	stmt, err := b.applyDeleteStmt(tab)
	if err != nil {
		return err
	}
	if err := stmt.Reset(); err != nil {
		return err
	}
	// PK columns are encoded in t.PK position order; bind to ?1..?N in
	// the same order. RangePK is invoked at most len(t.PK) times.
	pos := 0
	if err := tab.RangePK(r.PK, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

// applyDeleteStmt returns the cached DELETE for tab, built on first use.
func (b *Broker) applyDeleteStmt(tab *catalog.Table) (*sqlitebridge.Stmt, error) {
	b.stmtsMu.Lock()
	defer b.stmtsMu.Unlock()
	if stmt, ok := b.applyDeleteStmts[tab.ID]; ok {
		return stmt, nil
	}
	wheres := make([]string, len(tab.PK))
	for i, pk := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(pk.Name))
	}
	sql := fmt.Sprintf(`DELETE FROM %s WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return nil, err
	}
	if b.applyDeleteStmts == nil {
		b.applyDeleteStmts = map[crdt.TableID]*sqlitebridge.Stmt{}
	}
	b.applyDeleteStmts[tab.ID] = stmt
	return stmt, nil
}

// execBound prepares one-shot SQL, binds vals, steps, and finalizes. Use
// for SQL that varies per call — currently only UPDATE, whose Changed
// shape varies. Stable per-table SQL goes through cached *Stmt.
func execBound(conn *sqlitebridge.Conn, sql string, vals []crdt.ColValue) error {
	stmt, _, err := conn.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	for i, v := range vals {
		if err := bindColValue(stmt, i+1, v); err != nil {
			return err
		}
	}
	if _, err := stmt.Step(); err != nil {
		return err
	}
	return nil
}

// ensureRowPlaceholder issues `INSERT OR IGNORE INTO <table>(<pk_cols>,
// <blob_cols>) VALUES (?,..., x”,...)` so a blob_patch arriving before
// the base row creates the placeholder. Per BLOB_PATCH.md, tables
// receiving blob_patch must allow NULL/DEFAULT on every non-PK
// non-blob column - this is a DDL admission precondition (checked at
// CREATE TABLE time elsewhere).
func (b *Broker) ensureRowPlaceholder(tab *catalog.Table, pk crdt.PKBlob) error {
	blobCols, err := b.blobColumns(tab)
	if err != nil {
		return err
	}
	cols := make([]string, 0, len(tab.PK))
	placeholders := make([]string, 0, len(tab.PK))
	for _, c := range tab.PK {
		cols = append(cols, sqlitebridge.QuoteIdent(c.Name))
		placeholders = append(placeholders, "?")
	}
	for _, c := range tab.Columns {
		if c.PKPos > 0 || !blobCols[c.ID] {
			continue
		}
		cols = append(cols, sqlitebridge.QuoteIdent(c.Name))
		placeholders = append(placeholders, "x''")
	}
	sql := fmt.Sprintf(`INSERT OR IGNORE INTO %s (%s) VALUES (%s)`,
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	pos := 0
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

func (b *Broker) blobColumns(tab *catalog.Table) (map[crdt.ColumnID]bool, error) {
	stmt, _, err := b.cfg.AppApply.Prepare(`SELECT name, type, hidden FROM pragma_table_xinfo(?)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, tab.Name); err != nil {
		return nil, err
	}
	byName := make(map[string]bool)
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		if stmt.ColumnInt64(2) != 0 {
			continue
		}
		byName[stmt.ColumnText(0)] = strings.Contains(strings.ToUpper(stmt.ColumnText(1)), "BLOB")
	}
	out := make(map[crdt.ColumnID]bool, len(tab.Columns))
	for _, c := range tab.Columns {
		if byName[c.Name] {
			out[c.ID] = true
		}
	}
	return out, nil
}

// lookupRowid returns the rowid for the row keyed by pk. ok=false if
// the row does not exist (e.g., placeholder insert failed a constraint).
func (b *Broker) lookupRowid(tab *catalog.Table, pk crdt.PKBlob) (int64, bool, error) {
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	sql := fmt.Sprintf(`SELECT rowid FROM %s WHERE %s`,
		sqlitebridge.QuoteIdent(tab.Name),
		strings.Join(wheres, " AND "),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return 0, false, err
	}
	defer stmt.Finalize()
	pos := 0
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return 0, false, err
	}
	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return 0, false, err
	}
	return stmt.ColumnInt64(0), true, nil
}

// ensureBlobLen extends the (row, column) blob to at least n bytes by
// appending zeroblob. Per BLOB_PATCH.md, the COALESCE form is needed
// because a NULL column would otherwise dodge the WHERE filter and
// blob_open would error.
func (b *Broker) ensureBlobLen(tab *catalog.Table, col catalog.Column, pk crdt.PKBlob, n uint64) error {
	wheres := make([]string, len(tab.PK))
	for i, p := range tab.PK {
		wheres[i] = fmt.Sprintf("%s = ?", sqlitebridge.QuoteIdent(p.Name))
	}
	sql := fmt.Sprintf(`UPDATE %s SET %s = COALESCE(%s, x'') || zeroblob(? - COALESCE(length(%s), 0)) WHERE %s AND COALESCE(length(%s), 0) < ?`,
		sqlitebridge.QuoteIdent(tab.Name),
		sqlitebridge.QuoteIdent(col.Name),
		sqlitebridge.QuoteIdent(col.Name),
		sqlitebridge.QuoteIdent(col.Name),
		strings.Join(wheres, " AND "),
		sqlitebridge.QuoteIdent(col.Name),
	)
	stmt, _, err := b.cfg.AppApply.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, int64(n)); err != nil {
		return err
	}
	pos := 1
	if err := tab.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		pos++
		return bindColValue(stmt, pos, v)
	}); err != nil {
		return err
	}
	pos++
	if err := stmt.BindInt64(pos, int64(n)); err != nil {
		return err
	}
	_, err = stmt.Step()
	return err
}

func bindColValue(s *sqlitebridge.Stmt, i int, v crdt.ColValue) error {
	switch v.TypeTag {
	case crdt.ColNull:
		return s.BindNull(i)
	case crdt.ColInt:
		if len(v.Bytes) != 8 {
			return fmt.Errorf("broker: ColInt bytes len = %d; want 8", len(v.Bytes))
		}
		return s.BindInt64(i, int64(binary.BigEndian.Uint64(v.Bytes)))
	case crdt.ColReal:
		if len(v.Bytes) != 8 {
			return fmt.Errorf("broker: ColReal bytes len = %d; want 8", len(v.Bytes))
		}
		return s.BindFloat64(i, math.Float64frombits(binary.BigEndian.Uint64(v.Bytes)))
	case crdt.ColText:
		return s.BindText(i, string(v.Bytes))
	case crdt.ColBlob:
		return s.BindBlob(i, v.Bytes)
	}
	return fmt.Errorf("broker: unknown ColType %d", v.TypeTag)
}
