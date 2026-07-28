// Package broker bridges the local cache to peers: a Subscribe loop
// applies inbound changesets through the apply path. Locally-produced
// changesets are broadcast directly by the producer's OnEncoded hook
// off the commit-thread latency path.
package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// ErrSchemaUnhealthy means this node can no longer prove that its SQLite
// schema follows the durable schema log. The first terminal sequence and
// reason are persisted before this error is returned; recovery requires a
// fresh clone rather than a local metadata override.
var ErrSchemaUnhealthy = errors.New("sqlite: schema unhealthy; run syzy_clone to repair")

// StartSchemaCatchup spawns ONLY the schema-catch-up loop (plus its
// one-shot failed-local drain) without a transport. Used in
// publisher-only / single-node bucket mode: there are no peers to
// subscribe to, but the durable schema log can still lead the local
// schema_seq — a prior process advanced the head, or this node
// restarted after a schema-log Append committed but before the local
// DDL commit. Replaying the durable log into the catalog/schema_seq is
// what lets the next DDL stop losing the "head moved" CAS. Requires
// SchemaLog + a positive SchemaCatchupInterval. Idempotent with Start
// (both gate on startOnce); calling either twice errors.
func (b *Broker) StartSchemaCatchup(ctx context.Context) error {
	if b.cfg.SchemaLog == nil {
		return errors.New("broker: StartSchemaCatchup without SchemaLog")
	}
	if b.cfg.SchemaCatchupInterval <= 0 {
		return errors.New("broker: StartSchemaCatchup needs SchemaCatchupInterval > 0")
	}
	started := false
	b.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		b.cancel = cancel
		b.wg.Add(1)
		go b.schemaCatchupLoop(runCtx)
		started = true
	})
	if !started {
		return errors.New("broker: already started")
	}
	return nil
}

// RunSchemaCatchupOnce synchronously reads and applies one batch of
// pending schema-log events, exactly like a catch-up-loop tick.
// Exposed for DDL admission's Append-CAS-loss retry, which needs the
// catalog at head now rather than at the next tick. No-op when the
// broker has no SchemaLog. The APPLY of fetched events is serialized
// with the loop and with inbound applies via applyMu; the SchemaLog
// network read runs outside the lock (see runSchemaCatchup), so a
// long in-flight apply pass can still hold the caller past its
// deadline, but a slow log read no longer starves appliers.
func (b *Broker) RunSchemaCatchupOnce(ctx context.Context) error {
	return b.runSchemaCatchup(ctx)
}

// schemaCatchupLoop polls the SchemaLog for events past the
// local meta.schema_seq and applies them. Wakes on a tick or on
// schemaCatchupTrigger (currently unused; reserved for future
// "DML deferred for schema" wake signal). Errors back off and retry.
//
// Before the first poll, drainFailedLocalSchemaEvents walks any rows
// left in apply_state='failed_local'. Receiver-flavored rows (older
// broker binaries that advanced schema_seq on apply failure) heal via
// the precheck: applyCatalogStructural sees SQLite is already in the
// desired shape and no-ops, then the metadata-tx writes the catalog
// rows the old apply skipped. Originator-flavored rows (rare metadata-
// UPSERT divergence in resolveLocalDDL) heal because applyCatalogOpTo-
// Meta's UPSERTs are idempotent.
func (b *Broker) schemaCatchupLoop(ctx context.Context) {
	defer b.wg.Done()
	if err := b.currentSchemaHealthError(); err != nil {
		b.setLastSubscribeError(err)
		return
	}
	if err := b.drainFailedLocalSchemaEvents(); err != nil {
		b.setLastSubscribeError(fmt.Errorf("schema drain: %w", err))
	}
	t := time.NewTicker(b.cfg.SchemaCatchupInterval)
	defer t.Stop()
	// Run once immediately so a freshly-started node catches up
	// without waiting for the first tick.
	if err := b.runSchemaCatchup(ctx); err != nil {
		b.setLastSubscribeError(fmt.Errorf("schema catchup: %w", err))
		if errors.Is(err, ErrSchemaUnhealthy) {
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := b.runSchemaCatchup(ctx); err != nil {
			b.setLastSubscribeError(fmt.Errorf("schema catchup: %w", err))
			if errors.Is(err, ErrSchemaUnhealthy) {
				return
			}
		}
	}
}

// drainFailedLocalSchemaEvents reconciles any syzy_schema_event rows
// that an older broker binary marked apply_state='failed_local'. For
// each such row, the drain re-runs applyCatalogStructural against the
// current SQLite state (no-op when already in shape, thanks to the
// precheck) and runs the metadata-side upserts the original apply
// skipped, then flips the row to 'applied'.
//
// One-shot: runs once at broker start. Under the post-fix invariant
// (failure means schema_seq is NOT advanced and no row is written),
// the receiver-side catchup never produces a fresh failed_local row,
// so this function drains the existing population and afterward has
// nothing to do.
func (b *Broker) drainFailedLocalSchemaEvents() error {
	if b.cfg.Meta == nil || b.cfg.Catalog == nil {
		return nil
	}
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	events, err := b.cfg.Meta.ReadFailedLocalSchemaEvents()
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	// Best-effort per row: a decode/apply failure on one row shouldn't
	// strand the rest. Contrast with runSchemaCatchup which aborts on
	// first error — there ordering matters (events chain via parent_seq)
	// and we want the next tick to re-fetch from the same point.
	var firstErr error
	record := func(stage string, seq uint64, err error) {
		if firstErr == nil {
			firstErr = fmt.Errorf("%s failed_local seq=%d: %w", stage, seq, err)
		}
	}
	reconciled := 0
	for _, e := range events {
		op, err := crdt.DecodeCatalogOp(e.CatalogOp)
		if err != nil {
			record("decode", e.SchemaSeq, err)
			continue
		}
		if err := b.applyCatalogStructural(op); err != nil {
			record("reapply", e.SchemaSeq, err)
			continue
		}
		err = b.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
			if err := tx.ApplyCatalogOp(op, e.SchemaSeq); err != nil {
				return err
			}
			return tx.MarkSchemaEventApplied(e.SchemaSeq)
		})
		if err != nil {
			record("reconcile", e.SchemaSeq, err)
			continue
		}
		reconciled++
	}
	if reconciled > 0 {
		seq, _, err := b.cfg.Meta.GetSchemaSeq()
		if err != nil {
			return err
		}
		if err := b.reloadCatalogAt(seq); err != nil {
			return err
		}
	}
	return firstErr
}

// ReconcileSchemaToSQLite repairs the durable skew where the metadata
// catalog records a DDL as applied (schema_seq advanced, syzy_schema_event
// row = 'applied', catalog carries the column/table) but app.db is missing
// its structural effect. A two-stream restore reconstructs exactly this:
// the metadata stream is shipped to a tip past a DDL while the data stream's
// matching page change is not yet in the data tip (a rebaseline / push-
// ordering gap). The node then serves a schema behind its own catalog:
// runSchemaCatchup skips the event (seq <= localSeq) forever and every
// inbound row-write touching the absent column is dropped, diverging
// silently from the cluster.
//
// Reconciliation walks every applied syzy_schema_event in schema_seq order
// and, for any whose effect is absent from app.db, re-applies it via
// applyCatalogStructural, which renders the exact DDL from the catalog op
// (including NOT NULL ... DEFAULT, required to ADD a NOT NULL column to a
// non-empty table) and is internally idempotent. A healthy node applies
// nothing. Returns the number of events repaired.
//
// Run synchronously on Open before the node starts serving, so a restored
// or re-opened node never serves (or drops inbound writes) against a schema
// behind its catalog. Idempotent: a second call on a healed node is a no-op.
func (b *Broker) ReconcileSchemaToSQLite(ctx context.Context) (int, error) {
	if b.cfg.Meta == nil || b.cfg.Catalog == nil || b.cfg.AppApply == nil {
		return 0, nil
	}
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	events, err := b.cfg.Meta.ReadAppliedSchemaEvents()
	if err != nil {
		return 0, err
	}
	repaired, skipped := 0, 0
	decoded := make([]reconcileSchemaEvent, 0, len(events))
	for _, e := range events {
		select {
		case <-ctx.Done():
			return repaired, ctx.Err()
		default:
		}
		// Best-effort per event: an op that can't be decoded, prechecked, or
		// re-applied is logged and skipped, never aborting the pass. Ops
		// predating a catalog-op wire-format change no longer decode, but the
		// tables/columns they created already exist in app.db, so skipping them
		// is safe; a later event that DOES decode (the recent ADD COLUMN the
		// node is actually missing) still gets healed. A reconcile must
		// never wedge the whole pass on one stale op, nor block the node from
		// serving (see the Open call site).
		op, err := crdt.DecodeCatalogOp(e.CatalogOp)
		if err != nil {
			skipped++
			b.log.Debug("broker: reconcile skipping undecodable op",
				"schema_seq", e.SchemaSeq, "err", err)
			continue
		}
		decoded = append(decoded, reconcileSchemaEvent{schemaSeq: e.SchemaSeq, op: op})
	}
	for _, e := range terminalNamedDDLEvents(decoded) {
		select {
		case <-ctx.Done():
			return repaired, ctx.Err()
		default:
		}
		op := e.op
		missing, err := structuralEffectMissing(op, b.cfg.AppApply, b.cfg.Catalog)
		if err != nil {
			skipped++
			b.log.Warn("broker: reconcile skipping op; precheck failed",
				"schema_seq", e.schemaSeq, "op", op.Kind.String(), "err", err)
			continue
		}
		if !missing {
			continue
		}
		if err := b.applyCatalogStructural(op); err != nil {
			skipped++
			// A create-DDL op that fails because its target column/table is gone
			// is a SUPERSEDED op: a later migration dropped the dependency, so the
			// op is obsolete and unappliable by design (e.g. a CREATE INDEX whose
			// column a later DROP COLUMN removed). The pass already skips it; logging
			// it at ERROR is a false alarm that, fleet-wide and per reconcile, buries
			// real failures. Demote those to Info; keep everything else (lock, disk,
			// genuine unhealed skew) loud.
			if isSupersededDDLErr(err) {
				b.log.Info("broker: reconcile skipping superseded DDL op (dependency dropped)",
					"schema_seq", e.schemaSeq, "op", op.Kind.String(), "err", err)
			} else {
				b.log.Error("broker: reconcile skipping op; re-apply failed",
					"schema_seq", e.schemaSeq, "op", op.Kind.String(), "err", err)
			}
			continue
		}
		// Reload between repairs so a later op in this pass can resolve a
		// table/column id an earlier repair re-established.
		if err := b.cfg.Catalog.Reload(); err != nil {
			skipped++
			b.log.Error("broker: reconcile catalog reload failed after repair",
				"schema_seq", e.schemaSeq, "err", err)
			continue
		}
		repaired++
		b.log.Warn("broker: reconciled schema skew; re-applied DDL absent from app.db",
			"schema_seq", e.schemaSeq, "op", op.Kind.String())
	}
	if skipped > 0 {
		b.log.Info("broker: reconcile skipped events it could not decode or apply",
			"skipped", skipped, "repaired", repaired)
	}
	if repaired > 0 {
		seq, _, err := b.cfg.Meta.GetSchemaSeq()
		if err != nil {
			return repaired, err
		}
		if err := b.reloadCatalogAt(seq); err != nil {
			return repaired, err
		}
	}
	return repaired, nil
}

type reconcileSchemaEvent struct {
	schemaSeq uint64
	op        crdt.CatalogOp
}

type namedDDLKey struct {
	objectType string
	name       string
}

// terminalNamedDDLEvents removes historical named-object operations that a
// later event superseded. Unlike tables and columns, opaque indexes, views,
// virtual tables, and triggers are absent from the typed catalog, so their
// final state must be derived from the applied event sequence itself.
//
// Keeping only the last operation for each object also preserves the last
// CREATE's RawSQL when a name was dropped and reused. Otherwise a reconcile
// could replay an old CREATE, replay the later DROP, and repeat that cycle on
// every open while leaving the final schema unchanged.
func terminalNamedDDLEvents(events []reconcileSchemaEvent) []reconcileSchemaEvent {
	last := make(map[namedDDLKey]int)
	ordinal := 0
	for _, e := range events {
		walkNamedDDLOps(e.op, func(key namedDDLKey) {
			last[key] = ordinal
			ordinal++
		})
	}

	out := make([]reconcileSchemaEvent, 0, len(events))
	ordinal = 0
	for _, e := range events {
		op, keep := filterTerminalNamedDDL(e.op, last, &ordinal)
		if keep {
			e.op = op
			out = append(out, e)
		}
	}
	return out
}

func walkNamedDDLOps(op crdt.CatalogOp, visit func(namedDDLKey)) {
	if op.Kind == crdt.OpBundle {
		for _, sub := range op.SubOps {
			walkNamedDDLOps(sub, visit)
		}
		return
	}
	if key, ok := namedDDLOpKey(op); ok {
		visit(key)
	}
}

func filterTerminalNamedDDL(op crdt.CatalogOp, last map[namedDDLKey]int, ordinal *int) (crdt.CatalogOp, bool) {
	if op.Kind == crdt.OpBundle {
		subOps := make([]crdt.CatalogOp, 0, len(op.SubOps))
		for _, sub := range op.SubOps {
			filtered, keep := filterTerminalNamedDDL(sub, last, ordinal)
			if keep {
				subOps = append(subOps, filtered)
			}
		}
		op.SubOps = subOps
		return op, len(subOps) > 0
	}
	key, named := namedDDLOpKey(op)
	if !named {
		return op, true
	}
	current := *ordinal
	*ordinal++
	return op, current == last[key]
}

func namedDDLOpKey(op crdt.CatalogOp) (namedDDLKey, bool) {
	var objectType string
	switch op.Kind {
	case crdt.OpCreateIndex, crdt.OpDropIndex:
		objectType = "index"
	case crdt.OpCreateView, crdt.OpDropView:
		objectType = "view"
	case crdt.OpCreateVirtualTable, crdt.OpDropVirtualTable:
		objectType = "table"
	case crdt.OpCreateTrigger, crdt.OpDropTrigger:
		objectType = "trigger"
	default:
		return namedDDLKey{}, false
	}
	return namedDDLKey{objectType: objectType, name: op.ObjectName}, true
}

// runSchemaCatchup reads events past meta.schema_seq and applies them.
// One tick processes up to schemaCatchupBatch events; the next tick
// continues from the new schema_seq.
//
// Lock scope: the SchemaLog.Read happens OUTSIDE applyMu — an
// object-store-backed log's Read is network I/O with retries/backoff
// (up to minutes), and holding applyMu across it would starve the
// apply path on every tick. applyMu still covers the APPLY of the
// fetched events, which is the catchup-vs-apply mutual exclusion
// RunSchemaCatchupOnce callers rely on. The pre-fetch localSeq is
// re-checked under the lock; if another authority (wal_hook, another
// tick) advanced it during the fetch, the batch is dropped and
// re-fetched. The bounded retry then falls through to the apply pass,
// whose existing per-event schema_seq re-check keeps a stale batch
// safe (already-applied events are skipped).
func (b *Broker) runSchemaCatchup(ctx context.Context) error {
	if b.cfg.SchemaLog == nil || b.cfg.Meta == nil || b.cfg.Catalog == nil {
		return nil
	}
	if err := b.currentSchemaHealthError(); err != nil {
		return err
	}
	const batch = 64
	for attempt := 0; ; attempt++ {
		localSeq, _, err := b.cfg.Meta.GetSchemaSeq()
		if err != nil {
			return err
		}
		events, err := b.cfg.SchemaLog.Read(ctx, localSeq, batch)
		if err != nil {
			if errors.Is(err, schemalog.ErrBelowHorizon) {
				return b.markSchemaUnhealthy(localSeq+1,
					fmt.Errorf("schema log retention horizon passed local sequence %d: %w", localSeq, err))
			}
			return err
		}
		b.applyMu.Lock()
		if err := b.currentSchemaHealthError(); err != nil {
			b.applyMu.Unlock()
			return err
		}
		freshSeq, _, err := b.cfg.Meta.GetSchemaSeq()
		if err != nil {
			b.applyMu.Unlock()
			return err
		}
		if freshSeq != localSeq && attempt < 3 {
			b.applyMu.Unlock()
			continue // schema_seq moved during the fetch; re-fetch
		}
		err = b.applySchemaEventsLocked(freshSeq, events)
		b.applyMu.Unlock()
		return err
	}
}

// applySchemaEventsLocked applies one fetched batch of schema-log
// events. localSeq is the schema_seq read under the lock (>= the seq
// the batch was fetched at). Caller MUST hold applyMu.
func (b *Broker) applySchemaEventsLocked(localSeq uint64, events []schemalog.Event) error {
	if len(events) == 0 {
		// No new schema_log events to apply, but the originator's
		// extension may have advanced metadata.schema_seq via wal_hook
		// in a different process — in which case our in-memory catalog
		// is stale. Reload if the tracked catalog seq lags the metadata.
		return b.maybeReloadCatalog(localSeq)
	}
	startSeq := localSeq
	// If a FRESH LocalDDL intent is pending at one of these seqs, the
	// originator's wal_hook is finalizing it. Yield to wal_hook —
	// resolving from this side too would duplicate syzy_schema_event
	// rows and double-execute the SQLite DDL. Per sqlite/docs/DDL.md catch-up.
	// A STALE intent means the originator died mid-DDL (crashed guest,
	// killed process); yielding to a corpse would wedge the schema
	// pipeline forever, so catch-up becomes the recovery authority:
	// apply the event from the log and clear the dead slot.
	intents, err := pendingLocalDDLIntents(b.cfg.Meta)
	if err != nil {
		return err
	}
	for _, e := range events {
		if e.SchemaSeq <= localSeq {
			continue
		}
		if e.SchemaSeq != localSeq+1 || e.ParentSeq != localSeq {
			return b.markSchemaUnhealthy(localSeq+1, fmt.Errorf(
				"non-contiguous schema event: got seq=%d parent=%d after seq=%d",
				e.SchemaSeq, e.ParentSeq, localSeq))
		}
		staleOwners, fresh := ddlIntentsAt(intents, e.SchemaSeq, time.Now().UnixMicro())
		if fresh {
			return nil
		}
		// Re-read schema_seq before applying: the originator's wal_hook
		// (or another tick of this loop) may have advanced past this
		// event between our schema-log Read and now. Apply only when
		// we'd genuinely advance.
		freshSeq, _, err := b.cfg.Meta.GetSchemaSeq()
		if err != nil {
			return err
		}
		if freshSeq >= e.SchemaSeq {
			localSeq = freshSeq
			continue
		}
		op, err := crdt.DecodeCatalogOp(e.CatalogOp)
		if err != nil {
			return b.markSchemaUnhealthy(e.SchemaSeq,
				fmt.Errorf("decode op seq=%d: %w", e.SchemaSeq, err))
		}
		// Atomic apply: SQLite-side structural change must land before
		// any metadata-side write. A transient failure (BUSY, locked
		// WAL, missing parent, etc.) bubbles up so the next catchup tick
		// re-fetches and retries — there is no intermediate "applied in
		// metadata but not SQLite" state to recover from. The precheck
		// inside applyCatalogStructural makes the retry safe even after
		// a crash between SQLite commit and the metadata tx below.
		if err := b.applyCatalogStructural(op); err != nil {
			if isTerminalSchemaApplyError(err) {
				return b.markSchemaUnhealthy(e.SchemaSeq,
					fmt.Errorf("schema catchup apply seq=%d: %w", e.SchemaSeq, err))
			}
			return fmt.Errorf("schema catchup apply seq=%d: %w", e.SchemaSeq, err)
		}
		err = b.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
			if err := tx.ApplyCatalogOp(op, e.SchemaSeq); err != nil {
				return err
			}
			if err := tx.AppendSchemaEvent(metadata.SchemaEventEntry{
				SchemaSeq: e.SchemaSeq, ParentSeq: e.ParentSeq,
				CatalogOp: e.CatalogOp, RawSQL: e.RawSQL,
				AppliedAtUs: time.Now().UnixMicro(),
				ApplyState:  metadata.ApplyStateApplied,
			}); err != nil {
				return err
			}
			// The event this stale intent pre-reserved is now applied;
			// the dead originator's slot is moot. Clearing it here (in
			// the same tx) un-wedges its origin permanently. The owner
			// restarting later is safe: resolveLocalDDL's
			// already-applied fast path just clears and reloads.
			for _, o := range staleOwners {
				if err := tx.ClearOriginIntent(o); err != nil {
					return err
				}
			}
			return tx.SetMeta("schema_seq", packU64(e.SchemaSeq))
		})
		if err != nil {
			return fmt.Errorf("apply seq=%d: %w", e.SchemaSeq, err)
		}
		localSeq = e.SchemaSeq
		// Reload the in-memory catalog between events in the same batch.
		// Subsequent ops in this batch may reference table/column IDs
		// established by earlier ops — for example ALTER TABLE ADD COLUMN
		// uses the CREATE TABLE's TableID to resolve the table name in
		// catalogOpToSQL. Without a refresh, the second op fails with
		// "table id ... not in local catalog" and the whole batch
		// retries forever, never making progress.
		//
		// Catalog.Reload reads the metadata atomically and is idempotent;
		// the PKDefault refresh (which mutates Column structs in place
		// and is racy with apply readers) stays in reloadCatalogAt
		// below, called once per tick after the batch.
		if err := b.cfg.Catalog.Reload(); err != nil {
			return fmt.Errorf("catalog reload after seq=%d: %w", e.SchemaSeq, err)
		}
	}
	// Final reload picks up RefreshPKDefaults (column-pointer mutation)
	// at one point per tick — the per-event reloads above only refresh
	// the metadata-derived maps, not PKDefault.
	if localSeq > startSeq {
		return b.reloadCatalogAt(localSeq)
	}
	return nil
}

func (b *Broker) currentSchemaHealthError() error {
	if b.cfg.Meta == nil {
		return nil
	}
	health, unhealthy, err := b.cfg.Meta.GetSchemaHealth()
	if err != nil {
		return fmt.Errorf("read schema health: %w", err)
	}
	if !unhealthy {
		return nil
	}
	return fmt.Errorf("%w: seq=%d: %s", ErrSchemaUnhealthy, health.Seq, health.Reason)
}

func (b *Broker) markSchemaUnhealthy(seq uint64, cause error) error {
	reason := "terminal schema catch-up failure"
	if cause != nil {
		reason = strings.ToValidUTF8(cause.Error(), "�")
		if reason == "" {
			reason = "terminal schema catch-up failure"
		}
	}
	health, err := b.cfg.Meta.MarkSchemaUnhealthy(seq, reason)
	if err != nil {
		// Do not halt merely in memory: retry until the fail-closed marker is
		// durable, otherwise a process crash could restart an unsafe node.
		return fmt.Errorf("persist schema-unhealthy marker at seq=%d: %w", seq, err)
	}
	return fmt.Errorf("%w: seq=%d: %s", ErrSchemaUnhealthy, health.Seq, health.Reason)
}

// isTerminalSchemaApplyError separates deterministic structural rejection
// from conditions that can clear without changing the schema event. SQLite's
// generic SQL error, constraint, and misuse classes describe a statement the
// node cannot apply; lock, interrupt, I/O, and resource result classes retry.
func isTerminalSchemaApplyError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var sqliteErr sqlitebridge.Error
	if !errors.As(err, &sqliteErr) {
		// Catalog resolution/rendering errors are ordinary Go errors and are
		// deterministic for this catalog + event pair.
		return true
	}
	switch sqliteErr.Code {
	case sqlitebridge.ResultError, sqlitebridge.ResultConstraint, sqlitebridge.ResultMisuse:
		return true
	default:
		return false
	}
}

// reloadCatalogAt rebuilds the in-memory catalog via Catalog.RebuildWith-
// PKDefaults and records wantSeq as the catalog's most-recent-reloaded
// schema_seq. The seq bookkeeping is the broker-specific add-on: it lets
// maybeReloadCatalog short-circuit when another tick (or another writer's
// wal_hook) has already advanced past wantSeq.
func (b *Broker) reloadCatalogAt(wantSeq uint64) error {
	if b.cfg.Catalog == nil {
		return nil
	}
	if err := b.cfg.Catalog.RebuildWithPKDefaults(b.cfg.AppApply); err != nil {
		return err
	}
	// The schema changed: cached per-table DML statements were built
	// against the old column shape (an ADD COLUMN leaves a cached
	// INSERT one placeholder short — SQLITE_RANGE on every apply until
	// restart). Callers hold applyMu or run at startup, so no apply is
	// mid-statement.
	b.finalizeCachedStmts()
	b.catalogReloadSeq.Store(wantSeq)
	return nil
}

// maybeReloadCatalog rebuilds the in-memory catalog from the metadata
// when the broker's last reload lags wantSeq. Idempotent at the catalog
// layer (Reload reads metadata tables, snapshot is value-equal on no-op
// reloads), so a periodic call is safe but we still gate on a tracked
// seq to avoid the LoadCatalogSnapshot tx on every tick.
//
// Rebuild PK defaults too. The metadata sequence may have advanced through
// the originator's wal_hook, outside the broker; a metadata-only Reload here
// would erase the runtime gen_id annotation that path just restored. Catalog
// guards the rebuilt maps and default refresh with its own locks.
func (b *Broker) maybeReloadCatalog(wantSeq uint64) error {
	if b.cfg.Catalog == nil {
		return nil
	}
	if wantSeq <= b.catalogReloadSeq.Load() {
		return nil
	}
	if err := b.cfg.Catalog.RebuildWithPKDefaults(b.cfg.AppApply); err != nil {
		return fmt.Errorf("catalog reload: %w", err)
	}
	// Same invalidation as reloadCatalogAt: the shape the cached DML
	// statements were built for is gone. Caller holds applyMu.
	b.finalizeCachedStmts()
	b.catalogReloadSeq.Store(wantSeq)
	return nil
}

// isSupersededDDLErr reports whether err is a SQLite "no such column/table"
// failure — the signature of a create-DDL op whose dependency a later migration
// dropped, so the op is obsolete and its reconcile skip is benign. SQLite's
// messages for these are canonical and stable ("no such column: X" / "no such
// table: X"). Any other failure (BUSY, disk, a genuinely unhealed column the
// node still needs) is NOT matched and stays at ERROR.
func isSupersededDDLErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "no such column") || strings.Contains(s, "no such table")
}

// applyCatalogStructural runs the SQLite-side schema change required to
// satisfy op against AppApply. The trace_v2 originator path skips this
// (the user statement already executed); inbound apply needs to translate
// the typed CatalogOp back into SQL.
//
// Idempotent: every op is prechecked against sqlite_master /
// pragma_table_info before issuing the DDL, so a retry after a partial
// previous attempt (SQLite committed but the metadata tx failed, or the
// process crashed before it) skips the no-op DDL instead of erroring with
// "duplicate column" / "table already exists". That is what makes the
// runSchemaCatchup-on-failure-don't-advance approach safe under crash.
//
// A bundle carries one transaction's DDL, wrapped in a SAVEPOINT so a
// mid-bundle failure rolls back partial structural state before the
// schema_seq advance. Its sub-ops are a SEQUENCE, which needs two views
// of the schema:
//
//   - render: the schema as of this point in the sequence, so a sub-op can
//     name a table an earlier sub-op created or renamed.
//   - precheck: the schema the WHOLE bundle leaves behind, so idempotency
//     is judged by where an object ends up. A replay sees only the final
//     state; judging `CREATE TABLE t` by the literal name t would call it
//     missing and resurrect an intermediate a later sub-op renamed or
//     dropped away.
func (b *Broker) applyCatalogStructural(op crdt.CatalogOp) error {
	if op.Kind != crdt.OpBundle {
		return b.applyCatalogOpToSQLite(op, b.cfg.Catalog, b.cfg.Catalog)
	}
	if err := b.cfg.AppApply.Exec("SAVEPOINT _syzy_bundle"); err != nil {
		return err
	}
	final := bundleFinalState(b.cfg.Catalog, op.SubOps)
	sofar := catalog.NewOverlay(b.cfg.Catalog)
	for _, sub := range op.SubOps {
		err := b.applyCatalogOpToSQLite(sub, final, sofar)
		if err == nil {
			err = sofar.Apply(sub)
		}
		if err != nil {
			_ = b.cfg.AppApply.Exec("ROLLBACK TO _syzy_bundle; RELEASE _syzy_bundle")
			return err
		}
	}
	return b.cfg.AppApply.Exec("RELEASE _syzy_bundle")
}

// applyCatalogOpToSQLite applies one non-bundle op, resolving its
// idempotency precheck through precheck and its rendered SQL through
// render. Outside a bundle both are the committed catalog.
func (b *Broker) applyCatalogOpToSQLite(op crdt.CatalogOp, precheck, render catalog.TableResolver) error {
	applied, err := opAlreadyAppliedInSQLite(op, b.cfg.AppApply, precheck)
	if err != nil {
		return fmt.Errorf("precheck %v: %w", op.Kind, err)
	}
	if applied {
		return nil
	}
	sql, err := catalogOpToSQL(op, render)
	if err != nil {
		return err
	}
	if sql == "" {
		return nil
	}
	return b.cfg.AppApply.Exec(sql)
}

// bundleFinalState folds every sub-op into one overlay: the schema the
// bundle leaves behind. A sub-op that does not fold (a malformed bundle)
// would leave the overlay describing a schema that never existed, so the
// committed catalog is returned instead — the precheck then degrades to
// the per-sub-op behavior rather than failing the apply.
func bundleFinalState(base *catalog.Catalog, subs []crdt.CatalogOp) catalog.TableResolver {
	o := catalog.NewOverlay(base)
	for _, sub := range subs {
		if err := o.Apply(sub); err != nil {
			return base
		}
	}
	return o
}

// ddlIntentFreshUs is the window within which a pending LocalDDL
// intent marks a live originator finalizing its DDL. Past it the
// originator is presumed dead and catch-up recovers the slot —
// otherwise a producer that died between Append and wal_hook
// resolution would wedge the schema pipeline forever.
//
// StartedAtUs is stamped at statement START, and a long-running DDL
// body (big CREATE INDEX, table-rewriting ALTER) can exceed any fixed
// window while alive. That mid-execution span is protected anyway:
// the writer holds the SQLite write lock, so a takeover's structural
// apply fails BUSY and the tick retries (the intent clear sits in the
// same tx and never lands). The window therefore only needs to cover
// the post-commit gap before wal_hook resolution (normally
// milliseconds) with margin for paused VMs and producer/broker clock
// skew. An alive originator whose slot is nonetheless taken stays
// consistent: resolveLocalDDL re-checks schema_seq under its tx and
// just clears its own slot.
const ddlIntentFreshUs = 60 * 1000 * 1000

// pendingIntent is one origin's decoded LocalDDL intent slot.
type pendingIntent struct {
	origin    crdt.Origin
	schemaSeq uint64
	startedUs int64
}

// pendingLocalDDLIntents returns every origin's pending LocalDDL
// intent. Non-DDL kinds are skipped; an undecodable foreign slot is
// skipped rather than wedging catch-up for everyone.
func pendingLocalDDLIntents(sc *metadata.Store) ([]pendingIntent, error) {
	rows, err := sc.ListIntents()
	if err != nil {
		return nil, err
	}
	var out []pendingIntent
	for _, r := range rows {
		if metadata.IntentKindOf(r.Buf) != metadata.IntentLocalDDL {
			continue
		}
		intent, err := metadata.DecodeLocalDDL(r.Buf)
		if err != nil {
			continue
		}
		out = append(out, pendingIntent{
			origin: r.Origin, schemaSeq: intent.SchemaSeq,
			startedUs: intent.StartedAtUs,
		})
	}
	return out, nil
}

// ddlIntentsAt partitions the intents pending at seq: fresh reports
// whether any live originator is finalizing this event (caller must
// yield); stale collects the origins whose dead slots the caller
// should clear once it has applied the event itself.
func ddlIntentsAt(intents []pendingIntent, seq uint64, nowUs int64) (stale []crdt.Origin, fresh bool) {
	for _, pi := range intents {
		if pi.schemaSeq != seq {
			continue
		}
		if nowUs-pi.startedUs < ddlIntentFreshUs {
			fresh = true
		} else {
			stale = append(stale, pi.origin)
		}
	}
	return stale, fresh
}
