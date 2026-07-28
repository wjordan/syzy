// DDL admission for the producer. The trace_v2 hook fires before
// SQLite executes a statement; this file contains the classification +
// schemalog.Append + intent-write logic that runs inside it.
//
// On the happy path: classifyDDL → buildCatalogOp → schemalog.Append →
// metadata.SetIntent, all before SQLite touches the schema. The
// statement runs, app commits, and wal_hook resolves the intent (apply
// catalog op, advance schema_seq, clear intent).
//
// On any failure: set the trace-side reject flag and return SQLITE_INTERRUPT
// from the trace hook so SQLite never executes the body.

package producer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	corecatalog "github.com/wjordan/syzy/catalog"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// ddlAdmission owns the trace_v2 admission state for one Producer.
// Construct via newDDLAdmission; trace_v2 calls handleStmt with the
// SQL text.
type ddlAdmission struct {
	app *sqlitebridge.Conn
	// helper is a separate connection on the same app DB used to
	// execute synthesized cascade-trigger DDL. Running these via the
	// writer connection from inside trace_v2 doesn't persist (SQLite's
	// statement execution context isn't safe for nested DDL). The
	// helper is a vanilla connection without producer hooks installed,
	// so its CREATE TRIGGER calls go through SQLite's normal path.
	// May be nil; in that case cascade FKs degrade to local-only on
	// the originator (receivers still install triggers via the broker
	// apply path).
	helper *sqlitebridge.Conn
	sc     *metadata.Store
	cat    *catalog.Catalog
	log    schemalog.Log
	// origin scopes this producer's intent slot in the shared
	// metadata; admission must never touch another origin's slot.
	origin crdt.Origin

	nowMicros func() int64

	// replicateUnderscore mirrors Config.ReplicateUnderscoreTables.
	// When true, underscore-prefixed names are admitted to the
	// schema-log/replication path; when false (the default) they
	// take the local-only carve-out. sqlite_* is excluded regardless.
	replicateUnderscore bool

	// coordinatedUnique mirrors Config.CoordinatedUnique: a reservation
	// backend is available, so NOT NULL UNIQUE (coordinated, CP) keys are
	// admitted. When false (the default), such keys are rejected at
	// admission — there is no way to enforce by-construction uniqueness
	// without a reservation backend. See sqlite/docs/DDL.md#unique-keys.
	coordinatedUnique bool

	// rejected is set by handleStmt when admission rejects a statement.
	// commit_hook (already wired) consults this and returns nonzero so
	// the txn rolls back. trace_v2 also calls Conn.Interrupt to abort
	// SQLite's compiler before it executes the body.
	rejected atomic.Bool

	// Explicit-transaction DDL state. Serialized by the writer-conn
	// single-writer contract like the rest of admission. txnDDL holds
	// the one DDL admitted into the open explicit transaction: its
	// catalog op is built (and validated) at trace time, but the
	// schema-log Append is deferred to commit_hook — so an explicit
	// ROLLBACK replicates nothing, and an Append failure aborts the
	// COMMIT instead of interrupting a statement. savepointSeen marks
	// that a SAVEPOINT opened (or appeared inside) the current
	// transaction scope; DDL under savepoints is rejected because
	// ROLLBACK TO can partially undo it in ways the schema chain
	// cannot model.
	txnDDL        *txnPendingDDL
	savepointSeen bool

	// savepointRolledBack marks that a `ROLLBACK TO <savepoint>` ran in
	// the current transaction. SQLite reported the undone row changes
	// through the preupdate hook and does not un-report them, so the
	// touch journal's last image for a row can be a value the commit
	// never lands. Coordinated claims are derived from that journal, so
	// the commit is refused rather than reserved against a phantom
	// (producer.rejectCoordinatedTxnDML).
	savepointRolledBack bool

	// overlay resolves names against the transaction's own not-yet-
	// committed DDL. The catalog only learns about admitted ops when the
	// transaction commits and the intent resolves, so without this the
	// second statement of `CREATE TABLE t; CREATE INDEX ON t(v)` could
	// not find t. nil outside a transaction that has admitted DDL.
	overlay *catalog.Overlay

	// introspecting is raised while admission queries app.db about its
	// own schema. Those queries run on the writer connection and SQLite
	// traces them like any other statement, re-entering handleStmt; the
	// guard makes that re-entry a no-op. Without it, a query issued
	// before classification recurses until the stack is gone.
	introspecting bool

	// schemaCatchup, when set, synchronously applies pending schema-log
	// events (full node: the broker's catch-up). nil in producer-only
	// deployments, where an external authority (the daemon or a host-
	// side node sharing this metadata) advances meta.schema_seq and
	// admission polls for it. catchupWait bounds the polling wait;
	// the hook itself may extend it while an in-flight apply pass
	// holds the broker's apply lock.
	schemaCatchup func(context.Context) error
	catchupWait   time.Duration
}

// defaultSchemaCatchupWait bounds how long a CAS-losing autocommit DDL
// waits for schema catch-up before rejecting. External authorities
// poll the schema log on an interval (500ms in the full node), so the
// bound must cover several ticks without stalling the app's statement
// unduly on a log that nothing is catching up.
// schemaCatchupPoll paces the wait loop. Each iteration may re-run the
// broker catch-up pass (a schema-log Read, possibly remote), so the
// interval trades retry latency against log round-trips while yielding
// to a peer's pending intent. The happy retry path never sleeps.
const (
	defaultSchemaCatchupWait = 2 * time.Second
	schemaCatchupPoll        = 25 * time.Millisecond
)

// txnPendingDDL accumulates the DDL admitted in one transaction,
// awaiting the commit-time Append that publishes the whole set as a
// single schema event. Ops are fully validated at trace time; only the
// encoding is deferred.
type txnPendingDDL struct {
	ops []pendingDDLOp
	// parentSeq is pinned at the first admitted DDL: every op in the
	// transaction is built against that catalog position, and the
	// commit-time CAS validates the whole set against it.
	parentSeq uint64
}

// pendingDDLOp is one admitted statement awaiting commit.
type pendingDDLOp struct {
	op     crdt.CatalogOp
	rawSQL string
	check  structuralCheck
	// verified is set once check has been evaluated against app.db. The
	// evaluation happens at the next statement's trace_v2 — see
	// verifyPendingDDL.
	verified bool
}

// payload renders the pending ops as one schema-log event: a lone op
// verbatim, several wrapped in an OpBundle that receivers apply
// atomically.
func (p *txnPendingDDL) payload() (crdt.CatalogOp, string) {
	raw := make([]string, 0, len(p.ops))
	subs := make([]crdt.CatalogOp, 0, len(p.ops))
	for _, po := range p.ops {
		raw = append(raw, po.rawSQL)
		subs = append(subs, po.op)
	}
	sql := strings.Join(raw, ";\n")
	if len(subs) == 1 {
		return subs[0], sql
	}
	return crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: subs}, sql
}

// structuralCheck describes the SQLite-side post-state a DDL statement
// must have produced for its schema event to be truthful. trace_v2 fires
// BEFORE the body runs, so admission builds its op against a post-state
// that has not happened yet; verifyPendingDDL re-checks it against
// app.db once the body has actually executed.
//
// A zero structuralCheck (kind == "") checks nothing.
type structuralCheck struct {
	// kind is the sqlite_master type: "table", "index", "view" or
	// "trigger". name is the object; when column is non-empty the check
	// targets that column of the table instead of the object itself.
	kind   string
	name   string
	column string
	// present is the post-state the statement claims: true for creates,
	// false for drops.
	present bool
}

// structuralCheckFor derives the post-state check from the parsed
// statement rather than the typed op, because the two can disagree: a
// CREATE UNIQUE INDEX types as OpAddUniqueKey (pure syzy_key metadata on
// receivers) while its local SQLite artifact is an ordinary index.
func structuralCheckFor(p parsedDDL) structuralCheck {
	switch p.Kind {
	case ddlCreateTable:
		return structuralCheck{kind: "table", name: p.Name, present: true}
	case ddlDropTable:
		return structuralCheck{kind: "table", name: p.Name}
	case ddlCreateIndex, ddlCreateUniqueIndex:
		return structuralCheck{kind: "index", name: p.Name, present: true}
	case ddlDropIndex:
		return structuralCheck{kind: "index", name: p.Name}
	case ddlCreateView:
		return structuralCheck{kind: "view", name: p.Name, present: true}
	case ddlDropView:
		return structuralCheck{kind: "view", name: p.Name}
	case ddlCreateVirtualTable:
		return structuralCheck{kind: "table", name: p.Name, present: true}
	case ddlDropVirtualTable:
		return structuralCheck{kind: "table", name: p.Name}
	case ddlCreateTrigger:
		return structuralCheck{kind: "trigger", name: p.Name, present: true}
	case ddlDropTrigger:
		return structuralCheck{kind: "trigger", name: p.Name}
	case ddlAlterTableAddColumn:
		if len(p.Columns) != 1 {
			return structuralCheck{}
		}
		return structuralCheck{kind: "table", name: p.Name, column: p.Columns[0].Name, present: true}
	case ddlAlterTableDropColumn:
		return structuralCheck{kind: "table", name: p.Name, column: p.DropColumn}
	case ddlAlterTableRenameTo:
		return structuralCheck{kind: "table", name: p.NewName, present: true}
	case ddlAlterTableRenameColumn:
		return structuralCheck{kind: "table", name: p.Name, column: p.NewColumn, present: true}
	}
	return structuralCheck{}
}

// landed reports whether app.db now shows the post-state the statement
// claimed. A zero check always reports true.
func (c structuralCheck) landed(app *sqlitebridge.Conn) (bool, error) {
	if c.kind == "" {
		return true, nil
	}
	var got bool
	var err error
	if c.column != "" {
		got, err = sqlitebridge.ColumnExists(app, c.name, c.column)
	} else {
		got, err = sqlitebridge.ObjectExists(app, c.kind, c.name)
	}
	if err != nil {
		return false, err
	}
	return got == c.present, nil
}

// ddlBodyFallible reports whether SQLite can reject this DDL form AFTER
// prepare succeeds — i.e. after trace_v2 has already admitted it. Only
// forms whose validity depends on table *data* can: everything else is
// settled by the compiler and never reaches the trace hook when it
// fails.
//
//   - CREATE UNIQUE INDEX scans existing rows and fails on duplicates.
//   - DROP TABLE fails on a foreign-key violation from dependent rows
//     when PRAGMA foreign_keys is ON.
//
// These append at COMMIT instead of at trace time, so SQLite's own
// execution is what decides whether the event is published. The cost is
// that a schema-log CAS loss surfaces as a failed statement rather than
// a transparent catch-up + retry — the same trade the explicit-
// transaction path already makes.
func ddlBodyFallible(k ddlKind) bool {
	return k == ddlCreateUniqueIndex || k == ddlDropTable
}

func newDDLAdmission(app *sqlitebridge.Conn, sc *metadata.Store, cat *catalog.Catalog,
	log schemalog.Log, helper *sqlitebridge.Conn, origin crdt.Origin,
	nowMicros func() int64, replicateUnderscore, coordinatedUnique bool) *ddlAdmission {
	return &ddlAdmission{
		app: app, helper: helper, sc: sc, cat: cat, log: log, origin: origin,
		nowMicros: nowMicros, replicateUnderscore: replicateUnderscore,
		coordinatedUnique: coordinatedUnique,
		catchupWait:       defaultSchemaCatchupWait,
	}
}

// rejectedAndClear reports whether a previous handleStmt rejected the
// txn, and resets the flag. Called by commit_hook (and after rollback).
func (d *ddlAdmission) rejectedAndClear() bool { return d.rejected.Swap(false) }

// handleStmt is the trace_v2 callback for SQLITE_TRACE_STMT. It
// classifies the SQL, and on a replicated DDL form runs the full
// admission pipeline. Returns nonzero to abort the statement (SQLite
// stops execution before the body runs).
//
// Non-DDL statements (DML, SELECT, etc.) and local-only DDL forms
// (temp tables, _* objects, etc.) return 0 and let SQLite proceed
// without schema-log involvement.
func (d *ddlAdmission) handleStmt(sql string) int {
	if d.introspecting {
		// Admission's own schema queries land here too. They are never
		// replicated DDL, and re-entering admission from inside it would
		// recurse without bound.
		return 0
	}
	// Settle the previous statement's admission before looking at this
	// one: trace_v2 fires after that statement finished, so app.db now
	// shows whether its body actually took effect.
	d.verifyPendingDDL()
	parsed, err := classifyDDL(sql)
	if err != nil {
		// Unsupported DDL form. Reject the txn and abort the statement.
		return d.reject(fmt.Sprintf("classifyDDL: %v", err))
	}
	if parsed.Kind == ddlNone {
		if parsed.SavepointRollback {
			d.savepointRolledBack = true
		}
		if err := d.rejectUnverifiableTriggerOnCoordinated(parsed); err != nil {
			return d.reject(err.Error())
		}
		return 0 // not a DDL we admit; let SQLite run it
	}
	if parsed.Kind == ddlBeginOrSavepoint {
		if parsed.IsSavepoint {
			d.savepointSeen = true
		} else if d.app.InAutocommit() {
			// A BEGIN about to start a fresh transaction: any leftover
			// per-txn state is stale (commit_hook/rollback_hook clear it
			// on every txn end, but belt-and-braces against an end path
			// that fires neither hook — a leaked savepointSeen would
			// otherwise reject txn DDL forever on this conn).
			d.clearTxnState()
		}
		return 0
	}
	// Local-only objects (sqlite_* always; underscore-prefixed unless
	// ReplicateUnderscoreTables is set on the producer config) skip
	// the schema log. These never round-trip to peers; SQLite executes
	// directly — inside or outside a transaction.
	if isLocalOnlyName(parsed, d.replicateUnderscore) {
		return 0
	}
	// A vtab drop arrives as plain "DROP TABLE <name>" — textually
	// indistinguishable from a typed-table drop, so the parser can't
	// classify it. Upgrade it here (schema lookup) so it replicates as
	// OpDropVirtualTable instead of failing the typed-catalog lookup —
	// or, under IF EXISTS, silently executing local-only.
	if err := d.reclassifyVtabDrop(&parsed); err != nil {
		return d.reject(fmt.Sprintf("reclassify DROP: %v", err))
	}
	if !d.app.InAutocommit() {
		return d.handleTxnDDL(parsed)
	}
	// Build and normalize the typed catalog op.
	op, schemaSeq, err := d.prepareCatalogOp(parsed)
	if err != nil {
		return d.reject(err.Error())
	}
	// Data-dependent forms can still fail once SQLite runs the body, so
	// appending here would publish a schema change that never happened
	// locally. Defer to commit_hook: a failed body rolls the implicit
	// transaction back, commit_hook never fires, and nothing is
	// published. Bundles are excluded — their synth triggers execute on
	// the helper connection, which would block on the committing
	// transaction's write lock (the same reason handleTxnDDL rejects
	// them).
	if op.Kind != crdt.OpUnknown && op.Kind != crdt.OpBundle && ddlBodyFallible(parsed.Kind) {
		return d.stashPendingDDL(op, parsed, schemaSeq)
	}
	for attempt := 0; ; attempt++ {
		if op.Kind == crdt.OpUnknown {
			// Idempotent no-op (IF NOT EXISTS on existing object, IF
			// EXISTS on missing) — possibly only after catch-up applied
			// a peer's identical DDL. Skip schemalog.Append; SQLite
			// runs the no-op.
			return 0
		}
		encoded, err := crdt.EncodeCatalogOp(op)
		if err != nil {
			return d.reject(fmt.Sprintf("EncodeCatalogOp: %v", err))
		}
		// SetIntent BEFORE Append (using parentSeq+1, the seq the
		// schema log deterministically assigns on success). The intent
		// guard in the broker's catch-up loop relies on this ordering:
		// without it the broker can read the just-Appended event from
		// the schema log before the originator's intent lands, then run
		// CREATE TABLE on AppApply between Append and SetIntent — the
		// user's literal CREATE TABLE on the writer would then fail with
		// "table already exists".
		expectedSeq := schemaSeq + 1
		intent := metadata.LocalDDLIntent{
			StartedAtUs: d.nowMicros(),
			SchemaSeq:   expectedSeq,
			ParentSeq:   schemaSeq,
			CatalogOp:   encoded,
			RawSQL:      parsed.RawSQL,
		}
		if err := d.sc.SetOriginIntent(d.origin, metadata.EncodeLocalDDL(intent)); err != nil {
			return d.reject(fmt.Sprintf("SetIntent: %v", err))
		}
		newSeq, err := d.log.Append(context.Background(), schemaSeq, encoded, parsed.RawSQL)
		if err == nil {
			if newSeq != expectedSeq {
				// SchemaLog contract violation: Local and File both
				// assign parentSeq+1 on success. Anything else means
				// the backend implementation is buggy.
				_ = d.sc.ClearOriginIntent(d.origin)
				return d.reject(fmt.Sprintf("schemalog.Append: assigned seq=%d, expected %d", newSeq, expectedSeq))
			}
			break
		}
		// Clear the pre-reserved intent before retrying or rejecting.
		_ = d.sc.ClearOriginIntent(d.origin)
		if attempt > 0 || !errors.Is(err, schemalog.ErrHeadMoved) {
			return d.reject(fmt.Sprintf("schemalog.Append: %v", err))
		}
		// CAS loss: another producer appended since we read schema_seq.
		// Catch the local catalog up to the log head and rebuild the op
		// against the fresh schema, then retry the Append once. After
		// catch-up an already-applied identical DDL degrades to the
		// IF-NOT-EXISTS no-op / already-exists error above.
		if cuErr := d.catchUpToHead(); cuErr != nil {
			return d.reject(fmt.Sprintf("schemalog.Append: %v (catch-up: %v)", err, cuErr))
		}
		op, schemaSeq, err = d.prepareCatalogOp(parsed)
		if err != nil {
			return d.reject(err.Error())
		}
	}
	// For OpBundle CreateTable+synth-triggers and OpBundle DropTable+
	// synth-trigger-drops, the originator's SQLite hasn't yet installed
	// the triggers (they were never part of the user's DDL text).
	// Execute them on the helper connection so the next DML against the
	// parent table sees the trigger immediately. The writer's trace_v2
	// stack can't run nested DDL safely (SQLite doesn't persist it);
	// the helper is a vanilla connection without producer hooks.
	if op.Kind == crdt.OpBundle {
		if err := d.execSynthBundleSQL(op); err != nil {
			return d.reject(fmt.Sprintf("synth bundle exec: %v", err))
		}
	}
	return 0
}

// reclassifyVtabDrop upgrades a DROP TABLE whose target is a virtual
// table to ddlDropVirtualTable. Vtabs never acquire typed catalog
// entries (their DDL admits as opaque SQL), so without this the drop
// fails buildCatalogOp's typed-catalog lookup. Names in the typed
// catalog are ordinary tables and pass through untouched; names in
// neither (including IF EXISTS on a missing object) keep ddlDropTable
// and take its idempotent no-op path.
func (d *ddlAdmission) reclassifyVtabDrop(p *parsedDDL) error {
	if p.Kind != ddlDropTable {
		return nil
	}
	if _, ok := d.lookupTable(p.Name); ok {
		return nil
	}
	isVtab, err := sqlitebridge.IsVirtualTable(d.app, p.Name)
	if err != nil {
		return err
	}
	if isVtab {
		p.Kind = ddlDropVirtualTable
	}
	return nil
}

// catchUpToHead brings the local catalog up to the schema log's head
// after an Append CAS loss. With an in-process broker (full node) the
// schemaCatchup hook applies pending events synchronously; producer-
// only deployments wait for the external catch-up authority sharing
// this metadata to advance meta.schema_seq. Either way the in-memory
// catalog is rebuilt before returning so buildCatalogOp sees the new
// schema. Safe to call from trace_v2 in autocommit: the writer holds
// no write transaction yet, so the authority's apply conn can take
// the write lock (same precondition execSynthBundleSQL relies on).
func (d *ddlAdmission) catchUpToHead() error {
	ctx, cancel := context.WithTimeout(context.Background(), d.catchupWait)
	defer cancel()
	head, err := d.log.Head(ctx)
	if err != nil {
		return fmt.Errorf("schemalog.Head: %w", err)
	}
	for {
		if d.schemaCatchup != nil {
			// Re-run each iteration: a pending peer intent makes one
			// pass yield; the originator resolving it (advancing
			// schema_seq in the shared metadata) unblocks the next.
			if err := d.schemaCatchup(ctx); err != nil {
				return fmt.Errorf("schema catch-up: %w", err)
			}
		}
		seq, _, err := d.sc.GetSchemaSeq()
		if err != nil {
			return err
		}
		if seq >= head {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("schema_seq %d still behind log head %d: %w", seq, head, ctx.Err())
		case <-time.After(schemaCatchupPoll):
		}
	}
	return d.cat.RebuildWithPKDefaults(d.app)
}

// execSynthBundleSQL runs the SQL for every OpCreateTrigger /
// OpDropTrigger sub-op in a bundle on the helper connection (NOT the
// writer connection — nested DDL during trace_v2 doesn't persist).
// Skips the OpCreateTable / OpDropTable sub-op; that's the user's
// statement and SQLite executes it after trace_v2 returns 0.
func (d *ddlAdmission) execSynthBundleSQL(op crdt.CatalogOp) error {
	if d.helper == nil {
		return nil
	}
	for _, sub := range op.SubOps {
		switch sub.Kind {
		case crdt.OpCreateTrigger, crdt.OpDropTrigger:
			if sub.RawSQL == "" {
				continue
			}
			if err := d.helper.Exec(sub.RawSQL); err != nil {
				return fmt.Errorf("exec %s: %w", sub.ObjectName, err)
			}
		}
	}
	return nil
}

// handleTxnDDL admits a DDL statement inside an explicit transaction.
// Any number of DDL statements are allowed; they must all precede any
// DML, and savepoint scopes are excluded. Each catalog op is built and
// validated here (so a bad statement still fails at the statement, not
// at COMMIT) against the transaction's own overlay, and the cluster-
// wide commit point — a single schemalog.Append carrying the whole set
// — is deferred to commit_hook: an explicit ROLLBACK then replicates
// nothing, and a CAS loss at commit aborts the COMMIT instead of
// leaving an orphaned schema event.
func (d *ddlAdmission) handleTxnDDL(parsed parsedDDL) int {
	if d.savepointSeen {
		return d.reject("DDL inside a SAVEPOINT scope is not replicable (ROLLBACK TO could partially undo it); use a plain BEGIN...COMMIT transaction")
	}
	if d.app.TouchJournalLen() > 0 {
		return d.reject("DDL after DML in the same transaction is not supported; issue the DDL first")
	}
	op, parentSeq, err := d.prepareCatalogOp(parsed)
	if err != nil {
		return d.reject(err.Error())
	}
	// Every op in the transaction rides one schema event, so they all
	// share the parent the first one was built against.
	if d.txnDDL != nil {
		parentSeq = d.txnDDL.parentSeq
	}
	if op.Kind == crdt.OpUnknown {
		return 0 // IF [NOT] EXISTS no-op; nothing to replicate
	}
	if op.Kind == crdt.OpBundle {
		// Cascade-FK synthesis executes trigger DDL on the helper
		// connection, which would block on this transaction's write
		// lock until COMMIT — and the triggers must exist before any
		// same-transaction DML. Not supported inside a transaction.
		return d.reject("FOREIGN KEY cascade actions inside an explicit transaction are not supported; issue the CREATE TABLE outside a transaction")
	}
	return d.stashPendingDDL(op, parsed, parentSeq)
}

// lookupTable resolves a table name against the transaction's own
// pending DDL first, then the committed catalog.
func (d *ddlAdmission) lookupTable(name string) (*catalog.Table, bool) {
	if d.overlay != nil {
		return d.overlay.Table(name)
	}
	return d.cat.Table(name)
}

// stashPendingDDL parks a validated op for commit-time Append, together
// with the post-state check verifyPendingDDL settles it against, and
// folds it into the overlay so a later statement in the same
// transaction can resolve what it created.
func (d *ddlAdmission) stashPendingDDL(op crdt.CatalogOp, parsed parsedDDL, parentSeq uint64) int {
	if d.txnDDL == nil {
		d.txnDDL = &txnPendingDDL{parentSeq: parentSeq}
	}
	d.txnDDL.ops = append(d.txnDDL.ops, pendingDDLOp{
		op:     op,
		rawSQL: parsed.RawSQL,
		check:  structuralCheckFor(parsed),
	})
	if d.overlay == nil {
		d.overlay = catalog.NewOverlay(d.cat)
	}
	if err := d.overlay.Apply(op); err != nil {
		// A half-applied op would leave the overlay describing a schema
		// that never existed; drop the statement and rebuild from what
		// remains.
		d.txnDDL.ops = d.txnDDL.ops[:len(d.txnDDL.ops)-1]
		d.rebuildOverlay()
		return d.reject(fmt.Sprintf("catalog overlay: %v", err))
	}
	return 0
}

// rebuildOverlay reconstructs the transaction-local catalog view from
// the pending ops. Needed whenever the pending set shrinks — the overlay
// has no way to un-apply a single op.
func (d *ddlAdmission) rebuildOverlay() {
	if d.txnDDL == nil || len(d.txnDDL.ops) == 0 {
		d.overlay = nil
		return
	}
	overlay := catalog.NewOverlay(d.cat)
	for _, po := range d.txnDDL.ops {
		if err := overlay.Apply(po.op); err != nil {
			// Every op in the list applied cleanly when it was stashed, so
			// this cannot happen; keep the overlay rather than resolve
			// against a schema that is missing the transaction's own DDL.
			syzylog.Printf("producer: rebuilding catalog overlay: %v", err)
			return
		}
	}
	d.overlay = overlay
}

// commitPendingTxnDDL is called from commit_hook. If the closing
// transaction admitted DDL, this is its cluster-wide commit point: one
// schemalog.Append carrying every admitted op, then SetIntent. Returns
// nonzero to abort the COMMIT (SQLite rolls the transaction back),
// which is the rejection surface for CAS losses and schema-log errors.
// Runs inside sqlite3_commit_hook, so it must not touch the writer
// connection — metadata and the schema log are separate
// connections/files, which is all this needs.
func (d *ddlAdmission) commitPendingTxnDDL() int {
	pend := d.txnDDL
	if pend == nil || len(pend.ops) == 0 {
		d.clearTxnState()
		return 0
	}
	// Consume the pending slot up front: whatever happens below, this
	// transaction's admission attempt must not survive into another
	// (rollback_hook re-clears harmlessly; pend keeps the data local).
	d.clearTxnState()
	rejectCommit := func(reason string) int {
		// Unlike reject(), no Interrupt and no rejected-flag latch:
		// returning nonzero from commit_hook is itself the abort, and
		// the flag would leak into the next transaction (rollback_hook
		// clears it, but it fires after commit_hook's rollback with no
		// pending rejection context).
		syzylog.Printf("producer: DDL commit admission rejected: %s", reason)
		return 1
	}
	// Append BEFORE SetIntent — the inverse of the trace-time autocommit
	// path. That pre-reservation ordering exists so the broker can't
	// apply a just-Appended event before the originator's writer-conn
	// DDL has executed; here the DDL already executed under the
	// committing transaction's write lock (an implicit one for a
	// deferred autocommit form), which blocks any structural apply until
	// COMMIT, so the race cannot arise. Appending first means a crash between the
	// two writes leaves only a durable log event (recovered by normal
	// schema catch-up) — never a metadata intent for an event that
	// isn't in the log, which recovery would resolve into a schema_seq
	// beyond the log head (an unrepairable fork).
	//
	// The Append CAS at pend.parentSeq is also the freshness check: the
	// ops were built against the catalog as of parentSeq, so ANY event
	// landed since (CAS mismatch) means a stale view — abort the COMMIT
	// and let the application retry against the new schema.
	op, rawSQL := pend.payload()
	encoded, err := crdt.EncodeCatalogOp(op)
	if err != nil {
		return rejectCommit(fmt.Sprintf("EncodeCatalogOp: %v", err))
	}
	expectedSeq := pend.parentSeq + 1
	newSeq, err := d.log.Append(context.Background(), pend.parentSeq, encoded, rawSQL)
	if err != nil {
		return rejectCommit(fmt.Sprintf("schemalog.Append: %v", err))
	}
	if newSeq != expectedSeq {
		return rejectCommit(fmt.Sprintf("schemalog.Append: assigned seq=%d, expected %d", newSeq, expectedSeq))
	}
	intent := metadata.LocalDDLIntent{
		StartedAtUs: d.nowMicros(),
		SchemaSeq:   expectedSeq,
		ParentSeq:   pend.parentSeq,
		CatalogOp:   encoded,
		RawSQL:      rawSQL,
	}
	if err := d.sc.SetOriginIntent(d.origin, metadata.EncodeLocalDDL(intent)); err != nil {
		// The event is durable cluster-wide; aborting the COMMIT now
		// would roll back the local DDL and leave the event to diverge.
		// Let the commit proceed — wal_hook finds no intent, and the
		// catalog converges via schema catch-up (the event is in the
		// log). Metadata write failure is the disk-fault class; log it.
		syzylog.Printf("producer: SetIntent after txn DDL Append failed (catalog converges via catch-up): %v", err)
		return 0
	}
	// Success: the intent rides to wal_hook, which resolves it (catalog
	// upserts + schema_seq advance) before journaling the transaction's
	// touch records — so same-transaction DML drains under the new
	// schema_seq.
	return 0
}

// verifyPendingDDL settles a stashed admission against app.db's real
// post-state, dropping it if its statement did not take effect.
//
// trace_v2 admits a DDL *before* SQLite runs the body, so the stash
// describes a schema change that has not happened yet. Most bad DDL
// never gets here (the compiler rejects it before the trace hook), but
// forms whose validity depends on table data — see ddlBodyFallible —
// fail during execution. In an explicit transaction a failed statement
// does not abort the transaction, so an application that ignores the
// error can still COMMIT; without this check that COMMIT would publish
// a schema change the node does not have.
//
// Called from trace_v2, which is the only safe place to ask app.db:
// commit_hook must not touch the writer connection. COMMIT is itself a
// traced statement, so a pending admission is always settled before
// commitPendingTxnDDL runs. Each stash is checked exactly once, right
// after its own statement — re-checking later would misread a
// subsequent statement's effects (a table created and then dropped in
// the same transaction would look like it never landed).
//
// Autocommit deferrals need no check: a failed body rolls the implicit
// transaction back and commit_hook never fires.
func (d *ddlAdmission) verifyPendingDDL() {
	pend := d.txnDDL
	if pend == nil {
		return
	}
	// Raised for the whole scan: check.landed queries the writer
	// connection, and SQLite traces those queries back into handleStmt.
	d.introspecting = true
	defer func() { d.introspecting = false }()

	kept := make([]pendingDDLOp, 0, len(pend.ops))
	for _, po := range pend.ops {
		if !po.verified {
			// Marked before the check, and written back below, so an op is
			// never re-examined once settled: a later statement's effects
			// would misread it (a table created and then dropped in the
			// same transaction would look like it never landed).
			po.verified = true
			landed, err := po.check.landed(d.app)
			switch {
			case err != nil:
				// Introspection failure is the disk-fault class. Keep the
				// op; the commit-time Append is still CAS-guarded.
				syzylog.Printf("producer: verifying DDL post-state failed (publishing anyway): %v", err)
			case !landed:
				syzylog.Printf("producer: DDL not published — its statement did not take effect locally: %s", po.rawSQL)
				continue
			}
		}
		kept = append(kept, po)
	}
	dropped := len(kept) != len(pend.ops)
	pend.ops = kept
	if !dropped {
		return
	}
	if len(kept) == 0 {
		d.clearTxnState()
		return
	}
	// The overlay still carries the dropped statement's effects; rebuild
	// it so the rest of the transaction resolves against the schema that
	// really exists.
	d.rebuildOverlay()
}

// clearTxnState drops per-transaction admission state. Called when the
// transaction ends (commit_hook, rollback_hook) and when commit-time
// admission consumes the pending DDL.
func (d *ddlAdmission) clearTxnState() {
	d.txnDDL = nil
	d.savepointSeen = false
	d.savepointRolledBack = false
	d.overlay = nil
}

// reject latches the rejected flag, asks SQLite to stop the in-flight
// statement, and returns nonzero so the trace_v2 callback aborts. The
// app sees SQLITE_INTERRUPT with no detail, so the reason is logged
// here — that single line is what tells an operator why a CREATE TABLE
// was rejected (missing PK, unsupported form, schema-log error, etc.).
func (d *ddlAdmission) reject(reason string) int {
	syzylog.Printf("producer: DDL admission rejected: %s", reason)
	d.rejected.Store(true)
	d.app.Interrupt()
	return 1
}

// prepareCatalogOp builds one SQLite-native DDL operation.
func (d *ddlAdmission) prepareCatalogOp(parsed parsedDDL) (crdt.CatalogOp, uint64, error) {
	op, parentSeq, err := d.buildCatalogOp(parsed)
	if err != nil {
		return crdt.CatalogOp{}, 0, fmt.Errorf("buildCatalogOp: %w", err)
	}
	return op, parentSeq, nil
}

// isLocalOnlyName reports whether the parsed DDL targets a local-only
// object. sqlite_* names are unconditionally local (SQLite reserves
// them for its own bookkeeping). Underscore-prefixed names are local
// by convention unless replicateUnderscore is set, in which case the
// caller has explicitly opted such names back into replication
// (typically because a framework, e.g. PocketBase, names its system
// tables with leading underscores). The replicated catalog only tracks
// "main" schema names, so non-main schemas (temp.x) are also handled
// by the parser-level `ddlNone` route for TEMPORARY.
func isLocalOnlyName(p parsedDDL, replicateUnderscore bool) bool {
	name := p.Name
	switch p.Kind {
	case ddlCreateIndex, ddlCreateUniqueIndex:
		name = p.IndexTable
	}
	if strings.HasPrefix(name, "sqlite_") {
		return true
	}
	if strings.HasPrefix(name, "_") {
		return !replicateUnderscore
	}
	return false
}

// buildCatalogOp constructs a typed crdt.CatalogOp from the parsed
// DDL, also returning the local schema_seq the schemalog.Append should
// use as parentSeq. Returns Kind=OpUnknown for SQLite-no-ops (e.g. IF
// NOT EXISTS on existing) so the caller can skip Append.
func (d *ddlAdmission) buildCatalogOp(p parsedDDL) (crdt.CatalogOp, uint64, error) {
	parentSeq, _, err := d.sc.GetSchemaSeq()
	if err != nil {
		return crdt.CatalogOp{}, 0, fmt.Errorf("read schema_seq: %w", err)
	}

	switch p.Kind {
	case ddlCreateTable:
		// IF NOT EXISTS + table already exists in catalog → no-op.
		if p.IfNotExists {
			if _, ok := d.lookupTable(p.Name); ok {
				return crdt.CatalogOp{}, parentSeq, nil
			}
		} else if _, ok := d.lookupTable(p.Name); ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("table %q already exists", p.Name)
		}
		op, err := d.buildCreateTableOp(p)
		if err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if opHasCoordinatedKey(op) {
			if d.helper == nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE TABLE %q: a coordinated (NOT NULL UNIQUE) key requires a helper connection (Config.AppHelper) for index normalization", p.Name)
			}
			// A synth cascade trigger that UPDATEs this table, or an
			// existing trigger writing this name (SQLite admits trigger
			// bodies against not-yet-existing tables), would bypass the
			// reservation gate.
			if fkUpdatesChild(p.FKs) {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE TABLE %q: ON DELETE SET NULL/SET DEFAULT and ON UPDATE CASCADE actions write this table via a trigger, which bypasses the coordinated (NOT NULL UNIQUE) key's reservation gate", p.Name)
			}
			if err := d.rejectCoordinatedKeyIfTriggerTarget(p.Name); err != nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE TABLE %q: %w", p.Name, err)
			}
		}
		return wrapCascadeBundle(op, p.Name, op.TableID, p), parentSeq, nil

	case ddlAlterTableAddColumn:
		tab, ok := d.lookupTable(p.Name)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("ALTER TABLE %q: table not in catalog", p.Name)
		}
		if len(p.Columns) != 1 {
			return crdt.CatalogOp{}, 0, fmt.Errorf("ADD COLUMN expects exactly one column, got %d", len(p.Columns))
		}
		newCol := p.Columns[0]
		if _, exists := tab.Column(newCol.Name); exists {
			return crdt.CatalogOp{}, 0, fmt.Errorf("ADD COLUMN %q: column already exists", newCol.Name)
		}
		ord := nextOrdinal(tab.Columns)
		col, err := makeCatalogColumn(newCol, ord)
		if err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if col.ClockGroup == metadata.ClockGroupCounter && !tab.CellGroup() {
			return crdt.CatalogOp{}, 0, fmt.Errorf("ADD COLUMN %q: COUNTER columns require a cell-group table; run SetClockGroup(%q, 'cell') first", newCol.Name, tab.Name)
		}
		if fkUpdatesChild(p.FKs) && tableHasCoordinatedKey(tab) {
			return crdt.CatalogOp{}, 0, fmt.Errorf("ADD COLUMN %q: ON DELETE SET NULL/SET DEFAULT and ON UPDATE CASCADE actions write table %q via a trigger, which bypasses its coordinated (NOT NULL UNIQUE) key's reservation gate", newCol.Name, tab.Name)
		}
		op := crdt.CatalogOp{
			Kind: crdt.OpAddColumn, TableID: tab.ID,
			Columns: []crdt.CatalogColumn{col},
		}
		// A cascade-FK on an ADD COLUMN bundles a synth trigger
		// alongside the ALTER. Rare but well-defined.
		return wrapCascadeBundle(op, tab.Name, tab.ID, p), parentSeq, nil

	case ddlAlterTableRenameTo:
		tab, ok := d.lookupTable(p.Name)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("RENAME: table %q not in catalog", p.Name)
		}
		// The trigger ban is checked against a table's name, so a rename
		// re-opens it: SQLite admits a trigger body naming a table that
		// does not exist yet, and under legacy_alter_table the rename does
		// not rewrite trigger bodies away from the new name either.
		if tableHasCoordinatedKey(tab) {
			if d.helper == nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("RENAME: table %q has a coordinated (NOT NULL UNIQUE) key, which requires a helper connection (Config.AppHelper) to verify triggers against the new name", p.Name)
			}
			if err := d.rejectCoordinatedKeyIfTriggerTarget(p.NewName); err != nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("RENAME TO %q: %w", p.NewName, err)
			}
		}
		return crdt.CatalogOp{
			Kind: crdt.OpRenameTable, TableID: tab.ID, TableName: p.NewName,
		}, parentSeq, nil

	case ddlAlterTableRenameColumn:
		tab, ok := d.lookupTable(p.Name)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("RENAME COLUMN: table %q not in catalog", p.Name)
		}
		col, ok := tab.Column(p.OldColumn)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("RENAME COLUMN: %q.%q not in catalog", p.Name, p.OldColumn)
		}
		return crdt.CatalogOp{
			Kind: crdt.OpRenameColumn, TableID: tab.ID,
			ColumnID: col.ID, ColumnName: p.NewColumn,
		}, parentSeq, nil

	case ddlAlterTableDropColumn:
		tab, ok := d.lookupTable(p.Name)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("DROP COLUMN: table %q not in catalog", p.Name)
		}
		col, ok := tab.Column(p.DropColumn)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("DROP COLUMN: %q.%q not in catalog", p.Name, p.DropColumn)
		}
		// PK columns can't be dropped — SQLite refuses anyway, but we
		// surface it earlier as a clearer error.
		if col.PKPos > 0 {
			return crdt.CatalogOp{}, 0, fmt.Errorf("DROP COLUMN: cannot drop PK column %q", col.Name)
		}
		return crdt.CatalogOp{
			Kind: crdt.OpDropColumn, TableID: tab.ID, ColumnID: col.ID,
		}, parentSeq, nil

	case ddlDropTable:
		tab, ok := d.lookupTable(p.Name)
		if !ok {
			if p.IfExists {
				return crdt.CatalogOp{}, parentSeq, nil
			}
			return crdt.CatalogOp{}, 0, fmt.Errorf("DROP TABLE %q: not in catalog", p.Name)
		}
		dropOp := crdt.CatalogOp{Kind: crdt.OpDropTable, TableID: tab.ID}
		synth, err := d.sc.ListSynthTriggersForTable(tab.ID)
		if err != nil {
			return crdt.CatalogOp{}, 0, fmt.Errorf("list synth triggers: %w", err)
		}
		if len(synth) == 0 {
			return dropOp, parentSeq, nil
		}
		// Drop the synth triggers BEFORE the table itself: the
		// triggers live on parent tables, not on tab.Name, so SQLite
		// won't auto-cascade-drop them. Sequencing drops first lets
		// receivers' apply pass an idempotent "trigger gone" check
		// even if the parent later goes away.
		sub := make([]crdt.CatalogOp, 0, len(synth)+1)
		for _, st := range synth {
			sub = append(sub, crdt.CatalogOp{
				Kind:       crdt.OpDropTrigger,
				TableID:    tab.ID,
				ObjectName: st.TriggerName,
				// IF EXISTS for the same idempotency reason as the
				// CREATE side: catch-up may re-run after the originator's
				// helper has already issued the drop.
				RawSQL: fmt.Sprintf("DROP TRIGGER IF EXISTS %s",
					sqlitebridge.QuoteIdent(st.TriggerName)),
			})
		}
		sub = append(sub, dropOp)
		return crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: sub}, parentSeq, nil

	case ddlCreateIndex:
		// The target table must exist in the catalog (symmetric with
		// the UNIQUE INDEX path below). Without this, a CAS-loss retry
		// whose catch-up applied a concurrent DROP TABLE could publish
		// an index event no receiver can ever apply — a schema-log
		// poison pill.
		if tab, ok := d.lookupTable(p.IndexTable); !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE INDEX: table %q not in catalog", p.IndexTable)
		} else if err := rejectCoordinatedColumnShadow(tab, p.IndexColumns, "CREATE INDEX "+p.Name); err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if p.IfNotExists {
			exists, err := sqlitebridge.ObjectExists(d.app, "index", p.Name)
			if err != nil {
				return crdt.CatalogOp{}, 0, err
			}
			if exists {
				return crdt.CatalogOp{}, parentSeq, nil
			}
		}
		return crdt.CatalogOp{Kind: crdt.OpCreateIndex, ObjectName: p.Name, RawSQL: p.RawSQL}, parentSeq, nil

	case ddlDropIndex:
		if p.IfExists {
			exists, err := sqlitebridge.ObjectExists(d.app, "index", p.Name)
			if err != nil {
				return crdt.CatalogOp{}, 0, err
			}
			if !exists {
				return crdt.CatalogOp{}, parentSeq, nil
			}
		}
		// A DROP INDEX whose column list matches a coordinated key is the
		// key-removal statement: the key is catalog metadata (its local
		// index, if any, was normalized to a plain one at creation), so
		// the removal must replicate as the typed key-removal op —
		// OpDropIndex would only drop the local index and leave every
		// node's catalog enforcing a phantom key.
		if op, ok, err := d.coordinatedKeyRemovalOp(p); err != nil {
			return crdt.CatalogOp{}, 0, err
		} else if ok {
			return op, parentSeq, nil
		}
		return crdt.CatalogOp{Kind: crdt.OpDropIndex, ObjectName: p.Name, RawSQL: p.RawSQL}, parentSeq, nil

	case ddlCreateUniqueIndex:
		if p.IfNotExists {
			exists, err := sqlitebridge.ObjectExists(d.app, "index", p.Name)
			if err != nil {
				return crdt.CatalogOp{}, 0, err
			}
			if exists {
				return crdt.CatalogOp{}, parentSeq, nil
			}
		}
		// UNIQUE indexes are typed as AddUniqueKey when they target a
		// catalog table. An all-NOT-NULL key is coordinated (CP); a key
		// with any nullable member is eventual (loser-null) and must be
		// non-BLOB (sqlite/docs/DDL.md#unique-keys).
		tab, ok := d.lookupTable(p.IndexTable)
		if !ok {
			return crdt.CatalogOp{}, 0, fmt.Errorf("UNIQUE INDEX: table %q not in catalog", p.IndexTable)
		}
		coordinated, err := validateUniqueColumnsOnTable(d.app, tab, p.IndexColumns)
		if err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if coordinated && !d.coordinatedUnique {
			return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: a NOT NULL UNIQUE (coordinated) key requires a reservation backend; none is configured", p.Name)
		}
		// A partial index participates only in coordinated mode: its
		// predicate is evaluated solely at the writer (reserve) and
		// leaseholder (rebuild), never on a receiver's apply path, so the
		// cross-replica timing hazard that bars eventual partial keys does
		// not arise (sqlite/docs/DDL.md#ddl-rules).
		var predicate crdt.UniquePredicate
		if p.WherePred != nil {
			if !coordinated {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: a partial UNIQUE index requires all members NOT NULL (coordinated); an eventual partial index is not supported", p.Name)
			}
			if predicate, err = compilePartialPredicate(p.WherePred, d.app, tab); err != nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: %w", p.Name, err)
			}
		}
		members, err := keyMembersFromColumnNames(tab, p.IndexColumns)
		if err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if err := validateBinaryUniqueMembers(members, func(id crdt.ColumnID) (crdt.Collation, bool) {
			c, ok := tab.ColumnByID(id)
			return c.Collation, ok
		}, p.Name); err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		if coordinated {
			if d.helper == nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: a coordinated (NOT NULL UNIQUE) key requires a helper connection (Config.AppHelper) for index normalization", p.Name)
			}
			if err := validateNoCoordinatedOverlap(tab, members, predicate, p.Name); err != nil {
				return crdt.CatalogOp{}, 0, err
			}
			// Same ambiguity as rejectCoordinatedColumnShadow, from the
			// other side: this index is about to become the key's
			// (downgraded) backing index, so no other unfiltered index may
			// already cover exactly these columns. Partial keys are exempt
			// — DROP INDEX disambiguates those by predicate.
			if predicate.Root == nil {
				other, err := unfilteredIndexOnColumns(d.app, tab.Name, p.IndexColumns, p.Name)
				if err != nil {
					return crdt.CatalogOp{}, 0, err
				}
				if other != "" {
					return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: index %q already covers exactly these columns with no WHERE clause; DROP INDEX could not tell the two apart, and dropping either would remove the coordinated (NOT NULL UNIQUE) key — drop %q first", p.Name, other, other)
				}
			}
			// A composite or partial key names a row shape; per-cell merge
			// could assemble a row from writes that were never reserved
			// together, synthesizing a value the gate never saw.
			if (len(members) > 1 || predicate.Root != nil) && tab.CellGroup() {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: a composite or partial coordinated key requires whole-row merge, but table %q uses the cell clock group; run SetClockGroup(%q, 'row') first", p.Name, tab.Name, tab.Name)
			}
			if err := d.rejectCoordinatedKeyIfTriggerTarget(tab.Name); err != nil {
				return crdt.CatalogOp{}, 0, fmt.Errorf("CREATE UNIQUE INDEX %q: %w", p.Name, err)
			}
		}
		keyID := corecatalog.AllocKeyID()
		return crdt.CatalogOp{
			Kind: crdt.OpAddUniqueKey, TableID: tab.ID, KeyID: keyID,
			Keys: []crdt.CatalogKey{{KeyID: keyID, Members: members, Coordinated: coordinated, Predicate: predicate}},
		}, parentSeq, nil

	case ddlCreateView, ddlDropView:
		kind := crdt.OpCreateView
		if p.Kind == ddlDropView {
			kind = crdt.OpDropView
		}
		return crdt.CatalogOp{Kind: kind, ObjectName: p.Name, RawSQL: p.RawSQL}, parentSeq, nil

	case ddlCreateVirtualTable, ddlDropVirtualTable:
		kind := crdt.OpCreateVirtualTable
		if p.Kind == ddlDropVirtualTable {
			kind = crdt.OpDropVirtualTable
		}
		return crdt.CatalogOp{Kind: kind, ObjectName: p.Name, RawSQL: p.RawSQL}, parentSeq, nil

	case ddlCreateTrigger, ddlDropTrigger:
		kind := crdt.OpCreateTrigger
		if p.Kind == ddlDropTrigger {
			kind = crdt.OpDropTrigger
		} else if err := d.rejectTriggerOnCoordinated(p.RawSQL); err != nil {
			return crdt.CatalogOp{}, 0, err
		}
		return crdt.CatalogOp{Kind: kind, ObjectName: p.Name, RawSQL: p.RawSQL}, parentSeq, nil
	}
	return crdt.CatalogOp{}, 0, fmt.Errorf("unsupported DDL kind %v", p.Kind)
}

func (d *ddlAdmission) buildCreateTableOp(p parsedDDL) (crdt.CatalogOp, error) {
	tabID, err := d.createTableID(p)
	if err != nil {
		return crdt.CatalogOp{}, err
	}
	return buildCreateTableOpWithID(p, tabID, d.coordinatedUnique)
}

// buildCreateTableOpWithID is the pure CREATE projection used by live DDL.
func buildCreateTableOpWithID(p parsedDDL, tabID crdt.TableID, coordinatedUnique bool) (crdt.CatalogOp, error) {
	cols := make([]crdt.CatalogColumn, 0, len(p.Columns))
	for i, pc := range p.Columns {
		if err := validatePKDefault(p.Name, pc); err != nil {
			return crdt.CatalogOp{}, err
		}
		col, err := makeCatalogColumn(pc, i)
		if err != nil {
			return crdt.CatalogOp{}, err
		}
		// Mark PK position from the table-level PRIMARY KEY clause if
		// present (column-level PRIMARY KEY already set IsPK above).
		if pos := pkPosition(p.PKColumns, pc.Name); pos > 0 {
			if col.ClockGroup == metadata.ClockGroupCounter {
				return crdt.CatalogOp{}, fmt.Errorf("column %q: a COUNTER column cannot be a PRIMARY KEY member", pc.Name)
			}
			col.IsPK = true
			col.PKPos = pos
		}
		cols = append(cols, col)
	}
	if !hasPK(cols) {
		return crdt.CatalogOp{}, errors.New("CREATE TABLE: replicated tables require PRIMARY KEY")
	}
	// Defense-in-depth backstop. The SQL preprocessor
	// (ddl_rewrite.go:preprocessRowidAlias) is the primary path that
	// rewrites rowid-alias `INTEGER PRIMARY KEY` into the multi-writer-
	// safe shape before SQLite ever sees the DDL. But the preprocessor
	// only handles the first statement of a multi-statement string, and
	// declines to touch columns carrying CHECK/COLLATE/named CONSTRAINT/
	// REFERENCES/ON CONFLICT (it can't preserve those losslessly). If a
	// rowid-alias declaration reaches admission, reject it here rather
	// than silently letting a true rowid alias into the replicated
	// catalog where peers' inserts would collide.
	if err := validateNoRowidAlias(p, cols); err != nil {
		return crdt.CatalogOp{}, err
	}
	pkMembers := make([]crdt.CatalogKeyMember, 0)
	for _, c := range cols {
		if c.IsPK {
			pkMembers = append(pkMembers, crdt.CatalogKeyMember{ColumnID: c.ID, Ordinal: c.PKPos - 1})
		}
	}
	keys := []crdt.CatalogKey{{KeyID: crdt.KeyID{}, Members: pkMembers}}
	seenKeyCols := map[string]struct{}{}
	for _, uk := range p.UniqueKeys {
		// Two UNIQUE declarations over the same column tuple would mint two
		// keys with one meaning (and catalog repair would collapse them to
		// an arbitrary winner later); reject the redundancy up front.
		colsSig := strings.Join(uk, "\x00")
		if _, dup := seenKeyCols[colsSig]; dup {
			return crdt.CatalogOp{}, fmt.Errorf("CREATE TABLE %q: duplicate UNIQUE declaration on column(s) %v", p.Name, uk)
		}
		seenKeyCols[colsSig] = struct{}{}
		coordinated, err := validateUniqueColumnsByCols(cols, uk)
		if err != nil {
			return crdt.CatalogOp{}, err
		}
		if coordinated && !coordinatedUnique {
			return crdt.CatalogOp{}, fmt.Errorf("CREATE TABLE %q: a NOT NULL UNIQUE (coordinated) key on column(s) %v requires a reservation backend; none is configured", p.Name, uk)
		}
		// A COUNTER column makes this a cell-group table, and per-cell
		// merge could assemble a row from writes that were never reserved
		// together — a composite coordinated key names exactly such a row
		// shape. (Partial keys can't be declared in CREATE TABLE.)
		if coordinated && len(uk) > 1 && hasCounterColumn(cols) {
			return crdt.CatalogOp{}, fmt.Errorf("CREATE TABLE %q: a composite coordinated (NOT NULL UNIQUE) key on %v requires whole-row merge, but a COUNTER column makes this a cell-group table", p.Name, uk)
		}
		members, err := keyMembersFromColumnNamesByCols(cols, uk)
		if err != nil {
			return crdt.CatalogOp{}, err
		}
		if err := validateBinaryUniqueMembers(members, collationByColID(cols), fmt.Sprintf("on %v", uk)); err != nil {
			return crdt.CatalogOp{}, err
		}
		kid := corecatalog.AllocKeyID()
		keys = append(keys, crdt.CatalogKey{KeyID: kid, Members: members, Coordinated: coordinated})
	}
	return crdt.CatalogOp{
		Kind:         crdt.OpCreateTable,
		TableID:      tabID,
		TableName:    p.Name,
		Columns:      cols,
		Keys:         keys,
		WithoutRowid: p.WithoutRowid,
	}, nil
}

func (d *ddlAdmission) createTableID(parsedDDL) (crdt.TableID, error) {
	return catalog.AllocTableID(), nil
}

// validateNoRowidAlias is the admission-side backstop for the
// preprocessor: any rowid-alias INTEGER PRIMARY KEY that slipped past
// preprocessRowidAlias is rejected here with an actionable error. The
// rewrite runs for single-statement Prepare/Exec on syzy's own conns
// and, in extension mode, for the host app's sqlite3_prepare*/exec via
// the LD_PRELOAD interposers — so reaching this function means the
// statement itself wasn't rewritable: a column carrying a CHECK /
// COLLATE / CONSTRAINT-name / REFERENCES / ON CONFLICT clause the
// splice can't preserve losslessly, comment-prefixed DDL, a non-first
// statement of a multi-statement Conn.Exec on the linked path, or an
// ASC PK with trailing tokens we didn't anticipate. Pointing at the
// explicit `INT PRIMARY KEY NOT NULL DEFAULT (gen_id(...))` form gives
// a clean migration target.
func validateNoRowidAlias(p parsedDDL, cols []crdt.CatalogColumn) error {
	if p.WithoutRowid || len(p.PKColumns) != 1 {
		return nil
	}
	pkName := p.PKColumns[0]
	for _, c := range cols {
		if c.Name != pkName {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(c.Type), "INTEGER") {
			return fmt.Errorf("CREATE TABLE %q column %q: INTEGER PRIMARY KEY rowid alias is not safe under multi-writer replication, and this statement could not be auto-rewritten; declare the column as `%s INT PRIMARY KEY NOT NULL DEFAULT (gen_id(%q))` (the auto-rewrite skips columns with CHECK/COLLATE/CONSTRAINT-name/REFERENCES/ON CONFLICT clauses, and comment-prefixed statements)",
				p.Name, c.Name, c.Name, p.Name)
		}
	}
	return nil
}

// validatePKDefault rejects the cases where a recognized syzy default
// would silently misbehave: gen_id with a literal arg that doesn't
// match the table being defined, and the bare gen_id() form whose
// table-implicit rewrite isn't wired yet.
func validatePKDefault(tableName string, pc parsedColumn) error {
	d := catalog.ClassifyPKDefault(pc.Default)
	switch d.Kind {
	case catalog.PKDefaultGenID:
		if d.Arg != tableName {
			return fmt.Errorf("CREATE TABLE %q column %q: gen_id(%q) literal must match the table name",
				tableName, pc.Name, d.Arg)
		}
	case catalog.PKDefaultGenIDBare:
		// The preprocessor table-qualifies bare gen_id() before SQLite
		// sees the DDL; reaching this means the statement bypassed it
		// (comment-prefixed, or a non-first statement on the linked
		// path).
		return fmt.Errorf("CREATE TABLE %q column %q: gen_id() with no argument could not be auto-qualified; use gen_id(%q)",
			tableName, pc.Name, tableName)
	}
	return nil
}

// validateBinaryUniqueMembers rejects a unique key whose member columns
// are not all BINARY collation. The reservation tuple (coordinated) and
// the loser-null arbitration tuple (eventual) are the canonical byte
// encoding of the value, which neither folds case nor trims trailing
// spaces — so a NOCASE/RTRIM member would let two values the SQLite index
// considers equal encode as distinct, breaking cross-node uniqueness.
func validateBinaryUniqueMembers(members []crdt.CatalogKeyMember, collOf func(crdt.ColumnID) (crdt.Collation, bool), keyDesc string) error {
	for _, m := range members {
		if coll, ok := collOf(m.ColumnID); ok && coll != crdt.CollBinary {
			return fmt.Errorf("UNIQUE key %s: member column has %s collation; replicated unique keys require BINARY-collation members (the canonical value encoding does not fold case or trim)", keyDesc, coll.Name())
		}
	}
	return nil
}

// collationByColID returns a member-collation lookup over a freshly-built
// CREATE TABLE column list.
func collationByColID(cols []crdt.CatalogColumn) func(crdt.ColumnID) (crdt.Collation, bool) {
	return func(id crdt.ColumnID) (crdt.Collation, bool) {
		for i := range cols {
			if cols[i].ID == id {
				return cols[i].Collation, true
			}
		}
		return crdt.CollBinary, false
	}
}

func makeCatalogColumn(pc parsedColumn, ordinal int) (crdt.CatalogColumn, error) {
	cid := catalog.AllocColumnID()
	coll, ok := crdt.CollationFromName(pc.Collation)
	if !ok {
		// A custom (registered) collation has no built-in comparison
		// function a peer can reproduce, so reject rather than
		// silently diverge.
		return crdt.CatalogColumn{}, fmt.Errorf("column %q: COLLATE %s is not replicable (only BINARY, NOCASE, RTRIM are supported)", pc.Name, pc.Collation)
	}
	col := crdt.CatalogColumn{
		ID:         cid,
		Name:       pc.Name,
		Ordinal:    ordinal,
		Type:       pc.Type,
		NotNull:    pc.NotNull,
		Default:    pc.Default,
		IsPK:       pc.IsPK,
		ClockGroup: metadata.ClockGroupRow,
		Generated:  pc.Generated,
		Collation:  coll,
	}
	if metadata.CounterType(pc.Type) {
		if err := validateCounterColumn(pc); err != nil {
			return crdt.CatalogColumn{}, err
		}
		col.ClockGroup = metadata.ClockGroupCounter
	}
	if col.IsPK {
		col.PKPos = 1
	}
	return col, nil
}

// validateCounterColumn enforces the sqlite/docs/DDL.md#counter-columns admission
// rules on a COUNTER-typed column declaration. Counter cells merge by
// summation (CRDT.md F_counter), which needs a total, integer-valued
// cell with no identity semantics.
func validateCounterColumn(pc parsedColumn) error {
	if !metadata.IntAffinityType(pc.Type) {
		return fmt.Errorf("column %q: COUNTER requires INTEGER affinity (declare it INTEGER COUNTER)", pc.Name)
	}
	if !pc.NotNull {
		return fmt.Errorf("column %q: COUNTER requires NOT NULL (NULL + delta is NULL; declare NOT NULL DEFAULT 0)", pc.Name)
	}
	if pc.IsPK {
		return fmt.Errorf("column %q: a COUNTER column cannot be a PRIMARY KEY member", pc.Name)
	}
	if pc.IsUnique {
		return fmt.Errorf("column %q: a COUNTER column cannot be UNIQUE (a summed value has no stable identity to arbitrate)", pc.Name)
	}
	if pc.Generated {
		return fmt.Errorf("column %q: a COUNTER column cannot be GENERATED", pc.Name)
	}
	return nil
}

func nextOrdinal(cols []catalog.Column) int {
	max := -1
	for _, c := range cols {
		if c.Ordinal > max {
			max = c.Ordinal
		}
	}
	return max + 1
}

func hasPK(cols []crdt.CatalogColumn) bool {
	for _, c := range cols {
		if c.IsPK {
			return true
		}
	}
	return false
}

func pkPosition(pkCols []string, name string) int {
	for i, c := range pkCols {
		if c == name {
			return i + 1
		}
	}
	return 0
}

// validateUniqueColumnsByCols classifies a unique-key shape declared in a
// CREATE TABLE and reports whether it is coordinated (CP). A key whose
// members are all NOT NULL is coordinated — enforced by reservation
// before commit. BLOB members are rejected in both modes: the eventual
// apply conn cannot disable SQLite UNIQUE enforcement mid-blob_patch, and
// a coordinated key has no physical index anywhere, so nothing would stop
// sqlite3_blob_open from incrementally rewriting a key column whose
// blob-write fires carry no whole-value image for the reservation scan.
// Generated members are rejected: their values are recomputed per
// replica, never captured, so neither mode can gate or arbitrate them.
// Operates on the in-flight CREATE TABLE column list (Type / NotNull
// already populated from the parsed DDL).
func validateUniqueColumnsByCols(cols []crdt.CatalogColumn, names []string) (coordinated bool, err error) {
	coordinated = true
	for _, n := range names {
		var c *crdt.CatalogColumn
		for i := range cols {
			if cols[i].Name == n {
				c = &cols[i]
				break
			}
		}
		if c == nil {
			return false, fmt.Errorf("UNIQUE: column %q not in CREATE TABLE column list", n)
		}
		if c.ClockGroup == metadata.ClockGroupCounter {
			return false, fmt.Errorf("UNIQUE: column %q is a COUNTER column; a summed value has no stable identity to arbitrate", n)
		}
		if c.Generated {
			return false, fmt.Errorf("UNIQUE: column %q is GENERATED; generated columns cannot be members of a replicated unique key", n)
		}
		if isBlobType(c.Type) {
			return false, fmt.Errorf("UNIQUE: column %q has BLOB affinity; replicated unique keys cannot include BLOB columns", n)
		}
		if !c.NotNull {
			coordinated = false
		}
	}
	return coordinated, nil
}

// validateUniqueColumnsOnTable is the CREATE UNIQUE INDEX form. Reads
// nullability, declared type, and hidden-ness from app.db's
// pragma_table_xinfo because the in-memory catalog doesn't carry
// NotNull / Type after a metadata reload (xinfo, not table_info: the
// latter omits generated columns entirely, and a generated member must
// be rejected, not read as "column not in table"). Returns whether the
// key is coordinated (all members NOT NULL), with the same member rules
// as validateUniqueColumnsByCols.
func validateUniqueColumnsOnTable(app *sqlitebridge.Conn, tab *catalog.Table, names []string) (coordinated bool, err error) {
	stmt, _, err := app.Prepare(`SELECT name, type, "notnull", hidden FROM pragma_table_xinfo(?)`)
	if err != nil {
		return false, err
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, tab.Name); err != nil {
		return false, err
	}
	type colInfo struct {
		typ     string
		notNull bool
		hidden  int64
	}
	info := map[string]colInfo{}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return false, err
		}
		if !hasRow {
			break
		}
		info[stmt.ColumnText(0)] = colInfo{
			typ:     stmt.ColumnText(1),
			notNull: stmt.ColumnInt64(2) != 0,
			hidden:  stmt.ColumnInt64(3),
		}
	}
	coordinated = true
	for _, n := range names {
		ci, ok := info[n]
		if !ok {
			return false, fmt.Errorf("UNIQUE INDEX: column %q not in table %q", n, tab.Name)
		}
		if c, ok := tab.Column(n); ok && c.Counter() {
			return false, fmt.Errorf("UNIQUE INDEX: column %q is a COUNTER column; a summed value has no stable identity to arbitrate", n)
		}
		if ci.hidden != 0 {
			return false, fmt.Errorf("UNIQUE INDEX: column %q is GENERATED or hidden; such columns cannot be members of a replicated unique key", n)
		}
		if isBlobType(ci.typ) {
			return false, fmt.Errorf("UNIQUE INDEX: column %q has BLOB affinity; replicated unique keys cannot include BLOB columns", n)
		}
		if !ci.notNull {
			coordinated = false
		}
	}
	return coordinated, nil
}

// validateNoCoordinatedOverlap rejects a new coordinated key whose member
// columns exactly match an existing active coordinated key's, unless both
// carry predicates and the predicates differ (distinct partial scopes,
// e.g. per-tenant, are legitimate). Two reasons: an exact or total/partial
// overlap is redundant (the total key subsumes the partial), and catalog
// repair identifies the legacy predicate-less-shadow poison by exactly the
// total-beside-partial shape — admitting one would let repair silently
// drop a legitimate key.
func validateNoCoordinatedOverlap(tab *catalog.Table, members []crdt.CatalogKeyMember, newPred crdt.UniquePredicate, keyDesc string) error {
	for _, uk := range tab.UniqueKeys {
		if !uk.Coordinated || len(uk.Columns) != len(members) {
			continue
		}
		same := true
		for i := range members {
			if uk.Columns[i].ID != members[i].ColumnID {
				same = false
				break
			}
		}
		if !same {
			continue
		}
		if uk.Predicate.Root != nil && newPred.Root != nil &&
			!bytes.Equal(crdt.EncodeUniquePredicate(uk.Predicate), crdt.EncodeUniquePredicate(newPred)) {
			continue
		}
		return fmt.Errorf("UNIQUE key %s: a coordinated key on the same column(s) already exists; drop it first (a total key subsumes any partial key on those columns)", keyDesc)
	}
	return nil
}

// isBlobType reports whether a SQLite declared type maps to BLOB
// affinity. Per SQLite's affinity rules, "BLOB" anywhere in the
// declared type (case-insensitive) yields BLOB affinity. An empty
// declared type also yields BLOB affinity but we accept that — the
// concern is intentional BLOB use, not omitted types.
func isBlobType(declared string) bool {
	return strings.Contains(strings.ToUpper(declared), "BLOB")
}

func keyMembersFromColumnNames(tab *catalog.Table, names []string) ([]crdt.CatalogKeyMember, error) {
	out := make([]crdt.CatalogKeyMember, len(names))
	for i, n := range names {
		c, ok := tab.Column(n)
		if !ok {
			return nil, fmt.Errorf("UNIQUE: column %q not in table %q", n, tab.Name)
		}
		out[i] = crdt.CatalogKeyMember{ColumnID: c.ID, Ordinal: i}
	}
	return out, nil
}

func keyMembersFromColumnNamesByCols(cols []crdt.CatalogColumn, names []string) ([]crdt.CatalogKeyMember, error) {
	out := make([]crdt.CatalogKeyMember, len(names))
	for i, n := range names {
		var found bool
		for _, c := range cols {
			if c.Name == n {
				out[i] = crdt.CatalogKeyMember{ColumnID: c.ID, Ordinal: i}
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("UNIQUE: column %q not in CREATE TABLE column list", n)
		}
	}
	return out, nil
}

// wrapCascadeBundle returns op as-is when no cascade FK is declared,
// otherwise wraps it in an OpBundle with one OpCreateTrigger per
// cascade action. Each synth trigger carries the child TableID so
// receivers register the relationship in syzy_synth_trigger on apply.
func wrapCascadeBundle(op crdt.CatalogOp, tableName string, tableID crdt.TableID, p parsedDDL) crdt.CatalogOp {
	triggers := synthesizeCascadeTriggers(tableName, p.Columns, p.FKs)
	if len(triggers) == 0 {
		return op
	}
	sub := make([]crdt.CatalogOp, 0, 1+len(triggers))
	sub = append(sub, op)
	for _, t := range triggers {
		sub = append(sub, crdt.CatalogOp{
			Kind:       crdt.OpCreateTrigger,
			TableID:    tableID,
			ObjectName: t.Name,
			RawSQL:     t.SQL,
		})
	}
	return crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: sub}
}
