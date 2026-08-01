package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/schemalog"
)

// schemaCatchUpBatch bounds how many schema events catchUpSchema reads per call.
const schemaCatchUpBatch = 128

// appendDDLBundle is the originator's onDDLIntents hook (§6): it builds the
// committed transaction's intent rows into typed CatalogOps, wraps them in one
// Bundle, and appends that Bundle to the schema log at the node's current head.
// The whole transaction's schema change lands as one event, so a follower
// applies it atomically. buildCatalogOps mutates the local catalog (the OID⇄ID
// map) as it allocates ids — so this node already reflects its own DDL and
// catchUpSchema skips the event it just wrote (its seq is now schemaSeq).
//
// A pure-DDL or net-empty transaction that yields no ops appends nothing.
// Cross-node append contention (ErrHeadMoved) is surfaced loudly; serializing
// concurrent originators behind the DDL lease is a later increment.
func (e *Engine) appendDDLBundle(ctx context.Context, intents []ddlIntent) error {
	ops, err := buildCatalogOps(ctx, e.maint, e.cat, intents)
	if err != nil {
		if errors.Is(err, errUnsupportedDDL) {
			// The node committed a DDL it cannot put on the chain: its schema has
			// diverged. Record it durably and halt loudly (syzy_clone repairs) —
			// never silently skip, which would leave the local schema ahead of the
			// cluster. (Increment G's admission gate rejects these pre-commit, so
			// this post-commit halt becomes unreachable once it lands.)
			e.markSchemaUnhealthy(err.Error())
			return fmt.Errorf("%w: %s", ErrSchemaUnhealthy, err.Error())
		}
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	bundle := crdt.CatalogOp{Kind: crdt.OpBundle, SubOps: ops}
	encoded, err := crdt.EncodeCatalogOp(bundle)
	if err != nil {
		return fmt.Errorf("encode schema bundle: %w", err)
	}
	// Originator-side coordinated enforcement: the user's own index already
	// exists physically, but the gate triggers do not — install them before the
	// event is visible to the cluster. Runs on the apply (replica-role) session
	// so the trigger DDL doesn't spool a ddl intent; safe here because
	// appendDDLBundle runs on the orchestrator goroutine that owns that conn.
	for _, op := range ops {
		// A dropped key retires its triggers; an altered column changes the type
		// the reservation path encodes its values through.
		refresh := e.cat.coordUnique && (op.Kind == crdt.OpDropUniqueKey || op.Kind == crdt.OpAlterColumn)
		for _, k := range op.Keys {
			refresh = refresh || k.Coordinated
		}
		if refresh {
			if ti := e.cat.byID[op.TableID]; ti != nil {
				if err := ensureCoordinated(ctx, e.appl.conn, e.cat, ti); err != nil {
					return fmt.Errorf("coordinated key on %s: %w", ti.name, err)
				}
			}
		}
	}
	parent := e.schemaSeq.Load()
	raw := ddlAuditSQL(intents)
	seq, err := e.cfg.SchemaLog.Append(ctx, parent, encoded, raw)
	if errors.Is(err, schemalog.ErrHeadMoved) {
		// A concurrent originator advanced the chain head while our DDL was
		// committing (a partition outlasted the lease TTL, or no lease serializes
		// this cluster). Our DDL committed locally but the parent it was built on is
		// no longer head, and we do NOT rebase it (no commutativity check) — so this
		// node has diverged. Halt schema-unhealthy (§6 F, syzy_clone). The DDL lease
		// (§6 E) makes this rare; the parent-CAS already prevents SILENT divergence
		// (every node converges on the CAS winner's event, this loser halts loudly).
		// A fencing epoch that would pin the loss on a zombie holder rather than a
		// racing healthy peer is a follow-up — it changes only WHO halts, not whether
		// the cluster converges, since the CAS is the real serializer.
		reason := fmt.Sprintf("schema chain head moved past parent %d (concurrent DDL); local DDL cannot be appended", parent)
		e.markSchemaUnhealthy(reason)
		return fmt.Errorf("%w: %s", ErrSchemaUnhealthy, reason)
	}
	if err != nil {
		return fmt.Errorf("schemalog append (parent %d): %w", parent, err)
	}
	// Record the durable metadata catalog so a restart rebuilds the OID⇄ID map
	// without replaying the whole log (buildCatalogOps already mutated the
	// in-memory catalog). No-op when Meta is nil (schema-log-only tests).
	if err := e.persistSchemaEvent(bundle, seq, parent, encoded, raw); err != nil {
		return fmt.Errorf("persist schema bundle %d: %w", seq, err)
	}
	e.schemaSeq.Store(seq)
	return nil
}

// persistSchemaEvent records an applied schema event in the durable metadata
// catalog — the catalog rows (via the shared (*Tx).ApplyCatalogOp, same path
// the SQLite producer/broker use), the syzy_schema_event row, and schema_seq —
// all in one transaction. This is what lets Open rebuild the in-memory OID⇄ID
// map for DDL-created tables from metadata rather than re-deriving it from the
// schema log. No-op without a metadata store (the convergence tests run
// schema-log-only). Runs on the orchestrator goroutine, the sole metadata
// writer for schema state.
func (e *Engine) persistSchemaEvent(op crdt.CatalogOp, seq, parent uint64, encoded []byte, rawSQL string) error {
	if e.cfg.Meta == nil {
		return nil
	}
	return e.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
		if err := tx.ApplyCatalogOp(op, seq); err != nil {
			return err
		}
		// Persist the PG-local TableID→oid map alongside the catalog rows so
		// restore can rebind each table by oid (rename-invariant) instead of by
		// the possibly-stale recorded name. e.cat already reflects this op (the
		// caller mutated it before persisting), so this snapshot is current.
		if err := tx.SetMeta(pgTableOIDsKey, encodeTableOIDs(e.cat)); err != nil {
			return err
		}
		if err := tx.AppendSchemaEvent(metadata.SchemaEventEntry{
			SchemaSeq:   seq,
			ParentSeq:   parent,
			CatalogOp:   encoded,
			RawSQL:      rawSQL,
			AppliedAtUs: time.Now().UnixMicro(),
			ApplyState:  metadata.ApplyStateApplied,
		}); err != nil {
			return err
		}
		return tx.SetSchemaSeq(seq)
	})
}

// catchUpSchema applies every schema event above this node's schemaSeq, in
// order, advancing schemaSeq past each. It is the follower path: each event's
// Bundle is replayed via applyCatalogOp (structural DDL on the apply session +
// OID⇄ID map update). Idempotent — applyCatalogOp uses IF [NOT] EXISTS guards —
// so a re-run after a crash mid-batch is safe. Runs on the orchestrator/capture
// goroutine (the sole catalog writer).
func (e *Engine) catchUpSchema(ctx context.Context) error {
	if e.cfg.SchemaLog == nil {
		return nil
	}
	for {
		from := e.schemaSeq.Load()
		events, err := e.cfg.SchemaLog.Read(ctx, from, schemaCatchUpBatch)
		if errors.Is(err, schemalog.ErrBelowHorizon) {
			// This node fell behind the log's retention window — the events it needs
			// to catch up were compacted away. It cannot reconcile locally; the
			// universal repair is syzy_clone. Halt schema-unhealthy (§6 F).
			reason := fmt.Sprintf("schema_seq %d below the log retention horizon", from)
			e.markSchemaUnhealthy(reason)
			return fmt.Errorf("%w: %s", ErrSchemaUnhealthy, reason)
		}
		if err != nil {
			return fmt.Errorf("schemalog read (from %d): %w", from, err)
		}
		if len(events) == 0 {
			return nil
		}
		for _, ev := range events {
			op, err := crdt.DecodeCatalogOp(ev.CatalogOp)
			if err != nil {
				return fmt.Errorf("decode schema event %d: %w", ev.SchemaSeq, err)
			}
			if err := applyCatalogOp(ctx, e.appl.conn, e.cat, op, e.cfg.NodeOrdinal); err != nil {
				// A SQL-level failure to apply a SUPPORTED cluster DDL means this
				// node's physical schema cannot track the chain — a divergence.
				// Record the event failed_local and halt schema-unhealthy (syzy_clone,
				// §6 F). A transient (connection/ctx) error is NOT terminal: surface it
				// so the caller retries on the next catch-up rather than poisoning the
				// durable marker.
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					e.recordFailedLocal(ev)
					reason := fmt.Sprintf("apply schema event %d failed: %v", ev.SchemaSeq, pgErr.Message)
					e.markSchemaUnhealthy(reason)
					return fmt.Errorf("%w: %s", ErrSchemaUnhealthy, reason)
				}
				return fmt.Errorf("apply schema event %d: %w", ev.SchemaSeq, err)
			}
			if err := e.persistSchemaEvent(op, ev.SchemaSeq, ev.ParentSeq, ev.CatalogOp, ev.RawSQL); err != nil {
				return fmt.Errorf("persist schema event %d: %w", ev.SchemaSeq, err)
			}
			e.schemaSeq.Store(ev.SchemaSeq)
		}
	}
}

// recordFailedLocal durably records a schema event this node could not apply,
// in apply_state='failed_local' (§6 F / Schema Health). It is a diagnostic
// record of WHICH event diverged; the authoritative runtime/restart signal is
// the schema_unhealthy marker markSchemaUnhealthy writes alongside it. Best-effort
// (no-op without Meta); the in-memory + marker halt is what actually stops the node.
func (e *Engine) recordFailedLocal(ev schemalog.Event) {
	if e.cfg.Meta == nil {
		return
	}
	_ = e.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
		return tx.AppendSchemaEvent(metadata.SchemaEventEntry{
			SchemaSeq:   ev.SchemaSeq,
			ParentSeq:   ev.ParentSeq,
			CatalogOp:   ev.CatalogOp,
			RawSQL:      ev.RawSQL,
			AppliedAtUs: time.Now().UnixMicro(),
			ApplyState:  metadata.ApplyStateFailedLocal,
		})
	})
}

// ddlAuditSQL joins the transaction's audit queries for the schema event's
// RawSQL field (debug/audit only — the typed Bundle is authoritative).
func ddlAuditSQL(intents []ddlIntent) string {
	seen := make(map[string]bool, len(intents))
	var qs []string
	for _, in := range intents {
		if in.auditQuery == "" || seen[in.auditQuery] {
			continue
		}
		seen[in.auditQuery] = true
		qs = append(qs, in.auditQuery)
	}
	return strings.Join(qs, "\n")
}
