package postgres

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/unique"
)

// Coordinated unique keys, v1: leaseholder-routed writes (docs/postgres.md §8).
// Admission marks an all-NOT-NULL unique key Coordinated; every node holds the
// physical UNIQUE index; a gate trigger admits local coordinated-key writes
// only on the cluster's unique-write leaseholder. There is no per-value
// reservation RPC — the leaseholder node's own physical index is the
// serialization point, and the lease (ETag-CAS in the bucket, the same
// unique/lease object the SQLite leaseholder uses) elects that node.

//go:embed sql/coordinated.sql
var coordinatedSQL string

// ensureCoordGateSchema installs the gate table + trigger function. Runs on
// the apply (replica-role) session so the DDL doesn't spool a ddl intent.
func ensureCoordGateSchema(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, coordinatedSQL)
	return err
}

// coordIndexName is the follower-side physical index name for a coordinated
// key. The originator keeps the user's own index; followers create this one.
func coordIndexName(keyID [16]byte) string {
	return "syzy_uq_" + hex.EncodeToString(keyID[:8])
}

// ensureCoordinatedPhysical makes ti's coordinated keys physically enforced on
// this node: a UNIQUE index per coordinated key (skipped when a live unique
// index with the same column signature already exists — the originator's own)
// plus the gate triggers. Idempotent; runs on the apply (replica-role) session.
func ensureCoordinatedPhysical(ctx context.Context, conn *pgx.Conn, ti *tableInfo) error {
	var coordCols []*colInfo
	haveCoord := false
	for _, uk := range ti.uniqueKeys {
		if !uk.coordinated {
			continue
		}
		haveCoord = true
		coordCols = append(coordCols, uk.cols...)
	}
	if haveCoord {
		// One introspection covers all keys: signature → existing index OID.
		oids, err := liveUniqueIndexOIDs(ctx, conn, ti)
		if err != nil {
			return err
		}
		for _, uk := range ti.uniqueKeys {
			if !uk.coordinated {
				continue
			}
			if _, ok := oids[uniqueKeySig(uk.cols)]; ok {
				continue // physically enforced already (originator's index)
			}
			names := make([]string, len(uk.cols))
			for i, ci := range uk.cols {
				names[i] = quoteIdent(ci.name)
			}
			sql := "CREATE UNIQUE INDEX IF NOT EXISTS " + quoteIdent(coordIndexName(uk.keyID)) +
				" ON " + quoteIdent(appliedSchema) + "." + quoteIdent(ti.name) +
				" (" + strings.Join(names, ", ") + ")"
			if err := execDDLApply(ctx, conn, sql); err != nil {
				return fmt.Errorf("coordinated index on %s: %w", ti.name, err)
			}
		}
	}
	return ensureCoordGateTriggers(ctx, conn, ti, coordCols)
}

// ensureCoordGateTriggers (re)creates the two gate triggers on ti: INSERTs are
// always gated; UPDATEs only when a coordinated key column changes (the WHEN
// clause), so ordinary row updates stay leaseholder-free. With no coordinated
// keys left the triggers are dropped.
func ensureCoordGateTriggers(ctx context.Context, conn *pgx.Conn, ti *tableInfo, coordCols []*colInfo) error {
	tbl := quoteIdent(appliedSchema) + "." + quoteIdent(ti.name)
	for _, trig := range []string{"syzy_coord_gate_ins", "syzy_coord_gate_upd"} {
		if err := execDDLApply(ctx, conn, "DROP TRIGGER IF EXISTS "+trig+" ON "+tbl); err != nil {
			return err
		}
	}
	if len(coordCols) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var when []string
	for _, ci := range coordCols {
		if seen[ci.name] {
			continue
		}
		seen[ci.name] = true
		q := quoteIdent(ci.name)
		when = append(when, "NEW."+q+" IS DISTINCT FROM OLD."+q)
	}
	if err := execDDLApply(ctx, conn,
		"CREATE TRIGGER syzy_coord_gate_ins BEFORE INSERT ON "+tbl+
			" FOR EACH ROW EXECUTE FUNCTION public.syzy_coordinated_gate()"); err != nil {
		return err
	}
	return execDDLApply(ctx, conn,
		"CREATE TRIGGER syzy_coord_gate_upd BEFORE UPDATE ON "+tbl+
			" FOR EACH ROW WHEN ("+strings.Join(when, " OR ")+
			") EXECUTE FUNCTION public.syzy_coordinated_gate()")
}

// SetUniqueGate opens or closes this node's coordinated-write gate. ttl bounds
// how long an open gate outlives a dead sidecar: the trigger compares
// expires_at against Postgres' own clock, and the expiry is written as a
// relative interval, so object-store/PG clock skew cannot extend it.
func (e *Engine) SetUniqueGate(ctx context.Context, open bool, ttl time.Duration) error {
	if e.gateConn == nil {
		return fmt.Errorf("postgres: SetUniqueGate: coordinated unique not enabled")
	}
	_, err := e.gateConn.Exec(ctx,
		`UPDATE public.syzy_unique_gate SET open = $1, expires_at = now() + $2::interval`,
		open, ttl.String())
	return err
}

// Lease-loop tuning. TTL follows the SQLite leaseholder's order of magnitude;
// the gate expiry stays one renewal short of the lease so the gate always
// closes before another node can acquire.
const (
	uniqueLeaseTTL  = 15 * time.Second
	uniqueGateTTL   = uniqueLeaseTTL - uniqueLeaseTTL/3
	uniqueLeaseTick = uniqueLeaseTTL / 3
)

// RunUniqueLease contends for the cluster's unique-write lease and mirrors the
// outcome into this node's gate: held → open (refreshed each renewal), lost or
// unheld → closed. On first acquisition it waits drain before opening, giving
// a dead predecessor's in-flight coordinated writes time to arrive and land in
// the physical index (the same failover-drain assumption as the SQLite
// leaseholder's rebuild). Returns when ctx is done; the gate row's TTL closes
// the gate on its own if this process dies.
func (e *Engine) RunUniqueLease(ctx context.Context, ls *unique.LeaseStore, owner string, drain time.Duration) {
	var (
		rec  unique.LeaseRecord
		etag string
		held bool
	)
	closeGate := func() {
		// Runs on renewal failure AND on shutdown — ctx may already be
		// cancelled, so the close gets its own deadline.
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.SetUniqueGate(cctx, false, time.Hour); err != nil {
			slog.Warn("postgres: unique gate close failed (gate TTL will close it)", "err", err)
		}
	}
	for {
		nowUS := time.Now().UnixMicro()
		if held {
			nrec, netag, err := ls.Renew(ctx, rec, etag, nowUS, uniqueLeaseTTL.Microseconds())
			if err != nil {
				held = false
				closeGate()
				slog.Warn("postgres: unique lease renew failed; gate closed", "err", err)
			} else {
				rec, etag = nrec, netag
				if err := e.SetUniqueGate(ctx, true, uniqueGateTTL); err != nil && ctx.Err() == nil {
					slog.Warn("postgres: unique gate refresh failed", "err", err)
				}
			}
		} else {
			nrec, netag, err := ls.Acquire(ctx, owner, "", nowUS, uniqueLeaseTTL.Microseconds())
			if err == nil && nrec.Owner == owner {
				rec, etag, held = nrec, netag, true
				slog.Info("postgres: acquired unique-write lease", "generation", rec.Generation)
				// Failover drain: let a dead predecessor's shipped-but-unapplied
				// coordinated writes land before serving new ones.
				select {
				case <-time.After(drain):
				case <-ctx.Done():
					closeGate()
					_ = ls.Release(context.Background(), rec, etag)
					return
				}
				// Re-validate before opening: a stall during the drain (GC
				// pause, overloaded VM) can carry this process past the lease
				// expiry, and a successor may already hold it — opening a
				// stale gate here would make two writers. The Renew CAS both
				// proves the lease is still ours and restarts its TTL, so the
				// gate expiry set below is strictly inside the lease term.
				if nrec, netag, err = ls.Renew(ctx, rec, etag, time.Now().UnixMicro(), uniqueLeaseTTL.Microseconds()); err != nil {
					held = false
					slog.Warn("postgres: lease lost during failover drain; not opening gate", "err", err)
				} else {
					rec, etag = nrec, netag
					// A failed open is retried by the next tick's renewal
					// refresh; the gate's own TTL guarantees it can't be
					// stuck open.
					if err := e.SetUniqueGate(ctx, true, uniqueGateTTL); err != nil && ctx.Err() == nil {
						slog.Warn("postgres: unique gate open failed", "err", err)
					}
				}
			}
		}
		select {
		case <-time.After(uniqueLeaseTick):
		case <-ctx.Done():
			if held {
				closeGate()
				_ = ls.Release(context.Background(), rec, etag)
			}
			return
		}
	}
}
