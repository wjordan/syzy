package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// Adoption (§10): bringing an EXISTING database into a cluster. Replication
// starts at the slot's LSN, so rows that were already committed when the slot
// was created have no CRDT clock and would never reach a peer — the database
// would look empty to everyone but this node. Adoption publishes them once, as
// ordinary Insert records at the first generation, and records that it did so.
//
// Ordering is what makes it safe: the slot exists before the snapshot is read,
// so every commit the snapshot misses is one the stream will deliver. There is
// no gap. A row committed DURING adoption can be in both halves; that is a
// duplicate, not a loss, and the two converge by stamp like any other pair of
// writes. With a self-log the overlap is dropped outright — the adoption entry
// carries the snapshot's LSN, so re-delivered commits at or below it are
// skipped exactly as they are after a restart.
//
// It is an explicit operator action (Config.Adopt / -adopt) rather than a
// heuristic: "this database has rows the cluster has never seen" and "this node
// restored from a peer and is about to catch up" look identical from here, and
// guessing wrong would republish an entire database as fresh writes.

// pgAdoptedKey marks the database as adopted, so -adopt is idempotent and a
// second run cannot republish rows peers have since changed.
const pgAdoptedKey = "pg_adopted"

// adoptBatchRows bounds one adoption changeset. Small enough that a large table
// does not build one enormous message, large enough that the per-changeset
// overhead disappears.
const adoptBatchRows = 500

// adoptExisting publishes every pre-existing row of the replicated tables, once.
func (e *Engine) adoptExisting(ctx context.Context) error {
	// The marker is what makes -adopt idempotent, and there is nowhere to put it
	// without a metadata store. Refuse rather than republish the whole database
	// on every restart.
	if e.cfg.Meta == nil {
		return fmt.Errorf("postgres: adoption requires a metadata store (-data-dir)")
	}
	if _, ok, err := e.cfg.Meta.GetMeta(pgAdoptedKey); err != nil {
		return fmt.Errorf("read adoption marker: %w", err)
	} else if ok {
		return nil // already adopted; not an error, so -adopt can stay in a unit file
	}
	var lsnText string
	if err := e.apply.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsnText); err != nil {
		return fmt.Errorf("adoption: read wal lsn: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return fmt.Errorf("adoption: parse wal lsn %q: %w", lsnText, err)
	}

	// One repeatable-read snapshot for every table, so the published state is a
	// single consistent cut of the database rather than a per-table smear.
	tx, err := e.apply.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("adoption: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var rows int
	for _, ti := range e.adoptableTables() {
		n, err := e.adoptTable(ctx, tx, ti, lsn)
		if err != nil {
			return err
		}
		rows += n
	}
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		return fmt.Errorf("adoption: close snapshot: %w", err)
	}
	if e.selfLog != nil {
		// Everything at or below the snapshot LSN is now published, so a commit
		// the stream re-delivers from that range is a duplicate to drop.
		e.orch.skipThrough = lsn
	}
	if err := e.cfg.Meta.SetMeta(pgAdoptedKey, []byte(lsnText)); err != nil {
		return fmt.Errorf("adoption: record marker: %w", err)
	}
	e.adoptedRows = rows
	return nil
}

// adoptableTables is the catalog in a stable order, so an adoption run's
// changesets are reproducible and a partial run resumes comparably.
func (e *Engine) adoptableTables() []*tableInfo {
	out := make([]*tableInfo, 0, len(e.cat.byID))
	for _, ti := range e.cat.byID {
		if len(ti.pk) > 0 {
			out = append(out, ti)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].schema != out[j].schema {
			return out[i].schema < out[j].schema
		}
		return out[i].name < out[j].name
	})
	return out
}

// adoptTable streams one table's rows out of the snapshot as Insert records.
func (e *Engine) adoptTable(ctx context.Context, tx pgx.Tx, ti *tableInfo, lsn pglogrepl.LSN) (int, error) {
	cols := make([]string, len(ti.cols))
	for i, c := range ti.cols {
		cols[i] = quoteIdent(c.name) + "::text"
	}
	sql := "SELECT " + joinComma(cols) + " FROM " + tableRef(ti) +
		" ORDER BY " + joinComma(pkIdents(ti))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("adoption: read %s: %w", ti.name, err)
	}
	defer rows.Close()

	var batch []crdt.Record
	var total int
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := e.publishAdopted(batch, lsn); err != nil {
			return err
		}
		batch = nil
		return nil
	}
	scanners := make([]any, len(ti.cols))
	nulls := make([]*string, len(ti.cols))
	for i := range scanners {
		scanners[i] = &nulls[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanners...); err != nil {
			return total, fmt.Errorf("adoption: scan %s: %w", ti.name, err)
		}
		image := make([]crdt.ColValue, len(ti.cols))
		for i, c := range ti.cols {
			if nulls[i] == nil {
				image[i] = crdt.ColValue{Column: c.cid, TypeTag: crdt.ColNull}
				continue
			}
			cv, err := encodeColValue(c.cid, c.typeName, []byte(*nulls[i]))
			if err != nil {
				return total, fmt.Errorf("adoption: encode %s.%s: %w", ti.name, c.name, err)
			}
			image[i] = cv
		}
		pkVals := make([]crdt.ColValue, len(ti.pk))
		for i, c := range ti.pk {
			for _, cv := range image {
				if cv.Column == c.cid {
					pkVals[i] = cv
					break
				}
			}
		}
		pk, err := pkBlobTyped(pkVals)
		if err != nil {
			return total, fmt.Errorf("adoption: %s primary key: %w", ti.name, err)
		}
		batch = append(batch, crdt.Insert{Table: ti.tid, PK: pk, CL: 1, Image: image})
		total++
		if len(batch) >= adoptBatchRows {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return total, fmt.Errorf("adoption: read %s: %w", ti.name, err)
	}
	return total, flush()
}

// publishAdopted turns one batch into a local changeset: stamped, clocked into
// the Cache, and handed to the same durability path a fold uses. Without a
// self-log it is held for Run to broadcast, so adoption works in every
// configuration rather than only the durable one.
func (e *Engine) publishAdopted(records []crdt.Record, lsn pglogrepl.LSN) error {
	stamp := crdt.Stamp{Clock: e.cfg.Cache.StampHLC(time.Now().UnixMilli()), Origin: e.cfg.Origin}
	dot := crdt.Dot{Origin: e.cfg.Origin, Seq: e.cfg.Cache.AllocSelfSeq(e.cfg.Origin)}
	deps := crdt.Deps{crdt.SchemaChain: crdt.Seq(e.schemaSeq.Load())}
	cs, err := crdt.Build(dot, stamp, deps, e.cfg.Cluster, records)
	if err != nil {
		return fmt.Errorf("adoption: build changeset: %w", err)
	}
	if e.selfLog != nil {
		payload := encodeSelfLogPayload(lsn, cs.Encoded())
		if _, _, err := e.selfLog.Append(journal.KindLocalDML, cs.Stamp.Clock.Pack(), uint64(cs.Dot.Origin), payload); err != nil {
			return fmt.Errorf("adoption: self-log append: %w", err)
		}
		if err := e.selfLog.Sync(); err != nil {
			return fmt.Errorf("adoption: self-log sync: %w", err)
		}
	} else {
		e.pendingAdopt = append(e.pendingAdopt, cs)
	}
	for _, r := range records {
		h := r.Header()
		e.cfg.Cache.PutRowState(h.Table, h.PK, crdt.RowState{CL: h.CL, Base: stamp})
	}
	return nil
}

func joinComma(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ", "
		}
		out += x
	}
	return out
}
