package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// idPartitionBits is the per-node counter width in the bigint id space; the
// high bits select the node ordinal (1..2^16-1, 0 reserved), giving 65535 node
// slots × ~1.4e14 ids each (§6). 16 node bits + 47 counter bits = 63, leaving
// the sign bit clear so every minted id is positive.
const idPartitionBits = 47

// idSlice returns the [lo, hi] bigint id range owned by node ordinal: the high
// 16 bits hold the ordinal, the low idPartitionBits are its private counter.
// Computed in uint64 — ordinal+1 in uint16 would wrap the max ordinal (65535)
// to 0 and underflow hi; instead 65535 ⇒ hi = 2^63-1, the max positive bigint.
func idSlice(ordinal uint16) (lo, hi uint64) {
	return uint64(ordinal) << idPartitionBits, ((uint64(ordinal) + 1) << idPartitionBits) - 1
}

// pgQuerier is the subset of *pgx.Conn and pgx.Tx that the partition probes
// need, so the pristine check can run either on a bare connection or inside the
// partitioning transaction.
type pgQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// partitionSequences retunes the PK-backing sequences of the replicated tables
// to a node-disjoint slice of the bigint id space, so auto-increment PKs
// (bigserial / GENERATED … AS IDENTITY) minted on different nodes never collide
// (§6). Peer rows arrive with explicit, already-partitioned ids and apply
// upserts them directly, so a node's own sequence is only ever advanced by its
// own local inserts — within its own slice.
//
// This is the pre-created-schema form of the auto-increment story (the PK-only
// phase): every node runs identical schema and partitions its own sequence to
// its own ordinal, no coordination. A replicated CREATE TABLE does the same in
// applyCreateTable, which calls partitionTable on the follower's freshly-applied
// table; the originator leaves its sequence in the reserved [1, 2^47) low range.
//
// Only a bigint-width, pristine sequence on a bigint column is partitioned:
//   - the PK column AND its sequence must be bigint — bigint has the bits to
//     high-bit-partition, and a slice (ordinal << 47) overflows anything
//     narrower; an int4 serial is left alone (the step/offset fallback, which
//     caps node count, is a later mode — bigint is recommended).
//   - with a fixed schema, a used unpartitioned sequence is rejected rather
//     than rewound. DDL-originated tables retain the reserved low-range sequence
//     on their originator; followers are partitioned as the CREATE is applied.
//
// ordinal 0 disables partitioning (single-node / opt-out).
func partitionSequences(ctx context.Context, conn *pgx.Conn, cat *catalog, ordinal uint16, rejectUsed bool) error {
	if ordinal == 0 {
		return nil
	}
	lo, hi := idSlice(ordinal)
	for _, ti := range cat.byID {
		if err := partitionTable(ctx, conn, ti, lo, hi, rejectUsed); err != nil {
			return err
		}
	}
	return nil
}

// partitionTable retunes ti's pristine bigint PK sequences to [lo, hi]. It runs
// in two phases so a concurrent INSERT cannot slip an unpartitioned nextval
// between the pristine check and the RESTART: phase 1 finds candidate sequences
// without locking (so a table with no serial PK is never locked), phase 2 takes
// a table lock, re-checks pristine under it, then RESTARTs. EXCLUSIVE blocks
// writers (INSERT's ROW EXCLUSIVE conflicts) but not readers, and releases at
// commit — a brief, bootstrap-time pause.
func partitionTable(ctx context.Context, conn *pgx.Conn, ti *tableInfo, lo, hi uint64, rejectUsed bool) error {
	var cols []string // PK columns with pristine, unpartitioned bigint sequences
	for _, pk := range ti.pk {
		// The PK *column* must be bigint, not just its sequence: a high-bit
		// slice (ordinal << 47) overflows a narrower column. This rejects a
		// bigint sequence OWNED BY an int4/int2 column, which would otherwise
		// mint ids the column can't hold.
		if pk.typeName != "bigint" {
			continue
		}
		seq, ok, err := pkSequence(ctx, conn, ti, pk.name)
		if err != nil {
			return err
		}
		if !ok || seq.dataType != "bigint" || seq.partitioned(lo, hi) {
			continue
		}
		if !seq.pristine {
			if rejectUsed {
				return fmt.Errorf("%s.%s auto-ID sequence %s was used before node partitioning", ti.schema, ti.name, seq.name)
			}
			continue
		}
		cols = append(cols, pk.name)
	}
	if len(cols) == 0 {
		return nil // nothing to partition — no lock taken
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtable := quoteIdent(ti.schema) + "." + quoteIdent(ti.name)
	if _, err := tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s IN EXCLUSIVE MODE`, qtable)); err != nil {
		return fmt.Errorf("lock %s: %w", qtable, err)
	}
	for _, col := range cols {
		// Re-check under the lock: an INSERT that committed between phase 1 and
		// the lock may have advanced the sequence. Once we hold the lock no new
		// writer can, so a still-pristine sequence is safe to RESTART.
		seq, ok, err := pkSequence(ctx, tx, ti, col)
		if err != nil {
			return fmt.Errorf("recheck %s.%s: %w", qtable, col, err)
		}
		if !ok || seq.dataType != "bigint" || seq.partitioned(lo, hi) {
			continue
		}
		if !seq.pristine {
			if rejectUsed {
				return fmt.Errorf("%s.%s auto-ID sequence %s was used before node partitioning", ti.schema, ti.name, seq.name)
			}
			continue
		}
		// START must move with MINVALUE: ALTER validates the (unchanged) START
		// against the new MINVALUE, so setting MINVALUE alone on a START=1
		// sequence errors. RESTART sets the next value handed out.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`ALTER SEQUENCE %s START %d MINVALUE %d MAXVALUE %d RESTART %d INCREMENT 1`,
			seq.name, lo, lo, hi, lo)); err != nil {
			return fmt.Errorf("alter sequence %s: %w", seq.name, err)
		}
	}
	return tx.Commit(ctx)
}

type sequenceInfo struct {
	name                       string
	dataType                   string
	start, min, max, increment int64
	pristine                   bool
}

func (s sequenceInfo) partitioned(lo, hi uint64) bool {
	return s.start == int64(lo) && s.min == int64(lo) && s.max == int64(hi) && s.increment == 1
}

// pkSequence resolves the sequence backing column col of table ti. ok is false
// when the column has no owned sequence (a plain key, uuid, etc.).
func pkSequence(ctx context.Context, q pgQuerier, ti *tableInfo, col string) (info sequenceInfo, ok bool, err error) {
	qtable := quoteIdent(ti.schema) + "." + quoteIdent(ti.name)
	var seqName *string
	if err = q.QueryRow(ctx, `SELECT pg_get_serial_sequence($1, $2)`, qtable, col).Scan(&seqName); err != nil {
		return sequenceInfo{}, false, err
	}
	if seqName == nil {
		return sequenceInfo{}, false, nil
	}
	info.name = *seqName
	var isCalled bool
	if err = q.QueryRow(ctx, fmt.Sprintf(
		`SELECT seqtypid::regtype::text, seqstart, seqmin, seqmax, seqincrement,
		        (SELECT is_called FROM %s)
		 FROM pg_sequence WHERE seqrelid = $1::regclass`, *seqName), *seqName).
		Scan(&info.dataType, &info.start, &info.min, &info.max, &info.increment, &isCalled); err != nil {
		return sequenceInfo{}, false, err
	}
	info.pristine = !isCalled
	return info, true, nil
}
