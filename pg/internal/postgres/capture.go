package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/engine"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
)

// metaKeyCaptureLSN names the metadata-store key holding the slot LSN that
// the persisted Cache state covers (§2 recovery checkpoint). It is written
// in the same metadata tx as the Cache snapshot, so the pair is atomic; Open
// advances the slot's confirmed_flush to it so re-delivery resumes exactly
// where the snapshot ends.
const metaKeyCaptureLSN = "pg_capture_lsn"

// defaultCheckpointEvery bounds the commits between durable checkpoints when
// Config.CheckpointEvery is unset — a slot-WAL-retention / replay tradeoff.
const defaultCheckpointEvery = 64

// capturer streams the slot, decodes pgoutput into Changesets against the
// shared Cache (Seq, HLC, CL), and hands each committed transaction to the
// sink. It mirrors the SQLite producer's syncer.MetaSink, sourced from
// logical decoding instead of the touch journal.
type capturer struct {
	cfg  Config
	cat  *catalog
	prog *progress

	// winners is the engine's shared winner-repair stash (§9 Option A,
	// winners.go): apply records each peer-applied LWW winner here and the
	// fold checks it so a losing local write self-corrects instead of
	// shipping. nil-safe (a nil stash never reports a winner).
	winners *winnerStash

	// pruneConn deletes consumed syzy_ddl_intent rows after delivery (§6). A
	// plain connection, not origin-tagged: the intent relation has no catalog
	// entry, so capture already drops its deletes — and using the apply conn
	// would risk a capture↔apply deadlock. nil when DDL is not enabled.
	pruneConn *pgx.Conn
	ddlHWM    int64 // highest pruned syzy_ddl_intent seq (self-healing prune, §6)

	// onDDLIntents, when set, receives a committed transaction's decoded DDL
	// command descriptors (§6) before they are pruned — appendDDLBundle when a
	// schema log is configured. nil disables DDL handling.
	onDDLIntents func(context.Context, []ddlIntent) error

	// schemaSeq, when set, is the node's current schema-log head; foldCommit
	// stamps it as a built changeset's Deps[SchemaChain] so a follower holds the
	// DML until its catalog has caught up to that schema event (§6). nil ⇒ 0
	// (the pre-DDL phase, where every node shares a static bootstrap schema).
	schemaSeq *atomic.Uint64

	mu        sync.Mutex
	confirmed pglogrepl.LSN // last LSN acked to the slot
}

// runOpts are internal knobs for tests; Run uses the zero value (ack every
// committed txn, run until ctx is cancelled).
type runOpts struct {
	startLSN   pglogrepl.LSN // 0 ⇒ resume from confirmed_flush_lsn
	ackThrough int           // ack only the first N committed txns; 0 ⇒ all
	noAck      bool          // never advance the slot
	stopAfter  int           // stop after N committed txns; 0 ⇒ until ctx cancel
	// confirmedLSN, when set, is the live orchestrator's "shipped" position:
	// capture reports it (not its own per-commit flushLSN) as confirmed_flush and
	// does no checkpointing of its own (the orchestrator owns both). It is how the
	// slot advances only past commits whose changeset is durable in the self-log.
	confirmedLSN *atomic.Uint64
}

// draftProcess consumes one committed transaction's decoded draft, returning
// emitted=true when it produced a non-empty changeset (drives stopAfter). It is
// the single seam between decode and fold: the inline folder (commitTxn) folds
// immediately on the capture goroutine, while the live orchestrator's enqueue
// defers the fold to its one goroutine so the Cache has a single writer.
type draftProcess func(ctx context.Context, t *txnAccum) (emitted bool, err error)

func (c *capturer) Run(ctx context.Context, sink engine.Sink) error {
	return c.run(ctx, func(ctx context.Context, t *txnAccum) (bool, error) {
		return c.commitTxn(ctx, sink, t)
	}, runOpts{})
}

// Checkpoint returns the last LSN acked to the slot, as an opaque Marker.
func (c *capturer) Checkpoint() (engine.Marker, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return []byte(c.confirmed.String()), nil
}

// Ack advances the slot's confirmed_flush to the marker position via
// pg_replication_slot_advance on a normal connection. The running loop already
// acks in-stream after each delivered changeset (the standby-status path); Ack
// is for an orchestrator that drives the resume position while Run is idle, so
// the slot must be inactive.
func (c *capturer) Ack(m engine.Marker) error {
	lsn, err := pglogrepl.ParseLSN(string(m))
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, c.cfg.ConnURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, c.cfg.Slot, lsn.String()); err != nil {
		return err
	}
	c.mu.Lock()
	c.confirmed = lsn
	c.mu.Unlock()
	return nil
}

func (c *capturer) checkpointEvery() int {
	if c.cfg.CheckpointEvery > 0 {
		return c.cfg.CheckpointEvery
	}
	return defaultCheckpointEvery
}

// checkpoint persists the Cache's dirty state and the capture LSN it covers
// in one metadata transaction (§2). Called between committed txns from the
// single-threaded run loop, so the snapshot covers exactly the commits up to
// lsn — never more — which is what lets re-delivery resume cleanly from lsn.
// The caller acks the slot to lsn only after this returns.
func (c *capturer) checkpoint(lsn pglogrepl.LSN) error {
	snap := c.cfg.Cache.SnapshotIncremental()
	if err := c.cfg.Meta.WithTx(func(tx *metadata.Tx) error {
		if err := nodestate.WriteSnapshot(tx, snap); err != nil {
			return err
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(lsn))
		return tx.SetMeta(metaKeyCaptureLSN, b[:])
	}); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	c.cfg.Cache.ClearSnapshotDirty(snap)
	return nil
}

// relEntry is a cached pgoutput Relation. For a user table it carries only the
// relation OID and the tuple column names — NOT a resolved tableInfo: capture
// (the decode goroutine) must not read the catalog, because the orchestrator
// mutates it on DDL (§6 D4 resolution-at-fold). foldCommit resolves the OID →
// tableInfo and the names → columns on the orchestrator goroutine, in commit
// order, so a CREATE is folded (table added) before the DML that depends on it.
// The intent relation (syzy_ddl_intent, §6) is still recognized by name here
// and decoded at capture time — its rows carry stable ids directly and never
// touch the user catalog.
type relEntry struct {
	oid       uint32   // relation OID; resolved to a tableInfo at fold (user tables)
	colNames  []string // tuple column names in pgoutput order
	ddlIntent bool     // the syzy_ddl_intent relation (§6)
	intentIdx map[string]int
}

// txnAccum accumulates one in-flight decoded transaction. Capture records the
// raw user-table tuples (rawOps) without touching the catalog; foldCommit
// resolves and collapses them into the net per-row effect (order/rows) on the
// orchestrator goroutine. The collapse mirrors syncer/materialize.go: a
// PK-change update splits into delete(oldPK)+insert(newPK), so the per-row
// collapse only ever sees primitive insert/update/delete on a single key. Seq
// and the HLC stamp are consumed at commit only if a net effect survives, so a
// transaction touching only non-replicated tables (or a pure no-op) costs
// neither.
type txnAccum struct {
	commitMs int64
	endLSN   pglogrepl.LSN

	// rawOps are the transaction's user-table row changes in decode order, not
	// yet resolved to stable ids (the catalog is read only at fold). order/rows
	// are the collapsed net effect foldCommit builds from rawOps.
	rawOps []rawRowOp
	order  []rowKey // first-touch order, for deterministic emit
	rows   map[rowKey]*rowAccum

	// DDL command descriptors captured from syzy_ddl_intent rows (§6), in
	// command order; ddlIntentSeqs are the consumed rows to prune.
	ddlIntents    []ddlIntent
	ddlIntentSeqs []int64
}

type rowKey struct {
	tid crdt.TableID
	pk  string
}

type rowAccum struct {
	tid     crdt.TableID
	pk      crdt.PKBlob
	firstOp byte // 'i' | 'u' | 'd' at first touch this txn
	lastOp  byte
	image   []crdt.ColValue // latest new image (insert/update); nil for delete
	// oldImage is the row's image at FIRST touch this transaction, decoded from
	// the REPLICA IDENTITY FULL old tuple. Only cell-group tables carry it: the
	// net changed-column set is (first old → last new), so a row updated twice in
	// one transaction ships one record for the combined effect (§8).
	oldImage []crdt.ColValue
}

// feed records one primitive op on a row, updating its net first/last effect.
// Non-delete images are MERGED by column, not overwritten: pgoutput elides
// unchanged-TOAST columns ('u') from an UPDATE, so an INSERT-then-UPDATE in
// one txn must keep the insert's columns the update omitted — otherwise the
// collapsed Insert would ship a truncated row.
func (t *txnAccum) feed(tid crdt.TableID, pk crdt.PKBlob, op byte, image, oldImage []crdt.ColValue) {
	if t.rows == nil {
		t.rows = map[rowKey]*rowAccum{}
	}
	k := rowKey{tid, string(pk)}
	a := t.rows[k]
	if a == nil {
		a = &rowAccum{tid: tid, pk: pk, firstOp: op, oldImage: oldImage}
		t.rows[k] = a
		t.order = append(t.order, k)
	}
	a.lastOp = op
	if op == 'd' {
		a.image = nil
	} else {
		a.image = mergeImage(a.image, image)
	}
}

// mergeImage overlays overlay's columns onto base (overlay wins per column,
// base columns absent from overlay are retained), preserving base's column
// order and appending new columns.
func mergeImage(base, overlay []crdt.ColValue) []crdt.ColValue {
	if base == nil {
		return overlay
	}
	out := make([]crdt.ColValue, len(base))
	copy(out, base)
	at := make(map[crdt.ColumnID]int, len(out))
	for i, c := range out {
		at[c.Column] = i
	}
	for _, c := range overlay {
		if i, ok := at[c.Column]; ok {
			out[i] = c
		} else {
			at[c.Column] = len(out)
			out = append(out, c)
		}
	}
	return out
}

func (c *capturer) run(ctx context.Context, process draftProcess, opts runOpts) error {
	conn, err := pgconn.Connect(ctx, c.cfg.ReplConnURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Pin the same canonical GUCs as the apply session so pgoutput's text
	// output matches what apply expects (§4). A replication=database
	// connection runs SQL normally before START_REPLICATION.
	if err := pinCanonicalGUCs(func(sql string) error {
		return conn.Exec(ctx, sql).Close()
	}); err != nil {
		return fmt.Errorf("pin GUCs: %w", err)
	}

	pluginArgs := []string{
		"proto_version '4'",
		fmt.Sprintf("publication_names '%s'", c.cfg.Publication),
		"origin 'none'", // loopback filter: drop changes carrying any origin
	}
	if err := pglogrepl.StartReplication(ctx, conn, c.cfg.Slot, opts.startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	rels := map[uint32]*relEntry{}
	var cur *txnAccum
	var flushLSN pglogrepl.LSN
	var lastCommitLSN pglogrepl.LSN
	commits := 0  // committed txns seen (incl. empty: prune-deletes, no-ops)
	emitted := 0  // changesets delivered to the sink (stopAfter counts these)
	lastCkpt := 0 // commits count at the last durable checkpoint
	nextDeadline := time.Now().Add(time.Second)

	sendStandby := func() error {
		if opts.noAck {
			return nil
		}
		lsn := flushLSN
		if opts.confirmedLSN != nil {
			// Live orchestrator mode: report the shipped position — the highest
			// commit whose changeset is fsynced in the self-log — so confirmed_flush
			// never releases WAL for an un-shipped commit.
			lsn = pglogrepl.LSN(opts.confirmedLSN.Load())
		}
		// Record confirmed only AFTER the send succeeds: Checkpoint() returns it as
		// the acked marker, so a failed send must not advance it past what the slot
		// actually received.
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
			pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn, WALFlushPosition: lsn}); err != nil {
			return err
		}
		c.mu.Lock()
		c.confirmed = lsn
		c.mu.Unlock()
		return nil
	}
	// flush is durable mode's only path that advances the slot: it persists
	// the Cache snapshot + capture LSN atomically, then acks confirmed_flush
	// to that LSN. So the slot never runs ahead of the persisted Cache.
	flush := func() error {
		if c.cfg.Meta == nil || opts.noAck || commits == lastCkpt {
			return nil
		}
		if err := c.checkpoint(lastCommitLSN); err != nil {
			return err
		}
		lastCkpt = commits
		flushLSN = lastCommitLSN
		return sendStandby()
	}

	for {
		if opts.stopAfter != 0 && emitted >= opts.stopAfter {
			if err := flush(); err != nil {
				return err
			}
			return nil
		}
		if time.Now().After(nextDeadline) {
			if err := sendStandby(); err != nil {
				return err
			}
			nextDeadline = time.Now().Add(time.Second)
		}
		rctx, cancel := context.WithDeadline(ctx, nextDeadline)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			// Check the parent ctx first: when it is cancelled by deadline,
			// the error is DeadlineExceeded, which pgconn.Timeout also reports
			// true for — so testing Timeout first would loop forever.
			if ctx.Err() != nil {
				if opts.confirmedLSN != nil {
					// Live orchestrator mode: ack the shipped position one last time
					// so confirmed_flush is as current as the self-log on the way
					// out; the orchestrator persists the snapshot itself.
					fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
					lsn := pglogrepl.LSN(opts.confirmedLSN.Load())
					_ = pglogrepl.SendStandbyStatusUpdate(fctx, conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: lsn, WALFlushPosition: lsn})
					fcancel()
					return nil
				}
				// Persist uncheckpointed progress on the way out and align the
				// slot with it, so a re-run resumes cleanly even without a fresh
				// Open. The standby ack rides a fresh context (the parent is
				// cancelled) on the still-open conn; if even that fails, Open's
				// realign on the next start recovers from the persisted LSN.
				if c.cfg.Meta != nil && !opts.noAck && commits != lastCkpt {
					if err := c.checkpoint(lastCommitLSN); err == nil {
						c.mu.Lock()
						c.confirmed = lastCommitLSN
						c.mu.Unlock()
						fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = pglogrepl.SendStandbyStatusUpdate(fctx, conn, pglogrepl.StandbyStatusUpdate{
							WALWritePosition: lastCommitLSN, WALFlushPosition: lastCommitLSN})
						fcancel()
					}
				}
				return nil
			}
			if pgconn.Timeout(err) {
				continue // our own per-message deadline elapsed; keep polling
			}
			return fmt.Errorf("receive: %w", err)
		}
		cd, ok := raw.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return err
			}
			c.prog.advance(ka.ServerWALEnd) // no replicable data ≤ here
			if ka.ReplyRequested {
				if err := sendStandby(); err != nil {
					return err
				}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return err
			}
			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical msg: %w", err)
			}
			switch m := msg.(type) {
			case *pglogrepl.RelationMessage:
				rels[m.RelationID] = c.buildRelEntry(m)
			case *pglogrepl.BeginMessage:
				cur = &txnAccum{commitMs: m.CommitTime.UnixMilli()}
			case *pglogrepl.InsertMessage:
				switch e := rels[m.RelationID]; {
				case e != nil && e.ddlIntent:
					c.accumDDLIntent(cur, e, m)
				default:
					c.accumInsert(cur, e, m)
				}
			case *pglogrepl.UpdateMessage:
				c.accumUpdate(cur, rels[m.RelationID], m)
			case *pglogrepl.DeleteMessage:
				c.accumDelete(cur, rels[m.RelationID], m)
			case *pglogrepl.CommitMessage:
				if cur != nil {
					cur.endLSN = m.TransactionEndLSN
					emit, err := process(ctx, cur)
					if err != nil {
						return err
					}
					commits++
					if emit {
						emitted++
					}
					lastCommitLSN = cur.endLSN
					switch {
					case opts.noAck:
						// never advance the slot (deterministic test capture)
					case opts.confirmedLSN != nil:
						// Live orchestrator mode: it owns checkpointing and drives
						// confirmed_flush via the periodic standby (which reports the
						// shipped position). Nothing to advance per-commit.
					case c.cfg.Meta != nil:
						// Durable mode: advance only at a checkpoint, between
						// txns, so the snapshot covers exactly ≤ lastCommitLSN.
						if commits-lastCkpt >= c.checkpointEvery() {
							if err := flush(); err != nil {
								return err
							}
						}
					case opts.ackThrough == 0 || commits <= opts.ackThrough:
						flushLSN = cur.endLSN
						if err := sendStandby(); err != nil {
							return err
						}
					}
				}
				cur = nil
			}
			// Advance the gate watermark to this message's LSN. Safe because for
			// LOGICAL decoding xld.ServerWALEnd is the record's OWN lsn (Postgres
			// sets walEnd == dataStart == lsn — NOT the server's flush head, which
			// is the physical-replication meaning), and txns stream in commit
			// order with a commit frame handed to process (commitTxn or the
			// orchestrator's enqueue, above) BEFORE this advance. So prog.lsn ≥
			// some commit lsn_c implies that commit is already processed — folded
			// into the Cache inline, or at least queued so the gate's drain folds
			// it before arbitrating. Any frame reaching lsn_c is either lsn_c's own
			// commit frame (processed first) or a later txn's frame (streamed after
			// lsn_c's commit). Non-leading frames carry lsn < their own commit, so
			// they can never vault prog past an unprocessed commit.
			c.prog.advance(xld.ServerWALEnd)
		}
	}
}

func (c *capturer) buildRelEntry(m *pglogrepl.RelationMessage) *relEntry {
	if m.RelationName == ddlIntentTableName {
		e := &relEntry{
			ddlIntent: true,
			intentIdx: make(map[string]int, len(m.Columns)),
		}
		for i, col := range m.Columns {
			e.intentIdx[col.Name] = i
		}
		return e
	}
	// User table: record only the OID + column names. Resolution to a tableInfo
	// is deferred to foldCommit (the orchestrator goroutine), so capture never
	// reads the mutable catalog.
	e := &relEntry{oid: m.RelationID}
	for _, col := range m.Columns {
		e.colNames = append(e.colNames, col.Name)
	}
	return e
}

// rawCol is one pgoutput tuple column: the data-type byte ('n' null, 't' text,
// 'u' unchanged-TOAST) and a COPY of the bytes (the receive buffer is reused on
// the next message, and a raw op outlives its decode).
type rawCol struct {
	kind byte
	data []byte
}

// rawRowOp is one user-table row change, recorded at decode without catalog
// resolution. oldTup is present for a delete, and for an update whose REPLICA
// IDENTITY changed (a PK change); newTup is nil for a delete.
type rawRowOp struct {
	oid      uint32
	colNames []string
	op       byte // 'i' | 'u' | 'd'
	newTup   []rawCol
	oldTup   []rawCol
}

func copyTuple(t *pglogrepl.TupleData) []rawCol {
	if t == nil {
		return nil
	}
	out := make([]rawCol, len(t.Columns))
	for i, tc := range t.Columns {
		out[i] = rawCol{kind: tc.DataType, data: append([]byte(nil), tc.Data...)}
	}
	return out
}

func (c *capturer) accumInsert(cur *txnAccum, e *relEntry, m *pglogrepl.InsertMessage) {
	if cur == nil || e == nil {
		return
	}
	cur.rawOps = append(cur.rawOps, rawRowOp{oid: e.oid, colNames: e.colNames, op: 'i', newTup: copyTuple(m.Tuple)})
}

func (c *capturer) accumUpdate(cur *txnAccum, e *relEntry, m *pglogrepl.UpdateMessage) {
	if cur == nil || e == nil {
		return
	}
	cur.rawOps = append(cur.rawOps, rawRowOp{oid: e.oid, colNames: e.colNames, op: 'u', newTup: copyTuple(m.NewTuple), oldTup: copyTuple(m.OldTuple)})
}

func (c *capturer) accumDelete(cur *txnAccum, e *relEntry, m *pglogrepl.DeleteMessage) {
	if cur == nil || e == nil {
		return
	}
	cur.rawOps = append(cur.rawOps, rawRowOp{oid: e.oid, colNames: e.colNames, op: 'd', oldTup: copyTuple(m.OldTuple)})
}

// decodeRawTuple resolves a recorded tuple against the table's catalog entry —
// on the fold goroutine — into (image, pkBlob). Values are encoded in the
// canonical cross-engine form (value.go): typed SQLite storage classes, and
// the PK identity in the catalog.EncodePK byte layout — so a SQLite peer's
// clocks key the same row bytes. Columns marked unchanged-TOAST ('u') or
// unreplicated (not in the catalog) are omitted from the image. An unparsable
// value is an engine bug or corruption (the text came from Postgres itself)
// and fails the fold loudly.
func decodeRawTuple(ti *tableInfo, colNames []string, cols []rawCol) (image []crdt.ColValue, pk crdt.PKBlob, err error) {
	pkCVs := make([]crdt.ColValue, len(ti.pk))
	for i, rc := range cols {
		if i >= len(colNames) {
			break
		}
		col := ti.byName[colNames[i]]
		if col == nil {
			continue
		}
		switch rc.kind {
		case 'n':
			image = append(image, crdt.ColValue{Column: col.cid, TypeTag: crdt.ColNull})
		case 't':
			cv, cerr := encodeColValue(col.cid, col.typeName, rc.data)
			if cerr != nil {
				return nil, nil, fmt.Errorf("decode %s.%s: %w", ti.name, col.name, cerr)
			}
			image = append(image, cv)
			if col.isPK {
				pkCVs[pkIndexOf(ti, col)] = cv
			}
		case 'u':
			continue
		}
	}
	pk, perr := pkBlobTyped(pkCVs)
	if perr != nil {
		return nil, nil, perr
	}
	return image, pk, nil
}

func pkIndexOf(ti *tableInfo, col *colInfo) int {
	for i, p := range ti.pk {
		if p == col {
			return i
		}
	}
	return 0
}

// foldCommit assigns CLs and the HLC stamp + Seq against the shared Cache,
// builds one Changeset from the transaction's net per-row effects, and folds
// the local row state. It returns nil — consuming neither Seq nor HLC — when
// the net effect is empty (only non-replicated tables, an insert+delete that
// cancels, or an ignored intent-prune delete). This is the Cache-mutating step
// the orchestrator will own; capture decodes into the txnAccum draft,
// foldCommit turns it into a changeset + Cache state. localFold carries the
// per-fold follow-up work returned to the caller; nil when the commit was
// empty.
type localFold struct {
	// selfCorrect is the per-row repair work the orchestrator must execute on
	// the apply conn after this fold: for each entry, UPSERT the winner image
	// to the local table so the row converges to the cluster's known winner.
	// Populated when a local record's (CL, Stamp) lost LWW against a stash
	// from Cache.Winner — winner-repair Option A, docs/postgres.md §9. The
	// corresponding record is dropped from the outbound changeset and its
	// RowState is not staged (the winner's stamp stays in the Cache).
	selfCorrect []selfCorrectOp
}

// stagedClock is one row's clock effect from this fold, held until the
// changeset is built: the row state to publish and whether the prior
// generation's per-column overrides go with it.
type stagedClock struct {
	state      crdt.RowState
	clearCells bool
}

// selfCorrectOp is one row's winner-repair write: the orchestrator UPSERTs
// image into table at pk via the apply conn, repairing a local loser.
type selfCorrectOp struct {
	tid   crdt.TableID
	pk    crdt.PKBlob
	image []crdt.ColValue
	// The audit facts for the local write this repair discards (§9): the values
	// dropped, and the two stamps that arbitrated.
	lost     []crdt.ColValue
	winner   crdt.Stamp
	winnerCL uint64
	loser    crdt.Stamp
	loserCL  uint64
}

func (c *capturer) foldCommit(t *txnAccum) (*crdt.Changeset, *localFold, error) {
	// Resolve the raw user-table ops against the catalog and collapse them into
	// the net per-row effect (order/rows). This is the catalog read deferred from
	// decode (§6 D4): it runs here on the single fold goroutine, after any DDL in
	// this txn has been applied to the catalog, so a row on a just-created table
	// resolves. A relation not (yet) in the catalog is skipped — not replicated.
	for _, raw := range t.rawOps {
		ti := c.cat.byOID[raw.oid]
		if ti == nil {
			continue
		}
		switch raw.op {
		case 'i':
			image, pk, err := decodeRawTuple(ti, raw.colNames, raw.newTup)
			if err != nil {
				return nil, nil, err
			}
			t.feed(ti.tid, pk, 'i', image, nil)
		case 'u':
			image, newPK, err := decodeRawTuple(ti, raw.colNames, raw.newTup)
			if err != nil {
				return nil, nil, err
			}
			// OldTuple is present iff the PK changed (default REPLICA IDENTITY) or
			// the table is in the cell clock group (FULL, which logs the whole old
			// row — the per-column diff baseline, §8). A PK change is
			// delete(oldPK)+insert(newPK) (§3).
			var oldImage []crdt.ColValue
			if raw.oldTup != nil {
				old, oldPK, err := decodeRawTuple(ti, raw.colNames, raw.oldTup)
				if err != nil {
					return nil, nil, err
				}
				if !bytes.Equal(oldPK, newPK) {
					t.feed(ti.tid, oldPK, 'd', nil, nil)
					t.feed(ti.tid, newPK, 'i', image, nil)
					continue
				}
				oldImage = old
			}
			t.feed(ti.tid, newPK, 'u', image, oldImage)
		case 'd':
			_, pk, err := decodeRawTuple(ti, raw.colNames, raw.oldTup)
			if err != nil {
				return nil, nil, err
			}
			t.feed(ti.tid, pk, 'd', nil, nil)
		}
	}

	records := make([]crdt.Record, 0, len(t.order))
	// CLs are assigned against the Cache, staged so multiple effects on one
	// key (rare: PK-change collisions) compose; the stamp is allocated once,
	// only if some effect survives the net-effect collapse. A cell-group
	// partial-column update stages per-column stamps instead of a row baseline
	// (cellWrites), exactly as the peers applying that record will (§8).
	staged := map[rowKey]stagedClock{}
	var cellWrites []cellStamp
	var stamp crdt.Stamp
	stampOnce := func() crdt.Stamp {
		if stamp.IsZero() {
			stamp = crdt.Stamp{Clock: c.cfg.Cache.StampHLC(t.commitMs), Origin: c.cfg.Origin}
		}
		return stamp
	}
	rowStateOf := func(k rowKey) crdt.RowState {
		if sc, ok := staged[k]; ok {
			return sc.state
		}
		return c.cfg.Cache.RowState(k.tid, []byte(k.pk))
	}

	var selfCorrect []selfCorrectOp
	for _, k := range t.order {
		a := t.rows[k]
		ti := c.cat.table(a.tid)
		op := a.lastOp
		if op == 'd' && a.firstOp == 'i' {
			continue // created and removed within the txn — net no-op
		}
		rs := rowStateOf(k)
		var cl uint64
		var rec crdt.Record
		isRecreate := false // d-then-i pair: emits a preceding Delete and always dominates any Insert-shaped stash
		cellUpdate := false // cell-group UPDATE: arbitrates (and stamps) per column
		switch {
		case op == 'd':
			cl = rs.NextTombCL()
			if cl == rs.CL { // not currently live; tombstone at the next generation
				cl = rs.CL + 2 - rs.CL%2
			}
			rec = crdt.Delete{Table: a.tid, PK: a.pk, CL: cl}
		case a.firstOp == 'd':
			// Delete then re-insert of the same PK: tombstone the old generation
			// and relive at the next, matching the SQLite producer's PK-change
			// collapse (a recreate). A single Insert would leave the generation
			// unchanged.
			tomb := rs.NextTombCL()
			if tomb == rs.CL {
				tomb = rs.CL + 2 - rs.CL%2
			}
			cl = crdt.RowState{CL: tomb}.NextLiveCL()
			isRecreate = true
			records = append(records, crdt.Delete{Table: a.tid, PK: a.pk, CL: tomb})
			rec = crdt.Insert{Table: a.tid, PK: a.pk, CL: cl, Image: a.image}
		case a.firstOp == 'u':
			cl = rs.NextLiveCL()
			changed := a.image
			if ti != nil && ti.cellGroup() {
				// The payload unit must match the arbitration unit: a cell-group
				// receiver arbitrates each carried column on its own, so carrying a
				// column this transaction did not change would stomp a concurrent
				// disjoint write at our stamp. Ship the diff (counter columns as
				// summable contributions).
				diff, err := cellChanged(ti, a.oldImage, a.image)
				if err != nil {
					return nil, nil, err
				}
				if len(diff) == 0 {
					continue // no column actually changed — nothing to replicate
				}
				changed = diff
				cellUpdate = true
			}
			rec = crdt.Update{Table: a.tid, PK: a.pk, CL: cl, Changed: changed}
		default: // brand-new live row
			cl = rs.NextLiveCL()
			rec = crdt.Insert{Table: a.tid, PK: a.pk, CL: cl, Image: a.image}
		}
		// Winner-repair (§9 Option A): if a peer-applied stash dominates this
		// local (cl, stamp), the local write would ship as a loser and the
		// cluster's known winner sits in our Cache but its IMAGE never got
		// written to our table (apply wrote it; the app's subsequent UPDATE
		// overwrote it). Drop the loser, schedule a self-correct UPSERT of the
		// winner image, and don't stage RowState (the winner stamp stays). The
		// d-then-i recreate pair always bumps CL past any Insert-shaped stash
		// (CL ≥ rs.CL+1), so it isn't gated here.
		if !isRecreate {
			if w, ok := c.winners.winner(a.tid, a.pk); ok {
				local := crdt.RowState{CL: cl, Base: stampOnce()}
				if local.DominatedBy(w.CL, w.Stamp) {
					// A cell-group loss is per column: only the columns the winning
					// record actually carried are lost, and the rest of this write
					// still wins on every peer, so only those are repaired + dropped.
					if upd, isUpd := rec.(crdt.Update); isUpd && cellUpdate {
						kept, lost := splitCellLosers(upd.Changed, w.Cols)
						if len(lost) > 0 {
							selfCorrect = append(selfCorrect, selfCorrectOp{
								tid: a.tid, pk: a.pk, image: repairImage(ti, w.Image, lost),
								lost: lost, winner: w.Stamp, winnerCL: w.CL, loser: stampOnce(), loserCL: cl,
							})
						}
						if len(kept) == 0 {
							continue
						}
						upd.Changed = kept
						rec = upd
					} else {
						selfCorrect = append(selfCorrect, selfCorrectOp{tid: a.tid, pk: a.pk, image: w.Image,
							lost: localValues(rec), winner: w.Stamp, winnerCL: w.CL, loser: stampOnce(), loserCL: cl})
						continue
					}
				} else {
					// Local dominates — winner stash is stale; peers will adopt this write.
					c.winners.clear(a.tid, a.pk)
				}
			}
		}
		records = append(records, rec)
		if upd, isUpd := rec.(crdt.Update); isUpd && cellUpdate && !crdt.CoversAllNonPK(ti, upd.Changed) {
			// Partial coverage: advance only the carried columns' stamps. The row
			// baseline stays put so a concurrent remote write to another column
			// still merges in; a generation bump leaves Base zero, mirroring the
			// receiver-side rule (internal/broker applyCellUpdate).
			sc := stagedClock{state: rs}
			if cl > rs.CL {
				sc = stagedClock{state: crdt.RowState{CL: cl}, clearCells: true}
			}
			staged[k] = sc
			for _, v := range upd.Changed {
				if v.Format == crdt.FormatDelta {
					continue // counter cells carry no stamp — they sum
				}
				cellWrites = append(cellWrites, cellStamp{tid: a.tid, pk: a.pk, col: v.Column, stamp: stampOnce()})
			}
			continue
		}
		// Row group, or a cell-group write covering every column (collapse): the
		// write defines the whole row — absorb it into the baseline.
		staged[k] = stagedClock{state: crdt.RowState{CL: cl, Base: stampOnce()}, clearCells: rs.Cells != nil}
	}
	if len(records) == 0 {
		if len(selfCorrect) == 0 {
			return nil, nil, nil
		}
		// Self-correct-only fold: every local effect lost to a stashed winner, so
		// there is no changeset to broadcast. The caller still runs the repair
		// UPSERTs from lf.selfCorrect against the apply conn, and capture picks
		// the UPSERTed bytes up on the next decode + folds them at the now-higher
		// stamp (a normal local commit that will dominate the winner stash).
		return nil, &localFold{selfCorrect: selfCorrect}, nil
	}

	dot := crdt.Dot{Origin: c.cfg.Origin, Seq: c.cfg.Cache.AllocSelfSeq(c.cfg.Origin)}
	var schemaSeq crdt.Seq
	if c.schemaSeq != nil {
		schemaSeq = crdt.Seq(c.schemaSeq.Load())
	}
	cs, err := crdt.Build(dot, stampOnce(), crdt.Deps{crdt.SchemaChain: schemaSeq}, c.cfg.Cluster, records)
	if err != nil {
		return nil, nil, err
	}
	// Commit staged clocks to the shared Cache before delivery (mirrors
	// syncer/sink.go): row states first, then the per-column overrides.
	for k, sc := range staged {
		if c.cfg.Cache.PutRowState(k.tid, []byte(k.pk), sc.state) && sc.clearCells {
			c.cfg.Cache.ClearCellsForRow(k.tid, []byte(k.pk))
		}
	}
	for _, cw := range cellWrites {
		c.cfg.Cache.PutCellStamp(cw.tid, cw.pk, cw.col, cw.stamp)
	}
	return cs, &localFold{selfCorrect: selfCorrect}, nil
}

// commitTxn folds a decoded transaction into the Cache (foldCommit), then
// delivers the built changeset and prunes the consumed intent rows.
// emitted=false when the net effect was empty. The fold is split out as the
// step the orchestrator will own; sink + prune stay on the capture side.
func (c *capturer) commitTxn(ctx context.Context, sink engine.Sink, t *txnAccum) (emitted bool, err error) {
	cs, _, err := c.foldCommit(t)
	if err != nil {
		return false, err
	}
	// DDL command descriptors (§6) are handled BEFORE the DML sink, so a hook
	// failure can't leave the DML half-delivered for the same committed txn, and
	// because a schema change logically precedes the DML that targets it (mixed
	// DDL+DML lands as schema-then-DML). A pure-DDL transaction produces no
	// changeset but still carries intent rows; prune is self-healing (a missed
	// prune only leaves dead rows). (True mixed-txn atomicity — one Bundle + one
	// changeset as a unit — is increment D's job in the live path; this
	// deterministic path is tests.)
	if len(t.ddlIntents) > 0 {
		if c.onDDLIntents != nil {
			if err := c.onDDLIntents(ctx, t.ddlIntents); err != nil {
				return false, err
			}
		}
		c.pruneDDLIntents(ctx, t.ddlIntentSeqs)
	}
	if cs != nil {
		if err := sink(ctx, cs); err != nil {
			return false, err
		}
	}
	return cs != nil, nil
}
