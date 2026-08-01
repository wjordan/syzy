// Package postgres is the Postgres adapter: it satisfies the internal/engine
// ports over stock Postgres 17+ logical decoding (pgoutput) and pgx, with no
// server fork or extension. See docs/postgres.md.
//
// Scope (phase 2, per §11): PK-only DML, text-format values, capture+apply
// over the shared nodestate.Cache. Secondary-UNIQUE / NOT NULL UNIQUE
// arbitration (§5), DDL replication (§6), and lazy restore (§8) are later
// phases. Stable table/column IDs are introspected from pg_catalog here; the
// schema-log catalog replaces that under DDL replication.
package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/engine"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/pg/internal/pgwire"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/unique"
)

// selfLogSegmentSize bounds each self-origin-log segment file; the active
// segment is retained across a compaction checkpoint, so it also bounds the
// changeset bytes replayed on the next restart.
const selfLogSegmentSize = 1 << 20

// progress is capture's "decoded up to" watermark, read by the orchestrator's
// catch-up gate (drainToWALTarget): before arbitrating a remote write, the
// orchestrator folds every local draft up to the current WAL head into the
// Cache, so a remote apply can't win LWW against a committed-but-not-yet-folded
// local write and diverge (the async-decoding hazard SQLite's synchronous hooks
// don't have). Capture advances it as it decodes; the orchestrator only reads it.
type progress struct {
	lsn atomic.Uint64 // highest server WAL position capture has processed
}

func (p *progress) advance(lsn pglogrepl.LSN) {
	for {
		cur := p.lsn.Load()
		if uint64(lsn) <= cur || p.lsn.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

func (p *progress) load() pglogrepl.LSN { return pglogrepl.LSN(p.lsn.Load()) }

// Config configures one Postgres adapter bound to one database.
type Config struct {
	Name    string         // node label (logs)
	Origin  crdt.Origin    // this node's origin id
	Cluster crdt.ClusterID // cluster UUID (mis-route defense)
	Cache   *nodestate.Cache

	ConnURL     string // normal connection (apply writes + introspection)
	ReplConnURL string // replication=database connection (capture)

	// Cluster-global names (slots are unique cluster-wide; an origin is
	// acquired exclusively by one session), so callers derive them per node.
	Publication string
	Slot        string
	OriginName  string

	Tables []string // schema-qualified replicated set (PK-only phase)

	// Adopt publishes the rows that already existed when the replication slot
	// was created, once, so an EXISTING database can join a cluster (§10). It is
	// idempotent (a durable marker records that it ran) and deliberately an
	// explicit operator action — see adopt.go for why it is never inferred.
	Adopt bool

	// NodeOrdinal is the compact node id (1..2^16-1; 0 disables) that scopes
	// auto-increment PK partitioning (§6): each node retunes its bigint
	// sequences to a disjoint slice of the id space so bigserial/IDENTITY PKs
	// never collide across masters. Must be unique per live node and not reused
	// while a departed node's ids may still exist.
	NodeOrdinal uint16

	// Meta, when set, makes capture durable across restart (§2). Open
	// rehydrates the Cache from it (Seq/HLC/row state/frontier) and the
	// capture loop checkpoints — Cache snapshot + capture LSN persisted
	// atomically, then the slot acked to that LSN — so the slot's
	// confirmed_flush never runs ahead of the persisted Cache and re-derived
	// txns get identical Dots. nil ⇒ no durable recovery (eager per-txn ack;
	// the convergence/idempotency tests, which control acking explicitly).
	Meta *metadata.Store
	// CheckpointEvery bounds committed txns between durable checkpoints (slot
	// WAL-retention vs. replay-on-restart). 0 ⇒ defaultCheckpointEvery.
	// Ignored when Meta is nil.
	CheckpointEvery int

	// JournalDir, when set (and Meta is set), enables the self-origin log: the
	// live orchestrator appends each shipped local changeset's exact bytes here
	// and fsyncs before the slot's confirmed_flush advances past that commit
	// (pg-coordination-model §3). It is the durability boundary for live mode —
	// recovery replays these exact bytes rather than re-deriving Dot/Stamp, which
	// is what keeps a node convergent with peers across a restart when its local
	// folds interleaved with remote applies. Without it the live loop runs noAck
	// (the slot stays pinned; non-durable).
	JournalDir string

	// DDL, when set, installs the syzy_ddl_intent spool + ddl_command_end /
	// sql_drop event triggers so ordinary CREATE/ALTER/DROP is captured for
	// replication (§6). Off by default while the DDL surface is built in
	// increments (the gate/lease/schema-log append are later steps).
	DDL bool

	// SchemaLog, when set (with DDL), is the cluster schema-event log (§6): a
	// captured DDL transaction is built into one Bundle CatalogOp and appended
	// here, and a follower applies events read from it (catchUpSchema). nil ⇒
	// DDL is captured into typed ops but neither appended nor cross-node applied
	// (the increment-C local-build surface). Cross-node serialization (the lease)
	// is a later increment; until then SchemaLog must have a single originator.
	SchemaLog schemalog.Log

	// Lease, when set (with DDL + SchemaLog), serializes DDL cluster-wide (§6
	// increment E) so multiple nodes can originate schema changes safely. A
	// ddl_command_start gate blocks the first DDL of a transaction until this
	// node holds the lease and has applied pending peer DDL, so appends never
	// conflict. nil ⇒ single-originator (a concurrent peer DDL would surface
	// ErrHeadMoved on append).
	Lease Lease

	// Mirror, when set, is the per-origin journal store this node appends every
	// applied REMOTE changeset's wire bytes to (own-origin bytes stay in the
	// self-log). Together they let Engine.CatchupSource serve peer gap-fill
	// requests for any (origin, seq) this node has produced or applied — parity
	// with the SQLite mirror. nil ⇒ no remote-origin peer catchup.
	Mirror *mirror.Manager

	// GapFiller, when set, enables anti-entropy: Run plans missing (origin,
	// seq) ranges from the Cache and pulls them from peers (fetcher.go). Live
	// broadcast is best-effort, so without one a missed delivery is a
	// permanent gap. nil ⇒ live-delivery only.
	GapFiller transport.GapFiller

	// TipSource, when set, augments gap planning with externally-known origin
	// tips (e.g. the cluster's object store) so a node returning from offline
	// discovers origins it never saw live. Only consulted when GapFiller is
	// set.
	TipSource transport.TipSource

	// OnPublished, when set, receives every locally-folded changeset's exact
	// wire bytes as the publisher ships it (recovery re-ships included; the
	// consumer must dedupe idempotently — sealer.Sealer.OnEncoded does). This
	// is the feed that makes the cluster's object store hold this origin's
	// full history.
	OnPublished func(payload []byte)

	// SealedSelfSeq, when set with OnPublished, reports the highest self-
	// origin seq the sealer has made bucket-durable (sealer.UploadedSeq).
	// Checkpoint retention never truncates the self-log past it, and the
	// fetcher's TipSource snapshot gates mirror-segment truncation the same
	// way — the SQLite node's rule: GC follows object-store seal, never mere
	// delivery.
	SealedSelfSeq func() crdt.Seq

	// CoordinatedUnique enables CP unique keys (docs/postgres.md §7):
	// admission marks an all-NOT-NULL UNIQUE key Coordinated and the engine
	// enforces it by reserve-before-commit against Registry. No node holds a
	// physical index for such a key. Requires Registry and ReserveSocketDir;
	// without them NOT NULL UNIQUE stays rejected at DDL admission.
	CoordinatedUnique bool

	// Registry is the cluster's coordinated-uniqueness backend. Required
	// when CoordinatedUnique.
	Registry unique.Registry

	// ReserveSocketDir is a directory this process and the Postgres server
	// both see, in which the reservation endpoint binds its socket. The name
	// inside it is libpq's own (".s.PGSQL.<port>"), because the writer's
	// trigger reaches the endpoint through dblink, which builds a libpq
	// connection. Required when CoordinatedUnique.
	ReserveSocketDir string

	// ReservePort names the socket within ReserveSocketDir. Any value works
	// — the directory is the sidecar's — and it defaults to 5432.
	ReservePort int
}

// Engine is the Postgres adapter. It owns the apply connection and the
// introspected catalog; capture uses its own replication connection.
type Engine struct {
	cfg     Config
	apply   *pgx.Conn
	maint   *pgx.Conn // DDL intent-row pruning (§6); nil when DDL is disabled
	cat     *catalog
	prog    *progress
	capt    *capturer
	appl    *applier
	orch    *orchestrator
	selfLog *journal.Journal // self-origin durable log (§3); nil when disabled

	// pendingAdopt holds adoption changesets when there is no self-log to carry
	// them; Run broadcasts them before entering the loop (§10). adoptedRows is
	// the count, for the operator-facing log line.
	pendingAdopt []*crdt.Changeset
	adoptedRows  int

	// winners is the runtime-only winner-repair stash (§9 Option A,
	// winners.go), shared by the apply path (writer) and the fold path
	// (reader) — both on the orchestrator goroutine.
	winners *winnerStash

	// reserveSrv answers coordinated-key reservations from Postgres backends
	// blocked in their commit. nil unless Config.CoordinatedUnique.
	reserveSrv *pgwire.Server

	// enumConn is the leaseholder enumeration's own read-only session. It
	// exists because that enumeration runs on the lease-maintenance
	// goroutine while the orchestrator owns the apply conn, and a pgx
	// connection carries one session's state — sharing would be a race.
	enumConn *pgx.Conn

	// schemaSeq is the highest schema-log event this node has originated or
	// applied (§6); the parent for the next append and the floor for catch-up.
	// Touched only on the capture/orchestrator goroutine.
	schemaSeq atomic.Uint64

	// schemaUnhealthy is set when this node committed a DDL it cannot put on the
	// schema chain (§6 F): its physical schema has diverged and the only repair
	// is syzy_clone. Once set the orchestrator halts and Open refuses to resume.
	schemaUnhealthy atomic.Bool
}

// Open connects, ensures the publication / slot / replication origin exist
// (idempotently), configures the apply session, and introspects the catalog.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Cache == nil {
		return nil, fmt.Errorf("postgres: Config.Cache is required")
	}
	apply, err := pgx.Connect(ctx, cfg.ConnURL)
	if err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, apply: apply}

	if err := ensurePublication(ctx, apply, cfg.Publication); err != nil {
		e.Close()
		return nil, err
	}
	if err := ensureOrigin(ctx, apply, cfg.OriginName); err != nil {
		e.Close()
		return nil, err
	}
	// Pin the GUCs that shape text-format values so the same logical value
	// round-trips to identical bytes on every node (§4).
	if err := pinCanonicalGUCs(func(sql string) error { _, err := apply.Exec(ctx, sql); return err }); err != nil {
		e.Close()
		return nil, fmt.Errorf("pin GUCs: %w", err)
	}
	// Apply session: replica role suppresses triggers/FK; the origin tags
	// every commit so the capture slot's origin='none' filter drops it.
	if _, err := apply.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		e.Close()
		return nil, fmt.Errorf("replica role: %w", err)
	}
	if _, err := apply.Exec(ctx, `SELECT pg_replication_origin_session_setup($1)`, cfg.OriginName); err != nil {
		e.Close()
		return nil, fmt.Errorf("origin session: %w", err)
	}
	if err := ensureSlot(ctx, cfg.ReplConnURL, cfg.Slot); err != nil {
		e.Close()
		return nil, err
	}

	// Node-local support objects: the syzy_counter domain must exist before the
	// catalog is introspected (a bootstrap table may declare a counter column)
	// and before any counter-bearing changeset applies; the conflict log must
	// exist before the first apply can lose an arbitration.
	if err := installSidecarTables(ctx, apply); err != nil {
		e.Close()
		return nil, fmt.Errorf("install sidecar tables: %w", err)
	}

	cat, err := introspectCatalog(ctx, apply, cfg.Tables)
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("introspect: %w", err)
	}
	e.cat = cat
	cat.coordUnique = cfg.CoordinatedUnique
	if cfg.CoordinatedUnique {
		if err := e.startReservationEndpoint(ctx, cat); err != nil {
			e.Close()
			return nil, err
		}
	}
	e.prog = &progress{}
	e.winners = newWinnerStash()
	e.capt = &capturer{cfg: cfg, cat: cat, prog: e.prog, winners: e.winners}
	e.appl = &applier{cfg: cfg, cat: cat, conn: apply, winners: e.winners}
	e.orch = newOrchestrator(e.capt, e.appl, e.prog)
	e.orch.mirror = cfg.Mirror
	e.orch.selfOrigin = cfg.Origin
	e.orch.gapFiller = cfg.GapFiller
	e.orch.tipSource = cfg.TipSource
	e.orch.onPublished = cfg.OnPublished
	e.orch.sealedSelfSeq = cfg.SealedSelfSeq

	// Node-disjoint auto-increment (§6): partition this node's PK sequences so
	// bigserial/IDENTITY ids don't collide across masters. Sequence changes are
	// not logically decoded, so this stays local (never replicated).
	if err := partitionSequences(ctx, apply, cat, cfg.NodeOrdinal); err != nil {
		e.Close()
		return nil, fmt.Errorf("partition sequences: %w", err)
	}

	// DDL replication (§6): install the syzy_ddl_intent spool + event triggers
	// last, so the publication/slot setup above (themselves DDL) committed
	// before the triggers exist and so write no intent rows.
	if cfg.DDL {
		if err := installDDLSupport(ctx, apply); err != nil {
			e.Close()
			return nil, err
		}
		// When a schema log is configured, a captured DDL transaction is built
		// into one Bundle and appended (the originator path). buildCatalogOps
		// introspects pg_catalog on the maint conn (created below), on the same
		// capture goroutine that runs the hook — sequential, no lock.
		if cfg.SchemaLog != nil {
			e.capt.onDDLIntents = e.appendDDLBundle
			e.capt.schemaSeq = &e.schemaSeq
			e.orch.schemaSeq = &e.schemaSeq
			e.orch.catchUp = e.catchUpSchema
			e.appl.schemaSeq = &e.schemaSeq

			// DDL lease gate (§6 E): serialize cross-node DDL. The gate trigger
			// blocks the first DDL of a txn until this node holds the lease and is
			// caught up; a watcher on its own connection drives it (Run starts it).
			if cfg.Lease != nil {
				if err := installDDLGate(ctx, apply); err != nil {
					e.Close()
					return nil, fmt.Errorf("install ddl gate: %w", err)
				}
				gateConn, err := pgx.Connect(ctx, cfg.ConnURL)
				if err != nil {
					e.Close()
					return nil, fmt.Errorf("ddl gate conn: %w", err)
				}
				e.orch.gate = &gateManager{
					lease:      cfg.Lease,
					holder:     cfg.OriginName,
					conn:       gateConn,
					connURL:    cfg.ConnURL,       // reconnect + re-close if the gate conn dies
					catchUpReq: e.orch.catchUpReq, // route catch-up to the sole catalog writer
					poll:       ddlGatePoll,
					hbEvery:    ddlGateHeartbeat,
				}
				// Shut the gate now, before Open returns, so no DDL runs ungated in
				// the window before Run starts the watcher.
				if err := e.orch.gate.closeGate(ctx); err != nil {
					e.Close()
					return nil, fmt.Errorf("close ddl gate: %w", err)
				}
			}
		}
	}
	// The DDL intent path prunes consumed rows on a dedicated plain connection —
	// separate from the apply conn to avoid a capture↔apply deadlock.
	if cfg.DDL {
		maint, err := pgx.Connect(ctx, cfg.ConnURL)
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("intent prune conn: %w", err)
		}
		e.maint = maint
		e.capt.pruneConn = maint
	}

	// Durable recovery (§2): rehydrate the Cache (Seq/HLC/row state/frontier)
	// and realign the slot to the LSN that persisted state covers, so capture
	// resumes deterministically and never re-derives an already-folded txn.
	if cfg.Meta != nil {
		if err := cfg.Cache.LoadFromMeta(cfg.Meta); err != nil {
			e.Close()
			return nil, fmt.Errorf("rehydrate cache: %w", err)
		}
		lsn, ok, err := loadCaptureLSN(cfg.Meta)
		if err != nil {
			e.Close()
			return nil, err
		}
		if ok && lsn > 0 {
			// confirmed_flush ≤ persisted pg_capture_lsn (meta is written
			// before the slot ack), so this only advances it — to exactly the
			// snapshot's coverage point. START_REPLICATION then resumes from
			// confirmed_flush and re-delivers only txns strictly above it.
			if _, err := apply.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, cfg.Slot, lsn.String()); err != nil {
				e.Close()
				return nil, fmt.Errorf("realign slot: %w", err)
			}
			e.capt.confirmed = lsn
		}
	}

	// Self-origin log (§3): the durability boundary for live mode. Open it,
	// replay it into the now-rehydrated Cache (restoring row state / HLC / seq
	// from the exact shipped bytes), and seed the orchestrator's dedup boundary +
	// shipped position from its head so the slot can re-confirm already-shipped
	// commits and re-delivered ones below the head are dropped.
	if cfg.Meta != nil && cfg.JournalDir != "" {
		if err := os.MkdirAll(cfg.JournalDir, 0o755); err != nil {
			e.Close()
			return nil, fmt.Errorf("self-log dir: %w", err)
		}
		j, err := journal.Open(cfg.JournalDir, selfLogSegmentSize, journal.SyncOn)
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("open self-log: %w", err)
		}
		head, err := recoverSelf(cfg.Cache, j)
		if err != nil {
			_ = j.Close()
			e.Close()
			return nil, err
		}
		e.selfLog = j
		e.orch.selfLog = j
		e.orch.skipThrough = head
		e.orch.shipped.Store(uint64(head))
	}

	// Refuse to resume a node that durably recorded a schema divergence (§6 F):
	// it committed a DDL it cannot put on the chain, so its catalog is unsafe and
	// the repair is syzy_clone. Resuming would only re-hit the same rejection.
	if reason, unhealthy, err := loadSchemaHealth(cfg.Meta); err != nil {
		e.Close()
		return nil, fmt.Errorf("load schema health: %w", err)
	} else if unhealthy {
		e.Close()
		return nil, fmt.Errorf("%w: %s", ErrSchemaUnhealthy, reason)
	}

	// Rebuild the DDL-created-table catalog + schema_seq from the durable
	// metadata catalog (§6 F), so a restart resumes at the persisted schema head
	// instead of replaying the whole schema log. No-op without Meta/SchemaLog.
	if err := e.restoreSchemaCatalog(ctx); err != nil {
		e.Close()
		return nil, fmt.Errorf("restore schema catalog: %w", err)
	}
	// Startup catch-up (mirrors the SQLite producer's startup recovery): apply
	// any schema-log events above the restored schema_seq before capture runs.
	// This reconciles the persisted catalog with the cluster head and, on the
	// originator, replays its own appended-but-unpersisted event (crashed between
	// append and persist) as a follower would — so the re-delivered intent rows
	// then build no ops (the table is already cataloged) instead of re-appending.
	// Single-threaded here (the orchestrator goroutine starts only in Run).
	if err := e.catchUpSchema(ctx); err != nil {
		e.Close()
		return nil, fmt.Errorf("startup schema catch-up: %w", err)
	}
	// Adoption (§10): publish the rows that predate the slot, once, on explicit
	// operator request. After the self-log is open, so the publication is as
	// durable as any local write.
	if cfg.Adopt {
		if err := e.adoptExisting(ctx); err != nil {
			e.Close()
			return nil, err
		}
	}
	return e, nil
}

// loadCaptureLSN reads the persisted slot-coverage LSN (metaKeyCaptureLSN).
func loadCaptureLSN(m *metadata.Store) (pglogrepl.LSN, bool, error) {
	b, ok, err := m.GetMeta(metaKeyCaptureLSN)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(b) != 8 {
		return 0, false, fmt.Errorf("postgres: corrupt %s (%d bytes)", metaKeyCaptureLSN, len(b))
	}
	return pglogrepl.LSN(binary.BigEndian.Uint64(b)), true, nil
}

func (e *Engine) Capture() engine.Capture { return e.capt }
func (e *Engine) Applier() engine.Applier { return e.appl }

// Run drives the node as a single serialized actor: it starts capture (decode
// only) and, on one goroutine, folds local commits and applies inbound peer
// changesets — making it the sole writer of the Cache.
// inbox delivers decoded peer changesets; broadcast ships locally-folded
// changesets to the transport. It returns when ctx is cancelled.
func (e *Engine) Run(ctx context.Context, inbox <-chan *crdt.Changeset, broadcast engine.Sink) error {
	// Adoption changesets held for want of a self-log go out first, so peers see
	// the pre-existing rows before any write that builds on them (§10).
	for _, cs := range e.pendingAdopt {
		if err := broadcast(ctx, cs); err != nil {
			return fmt.Errorf("postgres: broadcast adopted rows: %w", err)
		}
	}
	e.pendingAdopt = nil
	return e.orch.Run(ctx, inbox, broadcast)
}

// Close closes the apply connection. It deliberately does NOT drop the
// replication slot: the slot's confirmed_flush_lsn is the durable resume
// position, so dropping it on shutdown would skip every local commit between
// the old position and a freshly-created slot's start. Slot teardown is an
// explicit admin/test action (DropSlot).
func (e *Engine) Close() error {
	if e.orch != nil && e.orch.gate != nil {
		e.orch.gate.stop() // idempotent — a no-op if Run already stopped it
	}
	if e.selfLog != nil {
		_ = e.selfLog.Close()
		e.selfLog = nil
	}
	if e.maint != nil {
		_ = e.maint.Close(context.Background())
		e.maint = nil
	}
	if e.reserveSrv != nil {
		_ = e.reserveSrv.Close()
		e.reserveSrv = nil
	}
	if e.enumConn != nil {
		_ = e.enumConn.Close(context.Background())
		e.enumConn = nil
	}
	if e.apply == nil {
		return nil
	}
	err := e.apply.Close(context.Background())
	e.apply = nil
	return err
}

// DropSlot removes the replication slot. For admin teardown / tests only;
// destroys the durable resume position. A missing slot is not an error
// (idempotent); an active slot surfaces Postgres's "is active" error, since
// dropping a slot in use would be wrong.
func (e *Engine) DropSlot(ctx context.Context) error {
	if _, err := e.apply.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, e.cfg.Slot); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42704" { // undefined_object
			return nil
		}
		return err
	}
	return nil
}

// canonicalGUCs are the session settings that make text-format output stable
// and OID-free across nodes (§4). Pinned identically on the apply session and
// the replication (capture) session so capture and apply agree on every
// value's external form.
var canonicalGUCs = []string{
	"SET TimeZone = 'UTC'",
	"SET DateStyle = 'ISO, YMD'",
	"SET IntervalStyle = 'iso_8601'",
	"SET extra_float_digits = 3",
	"SET bytea_output = 'hex'",
	"SET client_encoding = 'UTF8'",
	"SET lc_monetary = 'C'",
}

func pinCanonicalGUCs(exec func(string) error) error {
	for _, g := range canonicalGUCs {
		if err := exec(g); err != nil {
			return fmt.Errorf("%s: %w", g, err)
		}
	}
	return nil
}

func ensurePublication(ctx context.Context, conn *pgx.Conn, name string) error {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	// FOR ALL TABLES: a table created+inserted in one txn is captured, and
	// CREATE TABLE needs no publication-membership step (§3).
	if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, quoteIdent(name))); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

func ensureOrigin(ctx context.Context, conn *pgx.Conn, name string) error {
	// The create function is only evaluated when the WHERE row survives, i.e.
	// the origin does not yet exist (it is a cluster-shared catalog).
	if _, err := conn.Exec(ctx,
		`SELECT pg_replication_origin_create($1) WHERE pg_replication_origin_oid($1) IS NULL`, name); err != nil {
		return fmt.Errorf("create origin: %w", err)
	}
	return nil
}

func ensureSlot(ctx context.Context, replURL, slot string) error {
	repl, err := pgconn.Connect(ctx, replURL)
	if err != nil {
		return err
	}
	defer repl.Close(ctx)
	// CreateReplicationSlot errors if it exists; check first via a normal query
	// is awkward on a replication conn, so just attempt and tolerate existence.
	_, err = pglogrepl.CreateReplicationSlot(ctx, repl, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" { // duplicate_object
			return nil
		}
		return fmt.Errorf("create slot: %w", err)
	}
	return nil
}
