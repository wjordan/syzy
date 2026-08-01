package postgres

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"

	corecatalog "github.com/wjordan/syzy/catalog"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/pg/internal/pgwire"
	"github.com/wjordan/syzy/unique"
)

// Coordinated unique keys, v2: reserve-before-commit (docs/postgres.md §7).
//
// Admission marks an all-NOT-NULL unique key Coordinated. Enforcement is
// gate-only: NO node holds a physical UNIQUE index for such a key, because
// every node is a receiver, and a receiver-side index would fail the apply
// transaction with a 23505 before arbitration could run. Uniqueness instead
// holds by construction — the value is reserved in the cluster registry
// before the writing transaction commits.
//
// The pre-commit hook is a DEFERRABLE INITIALLY DEFERRED constraint trigger
// (sql/coordinated.sql), the one veto point stock Postgres offers a sidecar
// with no server extension. It reaches this process over dblink, which
// speaks the Postgres wire protocol, so the sidecar answers as a Postgres
// server for exactly one verb (internal/pgwire).

//go:embed sql/coordinated.sql
var coordinatedSQL string

// defaultReservePort names the endpoint socket inside the sidecar's own
// directory. libpq derives the file name from the port, so the value is only
// a name; nothing listens on a TCP port.
const defaultReservePort = 5432

// startReservationEndpoint brings up the coordinated-uniqueness machinery:
// the SQL objects, the endpoint the writers' triggers call, and the database
// GUC that tells them where it is.
func (e *Engine) startReservationEndpoint(ctx context.Context, cat *catalog) error {
	if e.cfg.Registry == nil {
		return fmt.Errorf("postgres: CoordinatedUnique requires Config.Registry")
	}
	if e.cfg.ReserveSocketDir == "" {
		return fmt.Errorf("postgres: CoordinatedUnique requires Config.ReserveSocketDir")
	}
	port := e.cfg.ReservePort
	if port == 0 {
		port = defaultReservePort
	}
	if err := os.MkdirAll(e.cfg.ReserveSocketDir, 0o755); err != nil {
		return fmt.Errorf("postgres: reservation socket dir: %w", err)
	}
	cat.coordIdx = newCoordIndex()
	srv, err := pgwire.Listen(pgwire.Config{
		// libpq (and therefore dblink) forms a unix connection as
		// "<host>/.s.PGSQL.<port>", so the endpoint must bind that exact
		// name for a conninfo of host=<dir> port=<port> to reach it.
		Socket:   filepath.Join(e.cfg.ReserveSocketDir, fmt.Sprintf(".s.PGSQL.%d", port)),
		Reserver: &coordReserver{idx: cat.coordIdx, reg: e.cfg.Registry},
	})
	if err != nil {
		return err
	}
	e.reserveSrv = srv

	// The enumeration session reads values as text, so it needs the same
	// canonical GUC pins capture uses — otherwise a differing DateStyle
	// would re-derive different bytes for a value already reserved.
	enum, err := pgx.Connect(ctx, e.cfg.ConnURL)
	if err != nil {
		return fmt.Errorf("postgres: enumeration conn: %w", err)
	}
	e.enumConn = enum
	if err := pinCanonicalGUCs(func(sql string) error { _, err := enum.Exec(ctx, sql); return err }); err != nil {
		return fmt.Errorf("postgres: enumeration conn GUCs: %w", err)
	}

	dbName, err := databaseName(ctx, e.apply)
	if err != nil {
		return err
	}
	conninfo := fmt.Sprintf("host=%s port=%d dbname=syzy user=syzy connect_timeout=5",
		e.cfg.ReserveSocketDir, port)
	return ensureCoordSchema(ctx, e.apply, dbName, conninfo)
}

// databaseName reads the connected database's name, needed to scope the
// ALTER DATABASE that publishes the endpoint address to writer sessions.
func databaseName(ctx context.Context, conn *pgx.Conn) (string, error) {
	var name string
	err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&name)
	return name, err
}

// ensureCoordSchema installs the accumulator table, trigger functions, and
// install/uninstall helpers. Runs on the apply (replica-role) session so the
// DDL does not spool a ddl intent.
func ensureCoordSchema(ctx context.Context, conn *pgx.Conn, dbName, conninfo string) error {
	if _, err := conn.Exec(ctx, coordinatedSQL); err != nil {
		return err
	}
	// The reservation endpoint's address must be visible to every writer
	// session, not just ours: the trigger runs in the application's own
	// backend. A database-level GUC is how a sidecar reaches sessions it
	// never opened.
	_, err := conn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET syzy.reserve_conninfo = %s",
		quoteIdent(dbName), quoteString(conninfo)))
	return err
}

// ensureCoordinated brings ti's coordinated keys to their enforced state on
// this node: no physical unique index, an accumulating trigger per key, and
// the table's deferred reservation trigger. Idempotent; runs on the apply
// (replica-role) session.
func ensureCoordinated(ctx context.Context, conn *pgx.Conn, cat *catalog, ti *tableInfo) error {
	var coord []*uniqueKey
	for _, uk := range ti.uniqueKeys {
		if uk.coordinated {
			coord = append(coord, uk)
		}
	}
	cat.coordIdx.setTable(ti, coord)

	// Drop any physical unique index backing a coordinated key, including
	// the one the originator's own CREATE TABLE built. Keeping it would make
	// this node reject a replicated row whose value is legitimately in
	// flight — a transfer between rows, or a value this node has not yet
	// seen released. The registry is the only arbiter.
	if len(coord) > 0 {
		oids, err := liveUniqueIndexOIDs(ctx, conn, ti)
		if err != nil {
			return err
		}
		for _, uk := range coord {
			oid, ok := oids[uniqueKeySig(uk.cols)]
			if !ok {
				continue
			}
			if err := dropUniqueIndex(ctx, conn, oid); err != nil {
				return fmt.Errorf("drop physical unique index on %s: %w", ti.name, err)
			}
		}
	}
	return ensureCoordTriggers(ctx, conn, ti, coord)
}

// dropUniqueIndex removes index oid, whether it is a bare index or the
// implementation of a UNIQUE constraint (which can only be dropped through
// the constraint).
func dropUniqueIndex(ctx context.Context, conn *pgx.Conn, oid uint32) error {
	var conname, relname *string
	err := conn.QueryRow(ctx, `
		SELECT c.conname, r.relname
		FROM pg_class i
		LEFT JOIN pg_constraint c ON c.conindid = i.oid AND c.contype IN ('u','p')
		LEFT JOIN pg_class r ON r.oid = c.conrelid
		WHERE i.oid = $1`, oid).Scan(&conname, &relname)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil // already gone
		}
		return err
	}
	if conname != nil && relname != nil {
		return execDDLApply(ctx, conn, "ALTER TABLE "+quoteIdent(appliedSchema)+"."+
			quoteIdent(*relname)+" DROP CONSTRAINT "+quoteIdent(*conname))
	}
	var idxname string
	if err := conn.QueryRow(ctx, `SELECT relname FROM pg_class WHERE oid = $1`, oid).Scan(&idxname); err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}
	return execDDLApply(ctx, conn, "DROP INDEX IF EXISTS "+
		quoteIdent(appliedSchema)+"."+quoteIdent(idxname))
}

// ensureCoordTriggers reconciles ti's accumulating triggers with coord: one
// per coordinated key, and none for a key that is gone.
func ensureCoordTriggers(ctx context.Context, conn *pgx.Conn, ti *tableInfo, coord []*uniqueKey) error {
	rel := quoteString(quoteIdent(appliedSchema) + "." + quoteIdent(ti.name))

	want := make(map[string]bool, len(coord))
	for _, uk := range coord {
		want[hex.EncodeToString(uk.keyID[:])] = true
	}
	rows, err := conn.Query(ctx, `
		SELECT substring(tgname from 18)
		FROM pg_trigger
		WHERE tgrelid = $1 AND NOT tgisinternal AND tgname LIKE 'syzy\_coord\_accum\_%'`, ti.oid)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var keyHex string
		if err := rows.Scan(&keyHex); err != nil {
			rows.Close()
			return err
		}
		if !want[keyHex] {
			stale = append(stale, keyHex)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, keyHex := range stale {
		if err := execDDLApply(ctx, conn, fmt.Sprintf("SELECT public.syzy_coord_uninstall(%s, %s)",
			rel, quoteString(keyHex))); err != nil {
			return err
		}
	}

	for _, uk := range coord {
		names := make([]string, len(uk.cols))
		for i, ci := range uk.cols {
			names[i] = quoteString(ci.name)
		}
		if err := execDDLApply(ctx, conn, fmt.Sprintf(
			"SELECT public.syzy_coord_install(%s, %s, %s, ARRAY[%s]::text[])",
			rel,
			quoteString(hex.EncodeToString(ti.tid[:])),
			quoteString(hex.EncodeToString(uk.keyID[:])),
			strings.Join(names, ", "),
		)); err != nil {
			return fmt.Errorf("install coordinated key on %s: %w", ti.name, err)
		}
	}
	return nil
}

// --- reservation service ---

// coordCol is one column as the reservation path needs it. It is a value
// copy, not a *colInfo: the catalog's colInfos are mutated in place by DDL
// apply on the orchestrator goroutine, and everything below runs on other
// goroutines.
type coordCol struct {
	cid      crdt.ColumnID
	name     string
	typeName string
}

func coordCols(cols []*colInfo) []coordCol {
	out := make([]coordCol, len(cols))
	for i, ci := range cols {
		out[i] = coordCol{cid: ci.cid, name: ci.name, typeName: ci.typeName}
	}
	return out
}

// coordTable is one table's coordinated-key metadata.
type coordTable struct {
	tid  crdt.TableID
	name string
	pk   []coordCol
	keys map[crdt.KeyID][]coordCol // key id -> member columns, declared order
}

// coordIndex is the coordinated-key metadata as seen from OFF the
// orchestrator goroutine.
//
// Two consumers need it concurrently with capture and apply: the pgwire
// endpoint, which serves Postgres backends blocked in their commits, and the
// leaseholder's enumeration tick. The catalog itself is deliberately
// lock-free and single-threaded, so rather than putting a mutex on the hot
// path, DDL apply publishes a decoupled copy here.
type coordIndex struct {
	mu     sync.RWMutex
	tables map[crdt.TableID]coordTable
}

func newCoordIndex() *coordIndex {
	return &coordIndex{tables: map[crdt.TableID]coordTable{}}
}

// setTable replaces ti's entry with exactly the given coordinated keys.
func (x *coordIndex) setTable(ti *tableInfo, coord []*uniqueKey) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(coord) == 0 {
		delete(x.tables, ti.tid)
		return
	}
	ct := coordTable{
		tid:  ti.tid,
		name: ti.name,
		pk:   coordCols(ti.pk),
		keys: make(map[crdt.KeyID][]coordCol, len(coord)),
	}
	for _, uk := range coord {
		ct.keys[uk.keyID] = coordCols(uk.cols)
	}
	x.tables[ti.tid] = ct
}

// dropTable forgets ti entirely (DROP TABLE).
func (x *coordIndex) dropTable(tid crdt.TableID) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.tables, tid)
}

// lookup returns the key's member columns and its table's PK columns.
func (x *coordIndex) lookup(tid crdt.TableID, kid crdt.KeyID) (members, pk []coordCol, ok bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	ct, ok := x.tables[tid]
	if !ok {
		return nil, nil, false
	}
	members, ok = ct.keys[kid]
	return members, ct.pk, ok
}

// snapshot returns every coordinated table, for enumeration.
func (x *coordIndex) snapshot() []coordTable {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]coordTable, 0, len(x.tables))
	for _, ct := range x.tables {
		out = append(out, ct)
	}
	return out
}

// coordReserver is the semantic half of the reservation endpoint: it turns a
// batch of text values into canonical claims and reserves them.
type coordReserver struct {
	idx *coordIndex
	reg unique.Registry
}

// Reserve implements pgwire.Reserver. ErrDenied means a genuine conflict and
// aborts the writer's commit as a 23505; any other error is reported as
// retryable, so a request this node cannot resolve fails the write closed
// rather than letting it commit unreserved.
func (r *coordReserver) Reserve(ctx context.Context, req pgwire.Request) error {
	claims := make([]unique.Claim, 0, len(req.Entries))
	for _, e := range req.Entries {
		tid, err := parseID[crdt.TableID](e.TableID)
		if err != nil {
			return fmt.Errorf("table id: %w", err)
		}
		kid, err := parseID[crdt.KeyID](e.KeyID)
		if err != nil {
			return fmt.Errorf("key id: %w", err)
		}
		members, pk, ok := r.idx.lookup(tid, kid)
		if !ok {
			// The writer's trigger is ahead of this node's catalog (a DDL
			// still propagating). Retryable, never a grant.
			return fmt.Errorf("no coordinated key %x on table %x", kid[:4], tid[:4])
		}
		value, err := encodeCanonical(members, e.Values)
		if err != nil {
			return fmt.Errorf("key value: %w", err)
		}
		owner, err := encodeCanonical(pk, e.Owner)
		if err != nil {
			return fmt.Errorf("owner pk: %w", err)
		}
		c := unique.Claim{Table: tid, Key: kid, Value: value, Owner: owner}
		if len(e.Prev) > 0 {
			if c.Prev, err = encodeCanonical(pk, e.Prev); err != nil {
				return fmt.Errorf("prev pk: %w", err)
			}
		}
		claims = append(claims, c)
	}
	ok, conflict, err := r.reg.Reserve(ctx, claims)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: table %x key %x", pgwire.ErrDenied, conflict.Table[:4], conflict.Key[:4])
	}
	return nil
}

// encodeCanonical renders text values into the canonical key encoding shared
// with capture, so a value reserved here is byte-identical to the same value
// captured from the WAL. Byte equality is what makes the registry's notion of
// "the same value" agree with the cluster's.
func encodeCanonical(cols []coordCol, vals []*string) ([]byte, error) {
	if len(vals) != len(cols) {
		return nil, fmt.Errorf("got %d values for %d columns", len(vals), len(cols))
	}
	out := make([]byte, 0, 32*len(cols))
	for i, ci := range cols {
		if vals[i] == nil {
			// Coordinated keys and PKs are NOT NULL by construction, and the
			// trigger drops NULL-bearing tuples before sending. A NULL here
			// means the two sides disagree about the schema.
			return nil, fmt.Errorf("NULL value for NOT NULL column %q", ci.name)
		}
		cv, err := encodeColValue(ci.cid, ci.typeName, []byte(*vals[i]))
		if err != nil {
			return nil, err
		}
		if out, err = corecatalog.AppendValue(out, cv); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// parseID decodes a 32-character hex id into a 16-byte array.
func parseID[T ~[16]byte](s string) (T, error) {
	var id T
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != 16 {
		return id, fmt.Errorf("got %d bytes, want 16", len(b))
	}
	copy(id[:], b)
	return id, nil
}
