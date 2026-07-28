package schemalog

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wjordan/syzy/sqlitebridge"
)

// File is a SQLite-file-backed schema log. Multiple nodes (each with
// their own *File handle on the same path) share one CAS log; SQLite's
// own writer lock + the BEGIN IMMEDIATE serialization implement the
// linearizable CAS Append needs.
//
// Each *File owns its own *sqlitebridge.Conn and serializes its
// internal calls; concurrent Append/Read/Head against the same handle
// is safe but processes-level concurrency is the load-bearing case.
type File struct {
	mu   sync.Mutex
	conn *sqlitebridge.Conn

	begin    *sqlitebridge.Stmt
	commit   *sqlitebridge.Stmt
	rollback *sqlitebridge.Stmt
	headStmt *sqlitebridge.Stmt
	insStmt  *sqlitebridge.Stmt
	readStmt *sqlitebridge.Stmt
}

// busy_timeout is load-bearing: multiple producer processes attach the same
// app DB,
// each opens its own *File on the shared schema.db, and BEGIN
// IMMEDIATE inside Append serializes writers cross-process via
// SQLite's WAL writer lock. Without busy_timeout, a concurrent
// Append returns SQLITE_BUSY immediately; the producer treats that
// as admission failure and surfaces SQLITE_INTERRUPT to the user.
// 5s matches the rest of syzy's writer/reader defaults.
const fileSchemaSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 500;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS schemalog_event (
  schema_seq  INTEGER PRIMARY KEY,
  parent_seq  INTEGER NOT NULL,
  catalog_op  BLOB    NOT NULL,
  raw_sql     TEXT
) STRICT;
`

// OpenFile opens or creates the SQLite file at path and prepares the
// schema-log statements. Operators point every node's schema-log
// handle at the same path.
func OpenFile(path string) (*File, error) {
	conn, err := sqlitebridge.Open(path, 0)
	if err != nil {
		return nil, fmt.Errorf("schemalog: open %q: %w", path, err)
	}
	if err := conn.Exec(fileSchemaSQL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("schemalog: schema: %w", err)
	}
	f := &File{conn: conn}
	for _, p := range []struct {
		sql string
		dst **sqlitebridge.Stmt
	}{
		{`BEGIN IMMEDIATE`, &f.begin},
		{`COMMIT`, &f.commit},
		{`ROLLBACK`, &f.rollback},
		{`SELECT COALESCE(MAX(schema_seq), 0) FROM schemalog_event`, &f.headStmt},
		{`INSERT INTO schemalog_event (schema_seq, parent_seq, catalog_op, raw_sql) VALUES (?, ?, ?, ?)`, &f.insStmt},
		{`SELECT schema_seq, parent_seq, catalog_op, raw_sql FROM schemalog_event WHERE schema_seq > ? ORDER BY schema_seq LIMIT ?`, &f.readStmt},
	} {
		stmt, _, err := conn.Prepare(p.sql)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("schemalog: prepare %q: %w", p.sql, err)
		}
		*p.dst = stmt
	}
	return f, nil
}

// Close releases the connection. Idempotent.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		return nil
	}
	for _, s := range []**sqlitebridge.Stmt{
		&f.begin, &f.commit, &f.rollback,
		&f.headStmt, &f.insStmt, &f.readStmt,
	} {
		if *s != nil {
			_ = (*s).Finalize()
			*s = nil
		}
	}
	c := f.conn
	f.conn = nil
	return c.Close()
}

func (f *File) Append(ctx context.Context, parentSeq uint64, op []byte, raw string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		return 0, errors.New("schemalog: closed")
	}
	if err := step0(f.begin); err != nil {
		return 0, fmt.Errorf("schemalog: BEGIN: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = step0(f.rollback)
		}
	}()

	head, err := f.headLocked()
	if err != nil {
		return 0, err
	}
	if parentSeq != head {
		return 0, ErrHeadMoved
	}
	next := head + 1
	if err := f.insStmt.Reset(); err != nil {
		return 0, err
	}
	if err := f.insStmt.BindInt64(1, int64(next)); err != nil {
		return 0, err
	}
	if err := f.insStmt.BindInt64(2, int64(parentSeq)); err != nil {
		return 0, err
	}
	if err := f.insStmt.BindBlob(3, op); err != nil {
		return 0, err
	}
	if raw == "" {
		if err := f.insStmt.BindNull(4); err != nil {
			return 0, err
		}
	} else {
		if err := f.insStmt.BindText(4, raw); err != nil {
			return 0, err
		}
	}
	if _, err := f.insStmt.Step(); err != nil {
		return 0, fmt.Errorf("schemalog: insert: %w", err)
	}
	if err := step0(f.commit); err != nil {
		return 0, fmt.Errorf("schemalog: COMMIT: %w", err)
	}
	committed = true
	return next, nil
}

func (f *File) Read(ctx context.Context, fromSeq uint64, limit int) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		return nil, errors.New("schemalog: closed")
	}
	if err := f.readStmt.Reset(); err != nil {
		return nil, err
	}
	if err := f.readStmt.BindInt64(1, int64(fromSeq)); err != nil {
		return nil, err
	}
	if err := f.readStmt.BindInt64(2, int64(limit)); err != nil {
		return nil, err
	}
	var out []Event
	for {
		hasRow, err := f.readStmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		ev := Event{
			SchemaSeq: uint64(f.readStmt.ColumnInt64(0)),
			ParentSeq: uint64(f.readStmt.ColumnInt64(1)),
			CatalogOp: f.readStmt.ColumnBlob(2),
		}
		if !f.readStmt.ColumnIsNull(3) {
			ev.RawSQL = f.readStmt.ColumnText(3)
		}
		out = append(out, ev)
	}
}

func (f *File) Head(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conn == nil {
		return 0, errors.New("schemalog: closed")
	}
	return f.headLocked()
}

func (f *File) headLocked() (uint64, error) {
	if err := f.headStmt.Reset(); err != nil {
		return 0, err
	}
	hasRow, err := f.headStmt.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, nil
	}
	v := uint64(f.headStmt.ColumnInt64(0))
	if _, err := f.headStmt.Step(); err != nil {
		return 0, err
	}
	return v, nil
}

func step0(stmt *sqlitebridge.Stmt) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
