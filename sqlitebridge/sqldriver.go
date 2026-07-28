package sqlitebridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// OpenDB returns a *sql.DB backed by a single *Conn. The pool is
// pinned to one connection (MaxOpenConns=1) so every database/sql
// operation routes through the producer-hooked conn the caller passed
// in. The caller retains ownership of the Conn; closing the *sql.DB
// is a no-op against the underlying Conn.
//
// Callers must not widen the pool with SetMaxOpenConns(n>1): a second
// Connect call returns driver.ErrBadConn, which database/sql surfaces
// as a connection error rather than serializing. The pinned-conn
// contract is fundamental to the design: the producer's preupdate
// hooks are bound to one Conn.
func OpenDB(conn *Conn) *sql.DB {
	db := sql.OpenDB(&pinnedConnector{conn: conn})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// OpenReadPool returns a *sql.DB backed by up to n independent READ-ONLY
// connections to path, for concurrent reads that must not serialize behind
// the single producer-hooked writer Conn (see OpenDB). In WAL mode these
// readers run concurrently with each other and with the writer, reading the
// last committed snapshot. Each pooled connection owns and closes its own
// *Conn when database/sql retires it. Read-only by construction: the
// connections carry no producer hooks, so only SELECTs may route here —
// writes and transactions still go through the OpenDB writer.
//
// A read-only WAL reader requires a concurrent read-write connection to have
// initialized the -shm; callers must keep that writer open on the same file.
func OpenReadPool(path string, n int) (*sql.DB, error) {
	if n < 1 {
		n = 1
	}
	db := sql.OpenDB(&readConnector{path: path})
	db.SetMaxOpenConns(n)
	db.SetMaxIdleConns(n)
	return db, nil
}

type readConnector struct{ path string }

func (c *readConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := Open(c.path, OpenReadOnly|OpenURI|OpenNoMutex)
	if err != nil {
		return nil, err
	}
	// Wait out a transient lock rather than failing a FUSE read.
	if err := conn.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &PinnedConn{conn: conn, ownsConn: true}, nil
}

func (c *readConnector) Driver() driver.Driver { return errPinnedDriver{} }

type pinnedConnector struct {
	conn *Conn

	mu   sync.Mutex
	used bool
}

func (p *pinnedConnector) Connect(_ context.Context) (driver.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.used {
		// MaxOpenConns(1) should make this impossible; surface as
		// ErrBadConn so database/sql doesn't silently corrupt.
		return nil, driver.ErrBadConn
	}
	p.used = true
	return &PinnedConn{conn: p.conn, connector: p}, nil
}

func (p *pinnedConnector) Driver() driver.Driver { return errPinnedDriver{} }

func (p *pinnedConnector) release() {
	p.mu.Lock()
	p.used = false
	p.mu.Unlock()
}

type errPinnedDriver struct{}

func (errPinnedDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("sqlitebridge: sql.Open is not supported; use sqlitebridge.OpenDB(*Conn)")
}

// PinnedConn is the driver.Conn database/sql sees for an OpenDB pool.
// Exported so callers can recover the underlying *Conn via (*sql.Conn).Raw
// to reach Conn-level APIs (OpenBlob, AppendBlobIntent, etc.) inside a
// transaction. Close returns the pinned conn to the connector's pool;
// it does not close the underlying *Conn.
type PinnedConn struct {
	conn      *Conn
	connector *pinnedConnector
	// ownsConn is set for read-pool connections (OpenReadPool): Close
	// closes the underlying *Conn. The pinned writer (OpenDB) leaves it
	// false — the caller owns that Conn.
	ownsConn bool

	mu     sync.Mutex
	closed bool
}

// Conn returns the underlying *Conn. Valid until the PinnedConn is
// closed by database/sql.
func (c *PinnedConn) Conn() *Conn { return c.conn }

func (c *PinnedConn) Prepare(query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, driver.ErrBadConn
	}
	stmt, _, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &pinnedStmt{conn: c, stmt: stmt}, nil
}

func (c *PinnedConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.connector != nil {
		c.connector.release()
		c.connector = nil
	}
	if c.ownsConn {
		return c.conn.Close()
	}
	return nil
}

func (c *PinnedConn) Begin() (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := c.conn.Exec(`BEGIN IMMEDIATE`); err != nil {
		return nil, err
	}
	return &pinnedTx{conn: c}, nil
}

type pinnedTx struct{ conn *PinnedConn }

func (t *pinnedTx) Commit() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	return t.conn.conn.Exec(`COMMIT`)
}

func (t *pinnedTx) Rollback() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	return t.conn.conn.Exec(`ROLLBACK`)
}

type pinnedStmt struct {
	conn *PinnedConn
	stmt *Stmt
}

func (s *pinnedStmt) Close() error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.stmt.Finalize()
}

func (s *pinnedStmt) NumInput() int { return -1 }

func (s *pinnedStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if err := bindArgs(s.stmt, args); err != nil {
		return nil, err
	}
	if _, err := s.stmt.Step(); err != nil {
		_ = s.stmt.Reset()
		return nil, err
	}
	res := pinnedResult{
		lastID: s.conn.conn.LastInsertRowID(),
		rows:   s.conn.conn.Changes(),
	}
	if err := s.stmt.Reset(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *pinnedStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if err := bindArgs(s.stmt, args); err != nil {
		return nil, err
	}
	n := s.stmt.ColumnCount()
	cols := make([]string, n)
	timeCol := make([]bool, n)
	for i := range cols {
		cols[i] = s.stmt.ColumnName(i)
		timeCol[i] = isTimeDecltype(s.stmt.ColumnDecltype(i))
	}
	return &pinnedRows{stmt: s, cols: cols, timeCol: timeCol}, nil
}

func bindArgs(s *Stmt, args []driver.Value) error {
	for i, v := range args {
		idx := i + 1
		var err error
		switch x := v.(type) {
		case nil:
			err = s.BindNull(idx)
		case int64:
			err = s.BindInt64(idx, x)
		case float64:
			err = s.BindFloat64(idx, x)
		case bool:
			if x {
				err = s.BindInt64(idx, 1)
			} else {
				err = s.BindInt64(idx, 0)
			}
		case string:
			err = s.BindText(idx, x)
		case []byte:
			err = s.BindBlob(idx, x)
		default:
			err = fmt.Errorf("sqlitebridge: unsupported bind type %T at arg %d", v, i)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type pinnedResult struct {
	lastID int64
	rows   int64
}

func (r pinnedResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r pinnedResult) RowsAffected() (int64, error) { return r.rows, nil }

type pinnedRows struct {
	stmt    *pinnedStmt
	cols    []string
	timeCol []bool
	closed  bool
}

func (r *pinnedRows) Columns() []string { return r.cols }

func (r *pinnedRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.stmt.conn.mu.Lock()
	defer r.stmt.conn.mu.Unlock()
	return r.stmt.stmt.Reset()
}

func (r *pinnedRows) Next(dest []driver.Value) error {
	r.stmt.conn.mu.Lock()
	defer r.stmt.conn.mu.Unlock()
	hasRow, err := r.stmt.stmt.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		return io.EOF
	}
	for i := range dest {
		dest[i] = columnValue(r.stmt.stmt, i, r.timeCol[i])
	}
	return nil
}

func columnValue(s *Stmt, i int, isTime bool) driver.Value {
	switch s.ColumnType(i) {
	case ColumnNull:
		return nil
	case ColumnInt:
		return s.ColumnInt64(i)
	case ColumnReal:
		return s.ColumnFloat64(i)
	case ColumnText:
		text := s.ColumnText(i)
		if isTime {
			if t, ok := parseSQLiteTime(text); ok {
				return t
			}
		}
		return text
	case ColumnBlob:
		raw := s.ColumnBlob(i)
		out := make([]byte, len(raw))
		copy(out, raw)
		return out
	default:
		return nil
	}
}

func isTimeDecltype(decl string) bool {
	if decl == "" {
		return false
	}
	d := strings.ToUpper(decl)
	if i := strings.IndexByte(d, '('); i >= 0 {
		d = d[:i]
	}
	d = strings.TrimSpace(d)
	switch d {
	case "DATETIME", "TIMESTAMP", "DATE":
		return true
	}
	return false
}

var sqliteTimeFormats = []string{
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

func parseSQLiteTime(s string) (time.Time, bool) {
	for _, f := range sqliteTimeFormats {
		if t, err := time.Parse(f, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
