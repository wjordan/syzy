package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/wjordan/syzy/sqlitebridge"
)

// ErrTxClosed is returned by Tx methods called after Commit or Rollback.
var ErrTxClosed = errors.New("syzy: transaction already closed")

// ErrReadOnlyTx is returned when BlobWriteAt is called on a read-only Tx.
var ErrReadOnlyTx = errors.New("syzy: write on read-only transaction")

// Executor is the common SQL method set implemented by *DB and *Tx.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// DB is a small application-facing facade over a Node. Writes and
// transactions go through the node-owned writer pool; transactions pin that
// pool's single connection so BlobWriteAt can use sqlite3_blob_write inside
// the same transaction. Reads go to the read-only pool (Config.ReadPoolSize)
// and see the last committed snapshot, so a read issued while a transaction
// is open returns pre-transaction rows instead of waiting for the commit.
// DB facades do not own the pools, so multiple NewDB(node) handles are safe
// and Close is a no-op.
type DB struct {
	node *Node
	sql  *sql.DB
}

func NewDB(node *Node) *DB {
	if node == nil {
		return &DB{}
	}
	return &DB{node: node, sql: node.writerDB}
}

// Node returns the Node this DB facade was constructed from, or nil
// for a closed/empty facade. Lets callers reach Node-only surfaces
// (AppConn, AppPath, WriterDB) without a parallel handle.
func (d *DB) Node() *Node {
	if d == nil {
		return nil
	}
	return d.node
}

func (d *DB) Subscribe(filter SubscribeFilter) (<-chan Notification, func()) {
	if d == nil || d.node == nil {
		ch := make(chan Notification)
		close(ch)
		return ch, func() {}
	}
	return d.node.Subscribe(filter)
}

func (d *DB) Close() error {
	return nil
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if d == nil || d.sql == nil || d.node == nil {
		return nil, errors.New("syzy: DB is closed")
	}
	if d.node.isClosed() {
		return nil, ErrClosed
	}
	d.node.writeMu.Lock()
	defer d.node.writeMu.Unlock()
	if d.node.isClosed() {
		return nil, ErrClosed
	}
	return d.sql.ExecContext(ctx, query, args...)
}

func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

// readSQL returns the node's read-only pool, or the pinned writer when
// the pool is disabled.
func (d *DB) readSQL() *sql.DB {
	if d.node != nil && d.node.readerDB != nil {
		return d.node.readerDB
	}
	return d.sql
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if d == nil || d.sql == nil {
		return nil, errors.New("syzy: DB is closed")
	}
	return d.readSQL().QueryContext(ctx, query, args...)
}

func (d *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), query, args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.readSQL().QueryRowContext(ctx, query, args...)
}

func (d *DB) QueryRow(query string, args ...any) *sql.Row {
	return d.QueryRowContext(context.Background(), query, args...)
}

func (d *DB) Begin() (*Tx, error) {
	return d.BeginTx(context.Background(), nil)
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if d == nil || d.sql == nil || d.node == nil {
		return nil, errors.New("syzy: DB is closed")
	}
	readOnly := false
	if opts != nil {
		switch opts.Isolation {
		case sql.LevelDefault, sql.LevelSerializable:
		default:
			return nil, fmt.Errorf("syzy: unsupported isolation %v", opts.Isolation)
		}
		readOnly = opts.ReadOnly
	}
	if d.node.isClosed() {
		return nil, ErrClosed
	}
	d.node.writeMu.Lock()
	if d.node.isClosed() {
		d.node.writeMu.Unlock()
		return nil, ErrClosed
	}
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		d.node.writeMu.Unlock()
		return nil, err
	}
	begin := "BEGIN IMMEDIATE"
	if readOnly {
		begin = "BEGIN DEFERRED"
	}
	if _, err := conn.ExecContext(ctx, begin); err != nil {
		_ = conn.Close()
		d.node.writeMu.Unlock()
		return nil, err
	}
	return &Tx{db: d, conn: conn, readOnly: readOnly}, nil
}

type Tx struct {
	db       *DB
	conn     *sql.Conn
	readOnly bool
	once     sync.Once
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxClosed
	}
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx == nil || tx.conn == nil {
		return nil, ErrTxClosed
	}
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) QueryRow(query string, args ...any) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

// suppressDMLCapture toggles the per-conn flag on the tx's writer
// connection that tells the preupdate trampoline to skip the OLD/NEW
// row-image emission for ordinary DML fires. Used internally by helpers
// like BlobWriteAtExtending whose paired BLOB_INTENT carries the
// receiver-derivable effect; not exported because direct toggling is
// easy to mispair. Counter semantics.
func (tx *Tx) suppressDMLCapture(on bool) error {
	if tx == nil || tx.conn == nil {
		return ErrTxClosed
	}
	return tx.conn.Raw(func(dc any) error {
		c, ok := dc.(*sqlitebridge.PinnedConn)
		if !ok {
			return fmt.Errorf("syzy: raw conn = %T", dc)
		}
		c.Conn().SuppressDMLCapture(on)
		return nil
	})
}

// BlobWriteAtExtending writes data at offset on the (table, column,
// rowid) blob, growing the column with trailing zeroblob if
// offset+len(data) exceeds its current length. The row must already
// exist; only the named blob column is touched.
//
// Replicates as a single BLOB_INTENT — the local extension UPDATE is
// suppressed so peers don't see the full pre/post row image. The
// receiver's broker.applyBlobPatch / ensureBlobLen rederives the
// trailing-zero extension from the intent's offset+length.
//
// Use this in preference to a manual `data || zeroblob(...)` UPDATE +
// BlobWriteAt pair: the manual UPDATE emits an OLD/NEW capture
// proportional to current column size, which dominates replication
// traffic for append-style workloads.
func (tx *Tx) BlobWriteAtExtending(ctx context.Context, table, column string, rowid int64, offset int, data []byte) error {
	if tx == nil || tx.conn == nil {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrReadOnlyTx
	}
	if offset < 0 {
		return fmt.Errorf("syzy: BlobWriteAtExtending: negative offset %d", offset)
	}
	needed := int64(offset) + int64(len(data))
	// WHERE clause makes this a no-op (no preupdate fire) when the
	// blob is already large enough; suppression covers the case where
	// it does fire.
	extendQ := fmt.Sprintf(
		`UPDATE %q SET %q = %q || zeroblob(? - length(%q)) WHERE rowid = ? AND length(%q) < ?`,
		table, column, column, column, column)
	if err := tx.suppressDMLCapture(true); err != nil {
		return err
	}
	_, execErr := tx.ExecContext(ctx, extendQ, needed, rowid, needed)
	_ = tx.suppressDMLCapture(false)
	if execErr != nil {
		return fmt.Errorf("syzy: BlobWriteAtExtending: extend %s.%s: %w", table, column, execErr)
	}
	return tx.BlobWriteAt(table, column, rowid, offset, data)
}

func (tx *Tx) BlobWriteAt(table, column string, rowid int64, offset int, data []byte) error {
	if tx == nil || tx.conn == nil {
		return ErrTxClosed
	}
	if tx.readOnly {
		return ErrReadOnlyTx
	}
	return tx.conn.Raw(func(dc any) error {
		c, ok := dc.(*sqlitebridge.PinnedConn)
		if !ok {
			return fmt.Errorf("syzy: raw conn = %T", dc)
		}
		return c.Conn().BlobWriteAt(table, column, rowid, offset, data)
	})
}

func (tx *Tx) Commit() error {
	return tx.finish("COMMIT")
}

func (tx *Tx) Rollback() error {
	return tx.finish("ROLLBACK")
}

func (tx *Tx) finish(stmt string) error {
	if tx == nil {
		return nil
	}
	var err error
	tx.once.Do(func() {
		if tx.conn != nil {
			_, err = tx.conn.ExecContext(context.Background(), stmt)
			// A failed COMMIT (e.g. SQLITE_BUSY under writer contention) does
			// NOT auto-rollback: the transaction stays open and its BEGIN
			// IMMEDIATE write lock stays held. Returning that connection to the
			// pool strands the lock, and every later writer — this pool and the
			// broker's apply connection alike — then fails "database is locked"
			// indefinitely (a transient busy becomes a permanent wedge). Force
			// the connection clean with a ROLLBACK before it is recycled; a
			// ROLLBACK on an already-finished tx is a harmless "no transaction
			// is active" we deliberately ignore.
			if err != nil {
				_, _ = tx.conn.ExecContext(context.Background(), "ROLLBACK")
			}
			cerr := tx.conn.Close()
			if err == nil {
				err = cerr
			}
			tx.conn = nil
		}
		if tx.db != nil && tx.db.node != nil {
			tx.db.node.writeMu.Unlock()
		}
	})
	return err
}
