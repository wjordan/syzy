// Package metadata manages the syzy metadata SQLite file (app.db-syzy). It
// owns the schema for meta/frontier/row_clock and exposes typed
// accessors for the meta key/value layer.
//
// Higher-level operations (snapshotter checkpoint, recovery seed)
// compose the helpers here through transactions; the metadata package
// itself is policy-free.
package metadata

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wjordan/syzy/internal/ltxstream"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// ErrSchemaMismatch is returned by Open when the metadata's schema_version
// disagrees with the package's expected version.
var ErrSchemaMismatch = errors.New("metadata: schema version mismatch")

// ErrClosed is returned by methods called on a Store after Close.
// Callers (typically the snapshotter goroutine that may still be
// flushing during shutdown) treat this as a clean termination signal,
// not a fault.
var ErrClosed = errors.New("metadata: closed")

// Store wraps a sqlitebridge.Conn open against the metadata SQLite file.
// One Store serves one process. The internal mu serializes the
// underlying Conn (opened with OpenNoMutex) across goroutines —
// snapshotter writes and recovery reads can both touch the same
// Store concurrently.
type Store struct {
	mu    sync.Mutex
	conn  *sqlitebridge.Conn
	stmts *stmts
}

// Tx is a handle passed to WithTx callbacks. It exposes the subset of
// Store mutators that callbacks actually need, without re-acquiring
// the Store mutex (which WithTx is already holding).
type Tx struct {
	conn  *sqlitebridge.Conn
	stmts *stmts
}

// Open or create the metadata store at path. Applies pragmas and the
// canonical schema, then verifies (or seeds) meta.schema_version.
func Open(path string) (*Store, error) {
	t0 := time.Now()
	conn, err := sqlitebridge.Open(path, 0)
	if err != nil {
		return nil, fmt.Errorf("metadata: open %q: %w", path, err)
	}
	tOpen := time.Now()
	sc := &Store{conn: conn}
	if err := sc.applySchema(); err != nil {
		_ = sc.Close()
		return nil, err
	}
	tSchema := time.Now()
	if err := sc.prepareStmts(); err != nil {
		_ = sc.Close()
		return nil, err
	}
	if err := sc.checkSchemaVersion(); err != nil {
		_ = sc.Close()
		return nil, err
	}
	// One line per open; this sits on cold-start paths and the open/schema
	// split says whether the DDL re-application is the cost.
	syzylog.Debugf("metadata: open %s conn_us=%d schema_us=%d stmts_us=%d",
		filepath.Base(path), tOpen.Sub(t0).Microseconds(),
		tSchema.Sub(tOpen).Microseconds(), time.Since(tSchema).Microseconds())
	return sc, nil
}

func (s *Store) applySchema() error {
	if err := s.conn.Exec(pragmaSetupSQL); err != nil {
		return fmt.Errorf("metadata: pragmas: %w", err)
	}
	if err := s.ensureWAL(); err != nil {
		return fmt.Errorf("metadata: ensure WAL: %w", err)
	}
	if err := s.conn.Exec(schemaSQL); err != nil {
		return fmt.Errorf("metadata: apply schema: %w", err)
	}
	return nil
}

// ensureWAL reads PRAGMA journal_mode (no writer-slot acquisition) and
// only runs `journal_mode = WAL` when the file is not yet in WAL mode.
// This avoids the walTryBeginRead retry storm that PRAGMA journal_mode
// = WAL triggers under host/guest contention on shared metadata.db.
func (s *Store) ensureWAL() error {
	mode, err := s.readJournalMode()
	if err != nil {
		return err
	}
	if strings.EqualFold(mode, "wal") {
		return nil
	}
	if err := s.conn.Exec(pragmaConvertWALSQL); err != nil {
		return fmt.Errorf("convert to WAL: %w", err)
	}
	return nil
}

func (s *Store) readJournalMode() (string, error) {
	stmt, _, err := s.conn.Prepare(`PRAGMA journal_mode`)
	if err != nil {
		return "", fmt.Errorf("prepare journal_mode read: %w", err)
	}
	defer stmt.Finalize()
	hasRow, err := stmt.Step()
	if err != nil {
		return "", fmt.Errorf("step journal_mode read: %w", err)
	}
	if !hasRow {
		return "", nil
	}
	return stmt.ColumnText(0), nil
}

func (s *Store) checkSchemaVersion() error {
	got, ok, err := s.GetSchemaVersion()
	if err != nil {
		return fmt.Errorf("metadata: read schema_version: %w", err)
	}
	if !ok {
		if err := s.SetSchemaVersion(schemaVersion); err != nil {
			return fmt.Errorf("metadata: seed schema_version: %w", err)
		}
		return nil
	}
	if got != schemaVersion {
		return fmt.Errorf("%w: file=%d, package=%d", ErrSchemaMismatch, got, schemaVersion)
	}
	return nil
}

// DisableAutoCheckpoint sets PRAGMA wal_autocheckpoint=0. Used when
// the publisher takes ownership of WAL recycling: SQLite no longer
// auto-checkpoints, the publisher drains the LTX tailer first and then
// runs the coordinated recycle. Re-enable via wal_autocheckpoint
// pragma as needed.
func (s *Store) DisableAutoCheckpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ErrClosed
	}
	if err := s.conn.Exec(`PRAGMA wal_autocheckpoint = 0`); err != nil {
		return err
	}
	// The publisher owns WAL bounding now: disable the trampoline's own
	// backstop, and enable frame capture (after the pragma, which clobbers
	// any wal_hook) so RecycleCommit can prove its restarts.
	s.conn.SetWALCheckpointThreshold(-1)
	s.conn.EnableWALFrameCapture()
	return nil
}

// Checkpoint issues PRAGMA wal_checkpoint(<mode>) on the metadata connection
// under Store.mu. underFence, when non-nil, runs after Store.mu is acquired
// and receives the checkpoint hooks (Recycle is sqlitebridge.RecycleCommit
// on this connection), letting the publisher acquire the metadata tailer
// after this serialization and hold it across the last drain, checkpoint,
// and position reset.
//
// mode is one of "PASSIVE", "FULL", "RESTART", "TRUNCATE"
// (case-insensitive); the coordinated recycle uses PASSIVE and lets
// Recycle's commit restart the WAL.
func (s *Store) Checkpoint(mode string, underFence func(hooks ltxstream.CheckpointHooks) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ErrClosed
	}
	checkpoint := func() (ltxstream.CheckpointResult, error) {
		row, err := s.conn.QueryInt64Row(fmt.Sprintf(`PRAGMA wal_checkpoint(%s)`, mode))
		if err != nil {
			return ltxstream.CheckpointResult{}, err
		}
		return ltxstream.CheckpointResult{Busy: row[0] != 0, NLog: row[1], NCkpt: row[2]}, nil
	}
	if underFence == nil {
		_, err := checkpoint()
		return err
	}
	return underFence(ltxstream.CheckpointHooks{
		Checkpoint: checkpoint,
		Recycle:    s.conn.RecycleCommit,
	})
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	s.stmts.finalize()
	s.stmts = nil
	conn := s.conn
	s.conn = nil
	return conn.Close()
}

// WithTx runs fn inside a BEGIN IMMEDIATE transaction. fn returning an
// error rolls back; otherwise the txn commits. Nested calls are not
// supported (SQLite returns "cannot start a transaction within a
// transaction" — no auto-savepoint promotion). The Tx passed to fn
// shares Store's Conn and prepared-statement cache under the held
// mutex; fn must use tx.* methods rather than calling back into
// Store.* (which would deadlock on the re-acquire).
//
// Returns ErrClosed if the metadata has been closed. The snapshotter's
// final flush during process shutdown can race with Close — if Close
// won the lock first, this method bails out cleanly instead of
// dereferencing finalized statement handles.
func (s *Store) WithTx(fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withTxLocked(fn)
}

// withTxLocked is WithTx's transaction body. The caller must hold s.mu.
func (s *Store) withTxLocked(fn func(tx *Tx) error) error {
	if s.stmts == nil {
		return ErrClosed
	}
	if err := stepTxStmt(s.stmts.begin); err != nil {
		return fmt.Errorf("metadata: BEGIN: %w", err)
	}
	tx := &Tx{conn: s.conn, stmts: s.stmts}
	if err := fn(tx); err != nil {
		if rbErr := stepTxStmt(s.stmts.rollback); rbErr != nil {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if err := stepTxStmt(s.stmts.commit); err != nil {
		return fmt.Errorf("metadata: COMMIT: %w", err)
	}
	return nil
}

// stepTxStmt runs one no-binding statement (BEGIN/COMMIT/ROLLBACK). The
// stmt has zero parameters; reset+step is enough.
func stepTxStmt(stmt *sqlitebridge.Stmt) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
