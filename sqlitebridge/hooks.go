package sqlitebridge

/*
#include "syzy_sqlite.h"
#include "hooks.h"
*/
import "C"

import (
	"fmt"
	"runtime/cgo"
	"unsafe"
)

// CommitHook is invoked on COMMIT before the change becomes durable. Return
// nonzero to convert the COMMIT into a ROLLBACK; SQLite surfaces this to the
// app as SQLITE_CONSTRAINT_COMMITHOOK.
type CommitHook func() int

// RollbackHook fires after a ROLLBACK. Return value is ignored.
type RollbackHook func()

// PreupdateOp identifies the kind of row mutation visible to the preupdate
// hook.
type PreupdateOp int

const (
	PreupdateInsert PreupdateOp = C.SQLITE_INSERT
	PreupdateUpdate PreupdateOp = C.SQLITE_UPDATE
	PreupdateDelete PreupdateOp = C.SQLITE_DELETE
)

// PreupdateEvent describes a row mutation about to commit. The accessor
// methods are valid only for the duration of the callback — do not retain
// the event past return.
type PreupdateEvent struct {
	conn      *Conn
	Op        PreupdateOp
	DBName    string
	TableName string
	OldRowID  int64
	NewRowID  int64
}

// Count returns the number of columns in the row being mutated.
func (e *PreupdateEvent) Count() int {
	return int(C.sx_preupdate_count(e.conn.db))
}

// Depth returns the trigger-nesting depth (0 for direct DML; >0 inside a
// trigger or cascading FK).
func (e *PreupdateEvent) Depth() int {
	return int(C.sx_preupdate_depth(e.conn.db))
}

// BlobWrite returns the column index whose blob is being mutated by an
// in-progress sqlite3_blob_write, or -1 for ordinary DML.
func (e *PreupdateEvent) BlobWrite() int {
	return int(C.sx_preupdate_blobwrite(e.conn.db))
}

// PreupdateHook fires once per direct DML row before the txn commits.
type PreupdateHook func(*PreupdateEvent)

// WALHook fires after each WAL commit. dbName is the schema (typically "main");
// frameCount is the WAL frame total at that commit. Return SQLITE_OK normally.
type WALHook func(dbName string, frameCount int) int

// TraceEvent identifies which sqlite3_trace_v2 event fired.
type TraceEvent uint

const (
	TraceStmt    TraceEvent = C.SQLITE_TRACE_STMT
	TraceProfile TraceEvent = C.SQLITE_TRACE_PROFILE
	TraceRow     TraceEvent = C.SQLITE_TRACE_ROW
	TraceClose   TraceEvent = C.SQLITE_TRACE_CLOSE
)

// TraceHook fires for events selected by the mask passed to SetTraceHook. For
// TraceStmt, sql is the unexpanded SQL text; other events deliver an empty
// sql.
type TraceHook func(evt TraceEvent, sql string) int

type connState struct {
	conn        *Conn
	handle      cgo.Handle
	cstate      *C.syzy_conn_state
	commit      CommitHook
	commitCause error
	rollback    RollbackHook
	preupdate   PreupdateHook
	wal         WALHook
	producerWAL ProducerWALHook
	trace       TraceHook
	traceMask   TraceEvent

	// captureWAL keeps a wal_hook trampoline installed with no callback so
	// commitWALFrames is still recorded; commitWALFrames holds the WAL
	// frame count SQLite reported for this connection's most recent
	// committed write (recorded inside the committing writer's locked
	// region, so it is exact per commit).
	captureWAL      bool
	commitWALFrames int64
}

func (c *Conn) ensureState() *connState {
	if c.state == nil {
		s := &connState{conn: c}
		s.handle = cgo.NewHandle(s)
		s.cstate = C.syzy_state_new()
		if s.cstate == nil {
			s.handle.Delete()
			panic("sqlitebridge: out of memory allocating syzy_conn_state")
		}
		s.cstate.hook_handle = C.uintptr_t(s.handle)
		c.state = s
	}
	return c.state
}

func (c *Conn) clearState() {
	if c.state == nil {
		return
	}
	C.syzy_install_commit_hook(c.db, nil)
	C.syzy_install_rollback_hook(c.db, nil)
	C.syzy_install_preupdate_hook(c.db, nil)
	C.syzy_install_wal_hook(c.db, nil)
	C.syzy_install_trace_hook(c.db, 0, nil)
	C.syzy_state_free(c.state.cstate)
	c.state.handle.Delete()
	c.state = nil
}

// SetCommitHook registers fn as the commit hook. Pass nil to clear.
func (c *Conn) SetCommitHook(fn CommitHook) {
	c.ensureState().commit = fn
	c.reinstallCommit()
}

// SetCommitHookCause attaches a Go cause to the next
// SQLITE_CONSTRAINT_COMMITHOOK returned by this connection. A commit hook
// that returns nonzero may call this first to preserve a distinction SQLite's
// integer hook result cannot encode. Passing nil clears a pending cause.
func (c *Conn) SetCommitHookCause(err error) {
	c.ensureState().commitCause = err
}

// SetRollbackHook registers fn. Pass nil to clear.
func (c *Conn) SetRollbackHook(fn RollbackHook) {
	c.ensureState().rollback = fn
	c.reinstallRollback()
}

// SetPreupdateHook registers fn. Pass nil to clear.
func (c *Conn) SetPreupdateHook(fn PreupdateHook) {
	c.ensureState().preupdate = fn
	c.reinstallPreupdate()
}

// SetWALHook registers fn. Pass nil to clear.
func (c *Conn) SetWALHook(fn WALHook) {
	c.ensureState().wal = fn
	c.reinstallWAL()
}

// SetWALCheckpointThreshold overrides the WAL frame count at which this
// connection's wal_hook trampolines run their backstop PASSIVE checkpoint
// (the auto-checkpoint replacement that sqlite3_wal_hook displaces). n == 0
// restores the built-in default; n < 0 disables the backstop for embedders
// that own WAL bounding themselves (e.g. a publisher's coordinated recycle,
// which an uncoordinated backfill would force to rebaseline).
func (c *Conn) SetWALCheckpointThreshold(n int) {
	C.syzy_set_wal_checkpoint_threshold(c.ensureState().cstate, C.int(n))
}

// ProducerWALHook is the specialized callback signature for
// SetProducerWALHook. touchData aliases the connection's touch
// journal buffer and is valid for the duration of the call. nFrame is
// the WAL frame count delivered by SQLite. Returning non-zero aborts
// the WAL pipeline (avoid in steady state).
type ProducerWALHook func(touchData []byte, nFrame int) int

// SetProducerWALHook installs a specialized wal_hook that reads + clears
// the C-side touch journal in the trampoline and hands the data slice
// directly to fn — eliminating the TouchJournalTake cgo crossing the
// regular SetWALHook + TouchJournalTake pair would otherwise need.
//
// Mutually exclusive with SetWALHook on the same connection (the last
// installer wins). Pass nil to clear and revert to no wal_hook.
func (c *Conn) SetProducerWALHook(fn ProducerWALHook) {
	c.ensureState().producerWAL = fn
	c.reinstallWAL()
}

// SetTraceHook registers fn for events matching mask. Pass fn=nil to clear.
func (c *Conn) SetTraceHook(mask TraceEvent, fn TraceHook) {
	s := c.ensureState()
	s.trace = fn
	if fn != nil {
		s.traceMask = mask
	} else {
		s.traceMask = 0
	}
	c.reinstallTrace()
}

func (c *Conn) reinstallCommit() {
	if c.state.commit != nil {
		C.syzy_install_commit_hook(c.db, c.state.cstate)
	} else {
		C.syzy_install_commit_hook(c.db, nil)
	}
}

// reinstallRollback installs the C trampoline iff a Go callback is set or the
// touch journal is on (which needs the trampoline to auto-clear).
func (c *Conn) reinstallRollback() {
	hasGo := c.state.rollback != nil
	c.state.cstate.has_go_rollback = boolToCInt(hasGo)
	if hasGo || c.state.cstate.journal_enabled != 0 {
		C.syzy_install_rollback_hook(c.db, c.state.cstate)
	} else {
		C.syzy_install_rollback_hook(c.db, nil)
	}
}

// reinstallPreupdate installs the C trampoline iff a Go callback is set or
// the touch journal is on. has_go_preupdate gates the cgo crossing inside
// the trampoline so journal-only fires stay in C.
func (c *Conn) reinstallPreupdate() {
	hasGo := c.state.preupdate != nil
	c.state.cstate.has_go_preupdate = boolToCInt(hasGo)
	if hasGo || c.state.cstate.journal_enabled != 0 {
		C.syzy_install_preupdate_hook(c.db, c.state.cstate)
	} else {
		C.syzy_install_preupdate_hook(c.db, nil)
	}
}

func (c *Conn) reinstallWAL() {
	switch {
	case c.state.producerWAL != nil:
		C.syzy_install_producer_wal_hook(c.db, c.state.cstate)
	case c.state.wal != nil || c.state.captureWAL:
		C.syzy_install_wal_hook(c.db, c.state.cstate)
	default:
		C.syzy_install_wal_hook(c.db, nil)
	}
}

// EnableWALFrameCapture keeps a wal_hook trampoline installed on this
// connection even with no callback registered, so TakeCommitWALFrames can
// report the frame count of the connection's own commits — recorded by
// SQLite inside the committing writer's locked region, the only race-free
// way to tell a WAL-restarting commit (count reset) from an appending one.
// Installing a wal_hook displaces SQLite's wal_autocheckpoint and vice
// versa, so enable this after autocheckpoint is configured; mind
// SetWALCheckpointThreshold for the trampoline's own backstop.
func (c *Conn) EnableWALFrameCapture() {
	c.ensureState().captureWAL = true
	c.reinstallWAL()
}

// TakeCommitWALFrames returns the WAL frame count recorded for this
// connection's most recent committed write and clears it. 0 means no
// commit fired the connection's wal_hook since the last take (nothing
// committed, or no trampoline installed — see EnableWALFrameCapture).
func (c *Conn) TakeCommitWALFrames() int64 {
	if c.state == nil {
		return 0
	}
	n := c.state.commitWALFrames
	c.state.commitWALFrames = 0
	return n
}

// RecycleCommit runs the coordinated WAL-recycle write bracket
// (ltxstream.CheckpointHooks.Recycle): BEGIN IMMEDIATE, validate()
// (rolled back on failure), a same-value PRAGMA user_version rewrite —
// a minimal commit dirtying page 1, which invites SQLite to restart a
// fully-backfilled WAL — then COMMIT, returning the frame count recorded
// for that commit (see TakeCommitWALFrames; requires
// EnableWALFrameCapture or another wal_hook on this connection). The
// write lock held from BEGIN IMMEDIATE keeps validate's observation true
// through the commit.
func (c *Conn) RecycleCommit(validate func() error) (int64, error) {
	if err := c.Exec(`BEGIN IMMEDIATE`); err != nil {
		return 0, err
	}
	rollback := func(err error) (int64, error) {
		_ = c.Exec(`ROLLBACK`)
		return 0, err
	}
	if err := validate(); err != nil {
		return rollback(err)
	}
	row, err := c.QueryInt64Row(`PRAGMA user_version`)
	if err != nil {
		return rollback(err)
	}
	c.TakeCommitWALFrames()
	if err := c.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, row[0])); err != nil {
		return rollback(err)
	}
	if err := c.Exec(`COMMIT`); err != nil {
		return rollback(err)
	}
	return c.TakeCommitWALFrames(), nil
}

// ReassertWALHook re-installs the registered wal_hook trampoline.
//
// Needed by the loadable-extension shim: sqlite3's openDatabase calls
// sqlite3_wal_autocheckpoint(db, SQLITE_DEFAULT_WAL_AUTOCHECKPOINT)
// AFTER sqlite3AutoLoadExtensions, and wal_autocheckpoint registers
// SQLite's internal checkpoint wal_hook — silently clobbering the
// producer wal_hook an auto-loaded attach just installed. Without the
// re-assert, an autoloaded producer journals nothing and never
// resolves DDL intents. The shim's open interposers call this after
// the real open returns. No-op when no wal hook is registered.
// (Checkpointing stays covered: the producer trampoline runs a PASSIVE
// checkpoint past SYZY_WAL_CHECKPOINT_THRESHOLD frames.)
func (c *Conn) ReassertWALHook() {
	if c.state == nil || c.db == nil {
		return
	}
	c.reinstallWAL()
}

func (c *Conn) reinstallTrace() {
	if c.state.trace != nil {
		C.syzy_install_trace_hook(c.db, C.uint(c.state.traceMask), c.state.cstate)
	} else {
		C.syzy_install_trace_hook(c.db, 0, nil)
	}
}

func boolToCInt(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

//export syzyGoCommitHook
func syzyGoCommitHook(handle C.uintptr_t, ret *C.int) {
	s := cgo.Handle(handle).Value().(*connState)
	if s.commit != nil {
		*ret = C.int(s.commit())
	}
}

//export syzyGoRollbackHook
func syzyGoRollbackHook(handle C.uintptr_t) {
	s := cgo.Handle(handle).Value().(*connState)
	if s.rollback != nil {
		s.rollback()
	}
}

//export syzyGoPreupdateHook
func syzyGoPreupdateHook(
	handle C.uintptr_t,
	db *C.sqlite3,
	op C.int,
	zDb, zName *C.char,
	rowidOld, rowidNew C.sqlite3_int64,
) {
	s := cgo.Handle(handle).Value().(*connState)
	if s.preupdate == nil {
		return
	}
	e := PreupdateEvent{
		conn:      s.conn,
		Op:        PreupdateOp(op),
		DBName:    C.GoString(zDb),
		TableName: C.GoString(zName),
		OldRowID:  int64(rowidOld),
		NewRowID:  int64(rowidNew),
	}
	s.preupdate(&e)
}

//export syzyGoWALHook
func syzyGoWALHook(handle C.uintptr_t, _ *C.sqlite3, zDb *C.char, nFrame C.int, ret *C.int) {
	s := cgo.Handle(handle).Value().(*connState)
	s.commitWALFrames = int64(nFrame)
	if s.wal != nil {
		*ret = C.int(s.wal(C.GoString(zDb), int(nFrame)))
	}
}

//export syzyGoProducerWALHook
func syzyGoProducerWALHook(handle C.uintptr_t, touchData *C.uchar, touchLen C.size_t, nFrame C.int, ret *C.int) {
	s := cgo.Handle(handle).Value().(*connState)
	s.commitWALFrames = int64(nFrame)
	if s.producerWAL == nil {
		return
	}
	var touch []byte
	if touchLen > 0 {
		touch = unsafe.Slice((*byte)(unsafe.Pointer(touchData)), int(touchLen))
	}
	*ret = C.int(s.producerWAL(touch, int(nFrame)))
}

//export syzyGoTraceHook
func syzyGoTraceHook(handle C.uintptr_t, evt C.uint, x unsafe.Pointer, ret *C.int) {
	s := cgo.Handle(handle).Value().(*connState)
	if s.trace == nil {
		return
	}
	var sql string
	if TraceEvent(evt) == TraceStmt && x != nil {
		sql = C.GoString((*C.char)(x))
	}
	*ret = C.int(s.trace(TraceEvent(evt), sql))
}
