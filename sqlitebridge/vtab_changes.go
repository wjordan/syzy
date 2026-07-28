package sqlitebridge

/*
#include <stdint.h>
#include <stdlib.h>
#include "vtab_changes.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/wjordan/syzy/notify"
)

// idxNum bit assignments shared with vtab_changes.c.
const (
	syzyIdxTableEq   = 0x1
	syzyIdxTimeoutEq = 0x2
)

// Column indices, mirror of the schema declared in vtab_changes.c.
const (
	colOrigin         = 0
	colSeq            = 1
	colTableName      = 2
	colOp             = 3
	colPK             = 4
	colPKTruncated    = 5
	colTableTruncated = 6
)

// ChangesProvider supplies the per-connection extras the syzy_changes
// vtab and its companion scalars need: the connection's own origin
// (for syzy_my_origin) and a PK decoder keyed by table name (for
// syzy_pk_decode). Implementations must be safe for concurrent reads.
//
// The extension shim implements this; the linked binary's tests can
// pass nil to register the vtab without the scalars.
type ChangesProvider interface {
	Origin() uint64
	DecodePK(table string, pk []byte) (string, bool)
}

// goVtab is the per-connection state for one syzy_changes vtab
// instance. The C side carries an opaque uintptr that we use as a key
// into vtabRegistry; this avoids runtime/cgo.Handle and matches the
// extMap pattern in sqlite/cmd/syzy-ext/main.go. The residual buffer doesn't
// need a mutex: sqlitebridge connections are single-threaded by
// contract (SQLITE_THREADSAFE=2 + SQLITE_OPEN_NOMUTEX) so all xFilter
// / xNext / xClose / xColumn calls on this vtab serialize already.
type goVtab struct {
	feedPath string
	reader   *notify.Reader
	db       *C.sqlite3
	provider ChangesProvider
	residual []bufferedChange
}

// bufferedChange flattens (Notification, Change) into a single row.
// Lossy notifications become a synthetic entry with op == opLossy,
// table empty, pk nil — preserves ordering with the surrounding
// changes. PK and Table are owned (deep-copied off Reader scratch on
// enqueue).
type bufferedChange struct {
	origin         uint64
	seq            uint64
	op             vtabOp
	table          string
	pk             []byte
	pkTruncated    bool
	tableTruncated bool
}

// vtabOp is the wire-level op byte plus a synthetic "lossy" marker.
type vtabOp uint8

const opLossy vtabOp = 0xFF // synthesized for notify.Lossy notifications

// goCursor is per-cursor state. Walks the parent vtab's residual.
//
// rowSeen tracks whether xColumn has been called for the row at the
// current idx. SQLite's LIMIT clause stops iteration after emitting
// the limit-th row, calling xClose without first calling xNext, so
// xClose must use rowSeen to know that residual[idx] was consumed.
type goCursor struct {
	vtab    *goVtab
	idx     int
	rowSeen bool
	tableEq string         // "" == match all
	row     bufferedChange // cached snapshot for xColumn (avoids re-indexing per column)
}

var (
	vtabRegistry   sync.Map // map[uintptr]*goVtab keyed by *C.syzy_vtab
	cursorRegistry sync.Map // map[uintptr]*goCursor keyed by *C.syzy_cursor
	connStateByDB  sync.Map // map[uintptr]*changesConnState keyed by *C.sqlite3
)

// changesConnState carries the per-Conn extras the vtab and scalars
// pull from. Populated by RegisterChangesVTab; consumed by
// syzyVTabConnect (which adopts the Reader so events between
// registration and the first SELECT aren't dropped) and by the
// scalar callbacks (provider lookup).
type changesConnState struct {
	provider ChangesProvider
	// reader is the eagerly-opened Reader, captured at registration
	// time so its lastSeen bookmark predates any subsequent feed
	// activity. syzyVTabConnect adopts it once xConnect fires; if
	// registration's NewReader call failed, this is nil and xFilter
	// retries lazily (covers the auto-spawned-daemon-still-starting
	// race).
	reader *notify.Reader
}

// RegisterChangesVTab installs the eponymous "syzy_changes" virtual
// table and its companion scalar functions on c. feedPath must point
// to the writer-created notify feed (typically
// notify.FeedPath(appPath)).
//
// The notify Reader is opened eagerly so events between registration
// and the first SELECT are captured. If the feed file doesn't exist
// yet (auto-spawned daemon still starting), the Reader is left nil
// and xFilter retries on first SELECT, surfacing a clear error if
// it's still missing.
//
// provider supplies syzy_my_origin and syzy_pk_decode behaviours; pass
// nil to register the vtab without the scalars (linked-mode tests).
func RegisterChangesVTab(c *Conn, feedPath string, provider ChangesProvider) error {
	if c == nil || c.db == nil {
		return Error{Code: ResultMisuse, Msg: "RegisterChangesVTab: nil Conn"}
	}
	state := &changesConnState{provider: provider}
	if r, err := notify.NewReader(notify.ReaderConfig{Path: feedPath}); err == nil {
		state.reader = r
	}
	connStateByDB.Store(uintptr(unsafe.Pointer(c.db)), state)

	cpath := C.CString(feedPath)
	defer C.free(unsafe.Pointer(cpath))
	rc := C.syzy_register_changes_vtab(c.db, cpath)
	if rc != C.SQLITE_OK {
		if state.reader != nil {
			_ = state.reader.Close()
		}
		connStateByDB.Delete(uintptr(unsafe.Pointer(c.db)))
		return newErrorFromDB(rc, c.db)
	}
	if provider != nil {
		if err := registerChangesScalars(c); err != nil {
			return err
		}
	}
	return nil
}

// providerForDB looks up the ChangesProvider registered for db, used
// by the scalar callbacks when they fire.
func providerForDB(db *C.sqlite3) (ChangesProvider, bool) {
	v, ok := connStateByDB.Load(uintptr(unsafe.Pointer(db)))
	if !ok {
		return nil, false
	}
	st, ok := v.(*changesConnState)
	if !ok || st.provider == nil {
		return nil, false
	}
	return st.provider, true
}

//export syzyVTabConnect
func syzyVTabConnect(db *C.sqlite3, feedPath *C.char, vtabID C.uintptr_t, errOut **C.char) C.int {
	v := &goVtab{
		feedPath: C.GoString(feedPath),
		db:       db,
	}
	if val, ok := connStateByDB.Load(uintptr(unsafe.Pointer(db))); ok {
		st := val.(*changesConnState)
		v.provider = st.provider
		// Adopt the eagerly-opened Reader if registration captured
		// one. Subsequent xConnects on the same db (e.g. across
		// reprepares) reuse the same Reader so the bookmark survives.
		v.reader = st.reader
	}
	vtabRegistry.Store(uintptr(vtabID), v)
	return C.SQLITE_OK
}

//export syzyVTabDisconnect
func syzyVTabDisconnect(vtabID C.uintptr_t) {
	// Drop the per-vtab record but leave the Reader alive in
	// connStateByDB — its lifetime is the Conn's, not this vtab
	// instance's. clearChangesState (run at Conn.Close / Conn.Release
	// time) handles final cleanup.
	vtabRegistry.Delete(uintptr(vtabID))
}

// clearChangesState releases per-Conn vtab state. Mirrors
// clearGenIDState; called from Conn.Close and Conn.Release.
func clearChangesState(db *C.sqlite3) {
	val, ok := connStateByDB.LoadAndDelete(uintptr(unsafe.Pointer(db)))
	if !ok {
		return
	}
	if st, ok := val.(*changesConnState); ok && st.reader != nil {
		_ = st.reader.Close()
	}
}

//export syzyVTabCursorOpen
func syzyVTabCursorOpen(vtabID, cursorID C.uintptr_t) {
	val, ok := vtabRegistry.Load(uintptr(vtabID))
	if !ok {
		return
	}
	cursorRegistry.Store(uintptr(cursorID), &goCursor{vtab: val.(*goVtab)})
}

//export syzyVTabCursorClose
func syzyVTabCursorClose(vtabID, cursorID C.uintptr_t) {
	_ = vtabID
	val, ok := cursorRegistry.LoadAndDelete(uintptr(cursorID))
	if !ok {
		return
	}
	cur := val.(*goCursor)
	v := cur.vtab
	if v == nil {
		return
	}
	// Trim consumed events off the front of the residual; preserve
	// any tail the cursor didn't reach (e.g. LIMIT N) for the next
	// xFilter on this connection. SQLite's LIMIT path stops without
	// xNext on the last emitted row, so include the current position
	// when rowSeen is set.
	consumed := cur.idx
	if cur.rowSeen {
		consumed++
	}
	if consumed <= 0 {
		return
	}
	if consumed >= len(v.residual) {
		v.residual = v.residual[:0]
	} else {
		v.residual = append(v.residual[:0], v.residual[consumed:]...)
	}
}

// readerWakePoll bounds how long xFilter blocks in a single Read
// before re-checking sqlite3_is_interrupted. ~500ms keeps Ctrl-C
// latency low even when no events are flowing.
const readerWakePoll = 500 * time.Millisecond

//export syzyVTabFilter
func syzyVTabFilter(vtabID, cursorID C.uintptr_t, idxNum C.int,
	tableArg, timeoutArg *C.sqlite3_value, errOut **C.char) C.int {
	cur := loadCursor(cursorID)
	if cur == nil {
		return C.int(setOutErr(errOut, "cursor not registered"))
	}
	v := cur.vtab
	if v == nil {
		return C.int(setOutErr(errOut, "vtab not registered"))
	}

	cur.idx = 0
	cur.rowSeen = false
	cur.tableEq = ""

	if idxNum&syzyIdxTableEq != 0 && tableArg != nil &&
		C.sx_value_type(tableArg) == C.SQLITE_TEXT {
		n := int(C.sx_value_bytes(tableArg))
		if n > 0 {
			p := C.sx_value_text(tableArg)
			cur.tableEq = C.GoStringN((*C.char)(unsafe.Pointer(p)), C.int(n))
		}
	}
	timeoutMs := int64(-1) // <0 = block indefinitely
	if idxNum&syzyIdxTimeoutEq != 0 && timeoutArg != nil &&
		C.sx_value_type(timeoutArg) == C.SQLITE_INTEGER {
		timeoutMs = int64(C.sx_value_int64(timeoutArg))
	}

	// Lazy Reader open covers the registration-before-feed-file race
	// (auto-spawned daemon still starting). On success, stash the
	// Reader on connStateByDB so subsequent vtab instances on this
	// connection inherit it.
	if v.reader == nil {
		r, err := notify.NewReader(notify.ReaderConfig{Path: v.feedPath})
		if err != nil {
			return C.int(setOutErr(errOut,
				fmt.Sprintf("notify feed at %s unavailable: %v "+
					"(is the syzy daemon running?)", v.feedPath, err)))
		}
		v.reader = r
		if val, ok := connStateByDB.Load(uintptr(unsafe.Pointer(v.db))); ok {
			val.(*changesConnState).reader = r
		}
	}

	// Drop residual entries that don't match this query's filter.
	// Multi-statement filter mixing on one connection is destructive
	// by design — see the "one filter per connection" note in the
	// spec.
	if cur.tableEq != "" && len(v.residual) > 0 {
		v.residual = filterInPlace(v.residual, cur.tableEq)
	}

	if err := drainUntilReady(v, cur.tableEq, timeoutMs); err != nil {
		if errors.Is(err, errInterrupted) {
			return C.SQLITE_INTERRUPT
		}
		return C.int(setOutErr(errOut, err.Error()))
	}
	return C.SQLITE_OK
}

var errInterrupted = errors.New("interrupted")

// drainUntilReady blocks (up to timeoutMs) until v.residual contains
// at least one event, sqlite3_interrupt fires, or the deadline
// expires. timeoutMs semantics: <0 = block indefinitely; 0 = peek
// (single TryRead, no waiting); >0 = bounded ms wait.
func drainUntilReady(v *goVtab, tableEq string, timeoutMs int64) error {
	if len(v.residual) > 0 {
		return nil
	}

	// Peek-only path (timeout_ms = 0): one TryRead and we're done.
	if err := drainPending(v, tableEq); err != nil {
		return err
	}
	if len(v.residual) > 0 || timeoutMs == 0 {
		return nil
	}

	var deadline time.Time
	if timeoutMs > 0 {
		deadline = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}
	for {
		if isInterrupted(v.db) {
			return errInterrupted
		}
		wait := readerWakePoll
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil // EOF — caller sees no rows.
			}
			if remaining < wait {
				wait = remaining
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		notifs, err := v.reader.Read(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			if errors.Is(err, notify.ErrClosed) {
				return errors.New("notify feed closed")
			}
			return fmt.Errorf("notify read: %w", err)
		}
		appendBuffered(&v.residual, notifs, tableEq)
		if len(v.residual) > 0 {
			return nil
		}
	}
}

// drainPending non-blocking-drains the Reader and appends matched
// events to v.residual. Returns nil when the feed is empty.
func drainPending(v *goVtab, tableEq string) error {
	notifs, err := v.reader.TryRead()
	if err != nil {
		if errors.Is(err, notify.ErrClosed) {
			return errors.New("notify feed closed")
		}
		return fmt.Errorf("notify try_read: %w", err)
	}
	if len(notifs) > 0 {
		appendBuffered(&v.residual, notifs, tableEq)
	}
	return nil
}

// appendBuffered deep-copies notifs into dst, applying the table-name
// filter ("" matches all). notify.Reader returns slices that alias
// internal scratch invalidated by the next Read, so PK bytes must own
// their backing array here.
func appendBuffered(dst *[]bufferedChange, notifs []notify.Notification, tableEq string) {
	for _, n := range notifs {
		if n.Lossy {
			*dst = append(*dst, bufferedChange{op: opLossy})
			continue
		}
		for _, c := range n.Changes {
			if tableEq != "" && c.Table != tableEq {
				continue
			}
			*dst = append(*dst, bufferedChange{
				origin:         c.Origin,
				seq:            c.Seq,
				op:             vtabOp(c.Op),
				table:          c.Table, // Reader.drain returns it as a fresh Go string
				pk:             append([]byte(nil), c.PK...),
				pkTruncated:    c.PKTruncated,
				tableTruncated: c.TableTruncated,
			})
		}
	}
}

// filterInPlace removes entries whose table doesn't match tableEq,
// preserving order. Lossy markers always survive.
func filterInPlace(buf []bufferedChange, tableEq string) []bufferedChange {
	w := 0
	for r := 0; r < len(buf); r++ {
		if buf[r].op == opLossy || buf[r].table == tableEq {
			buf[w] = buf[r]
			w++
		}
	}
	return buf[:w]
}

//export syzyVTabNext
func syzyVTabNext(vtabID, cursorID C.uintptr_t) {
	cur := loadCursor(cursorID)
	if cur == nil {
		return
	}
	cur.idx++
	cur.rowSeen = false
}

//export syzyVTabEof
func syzyVTabEof(vtabID, cursorID C.uintptr_t) C.int {
	cur := loadCursor(cursorID)
	if cur == nil || cur.vtab == nil {
		return 1
	}
	if cur.idx >= len(cur.vtab.residual) {
		return 1
	}
	return 0
}

//export syzyVTabColumn
func syzyVTabColumn(vtabID, cursorID C.uintptr_t, ctx *C.sqlite3_context, col C.int) {
	cur := loadCursor(cursorID)
	if cur == nil || cur.vtab == nil || cur.idx >= len(cur.vtab.residual) {
		C.sx_result_null(ctx)
		return
	}
	// Cache the row on first column read of a new position so
	// subsequent column reads on the same row don't re-index.
	// rowSeen also signals xClose that residual[idx] was consumed.
	if !cur.rowSeen {
		cur.row = cur.vtab.residual[cur.idx]
		cur.rowSeen = true
	}
	row := cur.row

	if row.op == opLossy {
		if int(col) == colOp {
			emitText(ctx, "lossy")
		} else {
			C.sx_result_null(ctx)
		}
		return
	}

	switch int(col) {
	case colOrigin:
		C.sx_result_int64(ctx, C.sqlite3_int64(row.origin))
	case colSeq:
		C.sx_result_int64(ctx, C.sqlite3_int64(row.seq))
	case colTableName:
		emitText(ctx, row.table)
	case colOp:
		emitText(ctx, notify.Op(row.op).String())
	case colPK:
		if len(row.pk) == 0 {
			C.sx_result_blob_copy(ctx, nil, 0)
		} else {
			C.sx_result_blob_copy(ctx, unsafe.Pointer(&row.pk[0]), C.int(len(row.pk)))
		}
	case colPKTruncated:
		C.sx_result_int64(ctx, C.sqlite3_int64(boolToCInt(row.pkTruncated)))
	case colTableTruncated:
		C.sx_result_int64(ctx, C.sqlite3_int64(boolToCInt(row.tableTruncated)))
	default:
		C.sx_result_null(ctx)
	}
}

//export syzyVTabRowid
func syzyVTabRowid(vtabID, cursorID C.uintptr_t) C.sqlite3_int64 {
	cur := loadCursor(cursorID)
	if cur == nil {
		return 0
	}
	return C.sqlite3_int64(cur.idx)
}

func loadCursor(id C.uintptr_t) *goCursor {
	v, ok := cursorRegistry.Load(uintptr(id))
	if !ok {
		return nil
	}
	return v.(*goCursor)
}

func isInterrupted(db *C.sqlite3) bool {
	if db == nil {
		return false
	}
	return C.sx_is_interrupted(db) != 0
}

// emitText writes s as the SQL TEXT result. SQLITE_TRANSIENT (inside
// sx_result_text_copy) makes SQLite copy out before returning, so we
// can hand it the Go string's backing pointer directly without a
// CString round-trip.
func emitText(ctx *C.sqlite3_context, s string) {
	if s == "" {
		C.sx_result_text_copy(ctx, nil, 0)
		return
	}
	C.sx_result_text_copy(ctx, (*C.char)(unsafe.Pointer(unsafe.StringData(s))), C.int(len(s)))
}

// setOutErr copies msg into a malloc'd C string at *out and returns
// SQLITE_ERROR. The C side wraps the string into an sqlite3_mprintf
// allocation for SQLite to free; we use plain malloc/free here so we
// don't need to expose sqlite3_malloc to Go directly.
func setOutErr(out **C.char, msg string) int {
	if out == nil || msg == "" {
		return ResultError
	}
	*out = C.CString(msg)
	return ResultError
}

// registerChangesScalars wires syzy_my_origin and syzy_pk_decode onto
// c. Called from RegisterChangesVTab when a non-nil provider is
// supplied.
func registerChangesScalars(c *Conn) error {
	rc := C.syzy_register_changes_scalars(c.db)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, c.db)
	}
	return nil
}

//export syzyGoMyOrigin
func syzyGoMyOrigin(db *C.sqlite3, out *C.sqlite3_int64, errOut **C.char) {
	provider, ok := providerForDB(db)
	if !ok {
		setOutErr(errOut, "syzy_my_origin: no changes provider registered on this connection")
		return
	}
	*out = C.sqlite3_int64(provider.Origin())
}

//export syzyGoPKDecode
func syzyGoPKDecode(db *C.sqlite3, tablePtr *C.char, tableLen C.int,
	pkPtr unsafe.Pointer, pkLen C.int,
	outText **C.char, outLen *C.int, outNull *C.int, errOut **C.char) {
	provider, ok := providerForDB(db)
	if !ok {
		setOutErr(errOut, "syzy_pk_decode: no changes provider registered on this connection")
		return
	}
	table := C.GoStringN(tablePtr, tableLen)
	var pk []byte
	if pkLen > 0 {
		pk = C.GoBytes(pkPtr, pkLen)
	}
	text, decoded := provider.DecodePK(table, pk)
	if !decoded {
		*outNull = 1
		return
	}
	*outText = C.CString(text)
	*outLen = C.int(len(text))
}
