package sqlitebridge

/*
#include <stdint.h>
#include <stdlib.h>
#include "syzy_sqlite.h"
#include "funcs.h"
*/
import "C"

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// gen_id layout: 30-bit random partition selected lazily per (db, table)
// per process, then a 33-bit in-memory monotonic counter. The high bit of
// the int64 is always 0, so values are positive int63s.
const (
	genIDPartitionBits = 30
	genIDCounterBits   = 33
	genIDCounterMax    = (uint64(1) << genIDCounterBits) - 1
	genIDPartitionMax  = (uint32(1) << genIDPartitionBits) - 1
	// genIDProbeRetries caps how many random partitions we'll try before
	// giving up. With 30 partition bits and a probe per try, occupancy
	// would have to be extreme for even a few retries — 16 is generous.
	genIDProbeRetries = 16
)

type genIDKey struct {
	db    uintptr
	table string
}

type genIDState struct {
	partition uint32
	counter   atomic.Uint64
}

var (
	// genIDStates holds *genIDState keyed by genIDKey. Steady-state
	// reads bypass any mutex; only first-call partition selection
	// serializes via genIDInitMu.
	genIDStates sync.Map
	genIDInitMu sync.Mutex
)

// registerFuncs wires uuidv7 and gen_id onto c. Called from Open.
func registerFuncs(c *Conn) error {
	rc := C.syzy_register_funcs(c.db)
	if rc != C.SQLITE_OK {
		return newErrorFromDB(rc, c.db)
	}
	return nil
}

// clearGenIDState drops every cached partition keyed by db. Called from
// Conn.Close so a future Open that happens to receive the same handle
// pointer (SQLite recycles them) starts fresh.
func clearGenIDState(db *C.sqlite3) {
	target := uintptr(unsafe.Pointer(db))
	genIDStates.Range(func(k, _ any) bool {
		if k.(genIDKey).db == target {
			genIDStates.Delete(k)
		}
		return true
	})
}

//export syzyGoGenID
func syzyGoGenID(db *C.sqlite3, tablePtr *C.char, tableLen C.int, out *C.sqlite3_int64, errOut **C.char) {
	table := C.GoStringN(tablePtr, tableLen)
	id, err := genIDNext(db, table)
	if err != nil {
		*errOut = C.CString(err.Error())
		return
	}
	*out = C.sqlite3_int64(id)
}

// genIDNext returns the next id for (db, table). On first call it picks
// an unoccupied partition by random probe; subsequent calls are a single
// atomic increment.
func genIDNext(db *C.sqlite3, table string) (int64, error) {
	key := genIDKey{db: uintptr(unsafe.Pointer(db)), table: table}
	if v, ok := genIDStates.Load(key); ok {
		return issueGenID(v.(*genIDState))
	}

	genIDInitMu.Lock()
	defer genIDInitMu.Unlock()
	// Re-check after acquiring; another goroutine may have raced us in.
	if v, ok := genIDStates.Load(key); ok {
		return issueGenID(v.(*genIDState))
	}
	partition, err := genIDChoosePartition(db, table)
	if err != nil {
		return 0, err
	}
	st := &genIDState{partition: partition}
	genIDStates.Store(key, st)
	return issueGenID(st)
}

func issueGenID(st *genIDState) (int64, error) {
	counter := st.counter.Add(1)
	if counter > genIDCounterMax {
		return 0, fmt.Errorf("gen_id: counter exhausted in partition %d", st.partition)
	}
	return (int64(st.partition) << genIDCounterBits) | int64(counter), nil
}

// genIDChoosePartition discovers the table's PK column then probes random
// 30-bit partitions until it finds one with no existing rows.
func genIDChoosePartition(db *C.sqlite3, table string) (uint32, error) {
	// Synthesize a Conn around the raw handle just for Prepare/Step. The
	// state field stays nil; that's safe because Prepare/Step/Bind/Finalize
	// don't touch it. Don't pass this Conn anywhere that might call
	// SetXxxHook or Close.
	c := &Conn{db: db}

	pkCol, err := genIDFindPKColumn(c, table)
	if err != nil {
		return 0, err
	}
	probe := fmt.Sprintf(`SELECT 1 FROM %s WHERE %s >= ? AND %s < ? LIMIT 1`,
		QuoteIdent(table), QuoteIdent(pkCol), QuoteIdent(pkCol))

	for i := 0; i < genIDProbeRetries; i++ {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("gen_id: rand.Read: %w", err)
		}
		// Drop partition 0: its keyspace starts at id=1, which collides
		// with the most common manually-inserted starter values.
		partition := binary.LittleEndian.Uint32(b[:]) & genIDPartitionMax
		if partition == 0 {
			continue
		}
		lo := int64(partition) << genIDCounterBits
		hi := int64(partition+1) << genIDCounterBits
		occupied, err := genIDProbe(c, probe, lo, hi)
		if err != nil {
			return 0, err
		}
		if !occupied {
			return partition, nil
		}
	}
	return 0, fmt.Errorf("gen_id: no free partition for %q after %d probes", table, genIDProbeRetries)
}

// genIDFindPKColumn returns the name of the single-column primary key for
// table, or an error if the table is missing or has a composite PK.
func genIDFindPKColumn(c *Conn, table string) (string, error) {
	stmt, _, err := c.Prepare(fmt.Sprintf(`PRAGMA table_info(%s)`, QuoteIdent(table)))
	if err != nil {
		return "", fmt.Errorf("gen_id: PRAGMA table_info: %w", err)
	}
	if stmt == nil {
		return "", fmt.Errorf("gen_id: no such table %q", table)
	}
	defer stmt.Finalize()

	var pkCol string
	pkCount := 0
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return "", fmt.Errorf("gen_id: PRAGMA table_info step: %w", err)
		}
		if !hasRow {
			break
		}
		// columns: cid(0), name(1), type(2), notnull(3), dflt_value(4), pk(5)
		if stmt.ColumnInt64(5) > 0 {
			pkCount++
			pkCol = stmt.ColumnText(1)
		}
	}
	if pkCount == 0 {
		return "", fmt.Errorf("gen_id: table %q has no primary key", table)
	}
	if pkCount > 1 {
		return "", fmt.Errorf("gen_id: table %q has a composite primary key; gen_id supports single-column integer PKs only", table)
	}
	return pkCol, nil
}

// genIDProbe runs the prepared probe SQL and reports whether any row falls
// in [lo, hi).
func genIDProbe(c *Conn, probe string, lo, hi int64) (bool, error) {
	stmt, _, err := c.Prepare(probe)
	if err != nil {
		return false, fmt.Errorf("gen_id: prepare probe: %w", err)
	}
	defer stmt.Finalize()
	if err := stmt.BindInt64(1, lo); err != nil {
		return false, err
	}
	if err := stmt.BindInt64(2, hi); err != nil {
		return false, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return false, fmt.Errorf("gen_id: probe step: %w", err)
	}
	return hasRow, nil
}
