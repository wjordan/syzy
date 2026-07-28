package sqlite

import (
	"fmt"

	"github.com/wjordan/syzy/sqlitebridge"
)

// ConsistencyResult summarizes app.db <-> metadata.db row_clock agreement.
// Orphans is the total count of live (odd-CL) row_clock entries that have no
// corresponding row in app.db, summed across replicated tables. PerTable carries
// the per-table breakdown (only tables with a positive orphan count appear).
type ConsistencyResult struct {
	Orphans  int
	PerTable map[string]int
}

// MetadataPathFor returns the metadata.db sidecar path for an app.db at appPath
// (the "<app>-syzy/metadata.db" layout syzy writes).
func MetadataPathFor(appPath string) string { return appPath + "-syzy/metadata.db" }

// CheckRestoreConsistency compares a restored app.db against its metadata.db
// row_clock and reports orphan rows: a live (odd-CL) row_clock entry whose table
// has fewer materialized app.db rows than live clocks. A consistent restore has
// zero orphans; a non-zero count means the two streams were reconstructed to
// different logical points (e.g. a metadata baseline anchored ahead of the app
// stream, or a hole in the app delta chain), so the node's CRDT metadata claims
// rows the served table lacks.
//
// Read-only: both files are opened OpenReadOnly. Diagnostics-only — it never
// mutates either DB. It is a NET per-table count (live clocks minus rows), so it
// is a detection signal, not an exact orphan-PK enumeration.
func CheckRestoreConsistency(appPath, metaPath string) (ConsistencyResult, error) {
	res := ConsistencyResult{PerTable: map[string]int{}}

	meta, err := sqlitebridge.Open(metaPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return res, fmt.Errorf("open metadata %s: %w", metaPath, err)
	}
	defer meta.Close()
	app, err := sqlitebridge.Open(appPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return res, fmt.Errorf("open app %s: %w", appPath, err)
	}
	defer app.Close()

	// table_id (hex) -> name, and table_id (hex) -> live-clock count. hex() keeps
	// the blob key as text so no parameter binding is needed.
	names, err := scanPairs(meta, `SELECT hex(table_id), name FROM syzy_table`)
	if err != nil {
		return res, fmt.Errorf("scan syzy_table: %w", err)
	}
	liveByID, err := scanCounts(meta, `SELECT hex(table_id), count(*) FROM row_clock WHERE cl % 2 = 1 GROUP BY table_id`)
	if err != nil {
		return res, fmt.Errorf("scan row_clock: %w", err)
	}

	for id, name := range names {
		if name == "" {
			continue
		}
		live := liveByID[id]
		if live == 0 {
			continue
		}
		rows, ok := tableRowCount(app, name)
		if !ok {
			continue // table absent in app.db (dropped/legacy) — not an orphan signal
		}
		if d := live - rows; d > 0 {
			res.PerTable[name] = d
			res.Orphans += d
		}
	}
	return res, nil
}

// scanPairs runs q (two text columns) and returns col0->col1.
func scanPairs(c *sqlitebridge.Conn, q string) (map[string]string, error) {
	out := map[string]string{}
	st, _, err := c.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer st.Finalize()
	for {
		row, err := st.Step()
		if err != nil {
			return nil, err
		}
		if !row {
			return out, nil
		}
		out[st.ColumnText(0)] = st.ColumnText(1)
	}
}

// scanCounts runs q (text key, int count) and returns key->count.
func scanCounts(c *sqlitebridge.Conn, q string) (map[string]int, error) {
	out := map[string]int{}
	st, _, err := c.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer st.Finalize()
	for {
		row, err := st.Step()
		if err != nil {
			return nil, err
		}
		if !row {
			return out, nil
		}
		out[st.ColumnText(0)] = int(st.ColumnInt64(1))
	}
}

// tableRowCount returns the row count of a user table, or (0,false) if the table
// does not exist (so a metadata table with no app.db counterpart is skipped).
func tableRowCount(c *sqlitebridge.Conn, name string) (int, bool) {
	st, _, err := c.Prepare(`SELECT count(*) FROM "` + name + `"`)
	if err != nil {
		return 0, false
	}
	defer st.Finalize()
	if row, err := st.Step(); err != nil || !row {
		return 0, false
	}
	return int(st.ColumnInt64(0)), true
}
