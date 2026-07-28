package producer

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/wjordan/syzy/sqlitebridge"
)

// TestHookFireCountsAcrossDMLPatterns probes whether commit_hook and
// wal_hook fire 1:1 across various DML patterns. If they do, we can
// safely replace wal_hook with commit_hook to skip wal_hook's
// installation overhead. If commit_hook fires more times than
// wal_hook, the extra firings are commits that didn't actually write
// a WAL frame — those would create orphan journal records under a
// commit_hook-only design and require explicit orphan-record handling
// (timeout-based abort) in the confirmer.
//
// Captured findings (Linux x86_64, sqlite as built):
//
//	INSERT new row                : commit=1 wal=1
//	UPDATE changing value         : commit=1 wal=1
//	UPDATE same value (no-op)     : commit=1 wal=0   ← orphan under commit_hook
//	UPDATE matching no rows       : commit=1 wal=0   ← orphan under commit_hook
//	DELETE                        : commit=1 wal=1
//	BEGIN; COMMIT (empty)         : commit=0 wal=0
//	DDL CREATE TABLE              : commit=1 wal=1
//
// Diagnostic only; uses a fresh sqlitebridge.Conn so it doesn't
// depend on producer wiring.
func TestHookFireCountsAcrossDMLPatterns(t *testing.T) {
	cases := []struct {
		name  string
		setup string
		op    string
	}{
		{name: "INSERT new row",
			op: `INSERT INTO event (id, n) VALUES (x'02', 'b')`},
		{name: "UPDATE changing value",
			setup: `INSERT INTO event (id, n) VALUES (x'10', 'old')`,
			op:    `UPDATE event SET n = 'new' WHERE id = x'10'`},
		{name: "UPDATE same value (no-op write)",
			setup: `INSERT INTO event (id, n) VALUES (x'20', 'same')`,
			op:    `UPDATE event SET n = 'same' WHERE id = x'20'`},
		{name: "UPDATE matching no rows",
			op: `UPDATE event SET n = 'x' WHERE id = x'ff'`},
		{name: "DELETE",
			setup: `INSERT INTO event (id, n) VALUES (x'30', 'del')`,
			op:    `DELETE FROM event WHERE id = x'30'`},
		{name: "BEGIN; COMMIT (empty)",
			op: `BEGIN; COMMIT`},
		{name: "DDL (CREATE TABLE)",
			op: `CREATE TABLE diagnostic_x (k INTEGER PRIMARY KEY)`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			conn, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if err := conn.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
				t.Fatalf("schema: %v", err)
			}
			var commitFires, walFires atomic.Int64
			conn.SetCommitHook(func() int { commitFires.Add(1); return 0 })
			conn.SetWALHook(func(string, int) int { walFires.Add(1); return 0 })
			if c.setup != "" {
				if err := conn.Exec(c.setup); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}
			beforeCommit := commitFires.Load()
			beforeWAL := walFires.Load()
			if err := conn.Exec(c.op); err != nil {
				t.Fatalf("op: %v", err)
			}
			dCommit := commitFires.Load() - beforeCommit
			dWAL := walFires.Load() - beforeWAL
			t.Logf("commit_hook fires: %d, wal_hook fires: %d", dCommit, dWAL)
			if dCommit != dWAL {
				t.Logf("MISMATCH for %q: commit=%d wal=%d", c.name, dCommit, dWAL)
			}
		})
	}
}
