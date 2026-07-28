package producer

import (
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/sqlitebridge"
)

// TestJournalSyncOnEndToEnd: SyncOn is transparent — same wire
// output as the default mode, only the fsync syscalls differ.
func TestJournalSyncOnEndToEnd(t *testing.T) {
	dir := t.TempDir()
	f := setupTBWithConfig(t, Config{
		JournalDir:  filepath.Join(dir, "jrn"),
		JournalSync: JournalSyncForceOn,
	})
	if err := f.app.Exec(`INSERT INTO event (id, n) VALUES (x'01', 'hello')`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	f.waitDrain(t)
	payloads := f.emitted()
	if len(payloads) != 1 {
		t.Fatalf("emitted = %d; want 1", len(payloads))
	}
	cs, err := crdt.Decode(payloads[0])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if cs.Dot.Origin != 42 || cs.Dot.Seq != 1 {
		t.Errorf("Dot = %+v; want {42,1}", cs.Dot)
	}
	if len(cs.Records) != 1 {
		t.Errorf("records = %d; want 1", len(cs.Records))
	}
}

// TestResolveJournalSyncMode covers the auto-derive map
// (NORMAL/OFF → SyncOff, FULL/EXTRA → SyncOn) plus the two
// override settings. The CREATE TABLE before each pragma read
// ensures SQLITE_DEFAULT_WAL_SYNCHRONOUS has taken effect — see
// TestResolveJournalSyncMode_FreshWALDB for the pre-first-write case.
func TestResolveJournalSyncMode(t *testing.T) {
	cases := []struct {
		name       string
		pragma     string // empty = leave at WAL-default (NORMAL post-first-write)
		setting    JournalSyncSetting
		wantSyncOn bool
	}{
		{"auto/default(WAL→NORMAL)", "", JournalSyncAuto, false},
		{"auto/NORMAL", "PRAGMA synchronous = NORMAL", JournalSyncAuto, false},
		{"auto/FULL", "PRAGMA synchronous = FULL", JournalSyncAuto, true},
		{"auto/EXTRA", "PRAGMA synchronous = EXTRA", JournalSyncAuto, true},
		{"auto/OFF", "PRAGMA synchronous = OFF", JournalSyncAuto, false},
		{"forceOn/NORMAL ignores pragma", "PRAGMA synchronous = NORMAL", JournalSyncForceOn, true},
		{"forceOff/FULL ignores pragma", "PRAGMA synchronous = FULL", JournalSyncForceOff, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer app.Close()
			// PRAGMA WAL + a no-op DDL applies the build-time
			// SQLITE_DEFAULT_WAL_SYNCHRONOUS downgrade.
			if err := app.Exec(`PRAGMA journal_mode = WAL; CREATE TABLE _seed (a INT)`); err != nil {
				t.Fatalf("WAL+seed: %v", err)
			}
			if tc.pragma != "" {
				if err := app.Exec(tc.pragma); err != nil {
					t.Fatalf("set pragma: %v", err)
				}
			}
			mode, reason, err := resolveJournalSyncMode(app, tc.setting)
			if err != nil {
				t.Fatalf("resolveJournalSyncMode: %v", err)
			}
			gotOn := mode == journal.SyncOn
			if gotOn != tc.wantSyncOn {
				t.Errorf("mode = %s (%s); want SyncOn=%v", mode, reason, tc.wantSyncOn)
			}
			if reason == "" {
				t.Errorf("empty reason; want non-empty diagnostic")
			}
		})
	}
}

// TestResolveJournalSyncMode_FreshWALDB pins the SQLITE_DEFAULT_-
// WAL_SYNCHRONOUS timing subtlety: on a never-written WAL DB the
// PRAGMA reads as FULL, so auto-derive returns SyncOn — a
// durable-by-default fallback. See ARCHITECTURE.md "Host-Level Desync".
func TestResolveJournalSyncMode_FreshWALDB(t *testing.T) {
	dir := t.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer app.Close()
	if err := app.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatalf("WAL: %v", err)
	}
	mode, reason, err := resolveJournalSyncMode(app, JournalSyncAuto)
	if err != nil {
		t.Fatalf("resolveJournalSyncMode: %v", err)
	}
	if mode != journal.SyncOn {
		t.Errorf("fresh WAL DB pre-first-write: mode = %s (%s); want SyncOn (default-FULL fallback)", mode, reason)
	}
}
