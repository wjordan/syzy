package producer

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// BenchmarkCommitInsert_JournalSync sweeps the commit cost across
// the four operator-reachable points in the durability matrix:
// {JournalSync off|on} × {app.db synchronous=NORMAL|FULL}.
//
//   - off/NORMAL:  today's default (host-crash window on both files)
//   - off/FULL:    app.db sync only — journal can still lose trail
//   - on/NORMAL:   journal sync only — recovery handles app.db trail
//   - on/FULL:     symmetric, fully synchronous commit path
//
// In production the journal mode tracks app.db's synchronous via
// JournalSyncAuto; this bench uses explicit ForceOn/ForceOff to
// reach the asymmetric (off/FULL, on/NORMAL) cells.
//
// The benchmark requires a real-disk b.TempDir() to be meaningful.
// Tmpfs hides msync/fdatasync cost; configure $TMPDIR to a disk-
// backed path before running.
func BenchmarkCommitInsert_JournalSync(b *testing.B) {
	cells := []struct {
		label string
		mode  JournalSyncSetting
	}{
		{"off", JournalSyncForceOff},
		{"on", JournalSyncForceOn},
	}
	for _, cell := range cells {
		for _, appSync := range []string{"NORMAL", "FULL"} {
			name := fmt.Sprintf("journal=%s/app=%s", cell.label, appSync)
			b.Run(name, func(b *testing.B) {
				runJournalSyncCommitBench(b, cell.mode, appSync)
			})
		}
	}
}

func runJournalSyncCommitBench(b *testing.B, jSync JournalSyncSetting, appSynchronous string) {
	// Inline fixture: needs to set PRAGMA synchronous on the writer
	// before producer.New so the auto-derive path would also see it
	// (the bench forces explicitly so the asymmetric cells remain
	// reachable, but we keep the timing aligned with how an operator
	// would do it).
	dir := b.TempDir()
	app, err := sqlitebridge.Open(filepath.Join(dir, "app.db"), 0)
	if err != nil {
		b.Fatalf("open app: %v", err)
	}
	b.Cleanup(func() { _ = app.Close() })
	if err := app.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = ` + appSynchronous + `; CREATE TABLE event (id BLOB PRIMARY KEY NOT NULL, n TEXT)`); err != nil {
		b.Fatalf("init schema: %v", err)
	}

	sc, err := metadata.Open(filepath.Join(dir, "syzy.db"))
	if err != nil {
		b.Fatalf("open metadata: %v", err)
	}
	b.Cleanup(func() { _ = sc.Close() })
	if err := sc.SetClusterID(crdt.ClusterID{1, 2, 3}); err != nil {
		b.Fatalf("SetClusterID: %v", err)
	}
	if err := sc.SetNodeID(crdt.Origin(42)); err != nil {
		b.Fatalf("SetNodeID: %v", err)
	}
	cat, err := catalog.SeedFromSchema(app, sc)
	if err != nil {
		b.Fatalf("SeedFromSchema: %v", err)
	}
	cache := nodestate.New(crdt.Origin(42))
	p, err := New(app, sc, cat, Config{
		JournalDir:  filepath.Join(dir, "jrn"),
		Cache:       cache,
		JournalSync: jSync,
	})
	if err != nil {
		b.Fatalf("producer.New: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })

	stmt, _, err := app.Prepare(`INSERT INTO event (id, n) VALUES (?, 'x')`)
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Reset()
		var id [8]byte
		for j := 0; j < 8; j++ {
			id[j] = byte(i >> (8 * j))
		}
		if err := stmt.BindBlob(1, id[:]); err != nil {
			b.Fatalf("Bind: %v", err)
		}
		if _, err := stmt.Step(); err != nil {
			b.Fatalf("Step: %v", err)
		}
	}
}
