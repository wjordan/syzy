package sqlite_test

import (
	"testing"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/producer"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// extSim is the in-process stand-in for a SQLite loadable-extension
// producer: claims its own origin slot on the same app.db the daemon
// owns, installs the producer hooks, and writes the journal — without
// running the broadcast pipeline. The daemon's secondary-drainer scan
// is what carries those records out.
//
// The lifecycle mirrors what the c-shared `sqlite3_syzy_init` will do
// once that exists.
type extSim struct {
	originClaim *layout.OriginClaim
	writer      *sqlitebridge.Conn
	meta        *metadata.Store
	producer    *producer.Producer
}

func openSimulatedExtension(t *testing.T, dbPath string) *extSim {
	t.Helper()
	originClaim, err := layout.Acquire(dbPath, 0, 0)
	if err != nil {
		t.Fatalf("ext: acquire origin: %v", err)
	}

	writer, err := sqlitebridge.Open(dbPath, 0)
	if err != nil {
		originClaim.Release()
		t.Fatalf("ext: open writer: %v", err)
	}
	if err := writer.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000`); err != nil {
		writer.Close()
		originClaim.Release()
		t.Fatalf("ext: pragma: %v", err)
	}

	// Read metadata — daemon already created cluster_id + node_id.
	// Extension never writes to metadata.db.
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		writer.Close()
		originClaim.Release()
		t.Fatalf("ext: open metadata: %v", err)
	}

	cat, err := catalog.SeedFromSchema(writer, sc)
	if err != nil {
		sc.Close()
		writer.Close()
		originClaim.Release()
		t.Fatalf("ext: seed catalog: %v", err)
	}

	cache := nodestate.New(originClaim.Origin)
	if err := cache.LoadFromMeta(sc); err != nil {
		sc.Close()
		writer.Close()
		originClaim.Release()
		t.Fatalf("ext: cache LoadFromMeta: %v", err)
	}

	prod, err := producer.New(writer, sc, cat, producer.Config{
		JournalDir:   layout.JournalDir(dbPath, originClaim.Origin),
		Cache:        cache,
		Origin:       originClaim.Origin,
		ProducerOnly: true,
	})
	if err != nil {
		sc.Close()
		writer.Close()
		originClaim.Release()
		t.Fatalf("ext: producer.New: %v", err)
	}

	return &extSim{
		originClaim: originClaim,
		writer:      writer,
		meta:        sc,
		producer:    prod,
	}
}

func (e *extSim) Close() {
	if e == nil {
		return
	}
	if e.producer != nil {
		_ = e.producer.Close()
	}
	if e.writer != nil {
		_ = e.writer.Close()
	}
	if e.meta != nil {
		_ = e.meta.Close()
	}
	if e.originClaim != nil {
		_ = e.originClaim.Release()
	}
}
