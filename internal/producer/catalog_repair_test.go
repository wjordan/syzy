package producer

import (
	"io"
	"log/slog"
	"testing"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/unique"
)

// TestRepair_KeepsCoordinatedKeyWithoutPhysicalIndex is the receiver-restart
// hazard: a receiver holds a coordinated key as catalog metadata only (no
// physical UNIQUE index — receivers never materialize one), so a repair pass
// deriving the desired key set from pragma_index_list must not read the key
// as an orphan and drop it. Before the coordinated-keys-are-metadata-
// authoritative rule, the first restart after learning the key silently
// stopped reserving for local writes on this node.
func TestRepair_KeepsCoordinatedKeyWithoutPhysicalIndex(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: unique.NewLocal()})
	if err := f.app.Exec(`CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL DEFAULT (uuidv7()), email TEXT NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	tab, ok := f.cat.Table("users")
	if !ok {
		t.Fatalf("users not in catalog")
	}
	col, ok := tab.Column("email")
	if !ok {
		t.Fatalf("users.email not in catalog")
	}
	var kid crdt.KeyID
	kid[0] = 0xA1
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertKey(metadata.KeyEntry{
			TableID: tab.ID, KeyID: kid, ColumnID: col.ID, Ordinal: 0,
			State: metadata.StateActive, Coordinated: true, CreateSeq: 500,
		})
	}); err != nil {
		t.Fatalf("inject coordinated key: %v", err)
	}
	if err := f.cat.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	stats, err := RepairUniqueKeys(f.app, f.cat, f.sc, log)
	if err != nil {
		t.Fatalf("RepairUniqueKeys: %v", err)
	}
	if stats.Dropped != 0 {
		t.Fatalf("repair dropped %d key(s); a coordinated key without physical backing must survive", stats.Dropped)
	}
	tab, _ = f.cat.Table("users")
	found := false
	for _, uk := range tab.UniqueKeys {
		if uk.KeyID == kid {
			found = true
		}
	}
	if !found {
		t.Fatalf("coordinated key gone from catalog after repair")
	}
}
