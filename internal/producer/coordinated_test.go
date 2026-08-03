package producer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/unique"
)

type unavailableRegistry struct{}

func (unavailableRegistry) Reserve(context.Context, []unique.Claim) (bool, *unique.Claim, error) {
	return false, nil, unique.ErrUnavailable
}

func (unavailableRegistry) Release(context.Context, []unique.Claim) error { return nil }

// encodeCoordValue builds the canonical reservation value for a single
// text-column coordinated key, the same bytes the producer's commit-time
// reserve computes — so a test can pre-seed the registry with a
// conflicting claim.
func encodeCoordValue(t *testing.T, tab *catalog.Table, uk catalog.UniqueKey, colName, text string) []byte {
	t.Helper()
	cols := make([]crdt.ColValue, len(tab.Columns))
	for i, c := range tab.Columns {
		cols[i] = crdt.ColValue{TypeTag: crdt.ColNull, Column: c.ID}
		if c.Name == colName {
			cols[i] = crdt.ColValue{TypeTag: crdt.ColText, Bytes: []byte(text), Column: c.ID}
		}
	}
	v, hasNull, err := tab.EncodeKeyFromSlice(uk, cols)
	if err != nil {
		t.Fatalf("EncodeKeyFromSlice: %v", err)
	}
	if hasNull {
		t.Fatalf("EncodeKeyFromSlice: unexpected NULL for %q", text)
	}
	return v
}

func coordTable(t *testing.T, f *ddlFixture) (*catalog.Table, catalog.UniqueKey) {
	t.Helper()
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatalf("create coordinated table: %v", err)
	}
	f.waitDrain(t)
	tab, ok := f.cat.Table("u")
	if !ok {
		t.Fatal("u not in catalog")
	}
	if len(tab.UniqueKeys) != 1 || !tab.UniqueKeys[0].Coordinated {
		t.Fatalf("want one coordinated key, got %+v", tab.UniqueKeys)
	}
	return tab, tab.UniqueKeys[0]
}

// TestProducer_ReserveRejectsRemoteConflict pre-seeds the shared registry
// with a value owned by another node, then a local INSERT of that value
// must be rejected at commit (reserve-before-commit) — surfacing to the
// app as an ordinary constraint failure, not a silent conflict.
func TestProducer_ReserveRejectsRemoteConflict(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	// Another node owns "taken@x.com".
	val := encodeCoordValue(t, tab, uk, "email", "taken@x.com")
	ok, _, err := reg.Reserve(context.Background(), []unique.Claim{{
		Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID),
		Value: val, Owner: []byte("other-node"),
	}})
	if err != nil || !ok {
		t.Fatalf("seed reserve: ok=%v err=%v", ok, err)
	}

	// Local SQLite has no such row; the reserve at commit must still catch
	// the remotely-owned value.
	err = f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'taken@x.com')`)
	if err == nil {
		t.Fatal("INSERT of remotely-owned value accepted; want commit rejection")
	}
	if !errors.Is(err, unique.ErrConflict) || errors.Is(err, unique.ErrUnavailable) {
		t.Fatalf("remote conflict classification = %v", err)
	}
	if !sqlitebridge.IsCode(err, sqlitebridge.ResultConstraintCommitHook) {
		t.Fatalf("remote conflict lost commit-hook code: %v", err)
	}
	// A free value still commits.
	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'02', 'free@x.com')`); err != nil {
		t.Fatalf("INSERT of free value rejected: %v", err)
	}
	// And the free value is now reserved to this node's row.
	freeVal := encodeCoordValue(t, tab, uk, "email", "free@x.com")
	if o, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: freeVal}); !held || len(o) == 0 {
		t.Fatalf("free value not reserved after commit: held=%v", held)
	}
}

func TestProducer_ReserveUnavailableIsDistinctFromConflict(t *testing.T) {
	f := newDDLFixtureCfg(t, Config{
		UniqueRegistry:     unavailableRegistry{},
		ReserveRetryBudget: time.Nanosecond,
	})
	coordTable(t, f)

	err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'free@x.com')`)
	if !errors.Is(err, unique.ErrUnavailable) || errors.Is(err, unique.ErrConflict) {
		t.Fatalf("unavailable classification = %v", err)
	}
	if !sqlitebridge.IsCode(err, sqlitebridge.ResultConstraintCommitHook) {
		t.Fatalf("unavailable lost commit-hook code: %v", err)
	}
}

// TestProducer_ReserveRollbackOnRejectLeavesRegistryClean confirms a
// rejected insert reserves nothing (the batch is all-or-nothing and the
// conflicting value stays owned by the other node).
func TestProducer_ReserveRollbackOnRejectLeavesRegistryClean(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	val := encodeCoordValue(t, tab, uk, "email", "taken@x.com")
	reg.Reserve(context.Background(), []unique.Claim{{
		Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID),
		Value: val, Owner: []byte("other-node"),
	}})

	_ = f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'taken@x.com')`)
	// Still owned by the other node, not stolen.
	o, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: val})
	if !held || string(o) != "other-node" {
		t.Fatalf("conflicting value owner = %q held=%v; want other-node", o, held)
	}
}

// TestProducer_ReleaseFreesChangedValue confirms that changing a
// coordinated value releases the old one post-commit, so another row can
// reclaim it.
func TestProducer_ReleaseFreesChangedValue(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`); err != nil {
		t.Fatalf("insert a: %v", err)
	}
	aVal := encodeCoordValue(t, tab, uk, "email", "a@x.com")
	if _, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: aVal}); !held {
		t.Fatal("a not reserved after insert")
	}

	// Change the value: a is released (post-commit), b is reserved.
	if err := f.app.Exec(`UPDATE u SET email = 'b@x.com' WHERE id = x'01'`); err != nil {
		t.Fatalf("update to b: %v", err)
	}
	if _, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: aVal}); held {
		t.Fatal("a still reserved after value change; release did not fire")
	}

	// A different row can now take the freed value.
	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'02', 'a@x.com')`); err != nil {
		t.Fatalf("reclaim of released value rejected: %v", err)
	}
}

// TestProducer_ReleaseFreesDeletedValue confirms deleting a row releases
// its coordinated value.
func TestProducer_ReleaseFreesDeletedValue(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := f.app.Exec(`DELETE FROM u WHERE id = x'01'`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	aVal := encodeCoordValue(t, tab, uk, "email", "a@x.com")
	if _, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: aVal}); held {
		t.Fatal("value still reserved after row delete; release did not fire")
	}
}

// TestProducer_SameTxnChurnNetsToFinal confirms an insert-then-update in
// one transaction reserves only the final value (no leak of the
// intermediate).
func TestProducer_SameTxnChurnNetsToFinal(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	if err := f.app.Exec(`BEGIN;
		INSERT INTO u (id, email) VALUES (x'01', 'intermediate@x.com');
		UPDATE u SET email = 'final@x.com' WHERE id = x'01';
		COMMIT;`); err != nil {
		t.Fatalf("churn txn: %v", err)
	}
	interVal := encodeCoordValue(t, tab, uk, "email", "intermediate@x.com")
	if _, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: interVal}); held {
		t.Fatal("intermediate value leaked into registry")
	}
	finalVal := encodeCoordValue(t, tab, uk, "email", "final@x.com")
	if _, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: finalVal}); !held {
		t.Fatal("final value not reserved")
	}
}

// TestProducer_PKChangeKeepingValueTransfers confirms a PK-changing update
// that keeps the coordinated value transfers ownership rather than
// self-conflicting (the value stays held, by the new PK).
func TestProducer_PKChangeKeepingValueTransfers(t *testing.T) {
	reg := unique.NewLocal()
	f := newDDLFixtureCfg(t, Config{UniqueRegistry: reg})
	tab, uk := coordTable(t, f)

	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Change the PK while keeping the email — a transfer, not a conflict.
	if err := f.app.Exec(`UPDATE u SET id = x'02' WHERE id = x'01'`); err != nil {
		t.Fatalf("PK-changing update keeping value: %v", err)
	}
	// The value is still reserved, now owned by the new PK.
	val := encodeCoordValue(t, tab, uk, "email", "a@x.com")
	o, held := reg.Owner(unique.Claim{Table: [16]byte(tab.ID), Key: [16]byte(uk.KeyID), Value: val})
	if !held {
		t.Fatal("value lost its reservation after PK change")
	}
	if len(o) == 0 {
		t.Fatal("value has empty owner after transfer")
	}
}

// TestProducer_NoRegistryNoReserveOverhead confirms that with no registry
// the coordinated path is inert (and coordinated DDL is rejected, so no
// coordinated keys exist to reserve).
func TestProducer_NoRegistryNoReserveOverhead(t *testing.T) {
	f := newDDLFixture(t) // no UniqueRegistry
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT, UNIQUE(email))`); err != nil {
		t.Fatalf("nullable UNIQUE create: %v", err)
	}
	f.waitDrain(t)
	// Plain inserts work; eventual UNIQUE is unaffected by coordination.
	if err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func TestProducer_NoRegistryRejectsCoordinatedCatalogAddedAfterStart(t *testing.T) {
	f := newDDLFixture(t)
	if err := f.app.Exec(`CREATE TABLE u (id BLOB PRIMARY KEY NOT NULL, email TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	f.waitDrain(t)
	tab, ok := f.cat.Table("u")
	if !ok {
		t.Fatal("u missing from catalog")
	}
	email, ok := tab.Column("email")
	if !ok {
		t.Fatal("email missing from catalog")
	}
	if err := f.sc.WithTx(func(tx *metadata.Tx) error {
		return tx.UpsertKey(metadata.KeyEntry{
			TableID: tab.ID, KeyID: crdt.KeyID{1}, ColumnID: email.ID,
			State: metadata.StateActive, Coordinated: true, CreateSeq: 2,
		})
	}); err != nil {
		t.Fatalf("inject coordinated key: %v", err)
	}
	if err := f.cat.Reload(); err != nil {
		t.Fatalf("reload catalog: %v", err)
	}

	err := f.app.Exec(`INSERT INTO u (id, email) VALUES (x'01', 'a@x.com')`)
	if !errors.Is(err, unique.ErrRegistryRequired) {
		t.Fatalf("insert err = %v, want ErrRegistryRequired", err)
	}
}
