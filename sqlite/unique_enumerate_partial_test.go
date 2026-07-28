package sqlite

import (
	"context"
	"testing"

	"github.com/wjordan/syzy/schemalog"
)

// TestEnumerateCoordinatedClaims_Partial verifies the leaseholder
// rebuild path applies the partial index predicate: only participating
// (predicate-true) rows contribute a reservation claim, so a soft-deleted
// row that shares an email with a live one does not.
func TestEnumerateCoordinatedClaims_Partial(t *testing.T) {
	node, err := Open(context.Background(), Config{
		Path:      t.TempDir() + "/app.db",
		SchemaLog: schemalog.NewLocal(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	exec := func(sql string) {
		t.Helper()
		if err := node.Exec(sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, deleted_at INTEGER)`)
	exec(`CREATE UNIQUE INDEX uq_email ON users(email) WHERE deleted_at IS NULL`)
	exec(`INSERT INTO users(id,email,deleted_at) VALUES (1,'a@x',NULL)`) // live
	exec(`INSERT INTO users(id,email,deleted_at) VALUES (2,'b@x',NULL)`) // live
	exec(`INSERT INTO users(id,email,deleted_at) VALUES (3,'a@x',5)`)    // soft-deleted, shares email

	snap, err := enumerateCoordinatedClaims(node.catalog, node.appWrite)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	if len(snap.Keys) != 1 {
		t.Fatalf("enumerate returned %d key identities, want 1", len(snap.Keys))
	}
	if len(snap.Claims) != 2 {
		t.Fatalf("enumerate returned %d claims, want 2 (the two live rows)", len(snap.Claims))
	}

	// Soft-delete the remaining live 'a@x'; only 'b@x' should now enumerate.
	exec(`UPDATE users SET deleted_at=9 WHERE id=1`)
	snap, err = enumerateCoordinatedClaims(node.catalog, node.appWrite)
	if err != nil {
		t.Fatalf("enumerate after soft-delete: %v", err)
	}
	if len(snap.Claims) != 1 {
		t.Fatalf("enumerate returned %d claims, want 1 (only b@x)", len(snap.Claims))
	}
}
