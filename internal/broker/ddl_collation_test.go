package broker

import (
	"strings"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

// TestRenderColumnDefCollation confirms a receiver reconstructs a column
// with the origin's declared collation, so text comparisons and ordering
// match across replicas. BINARY is the default and omitted.
func TestRenderColumnDefCollation(t *testing.T) {
	t.Parallel()
	got, err := renderColumnDef(crdt.CatalogColumn{Name: "email", Type: "TEXT", NotNull: true, Collation: crdt.CollNocase})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "COLLATE NOCASE") {
		t.Fatalf("renderColumnDef = %q, want COLLATE NOCASE", got)
	}
	if got, err := renderColumnDef(crdt.CatalogColumn{Name: "email", Type: "TEXT"}); err != nil || strings.Contains(got, "COLLATE") {
		t.Fatalf("binary column rendered COLLATE: %q (err=%v)", got, err)
	}
}
