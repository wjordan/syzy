package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
)

func TestStatusReportsSchemaUnhealthy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "app.db")
	if err := os.MkdirAll(layout.MetaDir(dbPath), 0o755); err != nil {
		t.Fatalf("create metadata dir: %v", err)
	}
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		t.Fatalf("open metadata: %v", err)
	}
	if _, err := sc.MarkSchemaUnhealthy(9, "invalid schema event"); err != nil {
		_ = sc.Close()
		t.Fatalf("mark schema unhealthy: %v", err)
	}
	if err := sc.Close(); err != nil {
		t.Fatalf("close metadata: %v", err)
	}

	var out bytes.Buffer
	if err := statusCmdTo([]string{"--db", dbPath}, &out); err != nil {
		t.Fatalf("statusCmdTo: %v", err)
	}
	if got := out.String(); !strings.Contains(got,
		"schema_health unhealthy  seq=9  reason=invalid schema event") {
		t.Fatalf("status output = %q; want unhealthy sequence and reason", got)
	}
}
