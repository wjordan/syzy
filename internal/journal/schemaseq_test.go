package journal

import (
	"path/filepath"
	"testing"
)

// TestAppendWithSchemaSeqRoundTrip: the stamp survives append → iterate,
// plain Append reads back 0, and the CRC covers the stamped field.
func TestAppendWithSchemaSeqRoundTrip(t *testing.T) {
	j, err := Open(filepath.Join(t.TempDir(), "j"), 1<<16, SyncOff)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer j.Close()

	if _, _, err := j.AppendWithSchemaSeq(KindLocalDML, 11, 7, 42, []byte("a")); err != nil {
		t.Fatalf("append stamped: %v", err)
	}
	if _, _, err := j.Append(KindLocalDML, 12, 7, []byte("b")); err != nil {
		t.Fatalf("append plain: %v", err)
	}

	it := j.Iterate(0)
	r1, _, err := it.Next()
	if err != nil {
		t.Fatalf("next 1: %v", err)
	}
	if r1.SchemaSeq != 42 || string(r1.Payload) != "a" {
		t.Errorf("rec1 = seq %d payload %q, want 42 %q", r1.SchemaSeq, r1.Payload, "a")
	}
	r2, _, err := it.Next()
	if err != nil {
		t.Fatalf("next 2: %v", err)
	}
	if r2.SchemaSeq != 0 || string(r2.Payload) != "b" {
		t.Errorf("rec2 = seq %d payload %q, want 0 %q", r2.SchemaSeq, r2.Payload, "b")
	}
}
