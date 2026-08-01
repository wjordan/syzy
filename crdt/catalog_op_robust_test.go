package crdt

// Adversarial-input regressions for the CatalogOp decoders: a count
// field must never size an allocation beyond the buffer that carries
// it, and bundle nesting must be rejected before recursing. All of
// these must fail fast with an error — never panic, never balloon.

import (
	"encoding/binary"
	"testing"
)

func TestCatalogOp_HugeCountsRejected(t *testing.T) {
	tid := tabID(1)
	kid := opKeyID(2)
	huge := uint64(1) << 62

	framedHeader := func(kind CatalogOpKind) []byte {
		buf := []byte{catalogOpSentinel}
		buf = binary.AppendUvarint(buf, catalogOpVersion)
		buf = binary.AppendUvarint(buf, uint64(kind))
		return buf
	}

	cases := map[string][]byte{}

	// AddColumn: huge column count.
	b := framedHeader(OpAddColumn)
	b = append(b, tid[:]...)
	b = binary.AppendUvarint(b, huge)
	cases["column count"] = b

	// AddUniqueKey: huge member count (after coord + empty-predicate bytes).
	b = framedHeader(OpAddUniqueKey)
	b = append(b, tid[:]...)
	b = append(b, kid[:]...)
	b = append(b, 0, 0) // coordinated=false, predicate absent
	b = binary.AppendUvarint(b, huge)
	cases["key member count"] = b

	// Bundle: huge sub-op count.
	b = framedHeader(OpBundle)
	b = binary.AppendUvarint(b, huge)
	cases["bundle count"] = b

	// CreateTable: huge key count (one valid column first).
	b = framedHeader(OpCreateTable)
	b = append(b, tid[:]...)
	b = appendString(b, "t")
	b = appendColumns(b, []CatalogColumn{{ID: colID(1), Name: "a", ClockGroup: "row"}})
	b = binary.AppendUvarint(b, huge)
	cases["key count"] = b

	for name, buf := range cases {
		if _, err := DecodeCatalogOp(buf); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestPredicate_HugeCountsRejected(t *testing.T) {
	// PredAnd with a huge kid count.
	buf := []byte{1, byte(PredAnd)}
	buf = binary.AppendUvarint(buf, 1<<62)
	if _, _, err := readPredicate(buf); err == nil {
		t.Error("kid count: want error")
	}
	// PredIn with a huge literal count.
	buf = []byte{1, byte(PredIn)}
	cid := colID(1)
	buf = append(buf, cid[:]...)
	buf = binary.AppendUvarint(buf, 1<<62)
	if _, _, err := readPredicate(buf); err == nil {
		t.Error("literal count: want error")
	}
}

func TestCatalogOp_DeepBundleNestingRejected(t *testing.T) {
	tid := tabID(9)
	inner := append([]byte{byte(OpDropTable)}, tid[:]...)
	for range 10_000 {
		b := []byte{byte(OpBundle)}
		b = binary.AppendUvarint(b, 1)
		b = binary.AppendUvarint(b, uint64(len(inner)))
		b = append(b, inner...)
		inner = b
	}
	if _, err := DecodeCatalogOp(inner); err == nil {
		t.Fatal("nested bundle chain: want error")
	}
}
