package catalog

import (
	"bytes"
	"testing"

	"github.com/wjordan/syzy/crdt"
)

func TestTupleRoundTrip(t *testing.T) {
	var a, b crdt.ColumnID
	a[0], b[0] = 1, 2
	want := []crdt.ColValue{
		{Column: a, TypeTag: crdt.ColInt, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 7}},
		{Column: b, TypeTag: crdt.ColText, Bytes: []byte("seven")},
	}
	tuple, err := EncodeTuple(want)
	if err != nil {
		t.Fatal(err)
	}
	var got []crdt.ColValue
	if err := RangeTuple(tuple, func(value crdt.ColValue) error {
		value.Bytes = bytes.Clone(value.Bytes)
		got = append(got, value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d values, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Column != want[i].Column || got[i].TypeTag != want[i].TypeTag || !bytes.Equal(got[i].Bytes, want[i].Bytes) {
			t.Fatalf("value %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestTupleRejectsNull(t *testing.T) {
	if _, err := EncodeTuple([]crdt.ColValue{{TypeTag: crdt.ColNull}}); err == nil {
		t.Fatal("expected NULL key rejection")
	}
}
