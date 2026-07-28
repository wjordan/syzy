package physicalrestore

import (
	"testing"

	"github.com/wjordan/syzy/internal/objstore"
)

func TestSelectLTXChainPrefersL1Coverage(t *testing.T) {
	t.Parallel()
	l0 := []objstore.LTXFile{
		{Key: objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 1), MinTXID: 1, MaxTXID: 1},
		{Key: objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 2, 4), MinTXID: 2, MaxTXID: 4},
		{Key: objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 5, 5), MinTXID: 5, MaxTXID: 5},
	}
	l1 := []objstore.LTXFile{
		{Key: objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 2, 3), MinTXID: 2, MaxTXID: 3},
		{Key: objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 4, 4), MinTXID: 4, MaxTXID: 4},
	}

	got := SelectLTXChain(l0, l1, 0, 5)
	var keys []string
	for _, f := range got {
		keys = append(keys, f.Key)
	}
	want := []string{
		objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 1, 1),
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 2, 3),
		objstore.LTXKey(objstore.DBPrefix, objstore.L1Level, 4, 4),
		objstore.LTXKey(objstore.DBPrefix, objstore.L0Level, 5, 5),
	}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}
