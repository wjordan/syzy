package objstore

import "testing"

func TestClassify(t *testing.T) {
	for _, c := range []struct {
		key  string
		want string
	}{
		{"HEAD", "head"},
		{"db/0000/0000000000000001-0000000000000005.ltx", "db"},
		{"metadata/0000000000000005.cluster.db.zst", "metadata"},
		{"origins/abcd/epoch-0000000000000001-0000000000000010.zst", "origins"},
		{"events/00000001.bin", "events"},
		{"random/key", "other"},
	} {
		if got := Classify(c.key); got != c.want {
			t.Errorf("Classify(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}
