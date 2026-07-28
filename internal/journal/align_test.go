package journal

import (
	"path/filepath"
	"testing"
)

// AlignResume guards drainers against markers persisted by a previous
// journal generation. Production incident: a marker of 392 against a
// journal whose record boundaries were 64/272/376 made the drainer
// busy-spin forever (mid-record bytes parse as EOF below the published
// head while the misread publish word is nonzero), wedging the
// publisher's takeover baseline.
func TestAlignResume(t *testing.T) {
	t.Parallel()
	j, err := Open(filepath.Join(t.TempDir(), "jrn"), 1<<20, SyncOff)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer j.Close()
	var bounds []uint64
	bounds = append(bounds, uint64(fileHeaderSize))
	for i := 1; i <= 3; i++ {
		if _, _, err := j.Append(KindLocalDML, uint64(i), 7, make([]byte, 100+i*30)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		bounds = append(bounds, uint64(j.Head()))
	}
	head := uint64(j.Head())

	cases := []struct {
		name   string
		marker uint64
		want   uint64
	}{
		{"zero", 0, bounds[0]},
		{"on first boundary", bounds[1], bounds[1]},
		{"mid-record", bounds[2] + 16, bounds[2]},
		{"at head", head, head},
		{"beyond head", head + 5000, head},
		{"inside file header", 10, bounds[0]},
		{"missing future segment", makeRawOffset(t, 25, 48096), bounds[0]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := uint64(j.AlignResume(Offset(tc.marker)))
			if got != tc.want {
				t.Fatalf("AlignResume(%d) = %d, want %d (boundaries %v)", tc.marker, got, tc.want, bounds)
			}
		})
	}
}

func makeRawOffset(t *testing.T, seg uint32, byteOff uint64) uint64 {
	t.Helper()
	return uint64(makeOffset(seg, byteOff))
}
