package objstore

import "testing"

func TestParseOriginEpochKey(t *testing.T) {
	key := EpochKey("00000000000000ab", 0x10, 0x20)
	hex, lo, hi, ok := ParseOriginEpochKey(key)
	if !ok || hex != "00000000000000ab" || lo != 0x10 || hi != 0x20 {
		t.Fatalf("round-trip failed: %q -> (%q,%d,%d,%v)", key, hex, lo, hi, ok)
	}

	for _, bad := range []string{
		"db/0000/0000000000000000-0000000000000001.ltx",
		"origins/short/epoch-0000000000000010-0000000000000020.zst",
		"origins/00000000000000ab/notanepoch.zst",
		"origins/00000000000000ab/epoch-xxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyy.zst",
		"origins/00000000000000ab/epoch-0010-0020.zst", // non-16-width seqs
		"HEAD",
		"",
	} {
		if _, _, _, ok := ParseOriginEpochKey(bad); ok {
			t.Errorf("expected reject for %q", bad)
		}
	}
}
