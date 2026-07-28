package crdt

import (
	"bytes"
	"strings"
)

// Collation identifies a SQLite text collating sequence. Only the three
// built-ins are representable — they are the only collations that can be
// replayed identically on every replica without shipping a comparison
// function. A column or predicate that needs a custom (registered)
// collation is rejected at admission.
type Collation uint8

const (
	CollBinary Collation = 0 // memcmp; the default
	CollNocase Collation = 1 // ASCII case-folded (A–Z only), then memcmp
	CollRtrim  Collation = 2 // trailing spaces ignored, then memcmp
)

// CollationFromName normalizes a SQLite collation name. ok is false for a
// custom/unknown collation (the caller rejects it). The empty name and
// "BINARY" both map to CollBinary.
func CollationFromName(name string) (Collation, bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "", "BINARY":
		return CollBinary, true
	case "NOCASE":
		return CollNocase, true
	case "RTRIM":
		return CollRtrim, true
	default:
		return CollBinary, false
	}
}

// Name returns the canonical SQLite collation name, or "" for BINARY (the
// default, which a column omits).
func (c Collation) Name() string {
	switch c {
	case CollNocase:
		return "NOCASE"
	case CollRtrim:
		return "RTRIM"
	default:
		return ""
	}
}

// CompareText orders two text/blob byte strings under the collation,
// matching SQLite's built-in collating functions exactly: NOCASE folds
// only ASCII A–Z, RTRIM ignores trailing 0x20 spaces, both then memcmp
// with shorter-is-less on a common prefix.
func (c Collation) CompareText(a, b []byte) int {
	switch c {
	case CollNocase:
		return bytes.Compare(foldNocase(a), foldNocase(b))
	case CollRtrim:
		return bytes.Compare(rtrim(a), rtrim(b))
	default:
		return bytes.Compare(a, b)
	}
}

func foldNocase(s []byte) []byte {
	out := make([]byte, len(s))
	for i, b := range s {
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		out[i] = b
	}
	return out
}

func rtrim(s []byte) []byte {
	n := len(s)
	for n > 0 && s[n-1] == ' ' {
		n--
	}
	return s[:n]
}
