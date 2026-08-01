package postgres

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	corecatalog "github.com/wjordan/syzy/catalog"
	"github.com/wjordan/syzy/crdt"
)

// Canonical cross-engine value encoding (docs/postgres.md §13). Both engines
// carry values in the SQLite storage-class tag space with SQLite's canonical
// byte forms — ColInt/ColReal as 8-byte big-endian, ColText as UTF-8, ColBlob
// as raw bytes — so the same logical value produces the same wire bytes, the
// same PK identity, and the same claim bytes regardless of which engine
// originated it. Postgres types outside the four classes travel as their
// canonical PG text output under ColText (SQLite stores them as text; apply
// casts text back to the column type).

// sqliteClass maps a Postgres type name (colInfo.typeName, pg_catalog
// format_type output) to its cross-engine storage class. boolean rides ColInt
// as 0/1 (SQLite's own convention).
func sqliteClass(typeName string) crdt.ColType {
	switch typeName {
	case "smallint", "integer", "bigint", "int2", "int4", "int8", "boolean":
		return crdt.ColInt
	case "real", "double precision", "float4", "float8":
		return crdt.ColReal
	case "bytea":
		return crdt.ColBlob
	default:
		return crdt.ColText
	}
}

// encodeColValue converts one column's Postgres text output (pgoutput text
// mode, or a ::text read-back) into its canonical typed ColValue.
func encodeColValue(cid crdt.ColumnID, typeName string, data []byte) (crdt.ColValue, error) {
	switch sqliteClass(typeName) {
	case crdt.ColInt:
		var n int64
		if typeName == "boolean" {
			switch string(data) {
			case "t", "true":
				n = 1
			case "f", "false":
				n = 0
			default:
				return crdt.ColValue{}, fmt.Errorf("postgres: boolean text %q", data)
			}
		} else {
			var err error
			if n, err = strconv.ParseInt(string(data), 10, 64); err != nil {
				return crdt.ColValue{}, fmt.Errorf("postgres: %s text %q: %w", typeName, data, err)
			}
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		return crdt.ColValue{Column: cid, TypeTag: crdt.ColInt, Bytes: b[:]}, nil
	case crdt.ColReal:
		f, err := strconv.ParseFloat(string(data), 64)
		if err != nil {
			return crdt.ColValue{}, fmt.Errorf("postgres: %s text %q: %w", typeName, data, err)
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
		return crdt.ColValue{Column: cid, TypeTag: crdt.ColReal, Bytes: b[:]}, nil
	case crdt.ColBlob:
		if !strings.HasPrefix(string(data), `\x`) {
			return crdt.ColValue{}, fmt.Errorf("postgres: bytea text %q lacks \\x prefix", data)
		}
		raw, err := hex.DecodeString(string(data[2:]))
		if err != nil {
			return crdt.ColValue{}, fmt.Errorf("postgres: bytea text %q: %w", data, err)
		}
		return crdt.ColValue{Column: cid, TypeTag: crdt.ColBlob, Bytes: raw}, nil
	default:
		return crdt.ColValue{Column: cid, TypeTag: crdt.ColText, Bytes: data}, nil
	}
}

// colValueText renders a canonical typed ColValue back to a Postgres-castable
// text literal body. The inverse of encodeColValue up to text normalization
// ('1' casts to boolean true, so boolean needs no special case).
func colValueText(cv crdt.ColValue) (string, error) {
	switch cv.TypeTag {
	case crdt.ColInt:
		if len(cv.Bytes) != 8 {
			return "", fmt.Errorf("postgres: ColInt bytes len %d, want 8", len(cv.Bytes))
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(cv.Bytes)), 10), nil
	case crdt.ColReal:
		if len(cv.Bytes) != 8 {
			return "", fmt.Errorf("postgres: ColReal bytes len %d, want 8", len(cv.Bytes))
		}
		return strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(cv.Bytes)), 'g', -1, 64), nil
	case crdt.ColBlob:
		return `\x` + hex.EncodeToString(cv.Bytes), nil
	case crdt.ColText:
		return string(cv.Bytes), nil
	}
	return "", fmt.Errorf("postgres: unknown TypeTag %d", cv.TypeTag)
}

// pkBlobTyped is the canonical cross-engine PK identity: per PK column in key
// order, ColumnID + 1-byte class tag + uvarint length + canonical bytes. It is
// the core's tuple encoding, shared rather than restated, because both engines
// must key the same clocks from the same logical row — a second implementation
// that drifted would silently split one row into two identities.
//
// The error is the NULL-member case, which a PK column being NOT NULL should
// make unreachable — but it is returned rather than ignored, because reaching
// it means the row image and the catalog disagree, and encoding a PK from
// that would mint a wrong row identity.
func pkBlobTyped(cvs []crdt.ColValue) (crdt.PKBlob, error) {
	out, err := corecatalog.EncodeTuple(cvs)
	if err != nil {
		return nil, fmt.Errorf("postgres: encode PK: %w", err)
	}
	return out, nil
}

// decodePKBlobTyped reverses pkBlobTyped into its component typed values (in
// PK key order). Malformed input returns what parsed cleanly, since callers
// use this to render a diagnostic or a WHERE clause from bytes they already
// hold.
func decodePKBlobTyped(pk crdt.PKBlob) []crdt.ColValue {
	var out []crdt.ColValue
	_ = corecatalog.RangeTuple(pk, func(cv crdt.ColValue) error {
		out = append(out, cv)
		return nil
	})
	return out
}
