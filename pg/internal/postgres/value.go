package postgres

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

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
// order, ColumnID + 1-byte class tag + uvarint length + canonical bytes —
// byte-identical to internal/catalog's EncodePKFromSlice for the same logical
// row, so both engines key the same clocks.
func pkBlobTyped(cvs []crdt.ColValue) crdt.PKBlob {
	var out []byte
	var lenBuf [binary.MaxVarintLen64]byte
	for _, cv := range cvs {
		out = append(out, cv.Column[:]...)
		out = append(out, byte(cv.TypeTag))
		n := binary.PutUvarint(lenBuf[:], uint64(len(cv.Bytes)))
		out = append(out, lenBuf[:n]...)
		out = append(out, cv.Bytes...)
	}
	return out
}

// decodePKBlobTyped reverses pkBlobTyped into its component typed values (in
// PK key order). Malformed input returns what parsed cleanly.
func decodePKBlobTyped(pk crdt.PKBlob) []crdt.ColValue {
	var out []crdt.ColValue
	buf := []byte(pk)
	for len(buf) >= 17 {
		var cv crdt.ColValue
		copy(cv.Column[:], buf[:16])
		cv.TypeTag = crdt.ColType(buf[16])
		buf = buf[17:]
		l, sz := binary.Uvarint(buf)
		if sz <= 0 || uint64(len(buf)-sz) < l {
			break
		}
		cv.Bytes = buf[sz : sz+int(l)]
		buf = buf[sz+int(l):]
		out = append(out, cv)
	}
	return out
}
