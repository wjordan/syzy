package syzyext

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
)

// changesProvider is the per-connection adapter that
// sqlitebridge.RegisterChangesVTab queries for syzy_my_origin and
// syzy_pk_decode results. It wraps the producer's catalog + origin so
// the SQL surface decodes against the same schema view the writer
// sees.
type changesProvider struct {
	origin uint64
	cat    *catalog.Catalog
}

func newChangesProvider(origin uint64, cat *catalog.Catalog) *changesProvider {
	return &changesProvider{origin: origin, cat: cat}
}

func (p *changesProvider) Origin() uint64 { return p.origin }

// DecodePK runs the table's catalog decoder over pk and returns a JSON
// encoding: a bare scalar for single-column PKs ("1", "abc"), or an
// array for composites ([1,"abc"]). Returns ok=false when the table
// is unknown or the blob fails to decode; the SQL layer turns that
// into NULL.
func (p *changesProvider) DecodePK(table string, pk []byte) (string, bool) {
	if p.cat == nil {
		return "", false
	}
	t, ok := p.cat.Table(table)
	if !ok {
		return "", false
	}
	if len(t.PK) == 1 {
		var out string
		err := t.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
			s, ok := jsonOne(v)
			if !ok {
				return errSentinel
			}
			out = s
			return nil
		})
		if err != nil {
			return "", false
		}
		return out, true
	}
	var b strings.Builder
	b.WriteByte('[')
	first := true
	err := t.RangePK(pk, func(_ crdt.ColumnID, v crdt.ColValue) error {
		s, ok := jsonOne(v)
		if !ok {
			return errSentinel
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(s)
		return nil
	})
	if err != nil {
		return "", false
	}
	b.WriteByte(']')
	return b.String(), true
}

var errSentinel = stringErr("syzy_pk_decode: encode failed")

type stringErr string

func (e stringErr) Error() string { return string(e) }

func jsonOne(v crdt.ColValue) (string, bool) {
	switch v.TypeTag {
	case crdt.ColNull:
		return "null", true
	case crdt.ColInt:
		if len(v.Bytes) != 8 {
			return "", false
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(v.Bytes)), 10), true
	case crdt.ColReal:
		if len(v.Bytes) != 8 {
			return "", false
		}
		f := math.Float64frombits(binary.BigEndian.Uint64(v.Bytes))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "null", true
		}
		return strconv.FormatFloat(f, 'g', -1, 64), true
	case crdt.ColText:
		buf, err := json.Marshal(string(v.Bytes))
		if err != nil {
			return "", false
		}
		return string(buf), true
	case crdt.ColBlob:
		return `"0x` + hex.EncodeToString(v.Bytes) + `"`, true
	}
	return "", false
}
