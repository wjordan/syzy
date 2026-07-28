package catalog

import (
	"encoding/binary"
	"fmt"

	"github.com/wjordan/syzy/crdt"
)

// AppendValue appends one non-NULL canonical key member. Key tuples encode
// members in declared order as ColumnID, one-byte canonical type tag, length,
// and canonical bytes.
func AppendValue(dst []byte, value crdt.ColValue) ([]byte, error) {
	if value.TypeTag == crdt.ColNull {
		return nil, fmt.Errorf("catalog: NULL key member %x", value.Column)
	}
	if value.TypeTag > 0xff {
		return nil, fmt.Errorf("catalog: key type tag %d exceeds one-byte encoding", value.TypeTag)
	}
	dst = append(dst, value.Column[:]...)
	dst = append(dst, byte(value.TypeTag))
	dst = binary.AppendUvarint(dst, uint64(len(value.Bytes)))
	dst = append(dst, value.Bytes...)
	return dst, nil
}

// EncodeTuple encodes values in their supplied key order.
func EncodeTuple(values []crdt.ColValue) ([]byte, error) {
	out := make([]byte, 0, 32*len(values))
	var err error
	for _, value := range values {
		out, err = AppendValue(out, value)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RangeTuple visits a canonical key tuple without allocating member storage.
// Value bytes alias tuple for the duration of the call.
func RangeTuple(tuple []byte, fn func(crdt.ColValue) error) error {
	for off := 0; off < len(tuple); {
		if off+17 > len(tuple) {
			return fmt.Errorf("catalog: truncated tuple header at %d", off)
		}
		var value crdt.ColValue
		copy(value.Column[:], tuple[off:off+16])
		off += 16
		value.TypeTag = uint32(tuple[off])
		off++
		n, size := binary.Uvarint(tuple[off:])
		if size <= 0 {
			return fmt.Errorf("catalog: invalid tuple length at %d", off)
		}
		off += size
		if n > uint64(len(tuple)-off) {
			return fmt.Errorf("catalog: truncated tuple value at %d", off)
		}
		value.Bytes = tuple[off : off+int(n)]
		off += int(n)
		if err := fn(value); err != nil {
			return err
		}
	}
	return nil
}
