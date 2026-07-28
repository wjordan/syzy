package metadata

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/wjordan/syzy/crdt"
)

// IntentKind is the discriminator stored in the first byte of
// meta.intent. The active kinds are LocalDDL (DDL admission) and
// Clone (reserved for syzy_clone). DML has no intent kind — its
// durability lives in the per-origin journals.
type IntentKind uint8

const (
	IntentNone     IntentKind = 0
	IntentLocalDDL IntentKind = 1
	IntentClone    IntentKind = 2
)

// LocalDDLIntent is the durable record of a DDL whose Append succeeded
// at the SchemaLog but whose local metadata catalog UPSERTs and
// app.db structural mutation may not yet have been applied. Producer
// startup runs resolve_intent against this when it finds one in the
// metadata.
type LocalDDLIntent struct {
	StartedAtUs int64
	SchemaSeq   uint64
	ParentSeq   uint64
	CatalogOp   []byte
	RawSQL      string
}

// EncodeLocalDDL packs a LocalDDLIntent for storage in meta.intent.
// Format: kind(1) || started_at_us(8) || schema_seq(8) || parent_seq(8) ||
// catalog_op_len(varint) || catalog_op || raw_sql_len(varint) || raw_sql.
func EncodeLocalDDL(intent LocalDDLIntent) []byte {
	out := make([]byte, 0, 1+8+8+8+10+len(intent.CatalogOp)+10+len(intent.RawSQL))
	out = append(out, byte(IntentLocalDDL))
	out = binary.BigEndian.AppendUint64(out, uint64(intent.StartedAtUs))
	out = binary.BigEndian.AppendUint64(out, intent.SchemaSeq)
	out = binary.BigEndian.AppendUint64(out, intent.ParentSeq)
	out = binary.AppendUvarint(out, uint64(len(intent.CatalogOp)))
	out = append(out, intent.CatalogOp...)
	out = binary.AppendUvarint(out, uint64(len(intent.RawSQL)))
	out = append(out, intent.RawSQL...)
	return out
}

// DecodeLocalDDL reverses EncodeLocalDDL. The first byte must be
// IntentLocalDDL; other intent kinds are rejected with an error so the
// caller can dispatch on kind first.
func DecodeLocalDDL(buf []byte) (LocalDDLIntent, error) {
	if len(buf) < 1 {
		return LocalDDLIntent{}, fmt.Errorf("metadata: empty intent buffer")
	}
	if IntentKind(buf[0]) != IntentLocalDDL {
		return LocalDDLIntent{}, fmt.Errorf("metadata: intent kind = %d; want LocalDDL", buf[0])
	}
	if len(buf) < 1+24 {
		return LocalDDLIntent{}, fmt.Errorf("metadata: LocalDDL intent truncated header")
	}
	off := 1
	intent := LocalDDLIntent{
		StartedAtUs: int64(binary.BigEndian.Uint64(buf[off:])),
		SchemaSeq:   binary.BigEndian.Uint64(buf[off+8:]),
		ParentSeq:   binary.BigEndian.Uint64(buf[off+16:]),
	}
	off += 24
	opLen, n := binary.Uvarint(buf[off:])
	if n <= 0 {
		return LocalDDLIntent{}, fmt.Errorf("metadata: LocalDDL intent bad catalog_op length")
	}
	off += n
	if uint64(len(buf)-off) < opLen {
		return LocalDDLIntent{}, fmt.Errorf("metadata: LocalDDL intent truncated catalog_op")
	}
	intent.CatalogOp = append([]byte(nil), buf[off:off+int(opLen)]...)
	off += int(opLen)
	rawLen, n := binary.Uvarint(buf[off:])
	if n <= 0 {
		return LocalDDLIntent{}, fmt.Errorf("metadata: LocalDDL intent bad raw_sql length")
	}
	off += n
	if uint64(len(buf)-off) < rawLen {
		return LocalDDLIntent{}, fmt.Errorf("metadata: LocalDDL intent truncated raw_sql")
	}
	intent.RawSQL = string(buf[off : off+int(rawLen)])
	return intent, nil
}

// IntentKindOf returns the kind tag for an encoded intent buffer. Used
// by startup recovery to dispatch to the right decoder.
func IntentKindOf(buf []byte) IntentKind {
	if len(buf) == 0 {
		return IntentNone
	}
	return IntentKind(buf[0])
}

// Intent slots are origin-scoped: meta key "intent:<origin-hex>". One
// metadata store serves N producers (app connections, sidecars, a
// host-side node), and a shared slot let any of them overwrite or
// clear another's in-flight intent — the recovery breadcrumb for a
// DDL whose schema-log Append already committed. Scoping by origin
// makes cross-producer destruction structurally impossible; readers
// that need every pending intent (broker catch-up yield, adopt
// resets) use ListIntents / ClearAllIntents.

func intentKey(origin crdt.Origin) string {
	return fmt.Sprintf("intent:%016x", uint64(origin))
}

// GetOriginIntent returns origin's raw encoded intent blob, or
// (nil, false) if none. Decoders dispatch on IntentKindOf and call
// the appropriate Decode*.
func (s *Store) GetOriginIntent(origin crdt.Origin) ([]byte, bool, error) {
	return s.GetMeta(intentKey(origin))
}

// SetOriginIntent writes origin's encoded intent blob, replacing any
// existing one in the same slot.
func (s *Store) SetOriginIntent(origin crdt.Origin, buf []byte) error {
	return s.SetMeta(intentKey(origin), buf)
}

// ClearOriginIntent removes origin's pending intent. No-op if absent.
func (s *Store) ClearOriginIntent(origin crdt.Origin) error {
	return s.DeleteMeta(intentKey(origin))
}

// ClearOriginIntent removes origin's pending intent inside an open
// WithTx.
func (tx *Tx) ClearOriginIntent(origin crdt.Origin) error {
	return tx.deleteMeta(intentKey(origin))
}

// OriginIntent pairs an encoded intent blob with the origin that owns
// its slot.
type OriginIntent struct {
	Origin crdt.Origin
	Buf    []byte
}

// ListIntents returns every origin's pending intent. Used by the
// broker's catch-up yield check and diagnostics.
func (s *Store) ListIntents() ([]OriginIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.listIntents
	if err := stmt.Reset(); err != nil {
		return nil, err
	}
	var out []OriginIntent
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			return out, nil
		}
		// Strict parse; skip malformed keys rather than wedging every
		// caller (the broker's catch-up runs this each tick).
		key := stmt.ColumnText(0)
		o, err := strconv.ParseUint(strings.TrimPrefix(key, "intent:"), 16, 64)
		if err != nil {
			continue
		}
		out = append(out, OriginIntent{
			Origin: crdt.Origin(o),
			Buf:    append([]byte(nil), stmt.ColumnBlob(1)...),
		})
	}
}

// ClearAllIntents removes every origin's pending intent inside an open
// WithTx. Adopt paths (clone, fork) use this: the new identity starts
// with no in-flight DDL regardless of what the source carried.
func (tx *Tx) ClearAllIntents() error {
	stmt := tx.stmts.clearAllIntents
	if err := stmt.Reset(); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}
