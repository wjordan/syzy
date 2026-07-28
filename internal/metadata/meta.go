package metadata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/sqlitebridge"
)

// Meta keys recognized by the typed accessors below. Storage layer keys
// are TEXT; values are length-typed BLOBs whose interpretation is
// per-key.
const (
	keySchemaVersion = "schema_version"
	keyClusterID     = "cluster_id"
	keyNodeID        = "node_id"
	keyHLCLast       = "hlc_last"
	keySchemaSeq     = "schema_seq"
	keyCleanShutdown = "clean_shutdown"
	keyClusterRoot   = "cluster_root"
	// keySchemaUnhealthy is the durable fail-closed marker written when
	// schema catch-up can no longer prove that this node follows the schema
	// log. Its value is encodeSchemaHealth's sequence + diagnostic payload.
	keySchemaUnhealthy = "schema_unhealthy"
	// keyReplicateUnderscore stores the per-slot ReplicateUnderscoreTables
	// flag from producer.Config. Set on the slot's first producer.New
	// and immutable thereafter; survives AdoptClone and AdoptFork
	// (neither clears it explicitly, so it follows the parent's value
	// through to forks). Stored as a single byte (0 or 1).
	keyReplicateUnderscore = "replicate_underscore"
)

// SchemaHealth describes the first terminal schema-catch-up failure observed
// by this metadata store. Once present it is immutable; recovery creates a
// fresh store rather than clearing or overwriting the evidence of divergence.
type SchemaHealth struct {
	Seq    uint64
	Reason string
}

// GetSchemaHealth returns the durable terminal schema-catch-up failure, or
// ok=false when no terminal divergence has been observed.
func (s *Store) GetSchemaHealth() (health SchemaHealth, ok bool, err error) {
	raw, ok, err := s.GetMeta(keySchemaUnhealthy)
	if err != nil || !ok {
		return SchemaHealth{}, ok, err
	}
	health, err = decodeSchemaHealth(raw)
	return health, true, err
}

// MarkSchemaUnhealthy atomically records a terminal schema-catch-up failure.
// The first failure wins: later calls return the original record unchanged.
func (s *Store) MarkSchemaUnhealthy(seq uint64, reason string) (SchemaHealth, error) {
	want := SchemaHealth{Seq: seq, Reason: reason}
	raw, err := encodeSchemaHealth(want)
	if err != nil {
		return SchemaHealth{}, err
	}
	var got SchemaHealth
	err = s.WithTx(func(tx *Tx) error {
		existing, ok, err := getMeta(tx.stmts.getMeta, keySchemaUnhealthy)
		if err != nil {
			return err
		}
		if ok {
			got, err = decodeSchemaHealth(existing)
			return err
		}
		if err := tx.SetMeta(keySchemaUnhealthy, raw); err != nil {
			return err
		}
		got = want
		return nil
	})
	return got, err
}

func encodeSchemaHealth(health SchemaHealth) ([]byte, error) {
	if health.Seq == 0 {
		return nil, errors.New("metadata: schema health sequence must be positive")
	}
	if health.Reason == "" || !utf8.ValidString(health.Reason) {
		return nil, errors.New("metadata: schema health reason must be non-empty UTF-8")
	}
	raw := make([]byte, 8, 8+len(health.Reason))
	binary.BigEndian.PutUint64(raw, health.Seq)
	return append(raw, health.Reason...), nil
}

func decodeSchemaHealth(raw []byte) (SchemaHealth, error) {
	if len(raw) <= 8 || !utf8.Valid(raw[8:]) {
		return SchemaHealth{}, errors.New("metadata: corrupt schema_unhealthy marker")
	}
	health := SchemaHealth{
		Seq:    binary.BigEndian.Uint64(raw[:8]),
		Reason: string(raw[8:]),
	}
	if health.Seq == 0 {
		return SchemaHealth{}, errors.New("metadata: corrupt schema_unhealthy sequence")
	}
	return health, nil
}

// GetMeta returns the raw value stored under key. ok is false if absent.
func (s *Store) GetMeta(key string) (value []byte, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return getMeta(s.stmts.getMeta, key)
}

// SetMeta upserts value under key.
func (s *Store) SetMeta(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return setMeta(s.stmts.setMeta, key, value)
}

// SetMeta upserts value under key inside an open WithTx — see Store.SetMeta.
func (tx *Tx) SetMeta(key string, value []byte) error {
	return setMeta(tx.stmts.setMeta, key, value)
}

// SetHLCLast writes the packed HLC inside an open WithTx.
func (tx *Tx) SetHLCLast(c crdt.Clock) error {
	return tx.putUint64(keyHLCLast, c.Pack())
}

// SetSchemaSeq writes the highest applied schema sequence inside an open
// WithTx — the tx-scoped form of Store.SetSchemaSeq, so a node advances
// schema_seq atomically with the catalog rows + schema_event it just applied.
func (tx *Tx) SetSchemaSeq(v uint64) error {
	return tx.putUint64(keySchemaSeq, v)
}

func (tx *Tx) putUint64(key string, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return setMeta(tx.stmts.setMeta, key, buf[:])
}

func getMeta(stmt *sqlitebridge.Stmt, key string) ([]byte, bool, error) {
	if err := stmt.Reset(); err != nil {
		return nil, false, err
	}
	if err := stmt.BindText(1, key); err != nil {
		return nil, false, err
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, false, err
	}
	if !hasRow {
		return nil, false, nil
	}
	v := stmt.ColumnBlob(0)
	// Drain to DONE so the next Step+Reset cycle starts clean.
	if _, err := stmt.Step(); err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func setMeta(stmt *sqlitebridge.Stmt, key string, value []byte) error {
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, key); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, value); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

// DeleteMeta removes key. No-op if key is absent.
func (s *Store) DeleteMeta(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stmt := s.stmts.deleteMeta
	if err := stmt.Reset(); err != nil {
		return err
	}
	if err := stmt.BindText(1, key); err != nil {
		return err
	}
	_, err := stmt.Step()
	return err
}

// GetSchemaVersion returns the metadata's stored schema version, or
// (0, false, nil) if absent.
func (s *Store) GetSchemaVersion() (uint64, bool, error) {
	return s.getUint64(keySchemaVersion)
}

// SetSchemaVersion writes v as the schema version. Used internally by
// init; exposed for tests.
func (s *Store) SetSchemaVersion(v uint64) error {
	return s.putUint64(keySchemaVersion, v)
}

// GetClusterID returns the 16-byte cluster identity, or
// (zero, false, nil) if uninitialized.
func (s *Store) GetClusterID() (crdt.ClusterID, bool, error) {
	v, ok, err := s.GetMeta(keyClusterID)
	if err != nil || !ok {
		return crdt.ClusterID{}, ok, err
	}
	if len(v) != 16 {
		return crdt.ClusterID{}, true, fmt.Errorf("metadata: cluster_id wrong width: got %d, want 16", len(v))
	}
	var id crdt.ClusterID
	copy(id[:], v)
	return id, true, nil
}

// SetClusterID writes the 16-byte cluster identity.
func (s *Store) SetClusterID(id crdt.ClusterID) error {
	return s.SetMeta(keyClusterID, id[:])
}

// GetNodeID returns the local origin id, or (0, false, nil) if absent.
func (s *Store) GetNodeID() (crdt.Origin, bool, error) {
	v, ok, err := s.getUint64(keyNodeID)
	return crdt.Origin(v), ok, err
}

// SetNodeID writes the local origin id.
func (s *Store) SetNodeID(o crdt.Origin) error {
	return s.putUint64(keyNodeID, uint64(o))
}

// GetHLCLast returns the highest HLC observed by this node.
func (s *Store) GetHLCLast() (crdt.Clock, bool, error) {
	v, ok, err := s.getUint64(keyHLCLast)
	if err != nil || !ok {
		return crdt.Clock{}, ok, err
	}
	return crdt.UnpackClock(v), true, nil
}

// SetHLCLast writes the packed HLC.
func (s *Store) SetHLCLast(c crdt.Clock) error {
	return s.putUint64(keyHLCLast, c.Pack())
}

// GetSchemaSeq returns the highest applied schema sequence.
func (s *Store) GetSchemaSeq() (uint64, bool, error) {
	return s.getUint64(keySchemaSeq)
}

// SetSchemaSeq writes the highest applied schema sequence.
func (s *Store) SetSchemaSeq(v uint64) error {
	return s.putUint64(keySchemaSeq, v)
}

// GetSchemaSeq reads the highest applied schema sequence inside an
// open WithTx. Resolvers re-check it under the tx to guard against a
// concurrent catch-up authority having advanced it since their
// outside-the-tx read.
func (tx *Tx) GetSchemaSeq() (uint64, bool, error) {
	v, ok, err := getMeta(tx.stmts.getMeta, keySchemaSeq)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(v) != 8 {
		return 0, true, fmt.Errorf("metadata: schema_seq wrong width: got %d, want 8", len(v))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// GetCleanShutdown returns whether the previous lifecycle ended cleanly.
// Default false (treated as unclean) when absent.
func (s *Store) GetCleanShutdown() (bool, bool, error) {
	v, ok, err := s.GetMeta(keyCleanShutdown)
	if err != nil || !ok {
		return false, ok, err
	}
	if len(v) != 1 {
		return false, true, fmt.Errorf("metadata: clean_shutdown wrong width: got %d, want 1", len(v))
	}
	return v[0] != 0, true, nil
}

// GetReplicateUnderscoreTables returns the persisted per-slot flag.
// ok is false on a fresh slot before producer.New has stamped it.
func (s *Store) GetReplicateUnderscoreTables() (value bool, ok bool, err error) {
	v, present, err := s.GetMeta(keyReplicateUnderscore)
	if err != nil || !present {
		return false, present, err
	}
	if len(v) != 1 {
		return false, true, fmt.Errorf("metadata: replicate_underscore wrong width: got %d, want 1", len(v))
	}
	return v[0] != 0, true, nil
}

// SetReplicateUnderscoreTables stamps the per-slot flag. Intended for
// one-time initialization on first producer.New; later flips are a
// caller bug (existing tables aren't retroactively re-classified) and
// producer.New rejects mismatched Config.
func (s *Store) SetReplicateUnderscoreTables(v bool) error {
	b := byte(0)
	if v {
		b = 1
	}
	return s.SetMeta(keyReplicateUnderscore, []byte{b})
}

// SetCleanShutdown writes the clean-shutdown flag.
func (s *Store) SetCleanShutdown(clean bool) error {
	b := byte(0)
	if clean {
		b = 1
	}
	return s.SetMeta(keyCleanShutdown, []byte{b})
}

// GetClusterRoot returns the persisted cluster root URL (file:// or s3://),
// or ("", false, nil) if uninitialized. The root is the rendezvous point
// for all DBs in the same cluster: shared schema log, object backend, and
// peer discovery live under it.
func (s *Store) GetClusterRoot() (string, bool, error) {
	v, ok, err := s.GetMeta(keyClusterRoot)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(v), true, nil
}

// SetClusterRoot writes the cluster root URL. Set once on first daemon
// init; subsequent reopens read the persisted value rather than
// recomputing from flags/env.
func (s *Store) SetClusterRoot(root string) error {
	return s.SetMeta(keyClusterRoot, []byte(root))
}

func (s *Store) getUint64(key string) (uint64, bool, error) {
	v, ok, err := s.GetMeta(key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(v) != 8 {
		return 0, true, fmt.Errorf("metadata: %s wrong width: got %d, want 8", key, len(v))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func (s *Store) putUint64(key string, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return s.SetMeta(key, buf[:])
}
