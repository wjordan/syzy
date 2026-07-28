package unique

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wjordan/objectstore"
)

// handoffRecord is the taken-set snapshot a leaseholder publishes on a clean
// shutdown, tagged with the generation that produced it. The successor adopts
// it only when the tag is exactly its own generation minus one — i.e. it took
// over directly from the leader that wrote it (a graceful baton pass). Any
// other case (a crash left no fresh record, or an intervening generation) fails
// the tag check and the successor rebuilds from its replica behind the drain.
type handoffRecord struct {
	Generation uint64        `json:"generation"`
	Snapshot   tableSnapshot `json:"snapshot"`
}

// HandoffStore reads and writes the graceful-handoff taken-set object. It is a
// sibling of the lease object: the lease says who leads, the handoff says what
// the previous leader held so the next one need not reconstruct it.
type HandoffStore struct {
	bucket objectstore.Bucket
	key    string
}

// OpenHandoff binds a HandoffStore to the object at key in bucket.
func OpenHandoff(bucket objectstore.Bucket, key string) *HandoffStore {
	return &HandoffStore{bucket: bucket, key: key}
}

// Write publishes snap tagged with gen. Best-effort from the caller's view: a
// failure only costs the successor a one-time rebuild+drain, never correctness.
func (s *HandoffStore) Write(ctx context.Context, gen uint64, snap tableSnapshot) error {
	raw, err := json.Marshal(handoffRecord{Generation: gen, Snapshot: snap})
	if err != nil {
		return fmt.Errorf("unique: encode handoff: %w", err)
	}
	if _, err := s.bucket.Put(ctx, s.key, bytes.NewReader(raw), int64(len(raw)), nil); err != nil {
		return fmt.Errorf("unique: write handoff: %w", err)
	}
	return nil
}

// Read returns the published snapshot and its generation tag. A missing object
// reads as (zero, 0, false, nil). The read is strongly consistent: it drives a
// serve-immediately-vs-drain decision, so it must see the leader's true state,
// not a lagging replica (see WithConsistentRead / LeaseStore.Read).
func (s *HandoffStore) Read(ctx context.Context) (tableSnapshot, uint64, bool, error) {
	body, _, err := s.bucket.Get(objectstore.WithConsistentRead(ctx), s.key)
	if errors.Is(err, objectstore.ErrNotFound) {
		return tableSnapshot{}, 0, false, nil
	}
	if err != nil {
		return tableSnapshot{}, 0, false, fmt.Errorf("unique: read handoff: %w", err)
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return tableSnapshot{}, 0, false, fmt.Errorf("unique: read handoff body: %w", err)
	}
	var rec handoffRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return tableSnapshot{}, 0, false, fmt.Errorf("unique: decode handoff: %w", err)
	}
	return rec.Snapshot, rec.Generation, true, nil
}
