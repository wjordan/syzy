package unique

import (
	"bytes"
	"context"
	"sync"
)

// Local is an in-process Registry: a single mutex-guarded map from
// (Table, Key, Value) to owner. It models a single-writer / single-process
// deployment and backs tests. With no cluster there is no replication lag,
// so a Release frees its value immediately — quarantine-until-stable is a
// no-op when every change is trivially stable.
//
// The map is process state, so it starts empty; the rows do not. Since no
// node keeps a physical UNIQUE index for a coordinated key, an empty map
// would grant values the rows already hold — after any restart, and for
// every row predating a key created on a populated table. WithEnumerate
// closes that by deriving each key's taken-set from the rows the first
// time a claim names it (the same "rows are the durable truth, the
// registry is soft state" rule the leaseholder follows; Local needs it
// only once per key because it is the sole writer thereafter).
type Local struct {
	mu        sync.Mutex
	taken     map[string][]byte // claimKey -> owner
	enumerate func() (Snapshot, error)
	seeded    map[string]struct{} // keyRefKey of keys already derived
}

// NewLocal returns an empty in-process Registry.
func NewLocal() *Local {
	return &Local{taken: map[string][]byte{}, seeded: map[string]struct{}{}}
}

// WithEnumerate attaches the row enumerator Local derives unseen keys
// from. Without one, Local trusts its map alone — correct only when no
// coordinated row predates the process (tests, and callers that never
// restart). Call once before first use.
func (l *Local) WithEnumerate(fn func() (Snapshot, error)) *Local {
	l.enumerate = fn
	return l
}

// seedLocked derives the taken-set of any key in claims not yet derived,
// from one enumeration of the committed rows. Existing entries win: they
// are grants from this process, which is the only writer, so they are at
// least as current as the rows. Every key the snapshot reports is marked
// seeded, so one pass covers the whole schema.
func (l *Local) seedLocked(claims []Claim) error {
	if l.enumerate == nil {
		return nil
	}
	need := false
	for i := range claims {
		if _, ok := l.seeded[keyRefKey(claims[i].Table, claims[i].Key)]; !ok {
			need = true
			break
		}
	}
	if !need {
		return nil
	}
	snap, err := l.enumerate()
	if err != nil {
		return err
	}
	for _, k := range snap.Keys {
		l.seeded[keyRefKey(k.Table, k.Key)] = struct{}{}
	}
	for i := range snap.Claims {
		ck := claimKey(snap.Claims[i])
		if _, held := l.taken[ck]; !held {
			l.taken[ck] = append([]byte(nil), snap.Claims[i].Owner...)
		}
	}
	return nil
}

// claimKey is the map key for a Claim: Table || Key || Value. Table and
// Key are fixed-width [16]byte, so the concatenation is unambiguous
// without a separator.
func claimKey(c Claim) string {
	b := make([]byte, 0, 16+16+len(c.Value))
	b = append(b, c.Table[:]...)
	b = append(b, c.Key[:]...)
	b = append(b, c.Value...)
	return string(b)
}

func (l *Local) Reserve(ctx context.Context, claims []Claim) (bool, *Claim, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.seedLocked(claims); err != nil {
		return false, nil, err
	}
	// Phase 1: check every claim before granting any, so the batch is
	// all-or-nothing — a txn reserving N values fails atomically. batch
	// tracks values claimed earlier in this same batch so two different
	// owners cannot both take one value in one transaction.
	batch := make(map[string]string, len(claims))
	for i := range claims {
		ck := claimKey(claims[i])
		owner := string(claims[i].Owner)
		if prior, seen := batch[ck]; seen && prior != owner {
			c := claims[i]
			return false, &c, nil
		}
		if cur, held := l.taken[ck]; held && !bytes.Equal(cur, claims[i].Owner) && !bytes.Equal(cur, claims[i].Prev) {
			c := claims[i]
			return false, &c, nil
		}
		batch[ck] = owner
	}
	// Phase 2: grant. A re-reserve by the same owner (or a transfer from
	// Prev) overwrites the owner.
	for i := range claims {
		l.taken[claimKey(claims[i])] = append([]byte(nil), claims[i].Owner...)
	}
	return true, nil, nil
}

func (l *Local) Release(ctx context.Context, claims []Claim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range claims {
		ck := claimKey(claims[i])
		if cur, held := l.taken[ck]; held && bytes.Equal(cur, claims[i].Owner) {
			delete(l.taken, ck)
		}
	}
	return nil
}

// Owner returns the current owner of a claim and whether it is held. It
// is a test/inspection helper and is not part of the Registry interface.
func (l *Local) Owner(c Claim) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	o, ok := l.taken[claimKey(c)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), o...), true
}
