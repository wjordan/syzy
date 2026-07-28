package unique

import "sync"

// takenEntry is a live reservation: the owning row's PKBlob (as a string)
// and how the entry is evidenced. backed means the owner's row was seen
// holding the value in the last ingested enumeration snapshot — the
// durable, replicated truth. An unbacked entry is a recent grant whose
// row may not have replicated to the leaseholder's replica yet;
// grantedUS lets ingest age it out (through the release hold) if the row
// never appears.
type takenEntry struct {
	owner     string
	grantedUS int64
	backed    bool
}

// quarantineEntry records a released value awaiting reuse: its prior
// owner and the wall-clock µs at which the leaseholder observed the
// release (the value left the derived taken-set). The value cannot be
// reclaimed by a different owner until the quarantine window elapses
// (quarantine-until-stable), so a lagging receiver that has not yet seen
// the releasing change can never observe two rows claiming it. The window
// is a conservative time bound ≥ the cluster's bounded-staleness deadline
// (CRDT.md); after it, an unresponsive node is evicted, so every
// remaining member has observed the release.
//
// Unrelated to internal/quarantine (deterministic apply-failure
// poison handling), which shares only the word.
type quarantineEntry struct {
	owner     string
	releaseUS int64
}

// KeyRef identifies one coordinated unique key: (table, key) identity
// bytes, matching Claim.Table/Claim.Key.
type KeyRef struct {
	Table [16]byte
	Key   [16]byte
}

// Snapshot is one enumeration pass over the local replica: every active
// coordinated key identity — including keys with no participating rows —
// plus the row-backed claims under them. The key identities gate
// servability: reserve refuses claims on keys absent from the last
// ingested snapshot, so a key becomes reservable only once the
// leaseholder has both observed the key and seeded its existing rows'
// claims. (A claims-only enumeration could never activate an empty key.)
type Snapshot struct {
	Keys   []KeyRef
	Claims []Claim
}

// reservationTable is the leaseholder's reservation state: the taken-set
// derived from the replicated rows plus recent grants, the quarantine of
// recently-released values, and the servable-key set. It is
// mutex-serialized — every grant/ingest decision is made under one lock,
// so arbitration is correct by construction (the "single serialized
// actor" of docs/SCHEMA.md#unique-keys).
//
// The derivation invariant (see ingest): after each maintenance tick,
//
//	taken = this tick's row-backed claims ∪ grants younger than grace
//
// and any value leaving that set exits through the release hold. There
// is no other mutation path — releases are observed from the rows, never
// signalled.
type reservationTable struct {
	mu           sync.Mutex
	nowUS        func() int64
	quarantineUS int64
	taken        map[string]takenEntry      // claimKey -> live reservation
	quarantined  map[string]quarantineEntry // claimKey -> release record
	// servable is the set of key identities (Table||Key prefix bytes)
	// present in the last ingested snapshot. Claims on any other key are
	// refused as not-serving: the taken-set for that key has not been
	// derived yet, so granting would race the key's existing rows.
	servable map[string]struct{}
	// fenced is the set of claimKeys observed held by more than one live
	// row in the last ingested snapshot (an out-of-gate duplicate, e.g.
	// pre-key partitioned history). Grants for these values are refused
	// until an enumeration shows a single owner again; repair is manual
	// (see Leaseholder.DuplicateValues).
	fenced map[string]struct{}
	// stopped is set by snapshotAndStop during a graceful handoff: the table
	// refuses all further grants under the same lock that captures the
	// snapshot, so the published taken-set includes every already-granted
	// reserve and the successor (not this dying leader) serves anything later.
	stopped bool
}

func newReservationTable(nowUS func() int64, quarantineUS int64) *reservationTable {
	return &reservationTable{
		nowUS:        nowUS,
		quarantineUS: quarantineUS,
		taken:        map[string]takenEntry{},
		quarantined:  map[string]quarantineEntry{},
		servable:     map[string]struct{}{},
		fenced:       map[string]struct{}{},
	}
}

// keyRefKey is the map key for a KeyRef — the 32-byte Table||Key prefix
// every claimKey of that key starts with.
func keyRefKey(table, key [16]byte) string {
	b := make([]byte, 0, 32)
	b = append(b, table[:]...)
	b = append(b, key[:]...)
	return string(b)
}

// reserve atomically grants every claim. It is all-or-nothing: a claim
// whose value is held by a different owner, or quarantined within its
// window for a different owner, aborts the whole batch and is returned as
// the conflict. A claim already held by, or quarantined for, the same
// owner is an idempotent success. A (false, nil) return means the table
// is not serving these claims at all — stopped for handoff, or a claim's
// key is absent from the last ingested snapshot — and the caller should
// answer NotLeader so the client retries.
func (rt *reservationTable) reserve(claims []Claim) (bool, *Claim) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stopped {
		return false, nil
	}
	now := rt.nowUS()
	// batch guards against two different owners claiming one value within a
	// single transaction (all-or-nothing, like the cross-transaction case).
	batch := make(map[string]string, len(claims))
	for i := range claims {
		if _, ok := rt.servable[keyRefKey(claims[i].Table, claims[i].Key)]; !ok {
			// Key not yet derived from the rows (just created, or a schema
			// this leaseholder hasn't caught up to). Fail closed as
			// not-serving — never grant against an unseeded taken-set.
			return false, nil
		}
		ck := claimKey(claims[i])
		owner := string(claims[i].Owner)
		if prior, seen := batch[ck]; seen && prior != owner {
			c := claims[i]
			return false, &c
		}
		if !rt.grantable(ck, owner, string(claims[i].Prev), now) {
			c := claims[i]
			return false, &c
		}
		batch[ck] = owner
	}
	for i := range claims {
		ck := claimKey(claims[i])
		owner := string(claims[i].Owner)
		// A same-owner re-assert keeps the existing entry (and its row
		// backing); a fresh grant or a transfer installs an unbacked one.
		if cur, ok := rt.taken[ck]; !ok || cur.owner != owner {
			rt.taken[ck] = takenEntry{owner: owner, grantedUS: now}
		}
		delete(rt.quarantined, ck)
	}
	return true, nil
}

// grantable reports whether ck can be granted to owner at now. prev is an
// optional prior owner the value may be transferred from (a PK-changing
// update keeping the same value).
func (rt *reservationTable) grantable(ck, owner, prev string, now int64) bool {
	if _, bad := rt.fenced[ck]; bad {
		return false // duplicate rows observed for this value; manual repair
	}
	if cur, ok := rt.taken[ck]; ok {
		return cur.owner == owner || (prev != "" && cur.owner == prev)
	}
	if q, ok := rt.quarantined[ck]; ok {
		// The prior owner may reclaim its own value immediately (same
		// row); anyone else must wait out the quarantine window.
		return q.owner == owner || now >= q.releaseUS+rt.quarantineUS
	}
	return true
}

// ingest replaces the table's derived state with one enumeration
// snapshot, realizing the invariant
//
//	taken = snapshot's row-backed claims ∪ grants younger than graceUS
//
// under a single lock. Row-backed claims are authoritative for their
// owner. Grants younger than graceUS are kept — their rows may not have
// replicated here yet — and a young grant naming a different owner than
// the rows do wins the entry (an in-flight transfer: the grant precedes
// the claimant's committed row becoming visible). Everything else that
// was in the taken-set exits through the release hold, stamped at *this*
// observation: a value released by delete/value-change/predicate-flip,
// and a grant that aged past grace without its row ever appearing (the
// reserver crashed before commit, or its release signal was lost — both
// held, never freed straight to the pool).
//
// dup lists claimKeys the snapshot showed held by more than one live row;
// those values are fenced against all grants until a later snapshot shows
// a single owner.
//
// The servable-key set is replaced by the snapshot's key identities.
func (rt *reservationTable) ingest(snap Snapshot, graceUS int64, dup []Claim) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := rt.nowUS()
	newTaken := make(map[string]takenEntry, len(snap.Claims))
	for i := range snap.Claims {
		newTaken[claimKey(snap.Claims[i])] = takenEntry{
			owner: string(snap.Claims[i].Owner), backed: true,
		}
	}
	for ck, e := range rt.taken {
		if cur, ok := newTaken[ck]; ok {
			if !e.backed && now-e.grantedUS < graceUS && cur.owner != e.owner {
				newTaken[ck] = e // in-flight transfer: the grant's owner wins
			}
			continue
		}
		if !e.backed && now-e.grantedUS < graceUS {
			newTaken[ck] = e // in-flight grant: its row may not be visible yet
			continue
		}
		rt.quarantined[ck] = quarantineEntry{owner: e.owner, releaseUS: now}
	}
	rt.taken = newTaken
	servable := make(map[string]struct{}, len(snap.Keys))
	for _, k := range snap.Keys {
		servable[keyRefKey(k.Table, k.Key)] = struct{}{}
	}
	rt.servable = servable
	fenced := make(map[string]struct{}, len(dup))
	for i := range dup {
		fenced[claimKey(dup[i])] = struct{}{}
	}
	rt.fenced = fenced
}

// clear resets the derived state for a fresh leadership: rows will
// reseed taken via the next ingest, and — because the failover drain has
// elapsed — every value released before the takeover is already
// cluster-stable, so nothing remains quarantined. Nothing is servable
// until that ingest completes.
func (rt *reservationTable) clear() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.taken = map[string]takenEntry{}
	rt.quarantined = map[string]quarantineEntry{}
	rt.servable = map[string]struct{}{}
	rt.fenced = map[string]struct{}{}
}

// duplicateClaims returns the claims whose (Table, Key, Value) tuple is
// already held by a different owner earlier in the slice — two live rows
// holding one coordinated value.
//
// While the reservation gate is intact this cannot happen, so a non-empty
// result means the gate's premise was violated upstream of it. The reachable
// path does not require a gate bug: rows written on a partitioned node before
// the key was created are never seen by the create-time duplicate scan (which
// only reads the creating node's replica), and arrive afterwards as genuine
// duplicates. No node has a physical index for a coordinated key, so
// nothing rejects them.
//
// This is the only place the condition is observable. Enumerate already
// materializes every participating row each maintenance tick, so detection
// costs one map probe per claim and no extra I/O. The caller fences the
// affected values (see ingest) and surfaces them for manual repair.
func duplicateClaims(claims []Claim) []Claim {
	seen := make(map[string]string, len(claims)) // claimKey -> owner
	var dup []Claim
	for i := range claims {
		ck := claimKey(claims[i])
		if owner, held := seen[ck]; held && owner != string(claims[i].Owner) {
			dup = append(dup, claims[i])
			continue
		}
		seen[ck] = string(claims[i].Owner)
	}
	return dup
}

// tableSnapshot is the serializable handoff image of a reservationTable: the
// live taken-set and the in-flight quarantine, each keyed by its raw claimKey
// bytes (table||key||value). A graceful leaseholder publishes one so its
// successor resumes the exact state without rebuilding from a lagging replica.
// Backing evidence and the servable-key set are deliberately not carried:
// the successor re-derives both from its own replica on its first ingest,
// which runs in the same tick as adoption.
type tableSnapshot struct {
	Taken       []takenSnap      `json:"taken"`
	Quarantined []quarantineSnap `json:"quarantined"`
}

// Owner is the raw PKBlob (binary, not UTF-8), so it is []byte — a JSON string
// would mangle non-UTF-8 bytes into U+FFFD and corrupt the owner identity.
type takenSnap struct {
	Key        []byte `json:"k"` // claimKey bytes (table||key||value)
	Owner      []byte `json:"o"`
	ReservedUS int64  `json:"r"`
}

type quarantineSnap struct {
	Key       []byte `json:"k"` // claimKey bytes
	Owner     []byte `json:"o"`
	ReleaseUS int64  `json:"q"`
}

// snapshotAndStop atomically stops granting and returns the live state. Taken
// under the one lock every grant holds, so the snapshot includes exactly the
// reserves acked before the handoff and excludes everything after — the
// successor that loads it can serve immediately with no failover drain.
func (rt *reservationTable) snapshotAndStop() tableSnapshot {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.stopped = true
	s := tableSnapshot{
		Taken:       make([]takenSnap, 0, len(rt.taken)),
		Quarantined: make([]quarantineSnap, 0, len(rt.quarantined)),
	}
	for ck, e := range rt.taken {
		s.Taken = append(s.Taken, takenSnap{Key: []byte(ck), Owner: []byte(e.owner), ReservedUS: e.grantedUS})
	}
	for ck, q := range rt.quarantined {
		s.Quarantined = append(s.Quarantined, quarantineSnap{Key: []byte(ck), Owner: []byte(q.owner), ReleaseUS: q.releaseUS})
	}
	return s
}

// load installs a handed-off snapshot as the live state (the successor's
// graceful fast path, in place of the takeover clear). The quarantine is
// restored with its release timestamps so the remaining quarantine windows
// are honored. Every restored reservation counts as an unbacked grant as
// of now — the first ingest re-derives backing from this node's own rows,
// and grace covers entries whose rows are still replicating here.
func (rt *reservationTable) load(s tableSnapshot) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.stopped = false // a loaded table serves
	now := rt.nowUS()
	rt.taken = make(map[string]takenEntry, len(s.Taken))
	rt.quarantined = make(map[string]quarantineEntry, len(s.Quarantined))
	for _, t := range s.Taken {
		rt.taken[string(t.Key)] = takenEntry{owner: string(t.Owner), grantedUS: now}
	}
	for _, q := range s.Quarantined {
		rt.quarantined[string(q.Key)] = quarantineEntry{owner: string(q.Owner), releaseUS: q.ReleaseUS}
	}
}

// sweep drops quarantine entries whose window has elapsed. Reserve frees
// them lazily too; sweep bounds memory for values that are never reused.
func (rt *reservationTable) sweep() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := rt.nowUS()
	for ck, q := range rt.quarantined {
		if now >= q.releaseUS+rt.quarantineUS {
			delete(rt.quarantined, ck)
		}
	}
}

// ownerOf returns the current owner of a claim's value, for tests/GC.
func (rt *reservationTable) ownerOf(c Claim) (string, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	e, ok := rt.taken[claimKey(c)]
	return e.owner, ok
}
