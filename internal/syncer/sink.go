package syncer

import (
	"fmt"
	"sync"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// SelfLog is the durable capture boundary for locally-produced
// changesets. Apply appends every built changeset (with its source
// self-journal endOffset) and group-commits the batch's fsync via
// SyncSelf BEFORE anything is published or the marker advances, so a
// crash never forgets a shipped commit's exact bytes. mirror.Manager
// implements it for the self origin (AppendSelf/SyncSelf).
type SelfLog interface {
	AppendSelf(payload []byte, endOffset journal.Offset) error
	SyncSelf() error
}

// MetaSink is the deferred-drain DrainSink: it decodes journal
// record evidence into crdt.Records, allocates Dot.Seq, builds the
// Changeset, and advances nodestate.Cache (rowClock + senderNextSeq
// + self snapshot marker) per Apply call. When a SelfLog is set the
// batch is captured and fsync'd before encodedListeners fire, so
// broadcast/seal never precede durable capture (ARCHITECTURE.md "Self-log").
//
// Apply is called sequentially by the drainer goroutine — the
// scratch fields below are owned by Apply and reused across calls
// without locking.
type MetaSink struct {
	sc      *metadata.Store
	cat     *catalog.Catalog
	cluster crdt.ClusterID
	origin  crdt.Origin

	// cache is the authoritative store for self-side seq/HLC and
	// per-row LWW state. The drainer is the only writer to
	// cache.PutRowState here, so per-batch row_clock advances pass
	// through cache.
	cache *nodestate.Cache

	// blobRead is an optional read-only connection to app.db used by
	// the blob_patch materializer to read post-commit NEW blob bytes
	// via sqlite3_blob_open. Nil means blob_write fires are dropped
	// silently (no blob_patch records emitted).
	blobRead *sqlitebridge.Conn

	// selfLog, when set, is the durable capture boundary for our origin:
	// Apply appends every built changeset here and fsyncs the batch BEFORE
	// it broadcasts, feeds the sealer, or advances the marker, so a seq is
	// never published without its exact wire bytes durably captured for
	// verbatim replay (recoverSelf). Nil disables capture (tests and
	// daemon-drained secondaries publish inline and do not own a self-log).
	selfLog SelfLog

	// nowMicros is overridable for tests.
	nowMicros func() int64

	listenersMu sync.RWMutex
	listeners   []func()

	// encodedListenersMu guards encodedListeners. Listeners fire on the
	// drainer goroutine in the batch's publish phase — after the self-log
	// fsync — so broadcast/seal land off the commit-thread latency path and
	// never precede durable capture.
	encodedListenersMu sync.RWMutex
	encodedListeners   []func(payload []byte)

	// recordsListenersMu guards recordsListeners. Listeners fire on
	// the drainer goroutine after crdt.Build with the just-built
	// record slice. The slice aliases sink-owned scratch; retain by
	// copying.
	recordsListenersMu sync.RWMutex
	recordsListeners   []func(dot crdt.Dot, records []crdt.Record)

	// reassert, when set, fires per materialized commit BEFORE the
	// batch's row-clock updates publish to cache — the broker uses it
	// to re-apply locally-committed DML that an inbound apply
	// overwrote in the commit→drain window (broker.ReassertLocal).
	// Fires on the drainer goroutine; records alias sink scratch and
	// must not be retained.
	reassertMu sync.RWMutex
	reassert   func(records []crdt.Record, stamp crdt.Stamp) error

	// Scratch buffers, reused across Apply calls. Each is reset to len 0
	// at the top of Apply; capacity grows monotonically to the largest
	// batch size ever seen.
	parsed      []decodedRecord
	commits     []commitOut
	rowState    map[rckey]crdt.RowState
	records     []crdt.Record     // buildRecords output buffer
	clocks      []rowClockUpdate  // buildRecords output buffer
	cellClocks  []cellClockUpdate // buildRecords output buffer (cell-group partial updates)
	evidence    []recordEvidence  // buildRecordEvidence output buffer
	journalRecs []journalRecord   // parseJournal output buffer
	pub         [][]byte          // per-batch retained payload copies for deferred publish

	// warnedHistory tracks tables already warned about unreconstructable
	// capture-time layouts (drainer goroutine only).
	warnedHistory map[string]struct{}
}

// decodedRecord holds one parsed journal record plus its decoded
// evidence. The evidence slice references sink-owned scratch.
type decodedRecord struct {
	hlc     crdt.Clock
	evStart int // index into sink.evidence (inclusive)
	evEnd   int // index into sink.evidence (exclusive)
	jrec    *DrainRecord
}

// commitOut is one fully-staged commit. clockLo/clockHi index into
// sink.clocks; dot is the allocated identity used to advance
// senderNextSeq after the batch.
type commitOut struct {
	clockLo int
	clockHi int
	dot     crdt.Dot
}

// rckey identifies one (table, pk) pair in the per-batch row-state cache.
type rckey struct {
	t  crdt.TableID
	pk string // PKBlob bytes as map key (string-conversion is the standard idiom)
}

func NewMetaSink(sc *metadata.Store, cat *catalog.Catalog, cluster crdt.ClusterID, origin crdt.Origin, nowMicros func() int64) *MetaSink {
	return &MetaSink{
		sc: sc, cat: cat, cluster: cluster, origin: origin, nowMicros: nowMicros,
		rowState: make(map[rckey]crdt.RowState, 16),
	}
}

// SetCache attaches a nodestate.Cache as the seq/HLC/row_clock source
// of truth. Must be set before the drainer goroutine starts.
func (s *MetaSink) SetCache(c *nodestate.Cache) { s.cache = c }

// SetBlobRead attaches a read-only app.db connection used by the
// blob_patch materializer to read post-commit NEW blob bytes. Pass
// nil to disable blob_patch capture. Must be set before the drainer
// goroutine starts.
func (s *MetaSink) SetBlobRead(c *sqlitebridge.Conn) { s.blobRead = c }

// SetSelfLog attaches the durable capture boundary. When set, Apply
// captures and fsyncs each batch before publishing. Must be set
// before the drainer goroutine starts.
func (s *MetaSink) SetSelfLog(sl SelfLog) { s.selfLog = sl }

// OnCommit registers fn to fire after each successful Apply. Listeners
// run on the drainer goroutine; they must not block.
func (s *MetaSink) OnCommit(fn func()) {
	s.listenersMu.Lock()
	s.listeners = append(s.listeners, fn)
	s.listenersMu.Unlock()
}

// OnEncoded registers fn to fire on the drainer goroutine once per
// changeset, after the batch's self-log fsync (so broadcast/seal never
// precede durable capture). With no SelfLog set,
// it fires after the batch is built. Production wiring routes broadcast
// through here so it happens off the commit-thread latency path.
// Listeners must not block; the byte slice is valid only for the call —
// callers wanting durable retention must copy.
func (s *MetaSink) OnEncoded(fn func(payload []byte)) {
	s.encodedListenersMu.Lock()
	s.encodedListeners = append(s.encodedListeners, fn)
	s.encodedListenersMu.Unlock()
}

// OnRecords registers fn to fire on the drainer goroutine after each
// changeset is built. The records slice aliases sink-owned scratch;
// listeners that retain records must copy. Listeners must not block.
// Used by the notify dispatcher to publish per-record changes.
func (s *MetaSink) OnRecords(fn func(dot crdt.Dot, records []crdt.Record)) {
	s.recordsListenersMu.Lock()
	s.recordsListeners = append(s.recordsListeners, fn)
	s.recordsListenersMu.Unlock()
}

// SetReassert wires broker.ReassertLocal (or equivalent) into the
// drain. Errors are the callback's to handle (log + tolerate): a
// failed re-assert means this node's app.db may lag its own winning
// write until the row is written again, but the drain itself must
// keep going.
func (s *MetaSink) SetReassert(fn func(records []crdt.Record, stamp crdt.Stamp) error) {
	s.reassertMu.Lock()
	s.reassert = fn
	s.reassertMu.Unlock()
}

// LastDrainedOffset implements DrainSink. Resumes from the snapshot
// marker for THIS sink's origin; records past the last snapshot get
// re-processed silently to bring cache state forward. Each
// per-origin sink resumes from its own marker — important in the
// daemon model where one cache holds markers for many origins.
func (s *MetaSink) LastDrainedOffset() (journal.Offset, error) {
	return s.cache.SnapshotMarker(s.origin), nil
}

// Apply implements DrainSink. Records are guaranteed by the drainer to
// be in journal order and free of FlagAborted. KindEmpty entries are
// skipped here.
func (s *MetaSink) Apply(records []DrainRecord) (journal.Offset, error) {
	if len(records) == 0 {
		return 0, nil
	}
	endOffset := records[len(records)-1].NextOff

	// Reset scratch buffers — capacity is preserved.
	s.parsed = s.parsed[:0]
	s.evidence = s.evidence[:0]
	s.commits = s.commits[:0]
	s.records = s.records[:0]
	s.clocks = s.clocks[:0]
	s.cellClocks = s.cellClocks[:0]
	s.pub = s.pub[:0]
	clear(s.rowState)

	for i := range records {
		r := &records[i]
		if r.Kind == journal.KindEmpty || len(r.Payload) == 0 {
			continue
		}
		evStart := len(s.evidence)
		if err := s.buildRecordEvidence(r.Payload, uint64(r.SchemaSeq)); err != nil {
			return 0, fmt.Errorf("sink: decode record at %d: %w", r.Offset, err)
		}
		if len(s.evidence) == evStart {
			continue
		}
		s.parsed = append(s.parsed, decodedRecord{
			hlc:     crdt.UnpackClock(r.HLC),
			evStart: evStart,
			evEnd:   len(s.evidence),
			jrec:    r,
		})
	}
	if len(s.parsed) == 0 {
		// All-empty batch (KindEmpty / non-replicated DML). Just advance
		// the drainer's offset; nothing to persist.
		return endOffset, nil
	}

	baseSeq := s.cache.SenderNextSeq(s.origin)
	nextSeq := baseSeq

	// blobMutations bundles the per-row blob_range_clock side effects
	// that need a metadata txn after the batch's Changesets are built.
	var blobMutations []blobMutation

	for i := range s.parsed {
		d := &s.parsed[i]
		clk := d.hlc
		stamp := crdt.Stamp{Clock: clk, Origin: s.origin}
		dot := crdt.Dot{Origin: s.origin, Seq: nextSeq}
		nextSeq++

		recStart := len(s.records)
		clockStart := len(s.clocks)
		if err := s.buildRecords(s.evidence[d.evStart:d.evEnd], stamp); err != nil {
			return 0, fmt.Errorf("sink: build records at %d: %w", d.jrec.Offset, err)
		}
		// Compute local-commit blob_range_clock side effects for this
		// commit's records. Full DML drops the row's clock; BlobPatch
		// folds the patch into the existing IntervalMap with
		// c=stamp, baseline=rs.Base (the row's prior parent Stamp).
		for ri := recStart; ri < len(s.records); ri++ {
			rec := s.records[ri]
			h := rec.Header()
			if bp, ok := rec.(crdt.BlobPatch); ok {
				rs := s.cache.RowState(h.Table, h.PK)
				cols, err := s.foldBlobPatchLocal(h.Table, h.PK, bp, stamp, rs.Base)
				if err != nil {
					return 0, fmt.Errorf("sink: fold blob_patch at %d: %w", d.jrec.Offset, err)
				}
				blobMutations = append(blobMutations, blobMutation{
					table: h.Table, pk: h.PK,
					cols: cols, drop: len(cols) == 0,
				})
				continue
			}
			// Insert/Update/Delete: drop row's blob_range_clock entry.
			blobMutations = append(blobMutations, blobMutation{
				table: h.Table, pk: h.PK, drop: true,
			})
		}
		if len(s.records) == recStart {
			// Defensive: evidence existed but materialized to nothing.
			// Treat as no-op for this record (don't consume seq).
			nextSeq--
			continue
		}
		// Attach the schema-chain dep so receivers gate the apply
		// behind catch-up if their meta.schema_seq lags. We read the
		// metadata lazily here — schema_seq advances cold (per DDL)
		// and the snapshot per-batch cost is one tiny SQL read. A
		// future optimization caches it on the Cache.
		var deps crdt.Deps
		if s.sc != nil {
			schemaSeq, _, err := s.sc.GetSchemaSeq()
			if err != nil {
				return 0, fmt.Errorf("sink: read schema_seq: %w", err)
			}
			if schemaSeq > 0 {
				deps = crdt.Deps{crdt.SchemaChain: crdt.Seq(schemaSeq)}
			}
		}
		cs, err := crdt.Build(dot, stamp, deps, s.cluster, s.records[recStart:])
		if err != nil {
			return 0, fmt.Errorf("sink: build changeset at %d: %w", d.jrec.Offset, err)
		}
		// CAPTURE: durably stage the changeset in the self-log with its
		// source self-journal offset, and retain a copy for the deferred
		// publish. Nothing is broadcast/sealed yet — encoded-listeners
		// fire only after the batch fsync below.
		payload := cs.Encoded()
		if s.selfLog != nil {
			if err := s.selfLog.AppendSelf(payload, d.jrec.NextOff); err != nil {
				return 0, fmt.Errorf("sink: self-log capture at %d: %w", d.jrec.Offset, err)
			}
		}
		// crdt.Build allocates a fresh encoded slice per changeset, so
		// retaining it (rather than copying) keeps that array alive for
		// the deferred publish without aliasing reused scratch.
		s.pub = append(s.pub, payload)
		s.recordsListenersMu.RLock()
		recListeners := s.recordsListeners
		s.recordsListenersMu.RUnlock()
		if len(recListeners) > 0 {
			recs := s.records[recStart:]
			for _, fn := range recListeners {
				fn(dot, recs)
			}
		}
		s.reassertMu.RLock()
		reassert := s.reassert
		s.reassertMu.RUnlock()
		if reassert != nil {
			// Before this batch's row clocks publish below — the
			// broker's gate must compare the commit against the
			// pre-drain clock to detect an interleaved inbound apply.
			_ = reassert(s.records[recStart:], stamp)
		}
		s.commits = append(s.commits, commitOut{
			clockLo: clockStart,
			clockHi: len(s.clocks),
			dot:     dot,
		})
	}

	if len(s.commits) == 0 {
		// All records collapsed to no-ops. Just advance the drainer's
		// offset; nothing to persist.
		return endOffset, nil
	}

	// CAPTURE barrier: group-commit the batch's self-log appends. A fsync
	// failure is fatal — return before publishing or committing any cache
	// state, so neither the marker nor senderNextSeq advances past bytes
	// that may not be durable. On restart durability is decided by what
	// recoverSelf reads back; a retry-then-continue would re-derive
	// (risking two contents for one Dot), so we stop instead.
	if s.selfLog != nil {
		if err := s.selfLog.SyncSelf(); err != nil {
			return 0, fmt.Errorf("sink: self-log fsync: %w", err)
		}
	}

	// PUBLISH: only now that the bytes are durable do we broadcast and feed
	// the sealer. encoded-listeners fire once per captured changeset, in
	// build order, off the commit-thread latency path.
	s.encodedListenersMu.RLock()
	encListeners := s.encodedListeners
	s.encodedListenersMu.RUnlock()
	for _, payload := range s.pub {
		for _, fn := range encListeners {
			fn(payload)
		}
	}

	if err := s.persistBlobMutations(blobMutations); err != nil {
		return 0, fmt.Errorf("sink: persist blob_range_clock: %w", err)
	}

	last := s.commits[len(s.commits)-1]
	// Publish the per-batch row_clock updates so the apply path sees
	// them, advance senderNextSeq by the count of seqs we consumed off
	// baseSeq, and move the self snapshot marker so recovery resumes
	// from endOffset. hlcLast was already advanced by walHook's
	// StampHLC.
	for i := range s.commits {
		c := &s.commits[i]
		for k := c.clockLo; k < c.clockHi; k++ {
			u := &s.clocks[k]
			if s.cache.PutRowState(u.tableID, u.pk, u.state) && u.clearCells {
				s.cache.ClearCellsForRow(u.tableID, u.pk)
			}
		}
	}
	for i := range s.cellClocks {
		u := &s.cellClocks[i]
		s.cache.PutCellStamp(u.tableID, u.pk, u.col, u.stamp)
	}
	// AllocSelfSeq returns + consumes one; replicate the count here
	// without re-locking per commit.
	consumed := uint64(last.dot.Seq) + 1 - uint64(s.cache.SenderNextSeq(s.origin))
	for i := uint64(0); i < consumed; i++ {
		s.cache.AllocSelfSeq(s.origin)
	}
	s.cache.SetSnapshotMarker(s.origin, endOffset)

	s.listenersMu.RLock()
	listeners := s.listeners
	s.listenersMu.RUnlock()
	for i := 0; i < len(s.commits); i++ {
		for _, fn := range listeners {
			fn()
		}
	}
	return endOffset, nil
}

// blobMutation is one row's pending blob_range_clock side effect: either
// per-column entries to persist (a folded BlobPatch) or a drop (a full-row
// DML supersedes the byte-range clock). Shared by the forward drain
// (Apply) and recovery (ReplayBlobClock) so the two stay in lockstep.
type blobMutation struct {
	table crdt.TableID
	pk    crdt.PKBlob
	cols  []metadata.BlobRangeClockEntry
	drop  bool
}

// ReplayBlobClock re-applies a recovered self changeset's blob_range_clock
// effects, mirroring the forward drain's blobMutations: fold each
// BlobPatch's ranges (stamp-idempotent, so re-running converges) and drop a
// full-row DML's clock. Both are gated on the pre-effect cache state — a
// BlobPatch under a newer recreate (generation mismatch) and a dominated
// DML are skipped, so a remote write already in the snapshot keeps its
// clock (the Q4 hazard). Implements nodestate.SelfLogReplayer; called
// before RecoverSelf applies the changeset's row-clock effects.
func (s *MetaSink) ReplayBlobClock(cs *crdt.Changeset) error {
	if s.sc == nil {
		return nil
	}
	var muts []blobMutation
	for _, r := range cs.Records {
		h := r.Header()
		if bp, ok := r.(crdt.BlobPatch); ok {
			rs := s.cache.RowState(h.Table, h.PK)
			if rs.CL != bp.CL && !(rs.CL == 0 && bp.CL == 1) {
				continue // stale generation: a newer recreate dominates
			}
			cols, err := s.foldBlobPatchLocal(h.Table, h.PK, bp, cs.Stamp, rs.Base)
			if err != nil {
				return err
			}
			muts = append(muts, blobMutation{table: h.Table, pk: h.PK, cols: cols, drop: len(cols) == 0})
			continue
		}
		// Full-row DML drops the row's byte-range clock, but only if this
		// write still dominates the loaded state.
		rs := s.cache.RowState(h.Table, h.PK)
		if !rs.DominatedBy(h.CL, cs.Stamp) {
			continue
		}
		muts = append(muts, blobMutation{table: h.Table, pk: h.PK, drop: true})
	}
	return s.persistBlobMutations(muts)
}

// persistBlobMutations writes a batch of blob_range_clock side effects in
// one metadata tx. Shared by Apply and ReplayBlobClock.
func (s *MetaSink) persistBlobMutations(muts []blobMutation) error {
	if len(muts) == 0 || s.sc == nil {
		return nil
	}
	return s.sc.WithTx(func(tx *metadata.Tx) error {
		for _, m := range muts {
			if m.drop {
				if err := tx.DeleteBlobRangeClock(m.table, m.pk); err != nil {
					return err
				}
				continue
			}
			if err := tx.PutBlobRangeClock(m.table, m.pk, m.cols); err != nil {
				return err
			}
		}
		return nil
	})
}

// foldBlobPatchLocal loads the row's existing blob_range_clock,
// applies the patch with c=local stamp and baseline=parent Stamp,
// and returns the per-column entries to persist. An empty result
// means the row's blob_range_clock entry should be deleted.
func (s *MetaSink) foldBlobPatchLocal(table crdt.TableID, pk crdt.PKBlob,
	bp crdt.BlobPatch, stamp, baseline crdt.Stamp) ([]metadata.BlobRangeClockEntry, error) {
	existing, err := s.sc.GetBlobRangeClock(table, pk)
	if err != nil {
		return nil, err
	}
	maps := metadata.LoadIntervalMaps(existing)
	cur, ok := maps[bp.Col]
	if !ok {
		cur = crdt.NewIntervalMap()
		maps[bp.Col] = cur
	}
	for _, rg := range bp.Ranges {
		cur.Apply(rg.Offset, rg.End(), stamp, baseline)
	}
	return metadata.EntriesFromMaps(maps), nil
}

// rowClockUpdate is one (table, pk) → row_clock entry pending persist
// into Cache. clearCells additionally drops the row's cell overrides
// (cell-group opportunistic collapse / generation advance).
type rowClockUpdate struct {
	tableID    crdt.TableID
	pk         crdt.PKBlob
	state      crdt.RowState
	clearCells bool
}

// cellClockUpdate is one (table, pk, col) → stamp override pending
// persist into Cache: a cell-group table's local partial update
// advances per-column stamps instead of the row baseline.
type cellClockUpdate struct {
	tableID crdt.TableID
	pk      crdt.PKBlob
	col     crdt.ColumnID
	stamp   crdt.Stamp
}

// getRowState returns the current per-batch state for (table, pk).
// Cache is authoritative — its row_clock map is the runtime view of
// LWW state for both self and remote. The per-batch map exists so
// successive commits in the same Apply batch see each other's CL
// advances without re-querying the cache.
func (s *MetaSink) getRowState(table crdt.TableID, pk crdt.PKBlob) crdt.RowState {
	k := rckey{t: table, pk: string(pk)}
	if st, ok := s.rowState[k]; ok {
		return st
	}
	st := s.cache.RowState(table, pk)
	s.rowState[k] = st
	return st
}

func (s *MetaSink) setRowState(table crdt.TableID, pk crdt.PKBlob, st crdt.RowState) {
	s.rowState[rckey{t: table, pk: string(pk)}] = st
}

// buildRecords appends one commit's records and row_clock updates onto
// s.records and s.clocks. Successive commits in the same batch see
// in-batch CL advances via s.rowState.
func (s *MetaSink) buildRecords(evidence []recordEvidence, stamp crdt.Stamp) error {
	for i := range evidence {
		ev := &evidence[i]
		switch ev.op {
		case evOpInsert:
			cl := s.getRowState(ev.tableID, ev.newPK).NextLiveCL()
			newState := crdt.RowState{CL: cl, Base: stamp}
			s.records = append(s.records, crdt.Insert{Table: ev.tableID, PK: ev.newPK, CL: cl, Image: ev.image})
			s.clocks = append(s.clocks, rowClockUpdate{tableID: ev.tableID, pk: ev.newPK, state: newState})
			s.setRowState(ev.tableID, ev.newPK, newState)

		case evOpUpdate:
			cur := s.getRowState(ev.tableID, ev.newPK)
			cl := cur.NextLiveCL()
			s.records = append(s.records, crdt.Update{Table: ev.tableID, PK: ev.newPK, CL: cl, Changed: ev.changed})
			if tab, ok := s.cat.TableByID(ev.tableID); ok && tab.CellGroup() && !coversAllNonPK(tab, ev.changed) {
				// Cell group, partial coverage: the local commit
				// advances only the carried columns' stamps. The row
				// baseline stays put so concurrent remote writes to
				// other columns still merge in. A generation bump
				// (pre-syzy row's first UPDATE) leaves Base zero,
				// mirroring the receiver-side rule.
				newState := cur
				if cl > cur.CL {
					newState = crdt.RowState{CL: cl}
					s.clocks = append(s.clocks, rowClockUpdate{tableID: ev.tableID, pk: ev.newPK, state: newState, clearCells: true})
				}
				for _, v := range ev.changed {
					if v.Format == crdt.FormatDelta {
						// Counter contributions carry no Stamp — they
						// sum instead of arbitrating (CRDT.md F_counter).
						continue
					}
					s.cellClocks = append(s.cellClocks, cellClockUpdate{ev.tableID, ev.newPK, v.Column, stamp})
				}
				s.setRowState(ev.tableID, ev.newPK, newState)
				break
			}
			// Row group, or full coverage (cell-group collapse): the
			// write defines every column — absorb into the baseline.
			newState := crdt.RowState{CL: cl, Base: stamp}
			s.clocks = append(s.clocks, rowClockUpdate{tableID: ev.tableID, pk: ev.newPK, state: newState, clearCells: cur.Cells != nil})
			s.setRowState(ev.tableID, ev.newPK, newState)

		case evOpDelete:
			cl := s.getRowState(ev.tableID, ev.oldPK).NextTombCL()
			newState := crdt.RowState{CL: cl, Base: stamp}
			s.records = append(s.records, crdt.Delete{Table: ev.tableID, PK: ev.oldPK, CL: cl})
			s.clocks = append(s.clocks, rowClockUpdate{tableID: ev.tableID, pk: ev.oldPK, state: newState})
			s.setRowState(ev.tableID, ev.oldPK, newState)

		case evOpUpdatePKChange:
			oldState := crdt.RowState{
				CL:   s.getRowState(ev.tableID, ev.oldPK).NextTombCL(),
				Base: stamp,
			}
			newState := crdt.RowState{
				CL:   s.getRowState(ev.tableID, ev.newPK).NextLiveCL(),
				Base: stamp,
			}
			s.records = append(s.records,
				crdt.Delete{Table: ev.tableID, PK: ev.oldPK, CL: oldState.CL},
				crdt.Insert{Table: ev.tableID, PK: ev.newPK, CL: newState.CL, Image: ev.image},
			)
			s.clocks = append(s.clocks,
				rowClockUpdate{tableID: ev.tableID, pk: ev.oldPK, state: oldState},
				rowClockUpdate{tableID: ev.tableID, pk: ev.newPK, state: newState},
			)
			s.setRowState(ev.tableID, ev.oldPK, oldState)
			s.setRowState(ev.tableID, ev.newPK, newState)

		case evOpBlobPatch:
			// blob_patch carries the row's *current* CL — does not
			// advance row_clock. Patches apply only when the row is
			// live (CL odd) on the receiver. If we have no row_clock
			// for this row in cache (e.g., row pre-existed before
			// syzy), use CL=1 (the implicit live generation).
			rs := s.getRowState(ev.tableID, ev.newPK)
			cl := rs.CL
			if cl == 0 {
				cl = 1
			}
			s.records = append(s.records, crdt.BlobPatch{
				Table:  ev.tableID,
				PK:     ev.newPK,
				CL:     cl,
				Col:    ev.blobCol,
				Ranges: ev.blobRanges,
			})
			// row_clock unchanged — patch does not advance the row's
			// causal length / parent stamp.

		default:
			return fmt.Errorf("sink: unknown evidence op %d", ev.op)
		}
	}
	return nil
}
