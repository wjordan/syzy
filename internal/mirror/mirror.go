// Package mirror manages per-origin inbound mirror journals: each
// remote origin we've ever applied a record from has an append-only
// journal directory on this node, mirrored from the origin's wire
// format. Recovery replays these journals after loading the metadata
// snapshot; Serve (serve.go) answers peer catch-up requests from them.
//
// The broker.MirrorJournals interface is implemented here. Append
// pushes to a per-origin bounded channel; a writer goroutine per
// origin drains the channel into the journal's mmap. Backpressure:
// the channel is bounded; when full, Append blocks the apply path —
// keeps semantics simple (no silent drops) at the cost of pinning
// throughput to disk speed in the worst case.
package mirror

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
)

// DefaultSegmentSize is the per-origin journal segment size when the
// caller doesn't override it.
const DefaultSegmentSize = uint32(1 << 20)

// DefaultChanDepth is the per-origin writer chan buffer. Tuned for
// burst smoothing; larger doesn't add throughput, just memory.
const DefaultChanDepth = 64

// Config configures a Manager.
type Config struct {
	// Root is the base directory under which per-origin journal subdirs
	// live as "origin_<id>/". Required.
	Root string
	// SegmentSize overrides the per-segment size. 0 -> default.
	SegmentSize uint32
	// ChanDepth overrides the per-origin writer channel depth. 0 -> default.
	ChanDepth int
	// Log receives a per-Serve catchup line (records scanned/sent, bytes,
	// segments skipped). nil -> discard.
	Log *slog.Logger
	// Self, when nonzero, marks this node's own origin. Its mirror journal
	// is the durable self-log: opened journal.SyncOn and written inline via
	// AppendSelf (the drainer is its sole writer), never through the async
	// writer goroutine — that channel would defer durability past Append's
	// return. Remote origins stay SyncOff/async (a lost trailing remote
	// append is recoverable by peer re-delivery). Zero disables the policy
	// (all origins async SyncOff), for engines that don't route self here.
	Self crdt.Origin
}

// Manager owns per-origin journal handles + writer goroutines. Lazy:
// the first Append for an origin creates its journal + writer.
type Manager struct {
	cfg     Config
	segSize uint32
	depth   int
	self    crdt.Origin
	log     *slog.Logger

	mu       sync.Mutex
	closed   bool
	journals map[crdt.Origin]*originHandle

	// runWG tracks active writer goroutines so Close can wait for them.
	runWG sync.WaitGroup

	// lastStats records the most recent Serve call's scan/skip counters,
	// for observability (R5) and tests. Guarded by statMu.
	statMu    sync.Mutex
	lastStats ServeStats
}

type originHandle struct {
	j    *journal.Journal
	in   chan appendRequest
	done chan struct{} // closed when writer goroutine exits
	quit chan struct{} // closed by Reap to stop the writer WITHOUT closing in
	// sticky write error from the writer goroutine. Once set, future
	// Appends return this error rather than blocking on a dead chan.
	errMu sync.Mutex
	err   error

	// segIndex maps a journal segment number to the span of CRDT seqs it
	// holds, so Serve can skip whole segments below a catchup request's Lo
	// instead of scanning the journal from offset 0. Guarded by idxMu;
	// built once (lazily, on first Serve) by idxOnce and kept current by
	// the writer goroutine on every append.
	idxMu    sync.Mutex
	idxOnce  sync.Once
	segIndex map[uint32]*segSpan
}

type appendRequest struct {
	payload []byte
	ack     chan error
}

// segSpan is the CRDT-seq footprint of one journal segment: the lowest
// offset a mirror record landed at (where Serve resumes iteration) and
// the highest CRDT seq the segment carries (the skip key — a segment
// whose maxSeq < Lo cannot hold any record the request wants).
type segSpan struct {
	firstOff journal.Offset
	maxSeq   uint64
}

// New constructs a Manager. Root is created if absent.
func New(cfg Config) (*Manager, error) {
	if cfg.Root == "" {
		return nil, errors.New("mirror: Config.Root required")
	}
	segSize := cfg.SegmentSize
	if segSize == 0 {
		segSize = DefaultSegmentSize
	}
	depth := cfg.ChanDepth
	if depth <= 0 {
		depth = DefaultChanDepth
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := &Manager{
		cfg:      cfg,
		segSize:  segSize,
		depth:    depth,
		self:     cfg.Self,
		log:      log,
		journals: map[crdt.Origin]*originHandle{},
	}
	return m, nil
}

// Append implements broker.MirrorJournals: queue payload onto origin's
// writer chan. Lazy-creates the per-origin handle on first call.
// Blocks if the chan is full (backpressure). Returns the writer's
// sticky error if the writer has died.
func (m *Manager) Append(origin crdt.Origin, payload []byte) error {
	return m.append(origin, payload, false)
}

// AppendWait appends a remote-origin payload and waits until the writer has
// copied it into the journal mmap. It does not fsync. Locally-drained secondary
// producers use this as a capture-before-broadcast barrier so a peer reacting
// to that broadcast can immediately fetch the same bytes from this manager.
func (m *Manager) AppendWait(origin crdt.Origin, payload []byte) error {
	return m.append(origin, payload, true)
}

func (m *Manager) append(origin crdt.Origin, payload []byte, wait bool) error {
	if origin == m.self && m.self != 0 {
		return errors.New("mirror: self origin uses AppendSelf (durable inline path)")
	}
	h, err := m.handleFor(origin)
	if err != nil {
		return err
	}
	h.errMu.Lock()
	if h.err != nil {
		err := h.err
		h.errMu.Unlock()
		return err
	}
	h.errMu.Unlock()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	var ack chan error
	if wait {
		ack = make(chan error, 1)
	}
	select {
	case h.in <- appendRequest{payload: cp, ack: ack}:
	case <-h.done:
		return handleError(h)
	}
	if !wait {
		return nil
	}
	select {
	case err := <-ack:
		return err
	case <-h.done:
		return handleError(h)
	}
}

func handleError(h *originHandle) error {
	h.errMu.Lock()
	err := h.err
	h.errMu.Unlock()
	if err == nil {
		err = errors.New("mirror: writer goroutine exited")
	}
	return err
}

// AppendSelf durably stages a self-origin changeset payload into the
// self-log, carrying its source self-journal endOffset in the record
// header (the spare mirror-record hlc field; recovery reads it back as
// the drain resume offset). The write is inline — the drainer is the
// self-log's sole writer — so the payload is copied into the journal
// mmap before return; call SyncSelf to group-commit the batch's fsync.
// endOffset must be nonzero. Not safe for concurrent use; the drainer serializes.
func (m *Manager) AppendSelf(payload []byte, endOffset journal.Offset) error {
	if m.self == 0 {
		return errors.New("mirror: no self origin configured")
	}
	if endOffset == 0 {
		return errors.New("mirror: self-log endOffset must be nonzero")
	}
	h, err := m.handleFor(m.self)
	if err != nil {
		return err
	}
	off, _, err := h.j.Append(journal.KindMirror, uint64(endOffset), uint64(m.self), payload)
	if err != nil {
		return fmt.Errorf("mirror: self-log append: %w", err)
	}
	h.indexAppend(off, payload)
	return nil
}

// SyncSelf group-commits the self-log (one msync over the batch's
// appends). A failure is the caller's signal to stop without publishing
// — the capture-before-publish barrier. No-op when self is unconfigured.
func (m *Manager) SyncSelf() error {
	if m.self == 0 {
		return nil
	}
	h, err := m.handleFor(m.self)
	if err != nil {
		return err
	}
	return h.j.Sync()
}

// Journal returns the journal handle for origin (creating it if absent),
// for use by recovery to iterate. Caller must NOT close it.
func (m *Manager) Journal(origin crdt.Origin) (*journal.Journal, error) {
	h, err := m.handleFor(origin)
	if err != nil {
		return nil, err
	}
	return h.j, nil
}

// LookupJournal returns the journal handle for origin only if one
// already exists (this node has ever applied or produced a record from
// that origin). Unlike Journal it does NOT create a fresh handle, so
// catchup requests for unknown origins don't litter the mirror root.
func (m *Manager) LookupJournal(origin crdt.Origin) (*journal.Journal, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.journals[origin]
	if !ok {
		return nil, false
	}
	return h.j, true
}

// Origins returns the list of origins this manager has journals for.
// Used by recovery to know which journals to replay.
func (m *Manager) Origins() []crdt.Origin {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]crdt.Origin, 0, len(m.journals))
	for o := range m.journals {
		out = append(out, o)
	}
	return out
}

// LoadExisting opens any origin_*/ journals that already exist on disk,
// readying them for recovery replay even before any Append. Idempotent.
func (m *Manager) LoadExisting() error {
	matches, err := filepath.Glob(filepath.Join(m.cfg.Root, "origin_*"))
	if err != nil {
		return fmt.Errorf("mirror: scan root: %w", err)
	}
	for _, dir := range matches {
		var o crdt.Origin
		if _, err := fmt.Sscanf(filepath.Base(dir), "origin_%d", &o); err != nil {
			continue
		}
		if _, err := m.handleFor(o); err != nil {
			return err
		}
	}
	return nil
}

// Close stops all writer goroutines and closes every journal. Blocks
// until writers drain. Safe to call multiple times.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	handles := make([]*originHandle, 0, len(m.journals))
	for _, h := range m.journals {
		handles = append(handles, h)
	}
	m.mu.Unlock()
	for _, h := range handles {
		close(h.in)
	}
	m.runWG.Wait()
	for _, h := range handles {
		_ = h.j.Close()
	}
	return nil
}

// handleFor returns (or lazily creates) the handle for origin.
func (m *Manager) handleFor(origin crdt.Origin) (*originHandle, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("mirror: manager closed")
	}
	if h, ok := m.journals[origin]; ok {
		m.mu.Unlock()
		return h, nil
	}
	dir := filepath.Join(m.cfg.Root, fmt.Sprintf("origin_%d", origin))
	// Remote mirror journals are SyncOff: a lost trailing mirror append is
	// recoverable via peer re-delivery, so we don't pay msync per inbound
	// apply. The self-log (origin == m.self) is the durability boundary for
	// our own writes — opened SyncOn and written inline by the drainer via
	// AppendSelf, with no writer goroutine.
	self := origin == m.self && m.self != 0
	mode := journal.SyncOff
	if self {
		mode = journal.SyncOn
	}
	j, err := journal.Open(dir, m.segSize, mode)
	if err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("mirror: open journal for origin %d: %w", origin, err)
	}
	h := &originHandle{
		j:        j,
		in:       make(chan appendRequest, m.depth),
		done:     make(chan struct{}),
		quit:     make(chan struct{}),
		segIndex: map[uint32]*segSpan{},
	}
	m.journals[origin] = h
	if !self {
		m.runWG.Add(1)
		go m.writerLoop(origin, h)
	}
	m.mu.Unlock()
	return h, nil
}

// writerLoop drains in into the per-origin journal until the chan is
// closed. On a journal.Append error it sets h.err and exits — Append
// returns the sticky error from then on (preserves liveness; the
// alternative is panic, which would take down the whole node).
//
// Records are written with kind=KindMirror and zero hlc/origin
// metadata. The wire-format payload itself carries everything recovery
// needs (origin, seq, hlc, records).
func (m *Manager) writerLoop(origin crdt.Origin, h *originHandle) {
	defer m.runWG.Done()
	defer close(h.done)
	for {
		select {
		case <-h.quit:
			// Reap: stop without draining. Any buffered payload is lost but
			// reversible (re-fetchable), and we only reap quiescent origins.
			return
		case req, ok := <-h.in:
			if !ok {
				// in closed by Close: buffered payloads already drained above.
				return
			}
			off, _, err := h.j.Append(journal.KindMirror, 0, uint64(origin), req.payload)
			if err != nil {
				err = fmt.Errorf("mirror: journal append origin=%d: %w", origin, err)
				h.errMu.Lock()
				h.err = err
				h.errMu.Unlock()
				if req.ack != nil {
					req.ack <- err
				}
				return
			}
			h.indexAppend(off, req.payload)
			if req.ack != nil {
				req.ack <- nil
			}
		}
	}
}

// Reap removes the per-origin mirror journal for origin: stops its writer
// goroutine, closes the journal, and deletes its on-disk directory. Used by
// the Node-level reaper to drop a long-dead origin whose log is fully sealed
// to the object store — a reversible cache eviction (the data stays
// materialized in app.db, the log stays in the bucket, and a later live
// frame or DiscoverTips re-creates the journal via handleFor). The caller is
// responsible for the sealed+quiescent predicate; Reap itself just tears the
// journal down. It does NOT touch the cache's applied frontier, so the node
// never re-fetches the reaped origin (no thrash).
//
// quit (not in) is closed so a racing Append that already obtained the handle
// hits the <-h.done branch and returns a sticky error rather than sending on a
// closed channel. A resurrection between teardown and rmdir leaves the dir to
// the new handle.
func (m *Manager) Reap(origin crdt.Origin) error {
	if origin == m.self && m.self != 0 {
		// The self-log has no writer goroutine to stop and is trimmed via
		// RetainSealed, not reaped. Guard against a stray caller draining
		// on h.done forever.
		return errors.New("mirror: cannot reap the self origin")
	}
	dir := filepath.Join(m.cfg.Root, fmt.Sprintf("origin_%d", origin))
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	h, ok := m.journals[origin]
	if !ok {
		m.mu.Unlock()
		// No live handle: clear any stale dir left on disk.
		return os.RemoveAll(dir)
	}
	delete(m.journals, origin)
	close(h.quit)
	m.mu.Unlock()

	<-h.done
	_ = h.j.Close()

	// Resurrection guard: if a concurrent Append re-created a handle for this
	// origin while we tore the old one down, that new handle owns the dir.
	m.mu.Lock()
	_, resurrected := m.journals[origin]
	m.mu.Unlock()
	if resurrected {
		return nil
	}
	return os.RemoveAll(dir)
}
