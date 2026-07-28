package syncer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/sqlitecatalog"
	"github.com/wjordan/syzy/sqlitebridge"
)

// SecondaryDrainer is one origin's sink+drainer pair as run by the
// daemon when that origin's writes come from a different process
// (typically a SQLite loadable extension). Unlike the in-process
// producer, it doesn't install hooks or own a writer connection — it
// only reads the journal and pushes encoded changesets through the
// shared OnEncoded listener.
type SecondaryDrainer struct {
	Origin  crdt.Origin
	Journal *journal.Journal
	Sink    *MetaSink
	Drainer *Drainer

	// blobRead is owned by this drainer when non-nil; Close releases it.
	blobRead *sqlitebridge.Conn

	cancel context.CancelFunc
	done   chan struct{}

	// drainErr records a Run exit error. WaitForDrain checks it so a
	// dead drainer fails the wait instead of spinning forever on a
	// DrainedOffset that can no longer advance (mirrors
	// Producer.WaitForDrain's drainErr).
	drainErr atomic.Pointer[error]
}

// SecondaryConfig is the dependency bundle for NewSecondaryDrainer.
type SecondaryConfig struct {
	Origin     crdt.Origin
	JournalDir string
	Cluster    crdt.ClusterID
	Cache      *nodestate.Cache
	Meta       *metadata.Store
	Catalog    *catalog.Catalog
	// BlobRead is an optional read-only app.db connection used by the
	// drainer's sink to materialize blob_patch records (read post-commit
	// NEW bytes via sqlite3_blob_open). Nil silently drops blob_write
	// captures. Each SecondaryDrainer owns its own connection — drainers
	// run on their own goroutine and Conn isn't safe for concurrent use.
	BlobRead *sqlitebridge.Conn
	// OnEncoded fires on the drainer goroutine each time a record is
	// materialized into a Changeset. Production wiring routes broadcast
	// through here. The byte slice aliases sink-owned scratch — copy
	// before retaining.
	OnEncoded func(payload []byte)
	// PollInterval is the safety timeout while waiting on a journal
	// publish word. Zero defaults to 500ms; normal appends wake via
	// futex immediately.
	PollInterval time.Duration
}

// NewSecondaryDrainer opens the journal at cfg.JournalDir, builds a
// MetaSink wired for cfg.Origin, and prepares (but does not start)
// a Drainer with cross-process publish-word wake. Call Start to begin
// draining.
func NewSecondaryDrainer(cfg SecondaryConfig) (*SecondaryDrainer, error) {
	if cfg.Cache == nil {
		return nil, fmt.Errorf("syncer: SecondaryConfig.Cache required")
	}
	// SyncOff: we're a reader, not a durability owner. The writer
	// process picks the sync mode for its own appends.
	j, err := journal.Open(cfg.JournalDir, 0, journal.SyncOff)
	if err != nil {
		return nil, fmt.Errorf("syncer: open secondary journal %s: %w", cfg.JournalDir, err)
	}
	sink := NewMetaSink(cfg.Meta, cfg.Catalog, cfg.Cluster, cfg.Origin,
		func() int64 { return time.Now().UnixMicro() })
	sink.SetCache(cfg.Cache)
	if cfg.BlobRead != nil {
		sink.SetBlobRead(cfg.BlobRead)
	}
	if cfg.OnEncoded != nil {
		sink.OnEncoded(cfg.OnEncoded)
	}
	poll := cfg.PollInterval
	if poll == 0 {
		poll = 500 * time.Millisecond
	}
	dr, err := NewDrainer(j, sink, WithSharedWake(), WithPollInterval(poll))
	if err != nil {
		_ = j.Close()
		return nil, fmt.Errorf("syncer: secondary drainer: %w", err)
	}
	return &SecondaryDrainer{
		Origin:   cfg.Origin,
		Journal:  j,
		Sink:     sink,
		Drainer:  dr,
		blobRead: cfg.BlobRead,
	}, nil
}

// Start spawns the drainer goroutine under a context derived from
// parent. Idempotent: a second call is a no-op.
func (s *SecondaryDrainer) Start(parent context.Context) {
	if s.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		if err := s.Drainer.Run(ctx); err != nil {
			s.drainErr.Store(&err)
		}
		close(s.done)
	}()
}

// WaitForDrain blocks until the drainer has flushed every record up to
// the journal's current head, or ctx is cancelled. Mirrors
// Producer.WaitForDrain so callers that want a "all already-committed
// records reflected in cache" barrier can fan out across the producer
// and every secondary on the daemon.
//
// Note: this guarantees the drainer is caught up to head *as observed
// at call time*. New journal records appended concurrently aren't
// covered. Callers needing a stable barrier must independently prevent
// new commits (e.g. by holding the WAL writer slot on app.db) before
// invoking. Because secondary journals are written by another process,
// each check refreshes the mmap-backed head before comparing offsets.
func (s *SecondaryDrainer) WaitForDrain(ctx context.Context) error {
	if s == nil || s.Drainer == nil || s.Journal == nil {
		return nil
	}
	for {
		// Converged-first: a drainer that already consumed everything
		// observable satisfies the wait even if its goroutine has
		// since died — failing such a wait turns a benign dead
		// drainer into an unclean node close (and, on control
		// topics, an origin rotation at next boot).
		s.Journal.Refresh()
		head := s.Journal.Head()
		if s.Drainer.DrainedOffset() >= head {
			return nil
		}
		if errp := s.drainErr.Load(); errp != nil {
			return fmt.Errorf("drainer dead: %w", *errp)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// Close cancels the goroutine and closes the journal handle and any
// owned blob-read connection. Safe to call multiple times. Blocks until
// the goroutine exits.
func (s *SecondaryDrainer) Close() error {
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
	var firstErr error
	if s.Journal != nil {
		if err := s.Journal.Close(); err != nil {
			firstErr = err
		}
		s.Journal = nil
	}
	if s.blobRead != nil {
		if err := s.blobRead.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.blobRead = nil
	}
	return firstErr
}
