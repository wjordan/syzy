package sqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syncer"
)

// Secondary-drainer cadence. Vars (not consts) so tests can shrink
// them; production never mutates these.
var (
	// secondaryRescanInterval is how often the origins/*/ rescan looks
	// for newly-arriving extension origins to attach drainers to.
	secondaryRescanInterval = 2 * time.Second
	// secondaryDrainPollInterval is the fallback poll cadence for each
	// secondary drainer's journal wait (normally futex-woken; the poll
	// only matters when the shared wake is missed).
	secondaryDrainPollInterval = 500 * time.Millisecond
)

// scanSecondaries lists origins/*/ under appPath and spawns a
// SecondaryDrainer for any origin we haven't already attached. Self
// is skipped — the in-process producer drains its own journal.
// Idempotent on subsequent calls (cheap dir listing + map lookup).
//
// n.secondaries is left nil when Config.InProcessOnly is set (no
// cross-process producers exist), so this returns immediately and never
// attaches a drainer to a non-self origin. See Config.InProcessOnly for
// why that matters: every non-self origin on such a node is a retired
// self-origin whose re-drain amplifies the mirror journal unboundedly.
func (n *Node) scanSecondaries(parent context.Context, appPath string, log *slog.Logger) error {
	if n.secondaries == nil {
		return nil
	}
	entries, err := os.ReadDir(layout.OriginsRoot(appPath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read origins root: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var raw uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x", &raw); err != nil {
			continue
		}
		o := crdt.Origin(raw)
		if o == n.originClaim.Origin {
			continue
		}
		n.secMu.Lock()
		_, exists := n.secondaries[o]
		n.secMu.Unlock()
		if exists {
			continue
		}
		jdir := filepath.Join(layout.OriginsRoot(appPath), e.Name(), "journal")
		// The writer creates origins/<o>/journal/ and its first segment in
		// separate steps; an origin with no initialized (non-empty) segment
		// has nothing to drain yet. Skip it quietly and retry next scan —
		// this covers a writer still initializing and an orphaned origin
		// left by a departed VM alike. Probing here (a cheap dir stat)
		// keeps a not-ready origin from costing a sqlite aux conn and a
		// reader journal open every scan, which otherwise spins a fruitless
		// open+warn each tick (and the open plants a 0-byte segment that
		// makes the failure permanent).
		if !journal.HasDrainableSegment(jdir) {
			continue
		}
		// One read-only conn per secondary so the drainer can
		// materialize blob_patch records (BLOB_PATCH.md). Conn isn't
		// safe for concurrent use across drainers, so don't share.
		blobRead, err := openAuxConn(appPath, "blobread-"+layout.OriginHex(o), n.disableMmap, n.objectBackend != nil)
		if err != nil {
			log.Warn("open blob-read conn for secondary", "origin", layout.OriginHex(o), "err", err)
			continue
		}
		sd, err := syncer.NewSecondaryDrainer(syncer.SecondaryConfig{
			Origin:       o,
			JournalDir:   jdir,
			Cluster:      n.clusterID,
			Cache:        n.cache,
			Meta:         n.meta,
			Catalog:      n.catalog,
			BlobRead:     blobRead,
			OnEncoded:    n.secondaryPublishFn(o),
			PollInterval: secondaryDrainPollInterval,
		})
		if err != nil {
			_ = blobRead.Close()
			// Writer raced us between the probe and the open (its segment
			// vanished or hadn't landed yet) — retry quietly next scan.
			if errors.Is(err, journal.ErrNoSegments) {
				continue
			}
			log.Warn("spawn secondary drainer", "origin", layout.OriginHex(o), "err", err)
			continue
		}
		sd.Sink.OnRecords(n.publishRecords)
		sd.Sink.SetReassert(n.reassertFn(log))
		// Seal extension-claimed origins to objects/origins/ alongside
		// gossip. Without this, cross-process writes (loadable extension
		// in a separate sqlite3 process) have no logical-replication
		// trail; a peer returning from offline can't catch up via
		// objects/ tip discovery.
		if n.sealer != nil {
			sd.Sink.OnEncoded(n.sealer.OnEncoded)
		}
		if n.wakeListener != nil {
			// Cross-kernel secondary: install the listener-supplied
			// Waiter on this origin's journal so its drainer wakes
			// from vsock (or whatever transport the listener provides)
			// instead of futex.Wait timing out every 500ms.
			waiter := n.wakeListener.Register(layout.OriginHex(o))
			sd.Journal.SetWaitFunc(waiter.Wait)
		}
		sd.Start(parent)
		n.secMu.Lock()
		n.secondaries[o] = sd
		n.secMu.Unlock()
		log.Info("syzy: secondary drainer attached", "origin", layout.OriginHex(o))
	}
	return nil
}
