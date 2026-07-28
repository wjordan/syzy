// Package peerdisc is the S3-backed peer discovery used by the
// --cluster-s3 daemon mode. Each peer writes a heartbeat object at
// peers/<origin-hex>.json containing its TCP listen address; peers
// LIST the prefix and dial entries whose heartbeats are recent.
//
// The discovery loop is intentionally minimal: no leader, no
// coordination, eventually-consistent. New peers find existing live
// peers within one tick; departed peers age out within a TTL window.
// Pair --seeds with --cluster-s3 to bias toward known-good anchors.
//
// Steady-state cost per tick is one PUT (own heartbeat) + one LIST.
// GETs are only issued for peers we haven't seen at this ETag — a peer
// whose heartbeat object hasn't changed since our last fetch reuses
// the cached body.
package peerdisc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
)

const (
	peersPrefix = "peers/"

	DefaultHeartbeatInterval = 10 * time.Second
	DefaultStaleMultiplier   = 3
)

// Heartbeat is the JSON payload stored at peers/<origin-hex>.json.
// Field names are stable wire format — additions only.
type Heartbeat struct {
	Origin string `json:"origin"`
	Listen string `json:"listen"`
}

type Config struct {
	Backend  objectstore.Bucket
	Origin   crdt.Origin
	Listen   string // host:port advertised to peers; empty disables write side
	Interval time.Duration
	// OnPeers is called only when the peer set changes from the prior
	// tick (sorted, deduped, non-self). It does NOT fire for no-op
	// ticks, so callers can pass it straight to transport.SetSeeds
	// without churn.
	OnPeers func(peers []string)
	Now     func() time.Time
}

type Discoverer struct {
	cfg    Config
	cancel context.CancelFunc
	done   chan struct{}

	mu           sync.Mutex
	cache        map[string]cachedEntry
	lastPeers    []string               // last set passed to OnPeers
	lastBindings map[string]crdt.Origin // last bindings snapshot at OnPeers fire
	seenSeq      uint64
}

type cachedEntry struct {
	etag         string
	hb           Heartbeat
	lastModified time.Time
	seenSeq      uint64
}

func Start(ctx context.Context, cfg Config) (*Discoverer, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("peerdisc: Backend required")
	}
	if cfg.Origin == 0 {
		return nil, fmt.Errorf("peerdisc: Origin required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultHeartbeatInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	d := &Discoverer{
		cfg:   cfg,
		done:  make(chan struct{}),
		cache: make(map[string]cachedEntry),
	}

	if cfg.Listen != "" {
		if err := d.writeHeartbeat(ctx); err != nil {
			return nil, fmt.Errorf("peerdisc: initial heartbeat: %w", err)
		}
	}
	if err := d.discoverOnce(ctx); err != nil {
		return nil, fmt.Errorf("peerdisc: initial discovery: %w", err)
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	go d.loop(loopCtx)
	return d, nil
}

// Close stops the background loop. The final heartbeat is left in
// place — peers age it out via the TTL.
func (d *Discoverer) Close() error {
	if d.cancel == nil {
		return nil
	}
	d.cancel()
	d.cancel = nil
	<-d.done
	return nil
}

func (d *Discoverer) loop(ctx context.Context) {
	defer close(d.done)
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if d.cfg.Listen != "" {
				_ = d.writeHeartbeat(ctx)
			}
			_ = d.discoverOnce(ctx)
		}
	}
}

func (d *Discoverer) writeHeartbeat(ctx context.Context) error {
	hb := Heartbeat{
		Origin: layout.OriginHex(d.cfg.Origin),
		Listen: d.cfg.Listen,
	}
	body, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	key := peersPrefix + layout.OriginHex(d.cfg.Origin) + ".json"
	_, err = d.cfg.Backend.Put(ctx, key, bytes.NewReader(body), int64(len(body)), nil)
	return err
}

func (d *Discoverer) discoverOnce(ctx context.Context) error {
	objs, err := d.cfg.Backend.List(ctx, peersPrefix, "")
	if err != nil {
		return err
	}
	// Filter to heartbeat .json files only — for file:// clusters the
	// peers/ dir also contains the daemons' Unix listener sockets,
	// which List returns alongside the JSON heartbeats but are not
	// readable as regular files.
	heartbeats := objs[:0]
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".json") {
			heartbeats = append(heartbeats, o)
		}
	}
	objs = heartbeats

	stale := time.Duration(DefaultStaleMultiplier) * d.cfg.Interval
	cutoff := d.cfg.Now().Add(-stale)
	selfHex := layout.OriginHex(d.cfg.Origin)

	// Drop cache entries for keys that no longer exist in the bucket.
	d.mu.Lock()
	live := make(map[string]struct{}, len(objs))
	for _, o := range objs {
		live[o.Key] = struct{}{}
	}
	for k := range d.cache {
		if _, ok := live[k]; !ok {
			delete(d.cache, k)
		}
	}
	d.mu.Unlock()

	var peers []string
	for _, o := range objs {
		// LastModified is authoritative for staleness — skip stale
		// without a GET.
		if !o.LastModified.IsZero() && o.LastModified.Before(cutoff) {
			continue
		}
		hb, err := d.heartbeatFor(ctx, o)
		if err != nil {
			continue // best-effort
		}
		if hb.Origin == selfHex || hb.Listen == "" {
			continue
		}
		peers = append(peers, hb.Listen)
	}
	sort.Strings(peers)

	// Build the bindings snapshot under the same lock so the peer
	// set and bindings observed by OnPeers consumers are mutually
	// consistent. Fire OnPeers when either changes — a peer
	// restarting with a new origin at the same listen addr leaves
	// peers unchanged but rotates the binding, and consumers using
	// AddrFor / SetOriginAddrs need the refresh.
	d.mu.Lock()
	bindings := d.bindingsLocked()
	peersChanged := !slices.Equal(peers, d.lastPeers)
	bindingsChanged := !maps.Equal(bindings, d.lastBindings)
	if peersChanged {
		d.lastPeers = peers
	}
	if bindingsChanged {
		d.lastBindings = bindings
	}
	d.mu.Unlock()

	if (peersChanged || bindingsChanged) && d.cfg.OnPeers != nil {
		d.cfg.OnPeers(peers)
	}
	return nil
}

// Bindings returns a snapshot of currently-known peer bindings:
// addr (gossip listen address) → Origin. Self and stale entries are
// excluded.
func (d *Discoverer) Bindings() map[string]crdt.Origin {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bindingsLocked()
}

// bindingsLocked is the lock-held core of Bindings; the change
// detector in discoverOnce reuses it under its own lock acquisition.
//
// When two origins claim the same listen address (the unclean-restart
// rotation window), the most-recently-observed heartbeat wins. The
// tiebreak is on Discoverer.seenSeq rather than LastModified so the
// result is deterministic even when the backend reports equal or zero
// timestamps (FS and S3 quantize at second-or-coarser granularity).
func (d *Discoverer) bindingsLocked() map[string]crdt.Origin {
	type winner struct {
		origin crdt.Origin
		seq    uint64
	}
	winners := make(map[string]winner, len(d.cache))
	selfHex := layout.OriginHex(d.cfg.Origin)
	stale := time.Duration(DefaultStaleMultiplier) * d.cfg.Interval
	cutoff := d.cfg.Now().Add(-stale)
	for _, e := range d.cache {
		if e.hb.Listen == "" || e.hb.Origin == "" || e.hb.Origin == selfHex {
			continue
		}
		if !e.lastModified.IsZero() && e.lastModified.Before(cutoff) {
			continue
		}
		raw, err := strconv.ParseUint(e.hb.Origin, 16, 64)
		if err != nil {
			continue
		}
		if prev, ok := winners[e.hb.Listen]; ok && e.seenSeq <= prev.seq {
			continue
		}
		winners[e.hb.Listen] = winner{origin: crdt.Origin(raw), seq: e.seenSeq}
	}
	out := make(map[string]crdt.Origin, len(winners))
	for addr, w := range winners {
		out[addr] = w.origin
	}
	return out
}

// heartbeatFor returns the parsed Heartbeat for o, reusing the cached
// body when ETag matches. Backends that don't populate ETag will GET
// every tick (correctness preserved, just no caching benefit).
//
// The cache records o.LastModified so bindingsLocked can age out
// stale heartbeats whose objects linger in the bucket after the peer
// shut down. seenSeq stamps each fresh body fetch so the rotation
// tiebreak in bindingsLocked picks the newer origin; the ETag-hit
// path skips the write when LastModified hasn't moved, keeping the
// steady-state fast path lock-free.
func (d *Discoverer) heartbeatFor(ctx context.Context, o objectstore.ObjectInfo) (Heartbeat, error) {
	d.mu.Lock()
	cached, ok := d.cache[o.Key]
	d.mu.Unlock()
	if ok && o.ETag != "" && cached.etag == o.ETag {
		if o.LastModified.IsZero() || o.LastModified.Equal(cached.lastModified) {
			return cached.hb, nil
		}
		d.mu.Lock()
		cached.lastModified = o.LastModified
		d.cache[o.Key] = cached
		d.mu.Unlock()
		return cached.hb, nil
	}
	rc, etag, err := d.cfg.Backend.Get(ctx, o.Key)
	if err != nil {
		return Heartbeat{}, err
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, 4096))
	if err != nil {
		return Heartbeat{}, err
	}
	var hb Heartbeat
	if err := json.Unmarshal(body, &hb); err != nil {
		return Heartbeat{}, err
	}
	d.mu.Lock()
	d.seenSeq++
	d.cache[o.Key] = cachedEntry{
		etag:         etag,
		hb:           hb,
		lastModified: o.LastModified,
		seenSeq:      d.seenSeq,
	}
	d.mu.Unlock()
	return hb, nil
}
