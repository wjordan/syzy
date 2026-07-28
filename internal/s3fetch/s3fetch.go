// Package s3fetch implements transport.GapFiller and transport.TipSource
// over a objectstore.Bucket. The broker wires Source as cfg.GapFiller +
// cfg.TipSource so a returning-from-offline node catches up by listing
// per-origin epoch objects, locating the frame(s) covering each
// requested seq range, ranged-GETing the compressed bytes, and
// replaying records through the broker's apply path.
package s3fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/epoch"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/transport"
)

// Source is a Fetch-only data source backed by an object-storage
// bucket. It maintains a small in-memory cache of per-origin epoch
// listings to amortize LIST cost across consecutive Fetches.
type Source struct {
	backend objectstore.Bucket

	// cacheTTL bounds the staleness of per-origin epoch listings. In
	// steady state these entries are seeded in bulk by DiscoverTips from
	// its single origins/ prefix LIST (see below), so Fetch reuses that
	// grouped result instead of issuing a per-origin List; the default is
	// therefore aligned with tipsTTL so a seeded entry survives until the
	// next DiscoverTips refresh. Default 5m; 0 disables caching (each
	// Fetch issues a fresh per-origin List).
	cacheTTL time.Duration

	mu    sync.Mutex
	cache map[string]*epochList // originHex → cached epochs
	// bucketSnapAt is when the last full DiscoverTips prefix walk completed.
	// While it is fresh (within cacheTTL), an origin absent from cache was
	// absent from that complete listing, so epochListFor can answer "empty"
	// without a per-origin List — the walk is authoritative for absence just
	// as its seeded entries are for presence.
	bucketSnapAt time.Time

	// tipsTTL bounds reuse of a DiscoverTips result before a fresh full
	// LIST of origins/. The broker calls DiscoverTips on every fetch
	// round; origins/ can hold thousands of epoch objects, so listing it
	// each round dominates steady-state bucket egress. Tips advance
	// slowly and known origins are tracked live by the broker cache —
	// DiscoverTips only needs to eventually surface origins the live path
	// has not seen — so a longer reuse window cuts the LIST rate with no
	// correctness cost. Default 5m; 0 disables caching.
	tipsTTL time.Duration
	tipsMu  sync.Mutex
	tips    map[crdt.Origin]crdt.Seq
	// coverage is the per-origin merged [lo,hi] intervals across surviving
	// epochs, from the same walk that produced tips (so the two are
	// snapshot-consistent).
	coverage map[crdt.Origin][]transport.Range
	tipsAt   time.Time

	// dec is shared across Fetch calls; zstd Decoders are safe for
	// reuse but not concurrent across goroutines.
	decMu sync.Mutex
	dec   *zstd.Decoder
}

// epochList is one origin's discovered epoch keys, sorted ascending.
type epochList struct {
	at      time.Time
	entries []epochEntry
}

// epochEntry is one parsed epoch key.
type epochEntry struct {
	key    string
	loSeq  crdt.Seq
	hiSeq  crdt.Seq
	size   int64
	footer *epoch.Footer // populated lazily on first read
}

// NewSource constructs a Source over backend.
func NewSource(backend objectstore.Bucket) *Source {
	return &Source{
		backend:  backend,
		cacheTTL: 5 * time.Minute,
		tipsTTL:  5 * time.Minute,
		cache:    map[string]*epochList{},
	}
}

// SetCacheTTL overrides the per-origin listing cache duration. Pass 0
// to disable caching (each Fetch issues a fresh LIST).
func (s *Source) SetCacheTTL(d time.Duration) { s.cacheTTL = d }

// SetTipsTTL overrides how long DiscoverTips reuses a listing before
// re-LISTing origins/. Pass 0 to disable (every call lists).
func (s *Source) SetTipsTTL(d time.Duration) { s.tipsTTL = d }

// DiscoverTips returns the highest hiSeq found per origin under
// origins/. Used by the broker's fetcher to seed gap-fill ranges for
// origins the local cache has never observed live (e.g. peers that
// sealed writes to objects/ while we were offline).
//
// Implementation: one LIST against origins/, one parse per epoch key.
// No object body reads — the encoded hiSeq in the key is authoritative
// up to whatever frame compaction did, and frame-level boundaries are
// recovered later by Fetch when those origins' missing ranges are
// requested.
//
// The same page walk also seeds the per-origin epoch cache (s.cache):
// every epoch key it parses for the tip is exactly what Fetch's
// epochListFor would otherwise re-LIST per origin. Grouping them here
// turns the gap-fetch path's N-per-origin LISTs into zero — Fetch reuses
// this grouped snapshot — which is the dominant origins/ request cost
// (every node gap-fills every inherited origin). Seeding from the tip
// snapshot is also strictly consistent: a range is only requested up to
// the tip discovered here, and the epoch covering that tip is in the same
// listing.
func (s *Source) DiscoverTips(ctx context.Context) (map[crdt.Origin]crdt.Seq, error) {
	if s.tipsTTL > 0 {
		s.tipsMu.Lock()
		if s.tips != nil && time.Since(s.tipsAt) < s.tipsTTL {
			out := maps.Clone(s.tips)
			s.tipsMu.Unlock()
			return out, nil
		}
		s.tipsMu.Unlock()
	}
	// Paginate the full origins/ prefix. objectstore.List returns one page
	// (~1000 keys) per call, so a single unpaginated List silently
	// truncates on a large cluster — which would drop origins from the tip
	// map, both starving the fetcher of catch-up targets and (worse) making
	// origin GC read a live origin as "swept" and evict it. Mirror the
	// pagination every other objstore lister uses (e.g. listLTXFrom).
	tips := map[crdt.Origin]crdt.Seq{}
	coverage := map[crdt.Origin][]transport.Range{}
	grouped := map[string][]epochEntry{} // originHex → epochs, for cache seeding
	startAfter := ""
	for {
		objs, err := s.backend.List(ctx, objstore.OriginsPrefix, startAfter)
		if err != nil {
			return nil, fmt.Errorf("s3fetch: list origins: %w", err)
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			hex, lo64, hi64, ok := objstore.ParseOriginEpochKey(o.Key)
			if !ok {
				continue
			}
			v, err := strconv.ParseUint(hex, 16, 64)
			if err != nil {
				continue
			}
			origin := crdt.Origin(v)
			lo, hi := crdt.Seq(lo64), crdt.Seq(hi64)
			if hi > tips[origin] {
				tips[origin] = hi
			}
			coverage[origin] = append(coverage[origin], transport.Range{Origin: origin, Lo: lo, Hi: hi})
			grouped[hex] = append(grouped[hex], epochEntry{
				key: o.Key, loSeq: lo, hiSeq: hi, size: o.Size,
			})
		}
		if len(objs) < 1000 {
			break
		}
		startAfter = objs[len(objs)-1].Key
	}
	for o := range coverage {
		coverage[o] = transport.MergeIntervals(coverage[o])
	}
	now := time.Now()
	s.seedEpochCache(grouped, now)
	s.tipsMu.Lock()
	s.coverage = coverage
	if s.tipsTTL > 0 {
		s.tips = tips
		s.tipsAt = now
	}
	s.tipsMu.Unlock()
	return maps.Clone(tips), nil
}

// Coverage implements transport.CoverageSource: per origin, the merged
// [lo,hi] intervals across surviving epochs, from the same complete
// origins/ walk that produced the DiscoverTips snapshot (so absence is
// authoritative). The planner demotes missing ranges that intersect no
// interval to a slow-cadence probe instead of re-fetching them every
// round.
func (s *Source) Coverage(ctx context.Context) (map[crdt.Origin][]transport.Range, error) {
	// DiscoverTips owns the walk: it reuses a fresh snapshot (which already
	// populated coverage) or re-lists completely, erroring on a partial
	// listing.
	if _, err := s.DiscoverTips(ctx); err != nil {
		return nil, err
	}
	s.tipsMu.Lock()
	defer s.tipsMu.Unlock()
	return maps.Clone(s.coverage), nil
}

// Fetch satisfies transport.Transport.Fetch from the bucket. For each
// requested Range, the source enumerates the origin's epochs, locates
// the frames covering the seq range, and emits each frame's records
// through apply.
func (s *Source) Fetch(ctx context.Context, ranges []transport.Range, apply transport.ApplyFunc) error {
	for _, rg := range ranges {
		if err := s.fetchOne(ctx, rg, apply); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) fetchOne(ctx context.Context, rg transport.Range, apply transport.ApplyFunc) error {
	originHex := layout.OriginHex(rg.Origin)
	list, err := s.epochListFor(ctx, originHex)
	if err != nil {
		return fmt.Errorf("s3fetch: list origin %s: %w", originHex, err)
	}
	hi := rg.Hi
	openEnded := rg.OpenEnded()
	for i := range list.entries {
		ent := &list.entries[i]
		if ent.hiSeq < rg.Lo {
			continue
		}
		if !openEnded && ent.loSeq > hi {
			break
		}
		footer, err := s.footerFor(ctx, ent)
		if err != nil {
			return fmt.Errorf("s3fetch: footer %s: %w", ent.key, err)
		}
		// For each frame whose [LoSeq, HiSeq] overlaps the range,
		// fetch + decode + apply.
		var frames []epoch.FrameIndex
		if openEnded {
			frames = footer.FramesOverlapping(uint64(rg.Lo), ^uint64(0))
		} else {
			frames = footer.FramesOverlapping(uint64(rg.Lo), uint64(hi))
		}
		for _, fr := range frames {
			compressed, err := s.fetchBytes(ctx, ent.key, fr.Offset, fr.CompressedSize)
			if err != nil {
				return fmt.Errorf("s3fetch: read frame %s [%d,%d]: %w", ent.key, fr.Offset, fr.CompressedSize, err)
			}
			records, err := s.decodeFrame(compressed)
			if err != nil {
				return fmt.Errorf("s3fetch: decode frame %s: %w", ent.key, err)
			}
			for _, r := range records {
				if !rg.Contains(crdt.Seq(r.Seq)) {
					continue
				}
				if err := apply(ctx, r.Bytes); err != nil {
					return fmt.Errorf("s3fetch: apply %d: %w", r.Seq, err)
				}
			}
		}
	}
	return nil
}

func (s *Source) decodeFrame(compressed []byte) ([]epoch.Record, error) {
	s.decMu.Lock()
	defer s.decMu.Unlock()
	if s.dec == nil {
		d, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		s.dec = d
	}
	return epoch.DecodeFrame(compressed, s.dec)
}

func (s *Source) fetchBytes(ctx context.Context, key string, off, length int64) ([]byte, error) {
	rc, err := s.backend.GetRange(ctx, key, off, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := make([]byte, length)
	n, err := io.ReadFull(rc, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return buf[:n], nil
}

// footerFor returns the parsed footer for an epoch, fetching and
// caching it on first access. Concurrent callers may race on the same
// entry; s.mu protects the read+write window. We may double-fetch on
// races but never store inconsistent state.
func (s *Source) footerFor(ctx context.Context, ent *epochEntry) (*epoch.Footer, error) {
	s.mu.Lock()
	if ent.footer != nil {
		f := ent.footer
		s.mu.Unlock()
		return f, nil
	}
	s.mu.Unlock()

	footer, err := epoch.ReadFooter(ent.size, func(off, length int64) ([]byte, error) {
		return s.fetchBytes(ctx, ent.key, off, length)
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	ent.footer = footer
	s.mu.Unlock()
	return footer, nil
}

// seedEpochCache installs the per-origin epoch listings gathered by a
// full DiscoverTips prefix walk, so the subsequent gap-fetch reuses them
// instead of issuing a List per origin. Entries are stamped now and sorted
// ascending by loSeq to match epochListFor's contract (fetchOne relies on
// ascending order to stop scanning). Replaces any prior entry: the walk is
// authoritative and complete for the origins it observed.
func (s *Source) seedEpochCache(grouped map[string][]epochEntry, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Record the snapshot time unconditionally — an empty walk (no origins in
	// the bucket) is still an authoritative "everything absent" snapshot.
	s.bucketSnapAt = now
	for hex, entries := range grouped {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].loSeq < entries[j].loSeq
		})
		s.cache[hex] = &epochList{at: now, entries: entries}
	}
}

// epochListFor returns a cached or freshly-listed set of epoch entries
// for one origin.
func (s *Source) epochListFor(ctx context.Context, originHex string) (*epochList, error) {
	now := time.Now()
	s.mu.Lock()
	if list, ok := s.cache[originHex]; ok {
		if s.cacheTTL > 0 && now.Sub(list.at) < s.cacheTTL {
			s.mu.Unlock()
			return list, nil
		}
	}
	// A fresh full-prefix DiscoverTips snapshot is authoritative for absence:
	// an origin it did not surface holds no bucket epochs, so a per-origin List
	// would only confirm empty. Answer empty (and cache it) without the List,
	// bounded by the same cacheTTL window as the seeded present-origin entries.
	// This removes the steady-state per-origin List on churned origins the
	// broker still tracks (via senderNextSeq) but that have no bucket epochs.
	if s.cacheTTL > 0 && !s.bucketSnapAt.IsZero() && now.Sub(s.bucketSnapAt) < s.cacheTTL {
		empty := &epochList{at: s.bucketSnapAt}
		s.cache[originHex] = empty
		s.mu.Unlock()
		return empty, nil
	}
	s.mu.Unlock()

	prefix := objstore.OriginPrefixOf(originHex)
	objs, err := s.backend.List(ctx, prefix, "")
	if err != nil {
		return nil, err
	}
	entries := make([]epochEntry, 0, len(objs))
	for _, o := range objs {
		_, lo, hi, ok := objstore.ParseOriginEpochKey(o.Key)
		if !ok {
			continue
		}
		entries = append(entries, epochEntry{
			key:   o.Key,
			loSeq: crdt.Seq(lo),
			hiSeq: crdt.Seq(hi),
			size:  o.Size,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].loSeq < entries[j].loSeq
	})
	list := &epochList{at: now, entries: entries}
	s.mu.Lock()
	s.cache[originHex] = list
	s.mu.Unlock()
	return list, nil
}

// Compile-time assertions that Source satisfies the broker's
// optional contracts.
var (
	_ transport.GapFiller      = (*Source)(nil)
	_ transport.TipSource      = (*Source)(nil)
	_ transport.CoverageSource = (*Source)(nil)
)
