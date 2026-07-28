package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
)

func MaterializeStream(ctx context.Context, be objectstore.Bucket, prefix string, baseline *objstore.Baseline, dst string, capTXID uint64) error {
	t0 := time.Now()
	geo, err := restoreBaseline(ctx, be, baseline.LTXRef, dst)
	if err != nil {
		return err
	}
	baselineDur := time.Since(t0)
	t1 := time.Now()
	tip, err := chainTip(ctx, be, prefix, baseline.TXID)
	if err != nil {
		return err
	}
	// Couple to metadata's parent_app_txid when given: never materialize past
	// the TXID the row_clock state is consistent with, so the restored pair is a
	// coherent snapshot. capTXID==0 (meta streams, legacy/unstamped buckets)
	// restores to the stream tip. Clamp to the baseline TXID: a cap below the
	// baseline means the baseline already encodes a later point, so apply nothing
	// above it (the inverse decoupling — app state ahead of the meta pin — is not
	// an orphan and needs no trimming).
	through := tip
	if capTXID > 0 && capTXID < through {
		through = capTXID
	}
	if through < baseline.TXID {
		through = baseline.TXID
	}
	listDur := time.Since(t1)
	t2 := time.Now()
	files, chainBytes, chainGeo, err := applyLTXChain(ctx, be, prefix, baseline.TXID, through, dst)
	if err != nil {
		return fmt.Errorf("apply L0/L1 chain: %w", err)
	}
	if chainGeo.commit != 0 {
		geo = chainGeo
	}
	// The last applied frame's commit is the database's page count; a
	// shorter file means some frame that grew the database never made it
	// into the stream (e.g. a publisher restart stranding checkpointed-
	// but-unshipped txns), and the materialized image is malformed.
	// Refusing here keeps the bad image out of clone.Adopt.
	if err := verifyMaterializedSize(dst, geo); err != nil {
		return fmt.Errorf("%s stream: %w", prefix, err)
	}
	// Per-stream restore cost. The chain_files count is the number of L0/L1 frames
	// applied above the baseline; a large value means the baseline is stale and the
	// restore is replaying a long delta chain (one GET per frame), the dominant
	// cold-restore latency. baseline_txid..through is the TXID span bridged (tip is
	// the stream's own tip, which may exceed through under a parent_app_txid cap).
	slog.Info("syzy restore: stream materialized",
		"prefix", prefix, "baseline_txid", baseline.TXID, "tip", tip, "through", through,
		"baseline_bytes", baseline.LTXRef.Size, "baseline_dur", baselineDur.Round(time.Millisecond),
		"chain_list_dur", listDur.Round(time.Millisecond),
		"chain_files", files, "chain_bytes", chainBytes, "chain_apply_dur", time.Since(t2).Round(time.Millisecond))
	return nil
}

// chainTip returns max(L0 tip, L1 tip, baselineTXID) for one stream.
func chainTip(ctx context.Context, be objectstore.Bucket, prefix string, baselineTXID uint64) (uint64, error) {
	l0, err := objstore.MaxLTXTXID(ctx, be, prefix, objstore.L0Level)
	if err != nil {
		return 0, fmt.Errorf("max L0 TXID: %w", err)
	}
	l1, err := objstore.MaxLTXTXID(ctx, be, prefix, objstore.L1Level)
	if err != nil {
		return 0, fmt.Errorf("max L1 TXID: %w", err)
	}
	tip := l0
	if l1 > tip {
		tip = l1
	}
	if tip < baselineTXID {
		tip = baselineTXID
	}
	return tip, nil
}

// restoreBaseline materializes the baseline LTX into dstPath: one
// download — multipart-ranged for large objects, so a single
// path-pinned connection cannot cap a multi-GB restore — staged next
// to dstPath, verified, then decoded from local disk.
//
// The verify-before-decode matters: under a multi-region double-claim
// two publishers can write the same baseline key, leaving HEAD's
// pointer on one node's metadata and the object on another's bytes.
// Decoding a foreign baseline silently yields a corrupt database;
// failing the hash here names the problem instead.
func restoreBaseline(ctx context.Context, be objectstore.Bucket, ref objstore.FileRef, dstPath string) (ltxGeometry, error) {
	tmpPath := dstPath + ".baseline.ltx"
	defer os.Remove(tmpPath)
	if err := FetchVerifiedRef(ctx, be, ref, tmpPath); err != nil {
		return ltxGeometry{}, fmt.Errorf("baseline %s: %w", ref.Key, err)
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return ltxGeometry{}, err
	}
	defer f.Close()
	geo, err := decodeLTXReaderToFile(ctx, f, ref.Key, dstPath, true)
	if err != nil {
		return ltxGeometry{}, fmt.Errorf("apply baseline: %w", err)
	}
	return geo, nil
}

// FetchVerifiedRef downloads the object at ref.Key into dstPath and
// checks its size and sha256 against the FileRef recorded in HEAD
// (hash check skipped when ref carries no hash — older HEADs).
func FetchVerifiedRef(ctx context.Context, be objectstore.Bucket, ref objstore.FileRef, dstPath string) error {
	f, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := objectstore.FetchRangedAt(ctx, be, ref.Key, f, objectstore.FetchOpts{}); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	if ref.Sha256 == "" {
		return nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return fmt.Errorf("read for verify: %w", err)
	}
	if ref.Size != 0 && n != ref.Size {
		return fmt.Errorf("size mismatch: object %d bytes, HEAD recorded %d", n, ref.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != ref.Sha256 {
		return fmt.Errorf("sha256 mismatch: object %s, HEAD recorded %s (foreign bytes from a concurrent publisher?)", got, ref.Sha256)
	}
	return nil
}

// ltxGeometry is a decoded frame's database geometry: the committed
// page count and page size from its LTX header. The last frame applied
// to a stream fixes the materialized file's expected size.
type ltxGeometry struct {
	commit   uint32
	pageSize int64
}

// decodeLTXReaderToFile decodes an LTX read from r and writes/applies its pages
// to dstPath, returning the frame's geometry. Split from decodeLTXToFile so the
// chain restore can prefetch frame bytes concurrently (the network fetch) while
// applying them in strict TXID order (the decode). key is used only for error
// context.
func decodeLTXReaderToFile(ctx context.Context, r io.Reader, key, dstPath string, fresh bool) (ltxGeometry, error) {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return ltxGeometry{}, fmt.Errorf("decode header %s: %w", key, err)
	}
	hdr := dec.Header()
	pageSize := int64(hdr.PageSize)
	geo := ltxGeometry{commit: hdr.Commit, pageSize: pageSize}

	flag := os.O_RDWR | os.O_CREATE
	if fresh {
		flag |= os.O_TRUNC
	}
	dst, err := os.OpenFile(dstPath, flag, 0o644)
	if err != nil {
		return ltxGeometry{}, err
	}
	defer dst.Close()

	page := make([]byte, pageSize)
	var pageHdr ltx.PageHeader
	for {
		select {
		case <-ctx.Done():
			return ltxGeometry{}, ctx.Err()
		default:
		}
		if err := dec.DecodePage(&pageHdr, page); err != nil {
			if err == io.EOF {
				break
			}
			return ltxGeometry{}, fmt.Errorf("decode page in %s: %w", key, err)
		}
		off := int64(pageHdr.Pgno-1) * pageSize
		if _, err := dst.WriteAt(page, off); err != nil {
			return ltxGeometry{}, fmt.Errorf("write page %d: %w", pageHdr.Pgno, err)
		}
	}
	if fresh {
		// Truncate file to commit-many pages so trailing bytes from
		// the LTX format don't leak into the file size.
		if err := dst.Truncate(int64(hdr.Commit) * pageSize); err != nil {
			return ltxGeometry{}, fmt.Errorf("truncate dst: %w", err)
		}
	}
	if err := dec.Close(); err != nil {
		return ltxGeometry{}, fmt.Errorf("decoder close: %w", err)
	}
	return geo, dst.Sync()
}

// verifyMaterializedSize checks a materialized database file against the
// final applied frame's geometry. A file shorter than commit pages means
// the chain never delivered the page writes that grew the database — a
// content hole; the image is malformed (SQLite's header claims pages the
// file lacks) and must not be adopted. A longer file (the chain shrank
// the database; non-fresh applies never truncate) is trimmed to commit.
func verifyMaterializedSize(path string, geo ltxGeometry) error {
	if geo.commit == 0 || geo.pageSize == 0 {
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	want := int64(geo.commit) * geo.pageSize
	switch {
	case fi.Size() < want:
		return fmt.Errorf("materialized %d bytes but the last applied frame committed %d pages of %d bytes (%d): the LTX chain is missing the page writes that grew the database",
			fi.Size(), geo.commit, geo.pageSize, want)
	case fi.Size() > want:
		return os.Truncate(path, want)
	}
	return nil
}

// applyLTXChain lists L0 and L1 LTX files under streamPrefix, picks
// those whose TXID range falls inside (after, through], and applies
// them in ascending MinTXID order to dstPath. Returns the last applied
// frame's geometry (zero when the chain is empty).
func applyLTXChain(ctx context.Context, be objectstore.Bucket, streamPrefix string, after, through uint64, dstPath string) (int, int64, ltxGeometry, error) {
	l0, err := objstore.ListLTX(ctx, be, streamPrefix, objstore.L0Level)
	if err != nil {
		return 0, 0, ltxGeometry{}, err
	}
	l1, err := objstore.ListLTX(ctx, be, streamPrefix, objstore.L1Level)
	if err != nil {
		return 0, 0, ltxGeometry{}, err
	}
	entries := SelectLTXChain(l0, l1, after, through)
	bytes, geo, err := applyLTXEntries(ctx, be, entries, dstPath)
	return len(entries), bytes, geo, err
}

// restorePrefetch bounds how many LTX frames are downloaded concurrently while
// the apply loop consumes them in order. Restore wall time is dominated by
// sequential object-store round-trips (one GET per frame) to a possibly-distant
// bucket — a chain of N small frames costs N*RTT applied naively. Overlapping
// the downloads collapses that to ~ceil(N/restorePrefetch)*RTT; the apply stays
// strictly ordered (frames are page deltas that must land in TXID order).
const restorePrefetch = 16

// applyLTXEntries applies an ordered LTX chain to dstPath, prefetching each
// frame's bytes concurrently (bounded by restorePrefetch) ahead of the in-order
// apply. Frame bytes are small (page deltas); the bounded window keeps the
// download buffer to ~restorePrefetch frames regardless of chain length.
// Returns the last applied frame's geometry (zero when entries is empty).
func applyLTXEntries(ctx context.Context, be objectstore.Bucket, entries []objstore.LTXFile, dstPath string) (int64, ltxGeometry, error) {
	var total int64 // applies run serially in PrefetchOrdered, so no lock needed
	var last ltxGeometry
	err := PrefetchOrdered(ctx, len(entries), restorePrefetch,
		func(ctx context.Context, i int) ([]byte, error) {
			data, err := fetchObject(ctx, be, entries[i].Key)
			if err != nil {
				return nil, fmt.Errorf("download %s: %w", entries[i].Key, err)
			}
			return data, nil
		},
		func(i int, data []byte) error {
			total += int64(len(data))
			geo, err := decodeLTXReaderToFile(ctx, bytes.NewReader(data), entries[i].Key, dstPath, false)
			if err == nil {
				last = geo
			}
			return err
		},
	)
	return total, last, err
}

// restoreBound* bound every object-store request the restore makes, so a single
// hung connection can't stall a cold open. A restore walks many bucket calls
// (HEAD, list, chain tip, baseline, frames); a deadline-less Get or List can
// block indefinitely (not only on connect but on a body stream that stalls
// mid-read), and the ordered apply / prefetch then waits forever, so the node
// never finishes opening (observed: an ap-southeast-2 node wedged 7+ minutes,
// first in applyLTXEntries, then in chainTip). NewBoundedBucket wraps the bucket
// once at the restore entry so every call is bounded + retried in one place.
//
// vars (not consts) so tests can tighten them; production values are fixed.
var (
	restoreBoundAttempts = 6
	restoreBoundTimeout  = 30 * time.Second
	restoreBoundBackoff  = 250 * time.Millisecond
)

// boundedBucket wraps a Bucket so each Get/List runs under a per-call timeout
// with retry. Get buffers the object fully within that timeout (restore objects
// are small: frames are page deltas, the baseline is one DB snapshot), which is
// what lets the deadline also cover the body read, so a stalled stream fails the
// attempt instead of hanging the caller. A missing object or a cancelled parent
// ctx is terminal; other failures retry with backoff.
type boundedBucket struct {
	objectstore.Bucket
	attempts int
	timeout  time.Duration
	backoff  time.Duration
}

func NewBoundedBucket(b objectstore.Bucket) *boundedBucket {
	return &boundedBucket{Bucket: b, attempts: restoreBoundAttempts, timeout: restoreBoundTimeout, backoff: restoreBoundBackoff}
}

func (b *boundedBucket) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	var lastErr error
	for attempt := 0; attempt < b.attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		data, etag, err := b.getOnce(ctx, key)
		if err == nil {
			return io.NopCloser(bytes.NewReader(data)), etag, nil
		}
		lastErr = err
		if errors.Is(err, objectstore.ErrNotFound) || ctx.Err() != nil {
			return nil, "", err
		}
		if !sleepCtx(ctx, b.backoff) {
			return nil, "", ctx.Err()
		}
	}
	return nil, "", fmt.Errorf("bucket get %s after %d attempts: %w", key, b.attempts, lastErr)
}

// GetRange buffers the range fully under the per-call timeout, like
// Get. The baseline's multipart fetch bounds each range to one part
// (16MB), so buffering stays modest while the deadline covers the
// body read.
func (b *boundedBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	var lastErr error
	for attempt := 0; attempt < b.attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		data, err := hardTimeout(ctx, b.timeout, func(c context.Context) ([]byte, error) {
			rc, gerr := b.Bucket.GetRange(c, key, off, length)
			if gerr != nil {
				return nil, gerr
			}
			defer rc.Close()
			return io.ReadAll(rc)
		})
		if err == nil {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
		lastErr = err
		if errors.Is(err, objectstore.ErrNotFound) || ctx.Err() != nil {
			return nil, err
		}
		if !sleepCtx(ctx, b.backoff) {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("bucket get range %s after %d attempts: %w", key, b.attempts, lastErr)
}

func (b *boundedBucket) Stat(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	var lastErr error
	for attempt := 0; attempt < b.attempts; attempt++ {
		if ctx.Err() != nil {
			return objectstore.ObjectInfo{}, ctx.Err()
		}
		info, err := hardTimeout(ctx, b.timeout, func(c context.Context) (objectstore.ObjectInfo, error) {
			return b.Bucket.Stat(c, key)
		})
		if err == nil {
			return info, nil
		}
		lastErr = err
		if errors.Is(err, objectstore.ErrNotFound) || ctx.Err() != nil {
			return objectstore.ObjectInfo{}, err
		}
		if !sleepCtx(ctx, b.backoff) {
			return objectstore.ObjectInfo{}, ctx.Err()
		}
	}
	return objectstore.ObjectInfo{}, fmt.Errorf("bucket stat %s after %d attempts: %w", key, b.attempts, lastErr)
}

type getResult struct {
	data []byte
	etag string
}

func (b *boundedBucket) getOnce(ctx context.Context, key string) ([]byte, string, error) {
	r, err := hardTimeout(ctx, b.timeout, func(c context.Context) (getResult, error) {
		rc, etag, gerr := b.Bucket.Get(c, key)
		if gerr != nil {
			return getResult{}, gerr
		}
		defer rc.Close()
		data, rerr := io.ReadAll(rc)
		if rerr != nil {
			return getResult{}, rerr
		}
		return getResult{data: data, etag: etag}, nil
	})
	return r.data, r.etag, err
}

// hardTimeout runs op under a per-call deadline and returns op's result, or the
// deadline error if op has not returned in time. A ctx deadline alone is not
// enough: a black-holed object-store connection can leave the underlying call
// parked in a syscall the client never unblocks on cancellation (exactly what
// wedged the restore). The worker is then abandoned — it drains into a buffered
// channel and finishes (closing its body) once the client finally notices the
// cancelled ctx or the OS reaps the dead socket.
func hardTimeout[T any](ctx context.Context, d time.Duration, op func(context.Context) (T, error)) (T, error) {
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	type res struct {
		v   T
		err error
	}
	ch := make(chan res, 1)
	go func() { v, err := op(cctx); ch <- res{v: v, err: err} }()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-cctx.Done():
		var zero T
		return zero, cctx.Err()
	}
}

func (b *boundedBucket) List(ctx context.Context, prefix, startAfter string) ([]objectstore.ObjectInfo, error) {
	var lastErr error
	for attempt := 0; attempt < b.attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		out, err := hardTimeout(ctx, b.timeout, func(c context.Context) ([]objectstore.ObjectInfo, error) {
			return b.Bucket.List(c, prefix, startAfter)
		})
		if err == nil {
			return out, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, err
		}
		if !sleepCtx(ctx, b.backoff) {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("bucket list %s after %d attempts: %w", prefix, b.attempts, lastErr)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// fetchObject reads the whole object at key into memory. LTX frames are small
// page deltas, so buffering one (and at most restorePrefetch concurrently) is
// cheap; holding the bytes lets the network fetch run ahead of the ordered
// apply. The per-call timeout + retry live in the boundedBucket the restore
// entry wraps, so this stays a plain read.
func fetchObject(ctx context.Context, be objectstore.Bucket, key string) ([]byte, error) {
	rc, _, err := be.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// PrefetchOrdered runs fetch(i) for i in [0,n) concurrently (at most window in
// flight) and calls apply(i, value) for each result in ascending index order.
// The fetches overlap — they are the slow, latency-bound step — while the
// applies are serial and strictly ordered: apply(i) never runs before apply(i-1)
// returns. The first fetch- or apply-error aborts the run, cancels outstanding
// fetches, and is returned. A nil fetch error with the zero value is fine.
func PrefetchOrdered[T any](
	ctx context.Context,
	n, window int,
	fetch func(ctx context.Context, i int) (T, error),
	apply func(i int, v T) error,
) error {
	if n == 0 {
		return nil
	}
	if window < 1 {
		window = 1
	}
	type result struct {
		v   T
		err error
	}
	// One single-slot channel per index: each is written exactly once (by a fetch
	// goroutine, or by the launcher if the run is cancelled before that index is
	// launched) and read exactly once by the ordered apply loop, so no send ever
	// blocks and the buffered bytes are bounded by the in-flight window, not n.
	slots := make([]chan result, n)
	for i := range slots {
		slots[i] = make(chan result, 1)
	}
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, window)
	go func() {
		for i := 0; i < n; i++ {
			select {
			case sem <- struct{}{}:
			case <-fetchCtx.Done():
				slots[i] <- result{err: fetchCtx.Err()}
				continue
			}
			go func(i int) {
				defer func() { <-sem }()
				v, err := fetch(fetchCtx, i)
				slots[i] <- result{v: v, err: err}
			}(i)
		}
	}()
	for i := 0; i < n; i++ {
		var r result
		select {
		case r = <-slots[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.err != nil {
			return r.err
		}
		if err := apply(i, r.v); err != nil {
			return err
		}
	}
	return nil
}

func SelectLTXChain(l0, l1 []objstore.LTXFile, after, through uint64) []objstore.LTXFile {
	l1Entries := filterLTXRange(l1, after, through)
	sortLTXChain(l1Entries)
	l1Coverage := newRestoreLTXCoverage(l1Entries)

	entries := append([]objstore.LTXFile{}, l1Entries...)
	for _, f := range filterLTXRange(l0, after, through) {
		if l1Coverage.Covers(f.MinTXID, f.MaxTXID) {
			continue
		}
		entries = append(entries, f)
	}
	sortLTXChain(entries)
	return entries
}

func filterLTXRange(files []objstore.LTXFile, after, through uint64) []objstore.LTXFile {
	out := make([]objstore.LTXFile, 0, len(files))
	for _, f := range files {
		if f.MaxTXID <= after || f.MinTXID > through {
			continue
		}
		out = append(out, f)
	}
	return out
}

func sortLTXChain(files []objstore.LTXFile) {
	sort.Slice(files, func(i, j int) bool {
		if files[i].MinTXID != files[j].MinTXID {
			return files[i].MinTXID < files[j].MinTXID
		}
		return files[i].MaxTXID < files[j].MaxTXID
	})
}

type restoreTXRange struct {
	min uint64
	max uint64
}

type restoreLTXCoverage struct {
	ranges []restoreTXRange
}

func newRestoreLTXCoverage(files []objstore.LTXFile) restoreLTXCoverage {
	ranges := make([]restoreTXRange, 0, len(files))
	for _, f := range files {
		if len(ranges) == 0 {
			ranges = append(ranges, restoreTXRange{min: f.MinTXID, max: f.MaxTXID})
			continue
		}
		last := &ranges[len(ranges)-1]
		adjacent := last.max != ^uint64(0) && f.MinTXID == last.max+1
		if f.MinTXID <= last.max || adjacent {
			if f.MaxTXID > last.max {
				last.max = f.MaxTXID
			}
			continue
		}
		ranges = append(ranges, restoreTXRange{min: f.MinTXID, max: f.MaxTXID})
	}
	return restoreLTXCoverage{ranges: ranges}
}

func (c restoreLTXCoverage) Covers(minTX, maxTX uint64) bool {
	i := sort.Search(len(c.ranges), func(i int) bool {
		return c.ranges[i].max >= minTX
	})
	return i < len(c.ranges) && c.ranges[i].min <= minTX && maxTX <= c.ranges[i].max
}
