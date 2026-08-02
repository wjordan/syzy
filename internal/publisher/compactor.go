package publisher

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/superfly/ltx"
	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/internal/objstore"
	"github.com/wjordan/syzy/internal/syzylog"
)

const (
	defaultCompactorMinFiles       = 60
	defaultCompactorMaxInputFiles  = 64
	defaultCompactorMaxRunsPerPass = 8
)

// Compactor merges adjacent L0 LTX files into one L1 LTX, reducing
// the bucket's file count for cold restore. Each compaction round
// LISTs <streamPrefix>0000/ and <streamPrefix>0001/, skips L0 files
// that are already covered by L1 or an active baseline, picks
// contiguous eligible runs, downloads them, runs ltx.Compactor, and
// uploads the result to <streamPrefix>0001/.
//
// One Compactor handles one stream (StreamPrefix = DBPrefix or
// MetadataPrefix). The publisher runs one pass per stream on the
// same cadence and backend.
//
// L0 files are NOT deleted by the compactor — that's retention's job
// (with a grace period for in-flight readers). L0 and L1 covering
// the same TXID range overlap during the grace window, but the
// compactor must not re-read already-covered L0 on the next tick.
// Restore prefers L1 (fewer files, same content), while a Litestream
// follower mid-stream can finish reading an L0 it had already begun.
type Compactor struct {
	Backend      objectstore.Bucket
	StreamPrefix string
	Logger       *slog.Logger

	// MinFiles to bother compacting. Default 60 (one minute of 1s L0
	// cadence). Below this, the savings don't justify the round-trips.
	MinFiles int

	// MaxInputFiles caps one L1 compaction chunk. Each input becomes one
	// ltx.Decoder with LZ4 state, so very long L0 backlogs must be split
	// instead of building a decoder for every file in the run.
	// Default 64; values below MinFiles are raised to MinFiles.
	MaxInputFiles int

	// MaxRunsPerPass caps one compaction tick's object-store work.
	// Later ticks skip the L1 coverage just emitted and resume from
	// the remaining uncovered L0 backlog. Default 8.
	MaxRunsPerPass int
}

// CompactionResult summarizes one stream's compaction pass.
type CompactionResult struct {
	Stream          string
	L0Files         int
	L1Files         int
	BaselineTXID    uint64
	L0ScanAfterTXID uint64
	BaselineSkipped int
	CoveredSkipped  int
	EligibleFiles   int
	Runs            int
	InputFiles      int
}

// CompactOnce runs one compaction pass. Returns the number of L1
// files produced.
func (c *Compactor) CompactOnce(ctx context.Context) (int, error) {
	res, err := c.CompactOnceDetailed(ctx)
	return res.Runs, err
}

// CompactOnceDetailed runs one compaction pass and returns accounting
// for skipped and emitted work.
func (c *Compactor) CompactOnceDetailed(ctx context.Context) (CompactionResult, error) {
	if c.MinFiles <= 0 {
		c.MinFiles = defaultCompactorMinFiles
	}
	if c.MaxInputFiles <= 0 {
		c.MaxInputFiles = defaultCompactorMaxInputFiles
	}
	if c.MaxInputFiles < c.MinFiles {
		c.MaxInputFiles = c.MinFiles
	}
	if c.MaxRunsPerPass <= 0 {
		c.MaxRunsPerPass = defaultCompactorMaxRunsPerPass
	}
	if c.StreamPrefix == "" {
		return CompactionResult{}, fmt.Errorf("compactor: StreamPrefix required")
	}
	if c.StreamPrefix != objstore.DBPrefix && c.StreamPrefix != objstore.MetadataPrefix {
		return CompactionResult{}, fmt.Errorf("compactor: unsupported StreamPrefix %q", c.StreamPrefix)
	}
	logger := c.Logger
	if logger == nil {
		logger = syzylog.Default()
	}

	res := CompactionResult{Stream: c.StreamPrefix}
	baselineTXID, err := compactorBaselineTXID(ctx, c.Backend, c.StreamPrefix)
	if err != nil {
		return res, err
	}
	res.BaselineTXID = baselineTXID

	l1, err := objstore.ListLTX(ctx, c.Backend, c.StreamPrefix, objstore.L1Level)
	if err != nil {
		return res, err
	}
	sortLTXFiles(l1)
	l1Coverage := newLTXCoverage(l1, nil)
	res.L1Files = len(l1)
	res.L0ScanAfterTXID = l1Coverage.ContiguousMaxAfter(baselineTXID)

	l0, err := objstore.ListLTXAfter(ctx, c.Backend, c.StreamPrefix, objstore.L0Level, res.L0ScanAfterTXID)
	if err != nil {
		return res, err
	}
	sortLTXFiles(l0)
	res.L0Files = len(l0)

	files := make([]objstore.LTXFile, 0, len(l0))
	for _, f := range l0 {
		if baselineTXID > 0 && f.MaxTXID <= baselineTXID {
			res.BaselineSkipped++
			continue
		}
		if l1Coverage.Covers(f.MinTXID, f.MaxTXID) {
			res.CoveredSkipped++
			continue
		}
		files = append(files, f)
	}
	res.EligibleFiles = len(files)

	var runErrs []error
	attemptedRuns := 0
	for i := 0; i+c.MinFiles <= len(files) && attemptedRuns < c.MaxRunsPerPass; {
		// Find the longest contiguous run starting at i.
		j := i + 1
		for j < len(files) && files[j].MinTXID == files[j-1].MaxTXID+1 {
			j++
		}
		if j-i < c.MinFiles {
			i = j
			continue
		}
		// Compact the whole qualified run [i:j], not just its MinFiles-
		// aligned prefix. The old `k+MinFiles <= j` bound left the final
		// <MinFiles files of every run uncompacted; bounded above by the
		// L1 just emitted, that tail became an isolated sub-MinFiles
		// eligible run that the j-i<MinFiles guard skips forever — raw L0
		// that never collapses, so the contiguous-from-baseline frontier
		// stalls and cold restore replays the whole chain. Cover it.
		for k := i; k < j && attemptedRuns < c.MaxRunsPerPass; {
			end := k + c.MaxInputFiles
			if end >= j {
				end = j
			} else if j-end < c.MinFiles {
				// A full MaxInputFiles chunk here would orphan the sub-
				// MinFiles tail [end:j]. Split the remainder evenly so both
				// chunks are covered and no orphan tail forms.
				end = k + (j-k+1)/2
			}
			run := files[k:end]
			attemptedRuns++
			if err := c.compactRun(ctx, run); err != nil {
				runErrs = append(runErrs, fmt.Errorf("compact %s [%d..%d]: %w", c.StreamPrefix, run[0].MinTXID, run[len(run)-1].MaxTXID, err))
				logger.Warn("compactor: run failed",
					"stream", c.StreamPrefix,
					"min", run[0].MinTXID, "max", run[len(run)-1].MaxTXID, "err", err)
				k = end
				continue
			}
			logger.Info("compactor: emitted L1",
				"stream", c.StreamPrefix,
				"min_txid", run[0].MinTXID,
				"max_txid", run[len(run)-1].MaxTXID,
				"input_files", len(run))
			res.Runs++
			res.InputFiles += len(run)
			k = end
		}
		i = j
	}
	return res, errors.Join(runErrs...)
}

// compactRun downloads the given L0 files and streams their merged LTX into
// one immutable L1 object. A concurrent writer that creates the same key is
// accepted only when its bytes exactly match this output.
//
// Checksum attestations propagate through the merge: when every input
// carries them, the L1 inherits the first input's pre-apply and the
// last input's post-apply checksums. Inputs cannot mix — ltx's
// compactor would emit an invalid header — so a run straddling an
// attestation boundary (legacy objects meeting checksummed ones) is
// truncated to its leading uniform segment; the rest compacts on a
// later pass.
func (c *Compactor) compactRun(ctx context.Context, run []objstore.LTXFile) error {
	readers, closers, err := c.openRunReaders(ctx, run)
	if err != nil {
		return err
	}
	defer func() {
		for _, rc := range closers {
			_ = rc.Close()
		}
	}()

	attested, err := peekAttestations(readers)
	if err != nil {
		return err
	}
	uniform := 1
	for uniform < len(run) && attested[uniform] == attested[0] {
		uniform++
	}
	if uniform < len(run) {
		logger := c.Logger
		if logger == nil {
			logger = syzylog.Default()
		}
		logger.Warn("compactor: attestation boundary splits run",
			"stream", c.StreamPrefix,
			"min", run[0].MinTXID, "max", run[len(run)-1].MaxTXID,
			"uniform_files", uniform)
		run = run[:uniform]
		readers = readers[:uniform]
	}

	pr, pw := io.Pipe()
	h := sha256.New()
	var size byteCounter
	compactErr := make(chan error, 1)
	go func() {
		merger, err := ltx.NewCompactor(io.MultiWriter(pw, h, &size), readers)
		if err != nil {
			_ = pw.CloseWithError(err)
			compactErr <- fmt.Errorf("ltx.NewCompactor: %w", err)
			return
		}
		if !attested[0] {
			merger.HeaderFlags = ltx.HeaderFlagNoChecksum
		}
		err = merger.Compact(ctx)
		_ = pw.CloseWithError(err)
		if err != nil {
			compactErr <- fmt.Errorf("compact: %w", err)
			return
		}
		compactErr <- nil
	}()

	key := objstore.LTXKey(c.StreamPrefix, objstore.L1Level, run[0].MinTXID, run[len(run)-1].MaxTXID)
	_, putErr := c.Backend.PutStream(ctx, key, pr, objectstore.IfAbsent())
	_ = pr.Close()
	compErr := <-compactErr
	if compErr != nil && !errors.Is(compErr, io.ErrClosedPipe) {
		return compErr
	}
	if errors.Is(putErr, objectstore.ErrPreconditionFailed) {
		return objstore.VerifyLTXCollision(ctx, c.Backend, key, int64(size), hex.EncodeToString(h.Sum(nil)))
	}
	if putErr != nil {
		return fmt.Errorf("publish L1 %s: %w", key, putErr)
	}
	if compErr != nil {
		return compErr
	}
	return nil
}

type byteCounter int64

func (w *byteCounter) Write(p []byte) (int, error) {
	*w += byteCounter(len(p))
	return len(p), nil
}

func (c *Compactor) openRunReaders(ctx context.Context, run []objstore.LTXFile) ([]io.Reader, []io.Closer, error) {
	type result struct {
		index int
		key   string
		rc    io.ReadCloser
		err   error
	}
	results := make(chan result, len(run))
	for i, f := range run {
		go func(i int, key string) {
			rc, _, err := c.Backend.Get(ctx, key)
			results <- result{index: i, key: key, rc: rc, err: err}
		}(i, f.Key)
	}

	readers := make([]io.Reader, len(run))
	closers := make([]io.Closer, 0, len(run))
	var firstErr error
	for range run {
		res := <-results
		if res.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("get %s: %w", res.key, res.err)
			}
			continue
		}
		// Buffered so peekAttestations can read the header without
		// consuming it from the compactor's stream.
		readers[res.index] = bufio.NewReaderSize(res.rc, ltx.HeaderSize)
		closers = append(closers, res.rc)
	}
	if firstErr != nil {
		for _, rc := range closers {
			_ = rc.Close()
		}
		return nil, nil, firstErr
	}
	return readers, closers, nil
}

// peekAttestations reports, per input, whether the LTX carries
// checksum attestations, without consuming any reader bytes.
func peekAttestations(readers []io.Reader) ([]bool, error) {
	attested := make([]bool, len(readers))
	for i, r := range readers {
		br, ok := r.(*bufio.Reader)
		if !ok {
			return nil, fmt.Errorf("compactor: input %d is not peekable", i)
		}
		b, err := br.Peek(ltx.HeaderSize)
		if err != nil {
			return nil, fmt.Errorf("compactor: peek input %d header: %w", i, err)
		}
		var hdr ltx.Header
		if err := hdr.UnmarshalBinary(b); err != nil {
			return nil, fmt.Errorf("compactor: decode input %d header: %w", i, err)
		}
		attested[i] = !hdr.NoChecksum()
	}
	return attested, nil
}

func compactorBaselineTXID(ctx context.Context, b objectstore.Bucket, streamPrefix string) (uint64, error) {
	head, _, err := objstore.LoadHEAD(ctx, b)
	if err != nil {
		if errors.Is(err, objstore.ErrNoHEAD) {
			return 0, nil
		}
		return 0, err
	}
	switch streamPrefix {
	case objstore.DBPrefix:
		return baselineOrZero(head.Baseline), nil
	case objstore.MetadataPrefix:
		return baselineOrZero(head.MetaBaseline), nil
	default:
		return 0, fmt.Errorf("compactor: unsupported StreamPrefix %q", streamPrefix)
	}
}

func sortLTXFiles(files []objstore.LTXFile) {
	sort.Slice(files, func(i, j int) bool {
		if files[i].MinTXID != files[j].MinTXID {
			return files[i].MinTXID < files[j].MinTXID
		}
		return files[i].MaxTXID < files[j].MaxTXID
	})
}

type txRange struct {
	min uint64
	max uint64
}

type ltxCoverage struct {
	ranges []txRange
}

func newLTXCoverage(files []objstore.LTXFile, include func(objstore.LTXFile) bool) ltxCoverage {
	ranges := make([]txRange, 0, len(files))
	for _, f := range files {
		if include != nil && !include(f) {
			continue
		}
		if len(ranges) == 0 {
			ranges = append(ranges, txRange{min: f.MinTXID, max: f.MaxTXID})
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
		ranges = append(ranges, txRange{min: f.MinTXID, max: f.MaxTXID})
	}
	return ltxCoverage{ranges: ranges}
}

func (c ltxCoverage) Covers(minTX, maxTX uint64) bool {
	i := sort.Search(len(c.ranges), func(i int) bool {
		return c.ranges[i].max >= minTX
	})
	return i < len(c.ranges) && c.ranges[i].min <= minTX && maxTX <= c.ranges[i].max
}

func (c ltxCoverage) ContiguousMaxAfter(after uint64) uint64 {
	frontier := after
	for _, r := range c.ranges {
		if r.max <= frontier {
			continue
		}
		if frontier == ^uint64(0) || r.min > frontier+1 {
			break
		}
		frontier = r.max
	}
	return frontier
}

// runCompactorLoop drives the compactor on a tick until ctx cancels.
// Runs both stream prefixes (db/ and metadata/) under one timer so
// they don't drift apart. Used by the Publisher when started.
func (p *Publisher) runCompactorLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	compactors := []*Compactor{
		{Backend: p.mutationBackend(), StreamPrefix: objstore.DBPrefix, Logger: p.cfg.Logger},
		{Backend: p.mutationBackend(), StreamPrefix: objstore.MetadataPrefix, Logger: p.cfg.Logger},
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			mutationCtx, done, err := p.leaseMutationContext(ctx)
			if err != nil {
				return
			}
			for _, c := range compactors {
				t0 := time.Now()
				res, err := c.CompactOnceDetailed(mutationCtx)
				dur := time.Since(t0)
				err = p.fenceMutationError(err)
				if err != nil && errors.Is(err, context.Canceled) {
					done()
					return
				}
				p.stats.recordCompaction(c.StreamPrefix, res, dur, err)
				logAttrs := []any{
					"stream", c.StreamPrefix,
					"l0_files", res.L0Files,
					"l1_files", res.L1Files,
					"baseline_txid", res.BaselineTXID,
					"l0_scan_after_txid", res.L0ScanAfterTXID,
					"baseline_skipped", res.BaselineSkipped,
					"covered_skipped", res.CoveredSkipped,
					"eligible_files", res.EligibleFiles,
					"runs", res.Runs,
					"input_files", res.InputFiles,
					"duration", dur,
				}
				if err != nil {
					p.cfg.Logger.Warn("compactor: pass failed", append(logAttrs, "err", err)...)
					if errors.Is(err, errPublisherUnhealthy) || errors.Is(err, errLeaseLost) {
						done()
						return
					}
				} else {
					p.cfg.Logger.Info("compactor: pass complete", logAttrs...)
				}
			}
			// After both streams collapse, decide whether the db chain has
			// outgrown its baseline and a fresh coupled baseline is due.
			if err := p.maybeRebaseline(mutationCtx); err != nil {
				if errors.Is(err, context.Canceled) {
					done()
					return
				}
				p.cfg.Logger.Warn("publisher: rebaseline check failed", "err", err)
			}
			done()
		}
	}
}
