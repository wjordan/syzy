package physicalrestore

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/superfly/ltx"
	"github.com/wjordan/syzy/internal/objstore"
)

// chainVerifier enforces the attestations the LTX format carries as a
// stream is materialized. It maintains the rolling checksum of the
// database state actually on disk — seeded from an attested baseline's
// PostApplyChecksum (content-verified by the decoder) or lazily by
// scanning the materialized file, then advanced page-by-page from the
// bytes each frame overwrites — and checks it against every attested
// frame:
//
//   - PreApplyChecksum mismatch warns but does not fail: the publisher
//     legitimately re-ships content under fresh TXIDs after a failed
//     publish; applying the duplicate converges to the attested post
//     state.
//   - PostApplyChecksum is enforced fail-closed once the materialized
//     state is aligned with the attestation trajectory — i.e. running
//     has matched an attested pre or post value. Claim-time baselines
//     seed the publisher's checksum state, so those chains verify
//     aligned from the first frame. Mid-stream baselines are staged
//     backup copies: logically identical but byte-divergent from the
//     live file the frame attestations describe (the copy's header
//     page is rewritten, and it pins WAL content whose frames emit
//     afterwards). A restore anchored on one starts unaligned, applies
//     with warnings, and hardens permanently the moment the chain
//     converges to an attested state.
//
// The unaligned window is the strongest enforcement the format allows:
// a hole or corrupt frame immediately after an off-trajectory anchor
// is indistinguishable from the anchor's own benign divergence, and is
// caught only once (if ever) the chain re-aligns.
//
// Frames without attestations (encoded before checksums shipped) apply
// with the rolling checksum maintained across them, so a corrupt
// legacy frame is still caught by the next attested frame's post-apply
// check. A wholly unattested chain restores as before, logged at warn.
type chainVerifier struct {
	pageSize int64
	commit   uint32 // logical page count of the materialized state
	running  ltx.Checksum
	valid    bool // running tracks the on-disk state
	aligned  bool // running has matched an attested checksum

	// finalPost holds the PostApplyChecksum of the most recently
	// applied file when that file was attested; verifyFinal re-checks
	// the finished file against it. finalIsSelf marks it as coming from
	// the snapshot just materialized (self-consistent regardless of
	// trajectory alignment).
	finalPost   ltx.Checksum
	hasFinal    bool
	finalIsSelf bool
	unattested  int
	divergent   int // attested frames applied while unaligned
}

// beginFile validates per-file geometry against the chain state and
// returns whether the file carries attestations.
func (v *chainVerifier) beginFile(hdr ltx.Header, key string) (attested bool, err error) {
	if v.pageSize == 0 {
		v.pageSize = int64(hdr.PageSize)
	} else if v.pageSize != int64(hdr.PageSize) {
		return false, fmt.Errorf("ltx %s: page size %d differs from chain page size %d", key, hdr.PageSize, v.pageSize)
	}
	return !hdr.NoChecksum(), nil
}

// ensureSeeded makes running track the current on-disk state, scanning
// the materialized file if no attested baseline seeded it. Called
// before the first attested non-snapshot frame applies.
func (v *chainVerifier) ensureSeeded(f *os.File) error {
	if v.valid {
		return nil
	}
	sum, err := checksumFilePages(f, v.commit, v.pageSize)
	if err != nil {
		return fmt.Errorf("seed rolling checksum: %w", err)
	}
	v.running = sum
	v.valid = true
	return nil
}

// checkPreApply compares an attested frame's PreApplyChecksum with the
// tracked state. A match proves the materialized state is on the
// attestation trajectory; a mismatch is tolerated (see type comment)
// but logged when it breaks an established alignment.
func (v *chainVerifier) checkPreApply(hdr ltx.Header, key string) {
	if !v.valid {
		return
	}
	if hdr.PreApplyChecksum == v.running {
		v.aligned = true
		return
	}
	if v.aligned {
		slog.Warn("syzy restore: ltx pre-apply checksum differs; deferring to post-apply check",
			"key", key, "attested", hdr.PreApplyChecksum.String(), "materialized", v.running.String())
	}
}

// observePage folds one page overwrite into the rolling checksum,
// reading the bytes being replaced from dst. A page beyond the current
// logical commit (growth) has no prior contribution. Must run before
// the new bytes are written.
func (v *chainVerifier) observePage(dst *os.File, pgno uint32, data []byte) error {
	if !v.valid || pgno == ltx.LockPgno(uint32(v.pageSize)) {
		return nil
	}
	delta := ltx.ChecksumPage(pgno, data)
	if pgno <= v.commit {
		old, err := readPageAt(dst, pgno, v.pageSize)
		if err != nil {
			return fmt.Errorf("read prior page %d: %w", pgno, err)
		}
		if old != nil {
			delta ^= ltx.ChecksumPage(pgno, old)
		}
	}
	v.running = ltx.ChecksumFlag | (v.running ^ delta)
	return nil
}

// endFile settles a frame's effect on the chain state: absorbs a
// shrink (pages beyond the new commit drop out of the rolling
// checksum), advances the logical commit, and enforces the post-apply
// attestation when present. trailer is only meaningful when attested.
func (v *chainVerifier) endFile(dst *os.File, hdr ltx.Header, trailer ltx.Trailer, attested bool, key string) error {
	if v.valid {
		lockPgno := ltx.LockPgno(uint32(v.pageSize))
		for pgno := hdr.Commit + 1; pgno <= v.commit; pgno++ {
			if pgno == lockPgno {
				continue
			}
			old, err := readPageAt(dst, pgno, v.pageSize)
			if err != nil {
				return fmt.Errorf("read truncated page %d: %w", pgno, err)
			}
			if old != nil {
				v.running = ltx.ChecksumFlag | (v.running ^ ltx.ChecksumPage(pgno, old))
			}
		}
	}
	v.commit = hdr.Commit
	if !attested {
		v.unattested++
		v.hasFinal = false
		return nil
	}
	if v.valid {
		switch {
		case v.running == trailer.PostApplyChecksum:
			v.aligned = true
		case v.aligned:
			return fmt.Errorf("ltx %s: post-apply checksum mismatch: attested %s, materialized state is %s (chain hole, stale frame, or corrupt page content)",
				key, trailer.PostApplyChecksum, v.running)
		default:
			if v.divergent == 0 {
				slog.Warn("syzy restore: anchor baseline is off the attestation trajectory; deferring post-apply checks until the chain converges",
					"key", key, "attested", trailer.PostApplyChecksum.String(), "materialized", v.running.String())
			}
			v.divergent++
		}
	}
	v.finalPost = trailer.PostApplyChecksum
	v.hasFinal = true
	v.finalIsSelf = false
	return nil
}

// adoptSnapshot seeds the verifier from a freshly decoded snapshot
// (baseline). An attested snapshot's content was already verified
// against its PostApplyChecksum by the LTX decoder.
func (v *chainVerifier) adoptSnapshot(hdr ltx.Header, trailer ltx.Trailer, attested bool, key string) {
	v.commit = hdr.Commit
	if attested {
		v.running = trailer.PostApplyChecksum
		v.valid = true
		v.finalPost = trailer.PostApplyChecksum
		v.hasFinal = true
		v.finalIsSelf = true
		return
	}
	v.valid = false
	v.hasFinal = false
	v.unattested++
}

// verifyFinal re-reads the finished file and checks it against the
// last attested post-apply state, closing the loop between what the
// chain attested and the bytes actually on disk. Skipped when the last
// applied file carried no attestation (the rolling checksum alone
// would only verify this verifier's own bookkeeping) or when the chain
// never aligned with the attestation trajectory (the last frame's post
// describes a byte-reality this restore was never anchored on). One
// extra full read on a cold restore, ahead of the quick_check that
// reads it all anyway.
func (v *chainVerifier) verifyFinal(path string) error {
	if !v.hasFinal || !(v.aligned || v.finalIsSelf) {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sum, err := checksumFilePages(f, v.commit, v.pageSize)
	if err != nil {
		return fmt.Errorf("checksum materialized file: %w", err)
	}
	if sum != v.finalPost {
		return fmt.Errorf("materialized file checksum %s does not match the chain's attested post-apply state %s", sum, v.finalPost)
	}
	return nil
}

// logSummary records at warn how much of the applied chain carried no
// attestations (objects encoded before checksums shipped restore
// unverified beyond their FileChecksum).
func (v *chainVerifier) logSummary(prefix string) {
	if v.unattested > 0 {
		slog.Warn("syzy restore: ltx files without attestations applied unverified",
			"prefix", prefix, "unattested_files", v.unattested)
	}
	if v.divergent > 0 {
		slog.Warn("syzy restore: attested frames applied while off the attestation trajectory",
			"prefix", prefix, "frames", v.divergent, "converged", v.aligned)
	}
}

// checksumFilePages computes the rolling LTX checksum of the first
// commit pages of f, skipping the lock page — the same convention the
// encoder and decoder use for pre/post-apply checksums.
func checksumFilePages(f *os.File, commit uint32, pageSize int64) (ltx.Checksum, error) {
	lockPgno := ltx.LockPgno(uint32(pageSize))
	sum := ltx.ChecksumFlag
	buf := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if _, err := f.ReadAt(buf, int64(pgno-1)*pageSize); err != nil {
			return 0, fmt.Errorf("read page %d: %w", pgno, err)
		}
		sum = ltx.ChecksumFlag | (sum ^ ltx.ChecksumPage(pgno, buf))
	}
	return sum, nil
}

// readPageAt returns page pgno's current bytes, or nil when the file
// does not extend that far (the page has no prior content; a chain
// that should have written it will fail its attestation instead).
func readPageAt(f *os.File, pgno uint32, pageSize int64) ([]byte, error) {
	buf := make([]byte, pageSize)
	_, err := f.ReadAt(buf, int64(pgno-1)*pageSize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// verifyChainTXIDs rejects overlapping TXID ranges in a selected
// chain: overlap means two publishers interleaved ranges (double
// claim) or the listing is torn, and applying both in MinTXID order
// silently time-travels pages. Gaps are legal — a failed L0 publish
// leaks its TXIDs from the in-memory counter and the content re-ships
// under fresh ones; a gap that actually lost content fails the
// post-apply attestation instead.
func verifyChainTXIDs(entries []objstore.LTXFile) error {
	for i, f := range entries {
		if f.MinTXID > f.MaxTXID {
			return fmt.Errorf("ltx %s: inverted TXID range [%d..%d]", f.Key, f.MinTXID, f.MaxTXID)
		}
		if i > 0 && f.MinTXID <= entries[i-1].MaxTXID {
			return fmt.Errorf("ltx chain overlap: %s [%d..%d] overlaps %s [%d..%d]",
				entries[i-1].Key, entries[i-1].MinTXID, entries[i-1].MaxTXID, f.Key, f.MinTXID, f.MaxTXID)
		}
	}
	return nil
}
