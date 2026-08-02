package ltxstream

import "github.com/superfly/ltx"

// ChecksumState tracks the rolling LTX checksum of one database as its
// pages change: a per-page checksum for pages 1..commit (lock page
// excluded) plus the XOR aggregate over them. EncodeBaseline seeds it
// from a consistent snapshot copy; the tailer advances it once per
// emitted batch. The Pre/Post values it stages are absolute
// attestations of database state, so they stay true regardless of
// which baseline a restorer anchors on.
//
// Not safe for concurrent use; the owning tailer serializes access
// under its position mutex.
type ChecksumState struct {
	pages    []ltx.Checksum // index pgno-1; lock page slot stays zero
	pageSize uint32
	chksum   ltx.Checksum // rolling XOR aggregate (ltx.ChecksumFlag set)
}

// PageSize returns the page size the state was seeded with.
func (s *ChecksumState) PageSize() uint32 { return s.pageSize }

// Checksum returns the rolling checksum of the tracked state.
func (s *ChecksumState) Checksum() ltx.Checksum { return s.chksum }

// Stage computes the pre/post-apply checksums for one incremental
// batch (deduped latest bytes per page, post-batch commit) without
// mutating the state. Commit the result only after the batch has been
// durably accepted; an abandoned stage leaves the state unchanged so
// the re-emitted batch stages against the same pre-state.
func (s *ChecksumState) Stage(pageMap map[uint32][]byte, commit uint32) StagedChecksums {
	st := StagedChecksums{
		state:   s,
		Pre:     s.chksum,
		Post:    s.chksum,
		commit:  commit,
		updates: make(map[uint32]ltx.Checksum, len(pageMap)),
	}
	lockPgno := ltx.LockPgno(s.pageSize)
	for pgno, data := range pageMap {
		if pgno == lockPgno || pgno > commit {
			continue
		}
		var old ltx.Checksum
		if int(pgno) <= len(s.pages) {
			old = s.pages[pgno-1]
		}
		nw := ltx.ChecksumPage(pgno, data)
		st.Post = ltx.ChecksumFlag | (st.Post ^ old ^ nw)
		st.updates[pgno] = nw
	}
	// A batch that shrinks the database drops the truncated pages'
	// contributions.
	for pgno := commit + 1; int(pgno) <= len(s.pages); pgno++ {
		st.Post = ltx.ChecksumFlag | (st.Post ^ s.pages[pgno-1])
	}
	return st
}

// StagedChecksums is one staged batch: the pre/post-apply checksums to
// encode, plus the page updates to fold into the state once the batch
// is accepted.
type StagedChecksums struct {
	Pre, Post ltx.Checksum

	state   *ChecksumState
	commit  uint32
	updates map[uint32]ltx.Checksum
}

// Commit folds the staged batch into the state.
func (st StagedChecksums) Commit() {
	s := st.state
	if int(st.commit) <= len(s.pages) {
		s.pages = s.pages[:st.commit]
	} else {
		s.pages = append(s.pages, make([]ltx.Checksum, int(st.commit)-len(s.pages))...)
	}
	for pgno, c := range st.updates {
		s.pages[pgno-1] = c
	}
	s.chksum = st.Post
}
