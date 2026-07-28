package sqlite

import (
	"context"
	"errors"
	"os"

	"github.com/wjordan/syzy/crdt"
)

// Handoff carries a live daemon-role lock from a Detach'd node to its successor.
//
// The two files are the still-held flock'd open file descriptions for the
// daemon lock and the origin directory. Passing them to a successor — in the
// same process, or across fork/exec via cmd.ExtraFiles — transfers the lock
// WITHOUT releasing it: the flock rides the open file description, so a shared
// FD keeps it held and there is never a window where another opener could claim
// the role (proven by a dedicated lockflip spike harness). Origin
// pins the successor to the predecessor's exact origin (no rotation); its
// journal is the resume point.
//
// Multi-node aware. The local daemon + origin flocks transfer gaplessly via the
// FDs; the cluster-level publisher lease is RETAINED across the handoff (Detach
// sets Publisher.RetainLeaseOnStop) so the same-NodeID successor resumes it with
// no expiry window for a peer to force-rebaseline through; the broker/transport
// simply reconnects, which is normal cluster churn.
type Handoff struct {
	daemonFile *os.File
	originFile *os.File
	origin     crdt.Origin
}

// NewHandoff reconstructs a Handoff in a successor process from inherited file
// descriptors (e.g. ExtraFiles delivered as fd 3 and 4) plus the origin id the
// predecessor advertised out of band (see (*Node).OriginID / (*Handoff).OriginID).
// The successor then calls Attach.
func NewHandoff(daemonFD, originFD *os.File, origin uint64) *Handoff {
	return &Handoff{daemonFile: daemonFD, originFile: originFD, origin: crdt.Origin(origin)}
}

// DaemonFD and OriginFD expose the locked files so the caller can hand them to a
// successor process (cmd.ExtraFiles) before that successor Attaches.
func (h *Handoff) DaemonFD() *os.File { return h.daemonFile }
func (h *Handoff) OriginFD() *os.File { return h.originFile }

// OriginID is the origin to advertise to a successor process so it can rebuild
// the Handoff via NewHandoff.
func (h *Handoff) OriginID() uint64 { return uint64(h.origin) }

// Detach hands off the daemon role WITHOUT releasing the lock. It runs the full
// Close teardown (drain, stop snapshotter, close SQLite) but keeps the daemon
// and origin flocks held and leaves clean_shutdown=false — the origin is still
// live in the successor, so a crash before the successor's own clean Close must
// still rotate. Returns the Handoff to feed Attach (in the successor process or,
// on rollback, this one) and finally Commit.
func (n *Node) Detach() (*Handoff, error) {
	if err := n.closeWithOpts(false, false); err != nil {
		return nil, err
	}
	return &Handoff{
		daemonFile: n.daemonClaim.File(),
		originFile: n.originClaim.File(),
		origin:     n.originClaim.Origin,
	}, nil
}

// Attach opens a node by ADOPTING a predecessor's handed-off lock instead of
// claiming a fresh daemon role + origin. It shares the predecessor's open file
// descriptions (no re-flock) and resumes the predecessor's exact origin from its
// on-disk journal — so there is no lock window and no origin rotation. Used by a
// successor process, or by the original process to Resume after a failed handoff.
func Attach(ctx context.Context, cfg Config, h *Handoff) (*Node, error) {
	if h == nil || h.daemonFile == nil || h.originFile == nil {
		return nil, errors.New("syzy: Attach: nil or empty handoff")
	}
	return openWithAdopt(ctx, cfg, h)
}

// Commit finalizes a successful handoff in the predecessor process: it closes
// this process's references to the lock files. Because Release/Close drops only
// this fd (not LOCK_UN), the successor's inherited fd keeps the open file
// description — and thus the lock — held. After Commit the predecessor no longer
// participates. Idempotent.
func (h *Handoff) Commit() error {
	var errs []error
	if h.daemonFile != nil {
		errs = append(errs, h.daemonFile.Close())
		h.daemonFile = nil
	}
	if h.originFile != nil {
		errs = append(errs, h.originFile.Close())
		h.originFile = nil
	}
	return errors.Join(errs...)
}
