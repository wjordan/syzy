package sqlite

import (
	"log/slog"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/wake"
)

// DefaultTopic is the mesh topic single-database daemons serve
// (cmd/syzy daemon). Clone/bundle URLs without an explicit ?topic=
// query default to it. Multi-database hosts pick their own
// per-database topics.
const DefaultTopic = "db"

// SealerConfig tunes object-store changeset batching. Zero values use the
// built-in defaults.
type SealerConfig struct {
	MaxBytes   int
	MaxAge     time.Duration
	QueueDepth int
	Logf       func(format string, args ...any)
}

// Config configures a Node. Only Path is required; everything else
// has a working default.
type Config struct {
	// Path is the user's app database. Syzy state lives next to it
	// under "<Path>-syzy/" (metadata, per-origin journals, lock files).
	Path string

	// Transport selects the network transport for cross-node
	// replication. nil → single-node mode (durable, no peers).
	Transport transport.Transport

	// Log receives operational logs. nil → discard.
	Log *slog.Logger

	// SchemaLog enables DDL replication. nil → DDL is rejected at the
	// trace_v2 hook. Use schemalog.NewLocal() for single-process
	// deployments and tests, or schemalog.OpenFile(path) for
	// multi-process clusters that share one file-backed CAS log. When
	// non-nil and Transport is also set, the broker runs a
	// schema-catchup loop so receivers pull DDL events committed by
	// peers.
	SchemaLog schemalog.Log

	// SchemaCatchupInterval tunes how often the broker polls
	// SchemaLog.Read for events past meta.schema_seq. Ignored when
	// SchemaLog or Transport is nil. 0 → defaultSchemaCatchupInterval.
	SchemaCatchupInterval time.Duration

	// ObjectBackend, when non-nil, enables sealing of per-origin
	// Changeset epochs to object storage. The Sealer registers as a
	// second OnEncoded listener on the producer's drainer; flushed
	// epochs are uploaded under origins/<origin-hex>/.
	ObjectBackend objectstore.Bucket

	// SealerConfig tunes the in-process Sealer when ObjectBackend is
	// set. Zero values use built-in defaults.
	SealerConfig SealerConfig

	// SnapshotRetention is the age floor for post-snapshot journal GC: a
	// segment already covered by the durable snapshot marker (and, for
	// drained origins, the sealer watermark) is unlinked only once it is
	// older than this. It bounds disk to ~retention × write_rate per
	// origin while letting a peer offline less than this window catch up
	// incrementally instead of rebaselining. Only consulted when
	// ObjectBackend is set (which provides the sealer GC gate). 0 → a
	// built-in default (DefaultSnapshotRetention).
	SnapshotRetention time.Duration

	// LTXSyncInterval is the strict cadence at which the publisher's
	// LTX tailers run. Each Sync pass coalesces every committed
	// transaction since the last pass into a single LTX upload.
	// Higher values reduce per-PUT overhead at the cost of
	// proportionally higher commit→object-store latency. Ignored when
	// ObjectBackend is nil. 0 → publisher default (1s).
	LTXSyncInterval time.Duration

	// LeaseClaimSettle, when >0, makes a successful publisher
	// lease-claim wait this long and re-read HEAD before publishing,
	// proceeding only as the surviving holder. Required on object
	// stores with multi-region last-writer-wins replication (e.g.
	// Tigris), where conditional writes from different regions can
	// all "succeed": concurrent claimants then interleave baseline
	// and L0 uploads at colliding keys, producing a torn chain that
	// corrupts every fresh restore. Zero disables (single-region or
	// genuinely linearizable backends).
	LeaseClaimSettle time.Duration

	// Wake, when non-nil, replaces the in-process producer's
	// futex-based journal wake. Cross-kernel producers (e.g., the
	// syzy.so extension in a guest VM whose host-side daemon owns
	// the drainer) must set this to a wake transport that crosses
	// the kernel boundary — futex cannot. See wake/vsock for the
	// vsock-shaped implementation.
	Wake wake.Waker

	// WakeListener, when non-nil, supplies per-origin Waiters for
	// secondary drainers. The daemon's secondary-scan calls
	// WakeListener.Register(originHex) for each new origin and
	// installs the returned Waiter's Wait on that journal via
	// journal.SetWaitFunc; on drainer teardown the daemon calls
	// Unregister. Required on the host side when secondary
	// producers run in different kernels.
	WakeListener wake.Listener

	// InProcessOnly declares that this node has NO cross-process producers
	// — every write to this DB goes through this node's in-process producer.
	// When set, secondary drainers are never started: scanSecondaries is a
	// no-op and n.secondaries stays nil.
	//
	// This is essential for a long-lived single-process node (e.g. an
	// orchestrator's control plane). Such a node rotates to a fresh origin
	// on every unclean restart (see MintAndClaim), leaving its previous
	// origins behind under origins/. With secondary drainers enabled, the
	// scan attaches one to each of those RETIRED self-origins and re-drains
	// its journal, re-emitting every already-published record under a fresh
	// seq (SenderNextSeq never resets) that peers cannot dedupe. That
	// compounds on every restart into unbounded mirror-journal growth
	// (observed: a retired origin at sender_seq > 4,000,000 and a 1.4 GB
	// mirror for a 22 MB source). A node that hosts genuine cross-process
	// (extension) producers must leave this false; one that does not should
	// set it so retired origins are simply ignored.
	InProcessOnly bool

	// DisableMmap turns off SQLite's mmap I/O (mmap_size=0) on every
	// connection this node opens. Required when Path sits on a FUSE
	// filesystem served by the same process: with mmap, column reads
	// copy out of the mapping in Go code, and a page fault there can
	// deadlock against the runtime's stop-the-world (resolving the
	// fault needs the in-process FUSE server goroutines to run, but a
	// pending STW schedules nothing). pread-based I/O faults only
	// inside cgo, which releases the P and lets STW complete.
	DisableMmap bool

	// ReplicateUnderscoreTables passes through to producer.Config and
	// stamps the per-slot flag on first Open. sqlite_* always stays
	// local-only; underscore-prefixed names switch from "local by
	// convention" to "replicated like any other table" when this is
	// true. Immutable per-slot after first Open — see
	// producer.Config.ReplicateUnderscoreTables for the contract.
	ReplicateUnderscoreTables bool

	// IdempotentDDL passes through to producer.Config: a DDL whose effect
	// is already present is a no-op success instead of an error, so
	// redundant DDL converges without IF [NOT] EXISTS markers. Off by
	// default; see producer.Config.IdempotentDDL.
	IdempotentDDL bool

	// UniqueQuarantine overrides the coordinated-uniqueness reservation
	// quarantine window. Zero uses the leaseholder default.
	UniqueQuarantine time.Duration

	// ServeClones registers this node's clone-bundle producer with the
	// Transport at Open (and unregisters it at Close), so peers can
	// bootstrap full copies of this database (transport.BundleSource).
	// Off by default: serving whole-database clones to the mesh is a
	// security/resource policy decision, not wiring boilerplate. Open
	// fails if set with a Transport that cannot accept clone requests.
	ServeClones bool

	// LoopbackUnique acknowledges that the coordinated-uniqueness
	// leaseholder may bind a loopback listener even though a Transport
	// is configured. Loopback reservation is correct only when every
	// writer to the bucket runs in this process; on a real cluster it
	// publishes an undialable leaseholder address and silently breaks
	// cross-node NOT NULL UNIQUE. Open therefore fails when the
	// Transport cannot carry uniqueness RPCs unless this is set.
	// Single-process tests with an in-memory transport set it; the
	// built-in mesh transport never needs it.
	LoopbackUnique bool

	// NodeID, when non-zero, pins this node to a stable reserved origin
	// (its low 63 bits) on a clean Open instead of recycling the first
	// unlocked origin dir. A host daemon sharing a slot with guest-side
	// writers sets this: across the
	// pmem/virtiofs boundary the origin-dir flock is invisible between
	// host and guest kernels, so an unpinned host could recycle into an
	// origin a guest is actively producing into and then classify it as
	// self — excluding the guest's records from secondary draining, so
	// they are never sealed or published. Claiming a fixed host origin
	// keeps every guest origin a secondary. Guests in turn read this id
	// from metadata (node_id) and exclude it when claiming their own. An
	// unclean restart still rotates to a fresh origin (crash safety wins
	// over the pin). Zero keeps the legacy recycle/mint behavior.
	NodeID uint64
}
