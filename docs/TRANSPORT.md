# Syzy transport: topic mux and peer catchup

[`tcpmesh`](../tcpmesh/mesh.go) carries every logical
topic over one shared TCP connection per peer pair. This spec is
authoritative for the wire protocol: the gossip framing, connection
identity, the four one-shot ops (clone, catchup, unique RPC,
frontier) sharing the same listener, and the peer-pull catchup path.

Topic identity is a string the consumer chooses; transport
addressing is the transport's secret. **One port per host**,
regardless of topic count, with topic membership propagated
peer-to-peer in-band — no per-topic ports, no replicated topic→port
table, no secondary bundle port.

## Design overview

One shared `*tcpmesh.Mesh` per process owns:

- One listener (single port) carrying both long-lived gossip
  connections and one-shot request/response ops, dispatched by the
  connection's first byte (see "Listener dispatch").
- One outbound connection per seed address, plus an inbound
  counterpart that is reconciled away on hello (see "Connection
  identity").

Each `Channel(topic)` returns a `*tcpmesh.Channel` that satisfies
`transport.Transport`, `transport.BundleSource`, and the optional capability
interfaces in `transport/` (see "Interface shape"). On the serving
side the channel is a *registrar*, not a `CatchupSource` itself:
consumers install the producers via `SetCatchupSource`,
`SetFrontierSource`, and `SetBundleHandler`. The channel is a thin
facade over the shared transport: it stamps outbound payloads with
its topic and filters inbound payloads to its topic.

Topic membership is exchanged at connection establishment and
updated incrementally. Each peer tracks, per connection, the set of
topics held open by the remote end. Broadcast for topic X only
writes to connections whose remote peer has X open. Inbound frames
for topics not held open locally are *counted and logged* (protocol
bug, not silently dropped).

```
┌────────────────── one consumer process ─────────────────┐
│                                                          │
│ ┌─── app ────┐  ┌─── aux ────┐  ┌─ app-<uuid> ─┐  ...   │
│ │  *Node      │  │  *Node      │  │  *Node       │       │
│ └─────┬───────┘  └─────┬───────┘  └─────┬────────┘       │
│       │                │                │                 │
│       ▼                ▼                ▼                 │
│ ┌──────────────────────────────────────────────────────┐ │
│ │   *tcpmesh.Channel  *tcpmesh.Channel  *tcpmesh.Channel           │ │
│ │            ↘          ↓             ↙                │ │
│ │              *tcpmesh.Mesh (shared)                 │ │
│ │      ┌──────────────────────────────────┐            │ │
│ │      │ listener :7000                   │            │ │
│ │      │ (gossip + one-shot ops,          │            │ │
│ │      │  first-byte dispatch)            │            │ │
│ │      └──────────────────────────────────┘            │ │
│ └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
                        ▲     ▲
                       TLS   TLS
                        │     │
                  ┌─────┴─────┴─────┐
                  │  peer host (one │
                  │  shared conn)   │
                  └─────────────────┘
```

## Listener dispatch

Every inbound connection (after the TLS handshake, when configured)
is classified by its **first byte**:

- `0x53` (`'S'`, the first byte of the gossip magic `0x53595A32`
  "SYZ2") — a long-lived gossip connection. The server reads and
  verifies the remaining three magic bytes, then proceeds with the
  hello exchange.
- `< 0x40` — a one-shot request/response op (see "One-shot ops").
  The op space is **reserved below `0x40` forever**, so the two
  first-byte spaces stay disjoint and no wire-version byte is
  needed.
- Anything else — not a conforming peer. The server logs at error
  level and closes.

A connection that sends no first byte within the hello deadline
(5s) is closed.

Cutover: per the no-compatibility policy below, the merge of the
former second (bundle/catchup) listener onto the gossip port is a
coordinated-restart change. Skew degrades cleanly in both
directions: an old node dialing port+1 gets a refused connection
and its gap-filler chain fails over to the object store; a new
node's op byte hits an old gossip listener's magic check and is
loudly closed, with the same failover. Persisted uniqueness-lease
records carry the leaseholder's endpoint URL, but leases are
heartbeat-refreshed and re-claimed on restart, so the coordinated
restart also rewrites them.

## Connection identity

Every `*tcpmesh.Mesh` carries a process-random nonzero `uint64`
`NodeID`. `Config.NodeID = 0` (default) generates one at `New`
time; tests force ordering by setting explicit values.

Both sides exchange `NodeID` and a per-connection `connNonce` in the
hello frame. The nonce is minted by the DIALING side (strictly
increasing per process); the acceptor adopts the remote's value, so
both endpoints agree on one identity per conn. If two connections
exist between the same NodeID pair (typical when both sides dial
each other concurrently, or a redial races its predecessor), the
duplicate is resolved deterministically **before either connection
enters the ready set** by conn rank — a total order both endpoints
compute identically, keeping the maximum:

- rank = `(dialedByLowerNodeID, connNonce)`: a conn dialed by the
  lower-NodeID endpoint outranks one dialed by the higher; between
  conns dialed by the same endpoint, the higher (newer) nonce wins.
- A retired (closed) incumbent never vetoes its replacement.
- The superseded conn is closed on BOTH sides; the tie-break loser's
  dialLoop parks on the survivor's `closed` channel instead of
  redialing into fresh rejections.
- If `localNodeID == remoteNodeID` (random collision, 2⁻⁶⁴): both
  sides close the connection, log at error level, and let the dial
  retry interval reconnect. If wedged-equal (operator misconfigured
  identical IDs), the loud log is the signal to fix it.

Reconciliation happens during the setup goroutine, after hello is
read and before the connection joins the broadcast set. The loser is
closed; the winner proceeds. Broadcast and membership state never
see the loser, and a superseded peer's readLoop drops membership
frames once `peersByID` no longer points at it.

Liveness: every `PingInterval` (15s) each ready peer is sent a PING
(0x06, answered by PONG 0x07) via its control queue; any inbound
frame counts as life, and a peer silent past `PingTimeout` (60s) is
retired for the dial loop to replace. This catches a remote that
holds the socket ESTABLISHED but never reads it — TCP keepalive and
the per-frame write deadline (`WriteTimeout`, 30s; it only trips
once the kernel send buffer fills) both miss that state.

The goal "one TCP connection per peer pair" thus reduces to: every
peer pair has at most two transient connections during the
handshake window, exactly one of which becomes ready.

## Gossip wire protocol

### Connection establishment (hello)

After TLS handshake, both ends exchange a hello frame as the first
record. Hello is preceded by a 4-byte magic preamble so a
non-conforming peer fails framing immediately rather than
mis-parsing payload bytes:

```
4 bytes  magic = 0x53595A32 ("SYZ2")     // u32 BE > MaxFrameSize
u32 BE   frameLen                        // bytes of frame body that follow
byte     msgType = 0x01 (HELLO)
u64 BE   nodeID                          // process-random nonzero; load-bearing for tie-break
u64 BE   connNonce                       // dialer-minted conn identity (see "Connection identity")
u16 BE   listenAddrLen
listenAddr bytes                         // dial-back address the sender accepts on
u32 BE   nTopics
nTopics × { u16 BE topicLen, topic bytes }   // topics this side has open at hello-build
```

Hello is sent independently by each side and is not a request/reply.
The dialer sends its hello immediately on connect; the acceptor
first reads the dialer's initial byte to classify the connection
(see "Listener dispatch") and then sends its own hello, so a silent
dialer never receives one. Connections that fail to receive a valid
hello within `helloDeadline` (5s) are closed.

After exchanging hellos, the setup goroutine reconciles topic
membership (see "Hello vs Channel-open reconciliation") and only then
admits the connection to the ready set.

### Data and topic frames

Each subsequent frame:

```
u32 BE   frameLen           (bytes of header + payload that follow)
byte     msgType            (0x02 = DATA, 0x03 = TOPIC_ADD, 0x04 = TOPIC_REMOVE)
u16 BE   topicLen
N bytes  topic
M bytes  payload            (only for DATA; TOPIC_ADD/REMOVE have no payload)
```

Topic strings are UTF-8, capped at 256 bytes, case-sensitive,
otherwise opaque; they are not normalized.

Wire-frame overhead per data frame: 4 (len) + 1 (type) + 2
(topicLen) + |topic| bytes. `msgType=0x05` is reserved for a future
TOPIC_ASSIGN optimization that swaps topic strings for small
integer ids post-hello. `0x06`/`0x07` are PING/PONG (see
"Connection identity").

### Topic membership updates

When a `Channel` is opened against the mux post-hello, the mux
sends `TOPIC_ADD` for the topic name to every ready peer. When
closed: `TOPIC_REMOVE`. Peers update their per-peer membership set
on receipt.

Unknown-topic DATA frames received by the local mux are a protocol
violation (the sender should have filtered). The receiver
**increments a per-mux counter and logs at error level**; no silent
drop. The frame is discarded.

## Hello vs Channel-open reconciliation

`Mesh.Channel` is safe to call concurrently with connection setup
and with itself.

Invariant: every locally-held topic reaches every ready peer
exactly once, via hello or via TOPIC_ADD.

The invariant is maintained without holding any lock across network
I/O:

1. **Setup goroutine:** dial/accept → exchange hellos (no lock)
   → acquire `tcpmesh.openMu` → compare current locally-held topic set
   against the snapshot encoded in our outbound hello → record any
   "missed" topics (added between hello-send and now) into the
   per-peer pending-advertise set → mark connection as ready →
   release lock → enqueue the pending TOPIC_ADD frames on the
   peer's control queue.

2. **Mesh.Channel:** acquire `tcpmesh.openMu` → add topic to mux map
   → enumerate current ready peers → for each, record the topic
   into that peer's pending-advertise set → release lock → enqueue
   TOPIC_ADD frames on each peer's control queue.

Both paths only mutate small maps under `openMu`; network writes
happen on each peer's single writer goroutine. A slow peer cannot
block topic opens, other peers, or the enqueuing caller (see
"Outbound delivery" for the queue semantics).

## Outbound delivery

Each ready peer owns **one writer goroutine** fed by two bounded
queues:

- a **data queue** for DATA frames, bounded by frame count and by
  total queued bytes;
- a **control queue** (bounded priority allowance) for TOPIC_ADD /
  TOPIC_REMOVE, PING / PONG, and hello-reconcile frames. The writer
  always drains control frames first.

`Broadcast` encodes the frame once, enqueues it on every
topic-interested peer's data queue, and returns immediately —
"accepted for dispatch," never blocked on a peer's socket. When a
peer's data queue is full (either bound), the frame is **dropped
and counted** for that peer — the documented inbound
drop-and-count model applied outbound. The receiver's catch-up
chain recovers dropped seqs via per-(origin, seq) idempotency;
overflow to a live-but-slow peer therefore raises that peer's
repair latency to the fetcher cadence, and the byte/frame bounds
are sized so only a genuinely stalled or badly lagging peer hits
them.

Control frames are never silently dropped — but "never dropped"
must not mean unbounded growth or indefinite blocking: a peer that
cannot accept a control frame promptly (allowance full past a
short enqueue deadline) is **retired** and redialed, which
resynchronizes topic membership via the fresh hello anyway. The
writer's per-frame write deadline (`WriteTimeout`) remains the
backstop that turns a wedged socket into a retirement.

Ordering: the writer preserves enqueue order within each queue,
but a control frame may overtake data frames queued ahead of it.
That is benign by construction: membership updates are idempotent,
DATA received for a topic the receiver holds open is applied
normally, and DATA for a topic it doesn't hold open is counted as
unknown-topic (the existing rule). Per-(origin, seq) dedupe makes
replay and reorder harmless.

Retirement drops whatever remains in both queues; the redial's
hello rebuilds membership and the catch-up chain repairs the data
gap.

## One-shot ops

One-shot request/response endpoints share the mesh listener with
gossip, dispatched by first byte (see "Listener dispatch"). Every
request starts with a 1-byte op and a topic prefix:

```
client → server:
  byte    op
  u16 BE  topicLen
  N bytes topic               // required and non-empty
  (op-specific request body)
```

| Op | Path |
|----|------|
| `0x00` | bundle clone stream — serves the channel's `transport.BundleHandler` (full-database bootstrap) |
| `0x01` | catchup request/response (see "Peer catchup") |
| `0x02` | coordinated-uniqueness RPC — after an OK status the connection is handed raw to the channel's registered unique handler, which drives `net/rpc` over it ([`tcpmesh/unique.go`](../tcpmesh/unique.go)) |
| `0x03` | frontier query (see "Peer frontiers") |

Riding the already-peer-connected, firewall-open mesh port means
followers reach a uniqueness leaseholder — and peers pull frontiers
and clone bundles — with no new port: a peer is dialed for one-shot
ops at the same address it gossips on.

Server-side dispatch is uniform across ops: read op byte and topic,
look up the topic's channel, grab its registered handler
(`SetBundleHandler`, `SetCatchupSource`, `SetFrontierSource`,
`SetUniqueHandler` are per-Channel), respond with a status-prefixed
stream. The server allows 10s for the full request prefix+body
before timing out an idle client. Client-side dial is equally
uniform: one helper dials, applies TLS and the deadline, writes the
op + topic prefix (and op-specific body), and decodes the status
byte; ops contribute only their body codecs.

### Response status byte

Every one-shot response is **prefixed by a single status
byte**, before any payload:

| Code | Meaning |
|------|---------|
| `0x00` | OK — payload follows, terminated by clean EOF |
| `0x01` | unknown-topic — topic not registered on this mux |
| `0x02` | no-handler — topic registered but no handler/source set |
| `0x03` | unassigned (a closed channel deregisters its topic and answers `0x01`) |
| `0x04` | bad-request — malformed request body |
| `0x05` | internal-error — server-side serve loop returned error |

On non-zero status the server closes the connection immediately;
no further bytes follow. Clients translate non-zero statuses to a
typed error (`tcpmesh.BundleError`) so `PeerGapFiller` can fail over to
the next topic-holding peer instead of treating refusal as empty
success.

## Peer catchup (op `0x01`)

A write that misses some peers via live `Transport.Broadcast` used
to re-converge only through the object-store path: sealer batching
plus the fetcher's poll cadence could add up to minutes. Peer-pull
catchup short-circuits both: when a peer attaches, the gap planner
re-plans against known missing ranges and pulls them directly from
that peer. It works without an object store at all — a pure
mesh-only cluster converges via peers alone — and adds no
persistent state: the receiving apply path is idempotent on
`(origin, seq)`, so re-requesting is free.

### Request body (after op + topic prefix)

```
u32 BE  nRanges
nRanges × { u64 BE origin, u64 BE lo, u64 BE hi }   // hi=0 → open-ended
u32 BE  maxRecords                  // 0 = unbounded
u64 BE  maxBytes                    // 0 = unbounded
```

Caps: `nRanges ≤ catchupwire.MaxRanges` (1024,
`tcpmesh/internal/catchupwire`), rejected oversize with status
`0x04 bad-request`.

### Response stream

Single status byte, then zero or more length-prefixed payload
frames, then clean EOF:

```
byte    status
[ if status == 0x00:
    one or more frames:
      u32 BE  len
      bytes   changeset payload (canonical Changeset wire format)
    then clean EOF when ranges exhausted or caps reached
]
```

Payload bytes use the canonical [changeset protocol](PROTOCOL.md). Clients route
each through `transport.ApplyFunc`; Syzy dedupes on `(origin, seq)`.

### Error handling

- Bad request body: server writes status `0x04`, closes.
- Server-side scan error mid-stream: server closes the connection
  after partial bytes; client treats received frames as
  best-effort. (Status byte was already `0x00` — the server can't
  retract it.) The client reads EOF at a frame boundary — clean or
  truncating a frame *header* — as end-of-stream, not an error;
  whatever frames arrived were already applied.
- Connection failure mid-frame (dial error, read error, payload
  truncation): `PeerGapFiller` records the error and falls through
  to the next peer; see "Client side" for what `Fetch` ultimately
  returns and how the consumer's gap-filler chain proceeds.

### Server side: `transport.CatchupSource`

```go
type CatchupSource interface {
    Serve(ctx context.Context, req CatchupRequest, write func(payload []byte) error) error
}
```

Registered per-Channel via `Channel.SetCatchupSource`.
`internal/mirror.Manager` is the canonical implementation: it owns
per-origin journals of every changeset the node has produced or
applied, and `Serve` streams the payloads matching the requested
`(origin, seq)` ranges — see
[`internal/mirror/serve.go`](../internal/mirror/serve.go).
`MaxRecords` and `MaxBytes` bound the scan; the gap planner
caps its own per-round request size well below the server caps.

### Client side: `tcpmesh.PeerGapFiller`

`PeerGapFiller` ([`tcpmesh/catchup.go`](../tcpmesh/catchup.go))
implements `transport.GapFiller` over the channel's peer set: it
ranks ready topic-holding peers by ascending RTT (via
`PeerStatter`; zero-RTT peers sort last) and asks the best peer —
dialed at its gossip address — for the missing ranges. It falls
through to the next peer on
dial/transport error, non-zero status, or a clean-EOF response
that delivered zero frames (the peer answered but held none of
the requested ranges). Peers without the topic open are never
dialed.

`Fetch` returns nil as soon as one peer delivers at least one
frame. Once all peers are exhausted it returns the first recorded
error; when the only failures were zero-frame successes, that
error wraps `transport.ErrUnfilled` — the typed "clean empty"
  marker (every source answered, none held the ranges). Callers
  that *expect* emptiness (an unserveable-range probe)
demote only `ErrUnfilled` to INFO; substantive dial/decode/apply
errors keep WARN visibility. Either way the receiver dedupes by
`(origin, seq)`, so re-requesting the same ranges on the next
round is free.

### Composition with the object store

```go
gapFiller := gapfillerchain.New(peerFiller, s3Filler)
```

`gapfillerchain` tries each filler in order, tracking which
`(origin, seq)`s each one delivered and forwarding only the
still-missing sub-ranges to the next filler (stopping early once
nothing remains). A filler error does not abort the chain; the
chain returns nil when every requested range was filled, else the
last filler's error. The per-round planner re-requests
anything still missing on the next round.

S3 stays the durable, long-horizon path: a long-offline returnee
that exceeds peer journal retention, or a deployment with no peer
catchup available, converges via S3 alone.

### Connect-time trigger

`Channel.SetOnPeerConnect` installs a callback that fires when a
peer becomes ready for this topic — when the channel observes the
topic in a peer's hello, or receives a `TOPIC_ADD` from a ready
connection. Syzy wires it to `broker.KickFetcher`, which
non-blockingly wakes anti-entropy so the next iteration pulls whatever live
broadcast missed.
The callback is per channel and therefore per logical database topic.

Peer catchup only re-plans *known* missing ranges. Learning that an
unfamiliar origin exists is a `TipSource` concern — see "Peer
frontiers".

## Peer frontiers (op `0x03`)

A frontier query returns the responding node's applied-frontier for
the topic: per origin, the highest contiguous applied seq. The
request has no body beyond the op + topic prefix; the response is:

```
byte    status     (0x00 OK; non-zero per the status table)
[ if status == 0x00:
    u32 BE  count                       // capped at 2^20
    count × { u64 BE origin, u64 BE seq }
]
```

`tcpmesh.PeerFrontierSource`
([`tcpmesh/frontier.go`](../tcpmesh/frontier.go))
queries every connected peer and aggregates:

- `DiscoverTips` (satisfies `transport.TipSource`): the highest seq
  any peer holds per origin, so the fetcher discovers
  origins this node never received live — from the mesh in seconds,
  with the object-store TipSource merged in as backstop.
- `AllPeersApplied(origin, head)`: whether every currently-known
  peer holds the origin fully — the reaper's GC-safety predicate
  (see [PRUNING.md](PRUNING.md#peer-frontiers)). False when no
  peers are known.

A peer that errors or refuses the op is simply absent from the
refreshed cache, so a departed peer stops constraining
`AllPeersApplied`. The server side is registered per-Channel via
`SetFrontierSource`; a nil source answers `0x02 no-handler`.

For health surfaces, `Observations()` reports one entry per
**currently connected** topic-holding peer: its last-fetched
frontier, the observation's age, and its state — `ok`, `error`
(last refresh failed), or `unknown` (connected but not yet
queried). Connected peers with unknown or errored observations MUST
appear with that state, never be omitted — omission would turn
fetch failures into false-healthy readings, the exact bug class
this API exists to kill. Consumers judge staleness from the age.
`sqlite.Node.PeerFrontiers()` exposes this directly (the idle-safe
replication-lag signal: peer frontier vs local applied, meaningful
even on topics with no heartbeat writer).

## Endpoint URLs (clone and lease addresses)

A channel's peer-dialable endpoint is a URL over the mesh's one
advertised address plus the topic:

```
tcp://host:port?topic=app
unix:///abs/path.sock?topic=app
```

`Channel.Endpoint()` returns it (empty when the mesh has no
listener). The address is `Config.Advertise` when set (wildcard
binds behind 1:1 NAT), else the listener's own address — there is
no port arithmetic; the same port serves gossip and every one-shot
op. Clone URLs and uniqueness-lease records both carry this form.

`sqlite.Restore` parses these URLs with `url.Parse`, extracts
`topic` from the query (defaulting to `syzy.DefaultTopic` when
absent, matching single-database daemons), and threads it through
`tcpmesh.FetchBundle`. The dialer reconstructs `tcp://host:port`
(or `unix:/path`) without the query for the actual connect.

`Channel.Fetcher()` is a convenience that pre-binds the topic. When
serializing the URL form (for `cluster_inventory.bundle_addr` etc.),
the channel includes `?topic=...`.

## Topic membership semantics

- A Channel is "open" between `Mesh.Channel` and `Close`. Opening
  emits `TOPIC_ADD` to all currently-ready peers; closing emits
  `TOPIC_REMOVE`.
- A peer is "interested in" a topic if its hello included the topic
  OR if it sent a subsequent `TOPIC_ADD` for it. The mux maintains
  this set per connection.
- `Broadcast` for topic X iterates current ready peers, enqueuing
  the DATA frame only to peers interested in X (see "Outbound
  delivery"). Peers not interested receive no payload bytes for X.
- `Channel.PeerStats()` returns only ready peers currently
  advertising the channel's topic. `PeerGapFiller` uses this
  filtered list, so it never dials a peer that would refuse with
  status 0x01 unknown-topic. Each entry also carries the peer's
  outbound queue depth (frames and bytes) and its outbound drop
  count; mesh-wide `Stats` adds total outbound drops and peer
  retirements.
- New connection (inbound or outbound) starts with empty
  membership on both sides; hellos populate it. Until both hellos
  exchanged AND collision reconciled, no DATA frames are sent on
  that connection.

## Interface shape

### `tcpmesh`

One `*tcpmesh.Mesh` per process owns the listener and per-seed
dials; `Channel(topic)` returns the per-topic `*tcpmesh.Channel`.
Mesh.Channel is safe concurrently and idempotent per topic. The
listener either binds `Config.Listen` or is injected caller-owned
via `Config.Listener` (FD inheritance across re-exec, socket
activation, custom socket options). Full API in
[`tcpmesh/mesh.go`](../tcpmesh/mesh.go).

Each Channel owns a bounded inbound deliver queue, populated by the
mux demux loop when a DATA frame for its topic arrives. When the
queue is full, frames are dropped and counted; the catch-up
chain recovers the dropped seqs via per-(origin,seq) idempotency.

### Optional capability interfaces in `transport/`

Runtime wiring does not switch on concrete transport implementations. It
discovers capabilities through small optional interfaces in
`transport/transport.go` that `*tcpmesh.Channel` satisfies.

```go
type CatchupRegistrar    interface{ SetCatchupSource(CatchupSource) }
type PeerConnectNotifier interface{ SetOnPeerConnect(func()) }
type PeerCatchupBuilder  interface{ PeerCatchupBuilder() GapFiller }
type FrontierRegistrar   interface{ SetFrontierSource(FrontierSource) }
type PeerFrontierBuilder interface{ PeerFrontierBuilder() PeerFrontier }
```

- `CatchupRegistrar` — install the node's `CatchupSource`
  (`mirror.Manager`) for peers to pull from; nil unregisters.
- `PeerConnectNotifier` — callback when a peer becomes interested
  in this transport's scope (per-topic under mux); Syzy uses it to wake the
  gap fetcher.
- `PeerCatchupBuilder` — construct the peer-pull `GapFiller`
  (`tcpmesh.PeerGapFiller`); peers are dialed for catchup at their
  gossip address.
- `FrontierRegistrar` — install the `FrontierSource` (the node's
  applied-frontier map) that answers peers' frontier queries; nil
  refuses them.
- `PeerFrontierBuilder` — construct the `PeerFrontier` aggregate
  (`tcpmesh.PeerFrontierSource`) over connected peers.

Bundle clone serving is discovered the same way via
`transport.BundleSource` (`SetBundleHandler` + `Endpoint`,
`transport/bundle.go`), which `*tcpmesh.Channel` also satisfies;
`Channel.Fetcher()` pre-binds the topic into a `transport.BundleFetcher`.
Unlike the other capabilities, clone serving is opt-in: `sqlite.Open`
registers the node's bundle producer only when `Config.ServeClones` is
set, and fails closed if it is set on a Transport that is not a
`BundleSource`.
Each interface has compile-time `var _ X = (*Channel)(nil)`
assertions in `tcpmesh` (`tcpmesh.go`, `frontier.go`) so drift
fails at build time, not at runtime.

### `tcpmesh`

[`tcpmesh`](../tcpmesh/mesh.go) hosts one `*Mesh` per process;
`Mesh.Channel(topic)` returns the per-topic `*Channel` that satisfies
`transport.Transport` plus the capability interfaces above. Every
topic shares the mesh's one advertised address (`Addr()`);
`Channel.Endpoint()` is the URL form with the topic. `SetSeeds`
supports overlay-driven seed refresh. Wire helpers shared between
the gossip and one-shot paths live in `tcpmesh/internal/catchupwire`
and `tcpmesh/internal/peerrtt`.

## Boundaries

- One TCP connection per peer pair — no per-pair pooling, no QUIC.
  Fan-out behind the same channel API is the escape hatch if
  head-of-line blocking ever becomes a real problem.
- No wire-format backward compatibility. Wire changes cut over with
  a coordinated restart (or accept delayed object-store catchup
  during a rolling restart).
- Authentication is whatever `Config.TLSConfig` provides (mTLS
  gates connection establishment); there are no per-topic ACLs.
- Peer catchup does not replace the object-store path: `s3fetch`,
  the sealer, and the per-topic schema log remain the durable,
  long-horizon story.

## References

- `transport/transport.go` — `Transport`, `CatchupRequest`,
  `CatchupSource`, `GapFiller`, `TipSource`, `PeerStatter`,
  `FrontierSource`, `PeerFrontier`, and the optional capability
  interfaces above.
- `transport/bundle.go` — `transport.BundleSource` / `transport.BundleHandler` /
  `transport.BundleFetcher` interfaces.
- `tcpmesh/catchup.go`, `frontier.go`, `unique.go` — one-shot op
  implementations; `tcpmesh/internal/catchupwire` — the catchup
  body codec.
- `internal/mirror/serve.go` — journal-backed `CatchupSource`.
- `internal/broker/broker.go` — SQLite `KickFetcher` and fetcher loop.
- `internal/gapfillerchain/` — `Chain` composing peer + S3.
- [ARCHITECTURE.md](ARCHITECTURE.md) — distribution and anti-entropy
  responsibilities.
- [PRUNING.md](PRUNING.md) — how `AllPeersApplied` gates journal
  reaping.
