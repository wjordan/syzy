// Command syzy-pg is the Postgres sidecar: one process per Postgres node
// that wires postgres.Engine into a TCP transport so peers running their
// own syzy-pg + Postgres pair converge under syzy's CRDT model.
//
// One sidecar per Postgres database; one Postgres database per node.
// Peers are configured by gossip address (-seeds). Cluster identity is a
// 32-hex string shared across the cluster; replication-origin and slot
// names are derived from -origin so a slot collision means a config
// collision.
//
// See docs/postgres.md for the engine model.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/gapfillerchain"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/mirror"
	"github.com/wjordan/syzy/internal/nodestate"
	"github.com/wjordan/syzy/internal/s3fetch"
	"github.com/wjordan/syzy/internal/sealer"
	"github.com/wjordan/syzy/pg/internal/postgres"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/tcpmesh"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/unique"
)

const usage = `syzy-pg — Postgres sidecar for the syzy CRDT engine

Usage:
  syzy-pg [-flags]

Required:
  -conn URL          Postgres connection URL (e.g. postgres://user:pw@host:5432/db)
  -origin N          this node's origin id (1..65535; unique per cluster, never reused).
                     Also slices the bigint id space, so auto-increment PKs
                     (bigserial, IDENTITY) minted here cannot collide with a peer's
  -cluster-id HEX    32-character hex cluster id, shared across peers
  -data-dir PATH     local state: metadata, self-log, mirror

Networking:
  -listen ADDR       TCP bind for the peer mesh (e.g. :7000); one multiplexed
                     connection per peer carries gossip, changesets, and
                     catch-up — no second port
  -seeds A,B,C       peer addresses to dial (optional; learned at runtime via gossip)
  -topic NAME        mesh channel name; peers must agree (default: syzy-pg)
  -tls-cert FILE     PEM certificate for mesh mTLS (with -tls-key; both listeners
                     and outbound dials)
  -tls-key FILE      PEM private key for -tls-cert
  -tls-ca FILE       PEM CA bundle: verifies peer certs (client and server side)
  -insecure          acknowledge plaintext TCP beyond loopback (no -tls-*)

Postgres requirements: wal_level=logical, and max_replication_slots /
max_wal_senders sized for the cluster (one slot per sidecar; the PG default
of 10 is too low for real fleets — 64 is a sane floor).

Object storage (optional but recommended for production):
  -bucket URL        blobstore URL (s3://... or file://...) this node seals its
                     changeset history to. Enables offline-peer catch-up from
                     the bucket and bounded local journals (self-log + mirror
                     GC follow the sealed tip).

Replication scope:
  -tables T1,T2      schema-qualified replicated set (e.g. public.kv,public.items)
                     mutually-exclusive with -ddl
  -ddl               enable DDL replication (-tables empty; tables created by cluster DDL)
  -adopt             publish the rows that already existed in this database, once,
                     so an existing database can join a cluster. Idempotent (a
                     durable marker records that it ran); safe to leave set.

DDL schema log (with -ddl, exactly one source is required):
  -schema-log PATH   local schemalog file (single-host, or shared filesystem)
  -schema-log-dial ADDR  TCP addr of a peer hosting the schema log (follower)
  -schema-log-s3 URL     S3 bucket URL for the schema log (multi-host, no shared FS)
                         AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from env
  -schema-log-listen ADDR  also host this node's -schema-log file over TCP for
                           peers to -schema-log-dial (requires -schema-log)

Identity overrides (defaults derived from -conn dbname):
  -publication NAME  Postgres publication name (default: syzy_pub)
  -slot NAME         Postgres replication slot name (default: syzy_slot_<dbname>)
  -origin-name NAME  Postgres replication origin name (default: syzy_origin_<dbname>)

Other:
  -repl-conn URL     replication=database URL (default: -conn + "?replication=database")
  -checkpoint-every N  commits between durable checkpoints (default: engine default)
`

func main() {
	// One-shot subcommands dispatch before the daemon flag parse.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "status":
			if err := runStatus(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "syzy-pg: %v\n", err)
				os.Exit(1)
			}
			return
		case "-genid":
			id, err := genClusterID()
			if err != nil {
				fmt.Fprintf(os.Stderr, "syzy-pg: genid: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(id)
			return
		}
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "syzy-pg: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}

	meta, err := metadata.Open(filepath.Join(cfg.dataDir, "meta.db"))
	if err != nil {
		return fmt.Errorf("metadata open: %w", err)
	}
	defer meta.Close()
	if err := meta.SetClusterID(cfg.clusterID); err != nil {
		return fmt.Errorf("set cluster id: %w", err)
	}

	mgr, err := mirror.New(mirror.Config{Root: filepath.Join(cfg.dataDir, "mirror")})
	if err != nil {
		return fmt.Errorf("mirror open: %w", err)
	}
	defer mgr.Close()
	if err := mgr.LoadExisting(); err != nil {
		return fmt.Errorf("mirror load: %w", err)
	}

	var schemaLog schemalog.Log
	if cfg.ddl {
		sl, closer, err := schemalog.Dial(cfg.schemaLogPath, cfg.schemaLogDial, cfg.schemaLogS3)
		if err != nil {
			return fmt.Errorf("schema log: %w", err)
		}
		if closer != nil {
			defer closer.Close()
		}
		if cfg.schemaLogListen != "" {
			srv, err := schemalog.ListenTCP(cfg.schemaLogListen, sl)
			if err != nil {
				return fmt.Errorf("schema log listen %q: %w", cfg.schemaLogListen, err)
			}
			defer srv.Close()
			fmt.Fprintf(os.Stderr, "syzy-pg: hosting schema log on %s\n", cfg.schemaLogListen)
		}
		schemaLog = sl
	}

	cache := nodestate.New(cfg.origin)

	// One multiplexed mesh connection per peer carries every topic —
	// gossip, changeset broadcast, catch-up, and bundle transfer share
	// the single listener, so there is no second port to open.
	tlsConf, err := buildTLS(cfg)
	if err != nil {
		return err
	}
	mesh, err := tcpmesh.New(tcpmesh.Config{
		Listen:    cfg.listen,
		Seeds:     cfg.seeds,
		TLSConfig: tlsConf,
		Insecure:  cfg.insecure,
	})
	if err != nil {
		return fmt.Errorf("mesh transport: %w", err)
	}
	defer mesh.Close()
	ch, err := mesh.Channel(cfg.topic)
	if err != nil {
		return fmt.Errorf("open mesh channel %q: %w", cfg.topic, err)
	}

	// Object storage (optional): the sealer uploads this origin's changeset
	// epochs; s3fetch serves them back as tips + gap-fill for any peer (or a
	// restarted self) below the live/mirror horizon. GC of the self-log and
	// mirrors follows the sealed tip — without a bucket both are retained
	// unboundedly, because a truncated journal would strand an offline peer.
	gapFiller := ch.PeerCatchupBuilder()
	var tipSource transport.TipSource
	var onPublished func([]byte)
	var sealedSelfSeq func() crdt.Seq
	var coordBucket objectstore.Bucket
	if cfg.bucketURL != "" {
		be, err := objectstore.Open(context.Background(), cfg.bucketURL)
		if err != nil {
			return fmt.Errorf("objectstore.Open %s: %w", cfg.bucketURL, err)
		}
		sl := sealer.New(be, sealer.Config{Logf: func(format string, args ...any) { fmt.Fprintf(os.Stderr, "syzy-pg: sealer: "+format+"\n", args...) }})
		sealerDone := make(chan struct{})
		go func() { defer close(sealerDone); _ = sl.Run(context.Background()) }()
		defer func() { sl.Stop(); <-sealerDone }()
		src := s3fetch.NewSource(be)
		tipSource = src
		gapFiller = gapfillerchain.New(gapFiller, src)
		onPublished = sl.OnEncoded
		origin := cfg.origin
		sealedSelfSeq = func() crdt.Seq { return crdt.Seq(sl.UploadedSeq(uint64(origin))) }
		coordBucket = be
	}

	ctx, cancel := signalContext()
	defer cancel()

	// Coordinated uniqueness (docs/postgres.md §7). The leaseholder derives
	// the taken-set by enumerating this node's own rows, so it needs the
	// engine — which in turn needs the registry. The engine pointer is
	// filled in below, before anything can tick.
	var eng *postgres.Engine
	var uniqueReg unique.Registry
	var leaseholder *unique.Leaseholder
	if coordBucket != nil {
		leaseStore := unique.OpenLease(coordBucket, "unique/lease")
		leaseholder = unique.NewLeaseholder(unique.LeaseholderConfig{
			Store: leaseStore,
			// A clean shutdown publishes the taken-set so a successor
			// serves immediately instead of waiting out a failover drain.
			Handoff:   unique.OpenHandoff(coordBucket, "unique/handoff"),
			Owner:     fmt.Sprintf("pg-%d", cfg.origin),
			Transport: ch.UniqueServeTransport(),
			Enumerate: func(ctx context.Context) (unique.Snapshot, error) {
				if eng == nil {
					return unique.Snapshot{}, nil
				}
				return eng.EnumerateCoordinated(ctx)
			},
		})
		if err := leaseholder.Start(); err != nil {
			return fmt.Errorf("start unique leaseholder: %w", err)
		}
		defer leaseholder.Close()
		// Co-locate: when this node holds the lease, reserve in-process
		// rather than dialing the address published for remote peers.
		uniqueReg = unique.NewLeaseClientTransport(leaseStore, ch.UniqueDialTransport()).
			UseLocalLeaseholder(leaseholder)
	}

	eng, err = postgres.Open(ctx, postgres.Config{
		Name:              cfg.dbName,
		Origin:            cfg.origin,
		Cluster:           cfg.clusterID,
		Cache:             cache,
		ConnURL:           cfg.connURL,
		ReplConnURL:       cfg.replConnURL,
		Publication:       cfg.publication,
		Slot:              cfg.slot,
		OriginName:        cfg.originName,
		Tables:            cfg.tables,
		Meta:              meta,
		JournalDir:        filepath.Join(cfg.dataDir, "selflog"),
		Mirror:            mgr,
		GapFiller:         gapFiller,
		TipSource:         tipSource,
		OnPublished:       onPublished,
		SealedSelfSeq:     sealedSelfSeq,
		CoordinatedUnique: coordBucket != nil,
		Registry:          uniqueReg,
		ReserveSocketDir:  filepath.Join(cfg.dataDir, "reserve"),
		DDL:               cfg.ddl,
		Adopt:             cfg.adopt,
		// The origin id is already a cluster-unique, never-reused 1..65535
		// node number, which is exactly what the id-space slice needs — so it
		// doubles as the ordinal and auto-increment PKs (bigserial, IDENTITY)
		// minted on different nodes cannot collide. No separate claim
		// protocol, and no ordinal 0 (its low range stays reserved for the
		// node that created a table through replicated DDL).
		NodeOrdinal:     uint16(cfg.origin),
		SchemaLog:       schemaLog,
		CheckpointEvery: cfg.checkpointEvery,
	})
	if err != nil {
		return fmt.Errorf("postgres engine open: %w", err)
	}
	defer eng.Close()

	// Register the engine's catchup source so peers asking for missed
	// (origin, seq) ranges over the TCP catchup endpoint reach the
	// self-log (own bytes) + mirror (peer bytes).
	if src := eng.CatchupSource(); src != nil {
		ch.SetCatchupSource(src)
	}

	// Drive the lease loop now that the engine can answer enumeration. The
	// goroutine is joined before the engine closes, so the last enumeration
	// never races the connection teardown.
	if leaseholder != nil {
		leaseDone := make(chan struct{})
		go func() {
			defer close(leaseDone)
			leaseholder.RunMaintenance(ctx)
		}()
		defer func() { <-leaseDone }()
	}

	inbox := make(chan *crdt.Changeset, 128)
	broadcast := func(ctx context.Context, cs *crdt.Changeset) error {
		return ch.Broadcast(ctx, cs.Encoded())
	}

	// Subscribe loop: decode peer payloads, filter own-origin loopback
	// (rare but possible on self-mesh test setups), push to inbox.
	subDone := make(chan error, 1)
	go func() {
		subDone <- ch.Subscribe(ctx, func(ctx context.Context, payload []byte) error {
			cs, err := crdt.Decode(payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "syzy-pg: decode payload: %v\n", err)
				return nil // drop one bad frame; keep loop alive
			}
			if cs.Dot.Origin == cfg.origin {
				return nil
			}
			select {
			case inbox <- cs:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	fmt.Fprintf(os.Stderr, "syzy-pg: node origin=%d db=%s listen=%s seeds=%v\n",
		cfg.origin, cfg.dbName, cfg.listen, cfg.seeds)

	runErr := eng.Run(ctx, inbox, broadcast)
	// Ensure the subscribe goroutine winds down too.
	cancel()
	<-subDone
	// Capture's replication connection closed as Run returned; give PG a brief
	// moment to mark the slot inactive so a quick supervisor restart doesn't
	// trip "slot is active for PID X". Best-effort — eng.Close (deferred) keeps
	// the slot as the durable resume position and only closes the apply conn.
	time.Sleep(100 * time.Millisecond)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return fmt.Errorf("engine run: %w", runErr)
	}
	return nil
}

type sidecarConfig struct {
	connURL         string
	replConnURL     string
	origin          crdt.Origin
	clusterID       crdt.ClusterID
	dataDir         string
	listen          string
	seeds           []string
	topic           string
	insecure        bool
	tables          []string
	ddl             bool
	adopt           bool
	schemaLogPath   string
	schemaLogDial   string
	schemaLogS3     string
	schemaLogListen string
	publication     string
	slot            string
	originName      string
	dbName          string
	checkpointEvery int
	bucketURL       string
	tlsCert         string
	tlsKey          string
	tlsCA           string
}

func parseFlags(args []string) (*sidecarConfig, error) {
	fs := flag.NewFlagSet("syzy-pg", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(fs.Output(), usage) }

	connURL := fs.String("conn", "", "")
	replConnURL := fs.String("repl-conn", "", "")
	origin := fs.Uint("origin", 0, "")
	clusterHex := fs.String("cluster-id", "", "")
	dataDir := fs.String("data-dir", "", "")
	listen := fs.String("listen", "", "")
	seeds := fs.String("seeds", "", "")
	topic := fs.String("topic", "syzy-pg", "")
	insecure := fs.Bool("insecure", false, "")
	tables := fs.String("tables", "", "")
	ddl := fs.Bool("ddl", false, "")
	adopt := fs.Bool("adopt", false, "")
	schemaLog := fs.String("schema-log", "", "")
	schemaLogDial := fs.String("schema-log-dial", "", "")
	schemaLogS3 := fs.String("schema-log-s3", "", "")
	schemaLogListen := fs.String("schema-log-listen", "", "")
	publication := fs.String("publication", "syzy_pub", "")
	slot := fs.String("slot", "", "")
	originName := fs.String("origin-name", "", "")
	checkpointEvery := fs.Int("checkpoint-every", 0, "")
	bucketURL := fs.String("bucket", "", "")
	tlsCert := fs.String("tls-cert", "", "")
	tlsKey := fs.String("tls-key", "", "")
	tlsCA := fs.String("tls-ca", "", "")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *connURL == "" {
		return nil, errors.New("-conn required")
	}
	if *origin == 0 || *origin > 0xFFFF {
		return nil, fmt.Errorf("-origin must be in 1..65535 (got %d)", *origin)
	}
	if *clusterHex == "" {
		return nil, errors.New("-cluster-id required (32-char hex; use `syzy-pg -genid` to generate)")
	}
	if *dataDir == "" {
		return nil, errors.New("-data-dir required")
	}
	if *ddl && *tables != "" {
		return nil, errors.New("-ddl and -tables are mutually exclusive")
	}
	if !*ddl && *tables == "" {
		return nil, errors.New("either -tables or -ddl is required")
	}
	schemaSources := 0
	for _, v := range []string{*schemaLog, *schemaLogDial, *schemaLogS3} {
		if v != "" {
			schemaSources++
		}
	}
	if *ddl {
		if schemaSources == 0 {
			return nil, errors.New("with -ddl, one of -schema-log / -schema-log-dial / -schema-log-s3 is required")
		}
		if schemaSources > 1 {
			return nil, errors.New("at most one of -schema-log / -schema-log-dial / -schema-log-s3 may be set")
		}
		if *schemaLogListen != "" && *schemaLog == "" {
			return nil, errors.New("-schema-log-listen requires -schema-log (only a local file backend can be hosted)")
		}
	} else if schemaSources > 0 || *schemaLogListen != "" {
		return nil, errors.New("schema-log flags require -ddl")
	}

	clusterBytes, err := hex.DecodeString(*clusterHex)
	if err != nil || len(clusterBytes) != 16 {
		return nil, fmt.Errorf("-cluster-id must be 32 hex characters")
	}
	var clusterID crdt.ClusterID
	copy(clusterID[:], clusterBytes)

	dbName, err := extractDBName(*connURL)
	if err != nil {
		return nil, err
	}

	cfg := &sidecarConfig{
		connURL:         *connURL,
		replConnURL:     valueOr(*replConnURL, defaultReplURL(*connURL)),
		origin:          crdt.Origin(*origin),
		clusterID:       clusterID,
		dataDir:         *dataDir,
		listen:          *listen,
		seeds:           splitCSV(*seeds),
		topic:           *topic,
		insecure:        *insecure,
		tables:          splitCSV(*tables),
		ddl:             *ddl,
		adopt:           *adopt,
		schemaLogPath:   *schemaLog,
		schemaLogDial:   *schemaLogDial,
		schemaLogS3:     *schemaLogS3,
		schemaLogListen: *schemaLogListen,
		publication:     *publication,
		slot:            valueOr(*slot, "syzy_slot_"+dbName),
		originName:      valueOr(*originName, "syzy_origin_"+dbName),
		dbName:          dbName,
		checkpointEvery: *checkpointEvery,
		bucketURL:       *bucketURL,
		tlsCert:         *tlsCert,
		tlsKey:          *tlsKey,
		tlsCA:           *tlsCA,
	}
	if (cfg.tlsCert == "") != (cfg.tlsKey == "") {
		return nil, errors.New("-tls-cert and -tls-key must be set together")
	}
	return cfg, nil
}

// extractDBName pulls the database name from a Postgres URL's path.
// "postgres://user:pw@host:5432/mydb?opts" → "mydb".
func extractDBName(connURL string) (string, error) {
	u, err := url.Parse(connURL)
	if err != nil {
		return "", fmt.Errorf("-conn: parse: %w", err)
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return "", errors.New("-conn must include a database name in the path")
	}
	if strings.ContainsAny(db, "/?#") {
		return "", fmt.Errorf("-conn database name has unexpected characters: %q", db)
	}
	return db, nil
}

// defaultReplURL appends ?replication=database to connURL (or &replication=...
// when other query params are present).
func defaultReplURL(connURL string) string {
	if strings.Contains(connURL, "?") {
		return connURL + "&replication=database"
	}
	return connURL + "?replication=database"
}

func valueOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// signalContext returns a context cancelled by SIGINT or SIGTERM. Call the
// returned cancel during normal shutdown to unblock the signal goroutine
// (it exits on either a signal or ctx cancellation, so it never leaks).
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			fmt.Fprintf(os.Stderr, "syzy-pg: received %s, shutting down\n", sig)
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(sigCh)
	}()
	return ctx, cancel
}

// genClusterID returns a fresh 32-char hex cluster id. Surfaced via
// `syzy-pg -genid` for operators bootstrapping a new cluster. An error from
// the CSPRNG is propagated so a failed read never yields the all-zero id.
func genClusterID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// buildTLS assembles the mesh mTLS config from the -tls-* flags: node cert +
// key for both listeners and outbound dials, and the cluster CA for verifying
// peers in both directions. nil (plaintext) when no flags are given.
func buildTLS(cfg *sidecarConfig) (*tls.Config, error) {
	if cfg.tlsCert == "" && cfg.tlsCA == "" {
		return nil, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.tlsCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey)
		if err != nil {
			return nil, fmt.Errorf("tls keypair: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if cfg.tlsCA != "" {
		pem, err := os.ReadFile(cfg.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls ca: no certificates in %s", cfg.tlsCA)
		}
		tc.RootCAs = pool
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, nil
}
