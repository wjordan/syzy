package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/clusterurl"
	"github.com/wjordan/syzy/internal/ctrlsock"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
	"github.com/wjordan/syzy/internal/peerdisc"
	"github.com/wjordan/syzy/internal/syzylog"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
	"github.com/wjordan/syzy/transport"
	"github.com/wjordan/syzy/wake"
	"github.com/wjordan/syzy/wake/vsock"
)

// Default TCP ports for s3:// clusters (across-host deployments where
// firewalls expect a known port). For file:// clusters, the daemon
// defaults to Unix sockets under <cluster_root>/peers/. Pass --listen
// off to disable the default; pass --listen :8000 or unix:/path to
// override.
const (
	defaultMeshPort   = ":7000"
	listenOffSentinel = "off"
)

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func daemonCmd(args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "", "path to app database (required)")
	listen := fs.String("listen", "", "mesh listen address (gossip + clone/catchup/uniqueness on one port). Empty = auto: unix socket for file:// clusters, :7000 for s3://. Pass an explicit addr (\":8000\", \"unix:/path\") to override or \"off\" to disable.")
	seedsCSV := fs.String("seeds", "", "comma-separated peer addresses to dial")
	insecure := fs.Bool("insecure", false, "acknowledge running the mesh as plaintext TCP beyond loopback (the daemon has no TLS flags yet; without this, non-loopback TCP listen/seed addresses are refused)")
	logLevel := fs.String("log", "info", "log level: debug|info|warn|error")
	cluster := fs.String("cluster", "", "shared cluster root URL (file:///path or s3://bucket/prefix). Derives object backend (root/objects), schema log (root/schema), and S3-based peer discovery. Defaults to file://<metadata> when no other flags are set.")
	objectBackend := fs.String("object-backend", "", "object-backend URL for sealing changeset epochs (file:///path or s3://bucket/prefix). Use --cluster instead unless you need a separate location for objects vs schema log.")
	logPath := fs.String("schema-log", "", "path to local schema-log file (overrides --cluster derivation)")
	logListen := fs.String("schema-log-listen", "", "TCP listen addr to host the schema log over the network (requires --schema-log)")
	logDial := fs.String("schema-log-dial", "", "TCP addr of a remote schema log to use as a follower")
	logS3 := fs.String("schema-log-s3", "", "S3 bucket URL to use as the schema log (overrides --cluster derivation)")
	catchup := fs.Duration("schema-catchup", 0, "broker schema-catchup poll interval (0 → default)")
	idleTimeout := fs.Duration("idle-timeout", 5*time.Minute, "exit cleanly after this much time with zero attached extension clients. 0 disables (use for systemd-managed daemons that should run indefinitely).")
	sealerMaxAge := fs.Duration("sealer-max-age", 5*time.Second, "max time the sealer buffers an origin's records before uploading an epoch object. Lower = faster cross-node catch-up after offline gaps; higher = fewer object-store PUTs.")
	topic := fs.String("topic", syzy.DefaultTopic, "mesh topic this daemon serves. Peers replicating the same database must use the same topic; clone URLs default to it when ?topic= is omitted.")
	wakeListen := fs.String("wake-listen", "", "wake listener address for cross-kernel producers (format: unix:/path). When set, secondary drainers wake from this socket instead of futex.Wait. Required when guest VMs hold producer-only origins on the same DB.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		fs.Usage()
		return errors.New("--db is required")
	}
	logger := newLogger(*logLevel)
	// The daemon owns its process and its stderr is <db>-syzy/daemon.log,
	// so it logs by default — unlike the extension, which is a guest in
	// somebody else's process and stays silent unless SYZY_LOG says so.
	syzylog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Block until the daemon role frees up. WaitForDaemon polls every
	// 2s, then we drop our claim so syzy.Open can take its own — the
	// brief gap is harmless since no other daemon process is racing
	// us (operator restart pattern).
	claim, err := layout.WaitForDaemon(ctx, *dbPath, 0)
	if err != nil {
		return fmt.Errorf("acquire daemon role: %w", err)
	}
	if err := claim.Release(); err != nil {
		return fmt.Errorf("release pre-Open daemon claim: %w", err)
	}

	if *logListen != "" && *logPath == "" {
		return errors.New("--schema-log-listen requires --schema-log (the leader hosts a local file)")
	}
	if *cluster != "" && *objectBackend != "" {
		return errors.New("--cluster conflicts with --object-backend; pick one")
	}
	seeds := splitSeeds(*seedsCSV)

	clusterRoot, err := resolveClusterRoot(*dbPath, *cluster, *objectBackend, len(seeds) > 0)
	if err != nil {
		return err
	}

	listenAddr, err := resolveListenAddr(*listen, clusterRoot, *dbPath)
	if err != nil {
		return err
	}

	var tx transport.Transport
	var mesh *tcpmesh.Mesh
	if listenAddr != "" || len(seeds) > 0 {
		m, err := tcpmesh.New(tcpmesh.Config{
			Listen:   listenAddr,
			Seeds:    seeds,
			Insecure: *insecure,
		})
		if err != nil {
			return fmt.Errorf("start mesh transport: %w", err)
		}
		defer m.Close()
		ch, err := m.Channel(*topic)
		if err != nil {
			return fmt.Errorf("open mesh channel %q: %w", *topic, err)
		}
		mesh = m
		tx = ch
	}

	var (
		be                 objectstore.Bucket
		schemaLog          schemalog.Log
		resolvedObjBackend string
	)
	if clusterRoot != "" {
		objURL, schemaURL := clusterurl.Split(clusterRoot, *objectBackend != "")
		be, err = objectstore.Open(ctx, objURL)
		if err != nil {
			return fmt.Errorf("objectstore.Open objects %s: %w", objURL, err)
		}
		resolvedObjBackend = objURL
		// Schema log defaults to a sibling of objects; explicit
		// --schema-log* flags override.
		if *logPath == "" && *logDial == "" && *logS3 == "" && schemaURL != "" {
			sl, closer, err := clusterurl.OpenSchema(ctx, schemaURL)
			if err != nil {
				return err
			}
			if closer != nil {
				defer closer.Close()
			}
			schemaLog = sl
		}
	}
	if schemaLog == nil {
		var closer io.Closer
		schemaLog, closer, err = schemalog.Dial(*logPath, *logDial, *logS3)
		if err != nil {
			return err
		}
		if closer != nil {
			defer closer.Close()
		}
	}

	var wakeListener wake.Listener
	if *wakeListen != "" {
		ln, err := openWakeListener(*wakeListen)
		if err != nil {
			return fmt.Errorf("open wake-listen %q: %w", *wakeListen, err)
		}
		wakeListener = vsock.NewListener(ln)
		defer wakeListener.Close()
		logger.Info("syzy daemon: wake listener", "addr", *wakeListen)
	}

	node, err := syzy.Open(ctx, syzy.Config{
		Path:                  *dbPath,
		Transport:             tx,
		ObjectBackend:         be,
		Log:                   logger,
		SchemaLog:             schemaLog,
		SchemaCatchupInterval: *catchup,
		SealerConfig:          syzy.SealerConfig{MaxAge: *sealerMaxAge},
		WakeListener:          wakeListener,
		// The daemon always serves clones when it has a mesh: its whole
		// purpose is hosting the database for peers.
		ServeClones: mesh != nil,
	})
	if err != nil {
		return fmt.Errorf("syzy.Open: %w", err)
	}

	// Peer discovery: heartbeats land at <cluster_root>/peers/. For
	// file:// clusters, discovers same-host peers via shared FS. For s3://
	// clusters, discovers cross-host peers via the bucket. Skipped when
	// no cluster root (producer-only) or no transport.
	if clusterRoot != "" && mesh != nil {
		discBE, err := objectstore.Open(ctx, clusterRoot)
		if err != nil {
			return fmt.Errorf("open peerdisc backend %s: %w", clusterRoot, err)
		}
		staticSeeds := append([]string(nil), seeds...)
		var disc *peerdisc.Discoverer
		disc, err = peerdisc.Start(ctx, peerdisc.Config{
			Backend: discBE,
			Origin:  crdt.Origin(node.Origin()),
			Listen:  mesh.Addr(),
			OnPeers: func(found []string) {
				mesh.SetSeeds(append(append([]string(nil), staticSeeds...), found...))
				// disc is nil during the first discoverOnce inside
				// Start; replay bindings after Start returns.
				if disc == nil {
					return
				}
				node.SetOriginAddrs(toUint64Map(disc.Bindings()))
			},
		})
		if err != nil {
			return fmt.Errorf("peerdisc.Start: %w", err)
		}
		defer disc.Close()
		node.SetOriginAddrs(toUint64Map(disc.Bindings()))
		logger.Info("syzy daemon: peer discovery active")
	}
	logger.Info("syzy daemon: ready",
		"db", *dbPath,
		"cluster_id", fmt.Sprintf("%x", node.ClusterID()),
		"origin", node.OriginHex(),
		"listen", listenAddr,
		"seeds", *seedsCSV,
		"cluster_root", clusterRoot,
		"object_backend", resolvedObjBackend,
		"schema_log", schemaLog != nil,
	)

	if *logListen != "" {
		srv, err := schemalog.ListenTCP(*logListen, schemaLog)
		if err != nil {
			return fmt.Errorf("schemalog ListenTCP %q: %w", *logListen, err)
		}
		defer srv.Close()
		logger.Info("syzy schema log: listening", "addr", srv.Addr())
	}

	// Per-DB control socket: extension clients connect here on
	// `.load syzy`. Their hold on the connection prevents idle-exit.
	// The listen addr is normalized to a syzy.Restore URL so co-located
	// `syzy clone <local-path>` can reroute through the daemon when
	// the local-streaming source lock is held.
	ctrl, err := ctrlsock.Listen(*dbPath, node.OriginHex(),
		fmt.Sprintf("%x", node.ClusterID()),
		cloneSourceURL(listenAddr, *topic))
	if err != nil {
		return fmt.Errorf("ctrlsock.Listen: %w", err)
	}
	defer ctrl.Close()
	ctrl.SetWaitFunc(node.WaitReplicated)
	logger.Info("syzy daemon: control socket ready", "addr", ctrl.Addr())

	idleCtx, idleCancel := context.WithCancel(ctx)
	defer idleCancel()
	idle := ctrl.IdleWatcher(idleCtx, *idleTimeout, 30*time.Second)

	select {
	case <-ctx.Done():
	case <-idle:
		logger.Info("syzy daemon: idle exit", "timeout", *idleTimeout)
	}
	logger.Info("syzy daemon: shutting down")
	// Give Close a brief deadline so a wedged broker can't hang
	// systemd's stop sequence indefinitely.
	done := make(chan error, 1)
	go func() { done <- node.Close() }()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		return errors.New("close timed out after 15s")
	}
}

// resolveListenAddr derives the mesh listen address from the flag
// value, the cluster scheme, and a stable per-DB hash.
//
// Empty flag value → auto:
//   - file:// cluster → unix:<cluster_root_fs>/peers/<db-hash>.sock
//   - s3:// cluster   → :7000 (TCP, allowlist-friendly)
//   - no cluster      → empty (no listener; producer-only mode with seeds)
//
// Explicit value passes through. "off" disables.
func resolveListenAddr(listenFlag, clusterRoot, dbPath string) (string, error) {
	hash := layout.PathHash(dbPath)
	scheme, fsPath := clusterScheme(clusterRoot)
	return oneListenAddr(listenFlag, scheme, fsPath, hash)
}

func oneListenAddr(flagVal, scheme, fsPath, hash string) (string, error) {
	v := strings.TrimSpace(flagVal)
	if v == listenOffSentinel {
		return "", nil
	}
	if v != "" {
		return v, nil
	}
	switch scheme {
	case "file":
		dir := filepath.Join(fsPath, "peers")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("mkdir peers dir: %w", err)
		}
		path := filepath.Join(dir, hash+".sock")
		if layout.CheckUnixSocketPath(path) != nil {
			// A deep database path overflows sun_path. Peers dial the
			// address advertised in the peers/<origin>.json heartbeat,
			// not this filename, so relocating the socket to the short
			// per-user runtime directory is transparent to discovery.
			// Keyed on the database hash, not the cluster root: every
			// database in the cluster shares that root.
			short, err := layout.ShortSocketPathForHash(hash, "-peer")
			if err != nil {
				return "", err
			}
			path = short
		}
		return "unix:" + path, nil
	case "s3":
		return defaultMeshPort, nil
	}
	return "", nil
}

// clusterScheme returns ("file", "/abs/path") for "file:///abs/path",
// ("s3", "") for "s3://...", or ("", "") for empty/unparseable input.
func clusterScheme(clusterRoot string) (scheme, fsPath string) {
	if clusterRoot == "" {
		return "", ""
	}
	u, err := url.Parse(clusterRoot)
	if err != nil {
		return "", ""
	}
	if u.Scheme == "file" {
		return "file", u.Path
	}
	return u.Scheme, ""
}

// cloneSourceURL maps the daemon's mesh listen address ("unix:/path",
// ":7000", "1.2.3.4:7000", "") to a syzy.Restore-compatible endpoint
// URL carrying the daemon's topic. Empty addr → empty URL (the daemon
// has no listener; clone clients fall back to local streaming, which
// still fails for live sources but at least surfaces the right error).
func cloneSourceURL(addr, topic string) string {
	if addr == "" {
		return ""
	}
	if !strings.HasPrefix(addr, "unix:") && strings.HasPrefix(addr, ":") {
		// Bare port: advertise localhost so a co-located clone can dial.
		addr = "127.0.0.1" + addr
	}
	return tcpmesh.BuildEndpointURL(addr, topic)
}

func splitSeeds(csv string) []string {
	var out []string
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// resolveClusterRoot reads the persisted cluster_root from metadata
// (load-bearing on reopen) and, if absent, computes the first-init
// default from CLI flags / env / metadata dir, persisting it for future
// reopens.
//
// First-init precedence: --cluster flag > --object-backend > SYZY_CLUSTER
// env > file://<absolute-metadata-dir>. Producer-only with --seeds is
// represented by an empty cluster root.
//
// On reopen, if a flag/env value disagrees with the persisted root, fail
// loud — DBs are bound to a cluster identity and silently rebinding
// would split-brain replication.
func resolveClusterRoot(dbPath, clusterFlag, objectBackendFlag string, hasSeeds bool) (string, error) {
	sc, err := metadata.Open(layout.MetaDB(dbPath))
	if err != nil {
		return "", fmt.Errorf("peek metadata: %w", err)
	}
	defer sc.Close()
	persisted, ok, err := sc.GetClusterRoot()
	if err != nil {
		return "", fmt.Errorf("read cluster_root: %w", err)
	}
	requested := firstNonEmpty(
		strings.TrimSpace(clusterFlag),
		strings.TrimSpace(objectBackendFlag),
		strings.TrimSpace(os.Getenv("SYZY_CLUSTER")),
	)
	if ok {
		if requested != "" && requested != persisted {
			return "", fmt.Errorf(
				"--cluster %q conflicts with this DB's persisted cluster_root %q; this DB is bound to that cluster",
				requested, persisted,
			)
		}
		return persisted, nil
	}
	root := requested
	if root == "" {
		if hasSeeds {
			return "", nil
		}
		abs, err := filepath.Abs(layout.MetaDir(dbPath))
		if err != nil {
			abs = layout.MetaDir(dbPath)
		}
		root = "file://" + abs
	}
	if err := sc.SetClusterRoot(root); err != nil {
		return "", fmt.Errorf("persist cluster_root: %w", err)
	}
	return root, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// toUint64Map flips peerdisc's addr→origin map into the origin→addr
// shape Node.SetOriginAddrs takes. Both inputs are small (cluster size).
func toUint64Map(m map[string]crdt.Origin) map[uint64]string {
	out := make(map[uint64]string, len(m))
	for addr, o := range m {
		out[uint64(o)] = addr
	}
	return out
}

// openWakeListener parses --wake-listen and binds the underlying
// net.Listener. Format: "unix:/path". Future: "vsock:port" once we
// need to listen directly on AF_VSOCK rather than through CH's
// per-port Unix-socket bridge.
func openWakeListener(spec string) (net.Listener, error) {
	if strings.HasPrefix(spec, "unix:") {
		path := strings.TrimPrefix(spec, "unix:")
		// Remove any stale socket; bind fails on existing path.
		_ = os.Remove(path)
		return net.Listen("unix", path)
	}
	return nil, fmt.Errorf("wake-listen: unrecognized scheme in %q (want unix:/path)", spec)
}
