package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wjordan/syzy/pg/internal/pgtest"
)

const testClusterID = "ccccccccccccccccccccccccccccccdd"

// TestTwoSidecarsConverge spawns two syzy-pg subprocesses against the
// shared test PG container, connected via TCP. Node 0 INSERTs a row; the
// test polls node 1 until the row appears (apply path: capture -> tcp
// broadcast -> tcp subscribe on the peer -> orchestrator.applyRemote).
//
// This is the black-box smoke test for the binary itself — flag parsing,
// signal handling, TCP wiring, end-to-end engine startup. The
// pgtestcluster suite exercises the same code paths via memtransport;
// this proves the binary works on real network sockets.
func TestTwoSidecarsConverge(t *testing.T) {
	requirePG(t)

	tmp := t.TempDir()
	binPath := buildBinary(t, tmp)

	const (
		db0 = "syzy_sidecar_n0"
		db1 = "syzy_sidecar_n1"
	)
	createPGDB(t, db0)
	createPGDB(t, db1)
	t.Cleanup(func() { dropPGDB(db0); dropPGDB(db1) })

	port0 := freePort(t)
	port1 := freePort(t, port0)
	addr0 := fmt.Sprintf("127.0.0.1:%d", port0)
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)

	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p0 := startSidecar(t, startCtx, binPath, sidecarArgs{
		db: db0, origin: 1, listen: addr0, seeds: []string{addr1},
		dataDir: filepath.Join(tmp, "n0"),
	})
	p1 := startSidecar(t, startCtx, binPath, sidecarArgs{
		db: db1, origin: 2, listen: addr1, seeds: []string{addr0},
		dataDir: filepath.Join(tmp, "n1"),
	})

	// Wait for both sidecars to bind their listeners (they print a
	// startup line on stderr); give them a brief settle window so the
	// peers complete the gossip handshake before the first write.
	if err := waitForListen(addr0, 5*time.Second); err != nil {
		t.Fatalf("node 0 not listening: %v", err)
	}
	if err := waitForListen(addr1, 5*time.Second); err != nil {
		t.Fatalf("node 1 not listening: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Write on node 0.
	pgExec(t, db0, `INSERT INTO public.kv VALUES (42,'hello')`)

	// Poll node 1 until the row arrives.
	if err := waitForRow(db1, 42, "hello", 20*time.Second); err != nil {
		// Surface subprocess stderr to make a hang debuggable.
		t.Logf("node 0 stderr:\n%s", p0.stderr())
		t.Logf("node 1 stderr:\n%s", p1.stderr())
		t.Fatalf("waitForRow: %v", err)
	}

	// Round-trip the other direction.
	pgExec(t, db1, `INSERT INTO public.kv VALUES (43,'world')`)
	if err := waitForRow(db0, 43, "world", 20*time.Second); err != nil {
		t.Logf("node 0 stderr:\n%s", p0.stderr())
		t.Logf("node 1 stderr:\n%s", p1.stderr())
		t.Fatalf("waitForRow reverse: %v", err)
	}
}

// TestTwoSidecarsConvergeDDLOverTCP proves the multi-host DDL path: the
// schema log is hosted by node 0 over TCP (-schema-log-listen) and followed
// by node 1 (-schema-log-dial) — no shared filesystem. Node 0 creates a
// table and inserts a row via DDL replication; the row arriving on node 1
// requires node 1 to first catch the table up from the TCP-hosted schema
// log, then apply the gated DML.
func TestTwoSidecarsConvergeDDLOverTCP(t *testing.T) {
	requirePG(t)

	tmp := t.TempDir()
	binPath := buildBinary(t, tmp)

	const (
		db0 = "syzy_sidecar_ddl_n0"
		db1 = "syzy_sidecar_ddl_n1"
	)
	createEmptyPGDB(t, db0)
	createEmptyPGDB(t, db1)
	t.Cleanup(func() { dropPGDB(db0); dropPGDB(db1) })

	port0 := freePort(t)
	port1 := freePort(t, port0)
	slPort := freePort(t, port0, port1) // schema-log TCP listener on node 0
	addr0 := fmt.Sprintf("127.0.0.1:%d", port0)
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	slAddr := fmt.Sprintf("127.0.0.1:%d", slPort)

	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Node 0 owns the schema log file and hosts it over TCP.
	p0 := startSidecar(t, startCtx, binPath, sidecarArgs{
		db: db0, origin: 1, listen: addr0, seeds: []string{addr1},
		dataDir:         filepath.Join(tmp, "n0"),
		ddl:             true,
		schemaLog:       filepath.Join(tmp, "schema.db"),
		schemaLogListen: slAddr,
	})
	// Node 1 follows the schema log over TCP — no schema file of its own.
	p1 := startSidecar(t, startCtx, binPath, sidecarArgs{
		db: db1, origin: 2, listen: addr1, seeds: []string{addr0},
		dataDir:       filepath.Join(tmp, "n1"),
		ddl:           true,
		schemaLogDial: slAddr,
	})

	if err := waitForListen(addr0, 5*time.Second); err != nil {
		t.Fatalf("node 0 gossip not listening: %v", err)
	}
	if err := waitForListen(addr1, 5*time.Second); err != nil {
		t.Fatalf("node 1 gossip not listening: %v", err)
	}
	if err := waitForListen(slAddr, 5*time.Second); err != nil {
		t.Fatalf("schema-log listener not up: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Node 0 creates the replicated table via DDL, then writes a row.
	pgExec(t, db0, `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`)
	pgExec(t, db0, `INSERT INTO public.kv VALUES (7,'ddl-hello')`)

	if err := waitForRow(db1, 7, "ddl-hello", 30*time.Second); err != nil {
		t.Logf("node 0 stderr:\n%s", p0.stderr())
		t.Logf("node 1 stderr:\n%s", p1.stderr())
		t.Fatalf("waitForRow (DDL-created table): %v", err)
	}

	// Auto-increment PKs: each sidecar slices the bigint id space by its
	// -origin, so two nodes inserting without ids cannot mint the same one.
	pgExec(t, db0, `CREATE TABLE public.auto (id bigserial PRIMARY KEY, val text)`)
	pgExec(t, db0, `INSERT INTO public.auto (val) VALUES ('n0')`)
	// Node 1 must have applied the CREATE (and partitioned its own sequence)
	// before it inserts, so wait for node 0's row to land there first.
	if err := waitForAuto(db1, map[string]bool{"n0": true}, 30*time.Second); err != nil {
		t.Logf("node 1 stderr:\n%s", p1.stderr())
		t.Fatalf("bigserial table never reached node 1: %v", err)
	}
	pgExec(t, db1, `INSERT INTO public.auto (val) VALUES ('n1')`)
	both := map[string]bool{"n0": true, "n1": true}
	for _, db := range []string{db0, db1} {
		if err := waitForAuto(db, both, 30*time.Second); err != nil {
			t.Logf("node 0 stderr:\n%s\nnode 1 stderr:\n%s", p0.stderr(), p1.stderr())
			t.Fatalf("auto rows did not converge on %s: %v", db, err)
		}
	}
}

// waitForAuto polls public.auto until its val set matches want, and fails fast
// if the two nodes minted the same id.
func waitForAuto(db string, want map[string]bool, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var last map[int64]string
	for time.Now().Before(end) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := selectAuto(ctx, db)
		cancel()
		if err == nil {
			last = rows
			vals := map[string]bool{}
			for _, v := range rows {
				vals[v] = true
			}
			if len(rows) == len(want) && len(vals) == len(want) {
				for v := range want {
					if !vals[v] {
						return fmt.Errorf("public.auto on %s = %v, want vals %v", db, rows, want)
					}
				}
				return nil // distinct ids by construction: rows is keyed by id
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("public.auto on %s = %v, want vals %v within %s", db, last, want, deadline)
}

func selectAuto(ctx context.Context, db string) (map[int64]string, error) {
	c, err := pgx.Connect(ctx, pgtest.URL()+db)
	if err != nil {
		return nil, err
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id, val FROM public.auto`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return nil, err
		}
		out[id] = val
	}
	return out, rows.Err()
}

// --- helpers ---

func requirePG(t *testing.T) {
	t.Helper()
	pgtest.BaseURL(t)
}

func buildBinary(t *testing.T, dir string) string {
	t.Helper()
	out := filepath.Join(dir, "syzy-pg")
	cmd := exec.Command("go", "build", "-o", out, "./")
	cmd.Dir = "."
	if buf, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, buf)
	}
	return out
}

type sidecarArgs struct {
	db      string
	origin  int
	listen  string
	seeds   []string
	dataDir string

	// DDL mode: when set, the sidecar runs with -ddl (no -tables) and the
	// schema-log flags below instead of the static public.kv replicated set.
	ddl             bool
	schemaLog       string // -schema-log (local file backend)
	schemaLogListen string // -schema-log-listen (host the file over TCP)
	schemaLogDial   string // -schema-log-dial (follow a peer's TCP-hosted log)
}

type sidecarProc struct {
	cmd     *exec.Cmd
	stderrF *os.File
	stderrP string
}

func (p *sidecarProc) stderr() string {
	if p.stderrP == "" {
		return ""
	}
	b, _ := os.ReadFile(p.stderrP)
	return string(b)
}

func startSidecar(t *testing.T, ctx context.Context, bin string, a sidecarArgs) *sidecarProc {
	t.Helper()
	args := []string{
		"-conn", pgtest.URL() + a.db,
		"-origin", fmt.Sprintf("%d", a.origin),
		"-cluster-id", testClusterID,
		"-data-dir", a.dataDir,
		"-listen", a.listen,
	}
	if a.ddl {
		args = append(args, "-ddl")
		if a.schemaLog != "" {
			args = append(args, "-schema-log", a.schemaLog)
		}
		if a.schemaLogListen != "" {
			args = append(args, "-schema-log-listen", a.schemaLogListen)
		}
		if a.schemaLogDial != "" {
			args = append(args, "-schema-log-dial", a.schemaLogDial)
		}
	} else {
		args = append(args, "-tables", "public.kv")
	}
	for _, s := range a.seeds {
		args = append(args, "-seeds", s)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stderrPath := filepath.Join(a.dataDir + ".stderr")
	if err := os.MkdirAll(a.dataDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", a.dataDir, err)
	}
	f, err := os.Create(stderrPath)
	if err != nil {
		t.Fatalf("stderr file: %v", err)
	}
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sidecar: %v", err)
	}
	p := &sidecarProc{cmd: cmd, stderrF: f, stderrP: stderrPath}
	t.Cleanup(func() {
		// SIGTERM, give it a moment for clean origin/slot release.
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		f.Close()
		dropPGOrigin(a.db) // global; cleanup unconditionally
		dropPGSlot(a.db)
	})
	return p
}

// freePort reserves a gossip port whose bundle sibling (port+1, the sidecar's
// catch-up listener) is also free and doesn't collide with previously-reserved
// ports or their siblings. The kernel hands out sequential ephemeral ports, so
// naively picking two in a row makes node 1's gossip land exactly on node 0's
// bundle port.
func freePort(t *testing.T, taken ...int) int {
	t.Helper()
	conflict := func(p int) bool {
		for _, q := range taken {
			if p == q || p == q+1 || p+1 == q {
				return true
			}
		}
		return false
	}
	for i := 0; i < 50; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("freePort: %v", err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		if conflict(p) {
			continue
		}
		sib, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+1))
		if err != nil {
			continue
		}
		sib.Close()
		return p
	}
	t.Fatal("freePort: no conflict-free port pair found")
	return 0
}

func waitForListen(addr string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("dial %s never succeeded within %s", addr, deadline)
}

func waitForRow(db string, id int64, val string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		got, err := selectOne(ctx, db, id)
		cancel()
		if err == nil && got == val {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("row id=%d val=%q never arrived on %s within %s", id, val, db, deadline)
}

func selectOne(ctx context.Context, db string, id int64) (string, error) {
	c, err := pgx.Connect(ctx, pgtest.URL()+db)
	if err != nil {
		return "", err
	}
	defer c.Close(ctx)
	var v string
	err = c.QueryRow(ctx, `SELECT val FROM public.kv WHERE id=$1`, id).Scan(&v)
	return v, err
}

func createPGDB(t *testing.T, db string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, db)
	for i := 0; i < 50; i++ {
		var n int
		_ = admin.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE database=$1`, db).Scan(&n)
		if n == 0 {
			break
		}
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=$1 AND NOT active`, db)
		time.Sleep(20 * time.Millisecond)
	}
	_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db))
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, db)); err != nil {
		t.Fatalf("create %s: %v", db, err)
	}
	app, err := pgx.Connect(ctx, pgtest.URL()+db)
	if err != nil {
		t.Fatalf("schema connect %s: %v", db, err)
	}
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, `CREATE TABLE public.kv (id bigint PRIMARY KEY, val text)`); err != nil {
		t.Fatalf("schema %s: %v", db, err)
	}
}

// createEmptyPGDB recreates db with no application schema — the replicated
// table is created later via DDL replication. Mirrors createPGDB's slot/
// session cleanup so a re-run after a crashed test starts clean.
func createEmptyPGDB(t *testing.T, db string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, db)
	for i := 0; i < 50; i++ {
		var n int
		_ = admin.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE database=$1`, db).Scan(&n)
		if n == 0 {
			break
		}
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=$1 AND NOT active`, db)
		time.Sleep(20 * time.Millisecond)
	}
	_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db))
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, db)); err != nil {
		t.Fatalf("create %s: %v", db, err)
	}
}

func dropPGDB(db string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, db)
	_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db))
}

func dropPGSlot(db string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	slot := "syzy_slot_" + db
	// Terminate the slot's active session first — the sidecar's
	// SIGTERM-driven shutdown closes its conn, but the WAL sender
	// release can lag long enough that a plain pg_drop here returns
	// "is active for PID X."
	for i := 0; i < 50; i++ {
		_, _ = admin.Exec(ctx,
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name=$1 AND active`, slot)
		_, err := admin.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot)
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func dropPGOrigin(db string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgtest.URL()+"postgres")
	if err != nil {
		return
	}
	defer admin.Close(ctx)
	for i := 0; i < 50; i++ {
		_, err := admin.Exec(ctx, `SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL`, "syzy_origin_"+db)
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func pgExec(t *testing.T, db, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, pgtest.URL()+db)
	if err != nil {
		t.Fatalf("pgExec connect: %v", err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("pgExec %q: %v", sql, err)
	}
}
