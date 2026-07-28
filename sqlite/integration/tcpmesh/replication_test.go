package tcpmesh_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/sqlitebridge"
	"github.com/wjordan/syzy/tcpmesh"
)

// TestMesh_ThreeHosts_TwoTopics_Replicates is phase 7 of the
// docs/TRANSPORT.md implementation: a 3-host × 2-topic cluster
// converges over one shared mesh listener per
// host. Each host's mesh seeds the other two; rows written on
// host A's "app" topic replicate to hosts B and C's "app",
// and the cross-topic ("cdn") tables stay isolated.
//
// Verifies the end-to-end mux wiring: one TCP-like connection per
// peer pair carrying both topics, per-channel CRDT replication,
// topic membership propagation via TOPIC_ADD.
func TestMesh_ThreeHosts_TwoTopics_Replicates(t *testing.T) {
	dir := t.TempDir()
	rootBE, err := objectstore.OpenFS(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("objectstore: %v", err)
	}

	// One shared SchemaLog per topic — every host's instance of
	// that topic reads/writes the same schema chain.
	appLog := schemalog.NewLocal()
	cdnLog := schemalog.NewLocal()
	appBE := objectstore.Prefixed(rootBE, "app/")
	cdnBE := objectstore.Prefixed(rootBE, "cdn/")

	type host struct {
		mesh       *tcpmesh.Mesh
		nodeApp    *syzy.Node
		nodeCDN    *syzy.Node
		appDBPath  string
		cdnDBPath  string
		gossipAddr string
	}
	hosts := make([]*host, 3)

	ctx := context.Background()
	// Host 1 boots first, generating fresh cluster_ids for each
	// topic. Hosts 2 and 3 then JoinCluster against those ids so
	// all three hosts participate in the same logical cluster per
	// topic. Without this each host would have its own cluster_id
	// and cross-host changesets would be rejected.
	var appClusterID, cdnClusterID [16]byte
	for i := 0; i < 3; i++ {
		hostDir := filepath.Join(dir, fmt.Sprintf("host%d", i+1))
		if err := os.MkdirAll(hostDir, 0o755); err != nil {
			t.Fatalf("mkdir host %d: %v", i+1, err)
		}
		gossipAddr := "unix:" + filepath.Join(hostDir, "gossip.sock")

		mesh, err := tcpmesh.New(tcpmesh.Config{
			Listen:    gossipAddr,
			DialRetry: 25 * time.Millisecond,
			NodeID:    uint64(i+1) * 100, // deterministic ordering
		})
		if err != nil {
			t.Fatalf("host %d mesh: %v", i+1, err)
		}
		t.Cleanup(func() { _ = mesh.Close() })

		appTx, err := mesh.Channel("app")
		if err != nil {
			t.Fatalf("host %d app: %v", i+1, err)
		}
		cdnTx, err := mesh.Channel("cdn")
		if err != nil {
			t.Fatalf("host %d cdn: %v", i+1, err)
		}

		appDBPath := filepath.Join(hostDir, "app.db")
		cdnDBPath := filepath.Join(hostDir, "cdn.db")

		if i > 0 {
			if err := syzy.JoinCluster(appDBPath, appClusterID); err != nil {
				t.Fatalf("host %d JoinCluster app: %v", i+1, err)
			}
			if err := syzy.JoinCluster(cdnDBPath, cdnClusterID); err != nil {
				t.Fatalf("host %d JoinCluster cdn: %v", i+1, err)
			}
		}

		nodeApp, err := syzy.Open(ctx, syzy.Config{
			Path:                  appDBPath,
			Transport:             appTx,
			ObjectBackend:         appBE,
			SchemaLog:             appLog,
			SchemaCatchupInterval: 25 * time.Millisecond,
			ServeClones:           true,
		})
		if err != nil {
			t.Fatalf("host %d syzy.Open app: %v", i+1, err)
		}
		t.Cleanup(func() { _ = nodeApp.Close() })
		nodeCDN, err := syzy.Open(ctx, syzy.Config{
			Path:                  cdnDBPath,
			Transport:             cdnTx,
			ObjectBackend:         cdnBE,
			SchemaLog:             cdnLog,
			SchemaCatchupInterval: 25 * time.Millisecond,
			ServeClones:           true,
		})
		if err != nil {
			t.Fatalf("host %d syzy.Open cdn: %v", i+1, err)
		}
		t.Cleanup(func() { _ = nodeCDN.Close() })

		if i == 0 {
			appClusterID = nodeApp.ClusterID()
			cdnClusterID = nodeCDN.ClusterID()
		}

		hosts[i] = &host{
			mesh:       mesh,
			nodeApp:    nodeApp,
			nodeCDN:    nodeCDN,
			appDBPath:  appDBPath,
			cdnDBPath:  cdnDBPath,
			gossipAddr: gossipAddr,
		}
	}

	// Full-mesh seeding: each host seeds the other two via a
	// single mux-wide SetSeeds. No per-topic seed plumbing —
	// proving the overlay-refresh shape works.
	for i, h := range hosts {
		seeds := make([]string, 0, 2)
		for j, o := range hosts {
			if i == j {
				continue
			}
			seeds = append(seeds, o.gossipAddr)
		}
		h.mesh.SetSeeds(seeds)
	}

	// Wait for the mesh to converge: each host should observe
	// at least one ready peer (the NodeID tie-break picks one
	// connection per peer pair).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, h := range hosts {
			if len(h.mesh.PeerAddrs()) < 2 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, h := range hosts {
		if got := len(h.mesh.PeerAddrs()); got != 2 {
			t.Fatalf("host %d peer count = %d, want 2 (full mesh)", i+1, got)
		}
	}

	// Schema on host 1 propagates to all hosts via the shared
	// SchemaLog. Replicated writes follow.
	// syzy CRDT rejects INTEGER PRIMARY KEY rowid aliases; use
	// INT PRIMARY KEY NOT NULL.
	if err := hosts[0].nodeApp.Exec(`CREATE TABLE app (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("host1 app CREATE: %v", err)
	}
	if err := hosts[0].nodeCDN.Exec(`CREATE TABLE cdn_t (id INT PRIMARY KEY NOT NULL, v INT)`); err != nil {
		t.Fatalf("host1 cdn CREATE: %v", err)
	}

	// Wait for schema catch-up on hosts 2 and 3.
	waitForSchema(t, hosts[1].appDBPath, "app", 5*time.Second)
	waitForSchema(t, hosts[2].appDBPath, "app", 5*time.Second)
	waitForSchema(t, hosts[1].cdnDBPath, "cdn_t", 5*time.Second)
	waitForSchema(t, hosts[2].cdnDBPath, "cdn_t", 5*time.Second)

	// Writes from different hosts on each topic, then verify
	// each host sees all writes for its topic and nothing from
	// the other topic.
	if err := hosts[0].nodeApp.Exec(`INSERT INTO app VALUES (1, 100)`); err != nil {
		t.Fatalf("host1 app insert: %v", err)
	}
	if err := hosts[1].nodeApp.Exec(`INSERT INTO app VALUES (2, 200)`); err != nil {
		t.Fatalf("host2 app insert: %v", err)
	}
	if err := hosts[2].nodeCDN.Exec(`INSERT INTO cdn_t VALUES (10, 1000)`); err != nil {
		t.Fatalf("host3 cdn insert: %v", err)
	}

	wantApp := []string{"1=100", "2=200"}
	wantCDN := []string{"10=1000"}

	// Every host's app DB should converge to {1=100, 2=200}.
	for i, h := range hosts {
		waitForRows(t, h.appDBPath, `SELECT id || '=' || v FROM app ORDER BY id`,
			wantApp, fmt.Sprintf("host%d app", i+1), 5*time.Second)
	}
	// Every host's cdn DB should converge to {10=1000}.
	for i, h := range hosts {
		waitForRows(t, h.cdnDBPath, `SELECT id || '=' || v FROM cdn_t ORDER BY id`,
			wantCDN, fmt.Sprintf("host%d cdn", i+1), 5*time.Second)
	}

	// Cross-topic isolation: app DBs must not see cdn_t and vice
	// versa.
	for i, h := range hosts {
		if hasTable(t, h.appDBPath, "cdn_t") {
			t.Errorf("host%d app DB unexpectedly has cdn_t table — topic isolation broken", i+1)
		}
		if hasTable(t, h.cdnDBPath, "app") {
			t.Errorf("host%d cdn DB unexpectedly has app table — topic isolation broken", i+1)
		}
	}
}

// waitForSchema polls dbPath until the named table appears or the
// deadline expires.
func waitForSchema(t *testing.T, dbPath, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hasTable(t, dbPath, table) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("schema for table %q never appeared in %s within %v", table, dbPath, timeout)
}

// waitForRows polls dbPath until the SQL query returns exactly
// want (string-formatted) rows, or the deadline expires.
func waitForRows(t *testing.T, dbPath, sql string, want []string, label string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []string
	for time.Now().Before(deadline) {
		got = readColumn(t, dbPath, sql)
		sort.Strings(got)
		if slices.Equal(got, want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Errorf("%s: rows did not converge to %v within %v; last seen %v", label, want, timeout, got)
}

// readColumn opens dbPath read-only and returns column 0 of every
// row as a string slice.
func readColumn(t *testing.T, dbPath, sql string) []string {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return nil
	}
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		// Missing table is a transient condition during catch-up.
		return nil
	}
	defer stmt.Finalize()
	var out []string
	for {
		ok, err := stmt.Step()
		if err != nil || !ok {
			break
		}
		out = append(out, string(stmt.ColumnText(0)))
	}
	return out
}

// hasTable returns true if dbPath contains a table named table.
func hasTable(t *testing.T, dbPath, table string) bool {
	t.Helper()
	c, err := sqlitebridge.Open(dbPath, sqlitebridge.OpenReadOnly|sqlitebridge.OpenURI|sqlitebridge.OpenNoMutex)
	if err != nil {
		return false
	}
	defer c.Close()
	stmt, _, err := c.Prepare(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`)
	if err != nil {
		return false
	}
	defer stmt.Finalize()
	if err := stmt.BindText(1, table); err != nil {
		return false
	}
	ok, err := stmt.Step()
	if err != nil || !ok {
		return false
	}
	return stmt.ColumnText(0) != "0"
}
