package pgtestcluster

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestClusterDDLReplication proves that a CREATE TABLE issued on one
// node propagates through the shared schema log + capture/apply path to
// every other node, and that follow-up DML on the new table converges
// cluster-wide. Exercises the §6 DDL replication path end-to-end:
// event-trigger spool -> syzy_ddl_intent -> typed CatalogOp -> schemalog
// append -> peer catchUpSchema -> applyCatalogOp -> live writes.
func TestClusterDDLReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	c := New(t, Config{
		N:        2,
		DBPrefix: "syzy_clu_ddl",
		Schema:   "", // empty — DDL is replicated through the schema log
		Tables:   nil,
		DDL:      true,
	})
	c.Start(ctx)

	// Node 0 originates the table AND does the first insert. The follower
	// (node 1) has no physical CREATE TABLE until it sees a peer Changeset
	// whose Deps[SchemaChain] forces catchUpSchema — there is no periodic
	// schemalog poll in the PG engine (deliberate; see docs/postgres.md §6
	// "lease + gate"). Once that first insert lands, node 1 has the table
	// and can write its own rows.
	c.Nodes[0].AppExec(t, `CREATE TABLE public.items (id bigint PRIMARY KEY, label text)`)
	for i := 1; i <= 5; i++ {
		c.Nodes[0].AppExec(t, fmt.Sprintf(`INSERT INTO public.items VALUES (%d,'a%d')`, i, i))
	}

	// Wait for node 1 to catch the schema event + at least one DML, then
	// it can issue its own writes.
	if err := waitTableExists(c.Nodes[1], "public.items", 30*time.Second); err != nil {
		t.Fatalf("node 1 table catch-up: %v", err)
	}
	for i := 6; i <= 10; i++ {
		c.Nodes[1].AppExec(t, fmt.Sprintf(`INSERT INTO public.items VALUES (%d,'b%d')`, i, i))
	}

	if err := c.WaitConverge(30 * time.Second); err != nil {
		t.Fatalf("WaitConverge: %v", err)
	}

	want := map[int64]string{}
	for i := 1; i <= 5; i++ {
		want[int64(i)] = fmt.Sprintf("a%d", i)
	}
	for i := 6; i <= 10; i++ {
		want[int64(i)] = fmt.Sprintf("b%d", i)
	}
	for _, n := range c.Nodes {
		got := dumpItems(t, n)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s items mismatch:\n got  %v\n want %v", n.DB, got, want)
		}
	}
}

// waitTableExists polls node n until qname resolves to a relation. Bounded.
func waitTableExists(n *Node, qname string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for {
		if tableExists(n, qname) {
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("table %s never appeared on %s", qname, n.DB)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func tableExists(n *Node, qname string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := n.Connect(ctx)
	if err != nil {
		return false
	}
	defer c.Close(ctx)
	var ok bool
	if err := c.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, qname).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func dumpItems(t testing.TB, n *Node) map[int64]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := n.Connect(ctx)
	if err != nil {
		t.Fatalf("connect %s: %v", n.DB, err)
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, `SELECT id,label FROM public.items`)
	if err != nil {
		t.Fatalf("query %s: %v", n.DB, err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan %s: %v", n.DB, err)
		}
		out[id] = v
	}
	return out
}
