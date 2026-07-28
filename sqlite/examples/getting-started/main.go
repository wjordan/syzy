package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/wjordan/syzy/schemalog"
	syzy "github.com/wjordan/syzy/sqlite"
	"github.com/wjordan/syzy/tcpmesh"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "syzy-demo-")
	check(err)
	defer os.RemoveAll(dir)

	// NewLocal is appropriate here because both nodes share this process.
	// Multi-process deployments need a shared file- or S3-backed schema log.
	schema := schemalog.NewLocal()

	meshA, err := tcpmesh.New(tcpmesh.Config{Listen: "127.0.0.1:0"})
	check(err)
	defer meshA.Close()
	transportA, err := meshA.Channel(syzy.DefaultTopic)
	check(err)

	dbAPath := filepath.Join(dir, "a.db")
	nodeA, err := syzy.Open(ctx, syzy.Config{
		Path:          dbAPath,
		Transport:     transportA,
		SchemaLog:     schema,
		InProcessOnly: true,
	})
	check(err)
	defer nodeA.Close()

	meshB, err := tcpmesh.New(tcpmesh.Config{
		Seeds:     []string{meshA.Addr()},
		DialRetry: 25 * time.Millisecond,
	})
	check(err)
	defer meshB.Close()
	transportB, err := meshB.Channel(syzy.DefaultTopic)
	check(err)

	dbBPath := filepath.Join(dir, "b.db")
	check(syzy.JoinCluster(dbBPath, nodeA.ClusterID()))
	nodeB, err := syzy.Open(ctx, syzy.Config{
		Path:          dbBPath,
		Transport:     transportB,
		SchemaLog:     schema,
		InProcessOnly: true,
	})
	check(err)
	defer nodeB.Close()

	a, b := syzy.NewDB(nodeA), syzy.NewDB(nodeB)

	check(nodeA.Exec(`CREATE TABLE notes (
		id   INTEGER PRIMARY KEY,
		text TEXT NOT NULL
	)`))

	// DDL and DML replicate asynchronously. Wait for each fact before
	// depending on it instead of assuming a delivery time.
	waitFor(func() bool {
		return intQuery(b, `SELECT count(*) FROM sqlite_master
			WHERE type = 'table' AND name = 'notes'`) == 1
	})

	_, err = a.Exec(`INSERT INTO notes (text) VALUES ('from a')`)
	check(err)
	waitFor(func() bool {
		return intQuery(b, `SELECT count(*) FROM notes`) == 1
	})

	_, err = b.Exec(`INSERT INTO notes (text) VALUES ('from b')`)
	check(err)
	waitFor(func() bool {
		return intQuery(a, `SELECT count(*) FROM notes`) == 2
	})

	fmt.Println("a.db:", noteList(a))
	fmt.Println("b.db:", noteList(b))
}

func intQuery(db *syzy.DB, query string) (n int) {
	if err := db.QueryRow(query).Scan(&n); err != nil {
		return -1
	}
	return n
}

func noteList(db *syzy.DB) string {
	var notes string
	check(db.QueryRow(`SELECT group_concat(text, ', ')
		FROM (SELECT text FROM notes ORDER BY text)`).Scan(&notes))
	return notes
}

func waitFor(ok func() bool) {
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			fail("timed out waiting for replication")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func check(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(v any) {
	fmt.Fprintln(os.Stderr, v)
	os.Exit(1)
}
