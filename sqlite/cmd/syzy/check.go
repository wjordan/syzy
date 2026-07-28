package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/wjordan/syzy/internal/journal"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
)

func checkCmd(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "", "path to app database (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		fs.Usage()
		return errors.New("--db is required")
	}

	var problems int
	logProblem := func(format string, args ...any) {
		problems++
		fmt.Fprintf(os.Stderr, "PROBLEM: "+format+"\n", args...)
	}

	// Meta opens, schema check is built into Open.
	sc, err := metadata.Open(layout.MetaDB(*dbPath))
	if err != nil {
		logProblem("metadata open: %v", err)
	} else {
		defer sc.Close()
		if _, ok, err := sc.GetClusterID(); err != nil {
			logProblem("read cluster_id: %v", err)
		} else if !ok {
			logProblem("metadata has no cluster_id (database not initialized)")
		}
		// Reading frontier exercises the schema/queries.
		if _, err := sc.Frontier(); err != nil {
			logProblem("read frontier: %v", err)
		}
	}

	// Walk every per-origin journal directory under origins/. A clean
	// reopen + iterate-from-zero round-trip detects torn tails (which
	// the iterator surfaces as Err) and bad CRCs.
	originsRoot := layout.OriginsRoot(*dbPath)
	entries, err := os.ReadDir(originsRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logProblem("scan origins root: %v", err)
	}
	scanned := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		jdir := filepath.Join(originsRoot, e.Name(), "journal")
		j, err := journal.Open(jdir, 0, journal.SyncOff)
		if err != nil {
			logProblem("open journal %s: %v", e.Name(), err)
			continue
		}
		it := j.Iterate(0)
		records := 0
		for {
			_, _, err := it.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, journal.ErrPending) {
				break
			}
			if err != nil {
				logProblem("iterate journal %s: %v", e.Name(), err)
				break
			}
			records++
		}
		head := j.Head()
		_ = j.Close()
		fmt.Printf("ok  journal %s  records=%d  head=%d\n", e.Name(), records, head)
		scanned++
	}

	fmt.Println()
	if problems == 0 {
		fmt.Printf("check: OK (%d journals scanned)\n", scanned)
		return nil
	}
	return fmt.Errorf("%d problem(s) found", problems)
}
