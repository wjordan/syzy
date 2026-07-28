package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/wjordan/syzy/internal/ctrlsock"
)

const waitUsage = `syzy wait — block until this replica's writes have reached its peers

Usage:
  syzy wait <db> [--timeout 30s]

Run this on the host that wrote. It drains every local producer journal,
then blocks until every connected peer has applied those writes. Exits
non-zero if a peer cannot be reached or the timeout expires.
`

func waitCmd(args []string) error {
	fs := flag.NewFlagSet("wait", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, waitUsage) }
	dbFlag := fs.String("db", "", "path to app database (alternative to the positional argument)")
	timeout := fs.Duration("timeout", 30*time.Second, "give up after this long")
	if err := fs.Parse(args); err != nil {
		return err
	}

	dbPath := *dbFlag
	if rest := fs.Args(); len(rest) > 0 {
		if dbPath != "" {
			return errors.New("pass the database once, as --db or as the positional argument")
		}
		if len(rest) > 1 {
			return fmt.Errorf("wait takes one database, got %d", len(rest))
		}
		dbPath = rest[0]
	}
	if dbPath == "" {
		fs.Usage()
		return errors.New("a database path is required")
	}

	client, err := ctrlsock.Dial(dbPath)
	if err != nil {
		if errors.Is(err, ctrlsock.ErrNoDaemon) {
			return fmt.Errorf("no daemon is running for %s; nothing is replicating it, so there is nothing to wait for", dbPath)
		}
		return err
	}
	defer client.Close()

	return client.Wait(*timeout)
}
