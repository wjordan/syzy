// Command syzy is the operational CLI for syzy databases.
//
//	syzy daemon  run the syncer pipeline for a database
//	syzy status  print frontier + applied gaps + schema_seq
//	syzy join    seed a fresh database with an existing cluster_id
//	syzy check   metadata/journal integrity scan
//	syzy wait    block until this replica's writes have reached its peers
//
// Run any subcommand with -h for its options.
package main

import (
	"fmt"
	"os"
)

const usage = `syzy — operational CLI for syzy databases

Usage:
  syzy <subcommand> [options]

Subcommands:
  daemon    run the syncer pipeline for a database
  status    print frontier + applied gaps + schema_seq
  join      seed a fresh database with an existing cluster_id
  clone     bootstrap a fresh database from a peer or stopped local db
  check     metadata/journal integrity scan
  wait      block until this replica's writes have reached its peers
  snapshot  publish a logical snapshot of a stopped db to object storage
  s3-gc     remove unreferenced snapshot directories from a bucket
  version   print the build version

Run "syzy <subcommand> -h" for subcommand-specific options.
`

type subcommand func(args []string) error

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmds := map[string]subcommand{
		"daemon":    daemonCmd,
		"status":    statusCmd,
		"join":      joinCmd,
		"clone":     cloneCmd,
		"check":     checkCmd,
		"wait":      waitCmd,
		"snapshot":  snapshotCmd,
		"s3-gc":     s3GCCmd,
		"version":   versionCmd,
		"-h":        func([]string) error { fmt.Print(usage); return nil },
		"--help":    func([]string) error { fmt.Print(usage); return nil },
		"help":      func([]string) error { fmt.Print(usage); return nil },
		"--version": versionCmd,
	}
	cmd, ok := cmds[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "syzy: unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err := cmd(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "syzy %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}
