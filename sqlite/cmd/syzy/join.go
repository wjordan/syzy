package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"

	syzy "github.com/wjordan/syzy/sqlite"
)

func joinCmd(args []string) error {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "", "path to (new) app database (required)")
	clusterHex := fs.String("cluster", "", "32-char hex cluster_id from an existing peer (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *clusterHex == "" {
		fs.Usage()
		return errors.New("--db and --cluster are required")
	}
	raw, err := hex.DecodeString(*clusterHex)
	if err != nil {
		return fmt.Errorf("parse cluster_id hex: %w", err)
	}
	if len(raw) != 16 {
		return fmt.Errorf("cluster_id must be 16 bytes (32 hex chars); got %d bytes", len(raw))
	}
	var id [16]byte
	copy(id[:], raw)
	if err := syzy.JoinCluster(*dbPath, id); err != nil {
		return err
	}
	fmt.Printf("joined %s to cluster %x\n", *dbPath, id)
	return nil
}
