package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/metadata"
)

func statusCmd(args []string) error {
	return statusCmdTo(args, os.Stdout)
}

func statusCmdTo(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dbPath := fs.String("db", "", "path to app database (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		fs.Usage()
		return errors.New("--db is required")
	}

	sc, err := metadata.Open(layout.MetaDB(*dbPath))
	if err != nil {
		return fmt.Errorf("open metadata: %w", err)
	}
	defer sc.Close()

	cid, ok, err := sc.GetClusterID()
	if err != nil {
		return fmt.Errorf("read cluster_id: %w", err)
	}
	if ok {
		fmt.Fprintf(out, "cluster_id   %x\n", cid)
	} else {
		fmt.Fprintln(out, "cluster_id   (unset)")
	}

	if origin, ok, err := sc.GetNodeID(); err != nil {
		return fmt.Errorf("read node_id: %w", err)
	} else if ok {
		fmt.Fprintf(out, "node_id      %s (uint64=%d)\n", layout.OriginHex(origin), uint64(origin))
	} else {
		fmt.Fprintln(out, "node_id      (unset)")
	}

	if seq, ok, err := sc.GetSchemaSeq(); err != nil {
		return fmt.Errorf("read schema_seq: %w", err)
	} else if ok {
		fmt.Fprintf(out, "schema_seq   %d\n", seq)
	} else {
		fmt.Fprintln(out, "schema_seq   0")
	}
	if health, unhealthy, err := sc.GetSchemaHealth(); err != nil {
		return fmt.Errorf("read schema health: %w", err)
	} else if unhealthy {
		fmt.Fprintf(out, "schema_health unhealthy  seq=%d  reason=%s\n", health.Seq, health.Reason)
	} else {
		fmt.Fprintln(out, "schema_health healthy")
	}

	front, err := sc.Frontier()
	if err != nil {
		return fmt.Errorf("read frontier: %w", err)
	}
	gaps, err := sc.GetAppliedGaps()
	if err != nil {
		return fmt.Errorf("read applied_gaps: %w", err)
	}
	origins := make([]crdt.Origin, 0, len(front))
	for o := range front {
		origins = append(origins, o)
	}
	sort.Slice(origins, func(i, j int) bool { return uint64(origins[i]) < uint64(origins[j]) })

	fmt.Fprintln(out)
	fmt.Fprintln(out, "frontier:")
	if len(origins) == 0 {
		fmt.Fprintln(out, "  (empty)")
	}
	for _, o := range origins {
		fe := front[o]
		gapStr := "-"
		if g, ok := gaps[o]; ok && !g.IsEmpty() {
			gapStr = fmt.Sprintf("%v", g.Ranges())
		}
		fmt.Fprintf(out, "  %s  last_seq=%d  last_hlc=%d  gaps=%s\n",
			layout.OriginHex(o), fe.LastSeq, fe.LastHLC, gapStr)
	}

	return nil
}
