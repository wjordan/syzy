package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/wjordan/syzy/crdt"
	"github.com/wjordan/syzy/internal/metadata"
)

// runStatus implements `syzy-pg status -data-dir PATH`: a read-only snapshot of
// this node's durable state — schema head, per-origin applied frontier,
// quarantine backlog, and journal heads — for operators checking convergence
// without attaching a debugger. Reads the same files the daemon holds open;
// SQLite metadata reads are safe alongside a live daemon (WAL mode).
func runStatus(args []string) error {
	fs := flag.NewFlagSet("syzy-pg status", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "node state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataDir == "" {
		return fmt.Errorf("status: -data-dir required")
	}

	meta, err := metadata.Open(filepath.Join(*dataDir, "meta.db"))
	if err != nil {
		return fmt.Errorf("status: metadata open: %w", err)
	}
	defer meta.Close()

	seq, _, err := meta.GetSchemaSeq()
	if err != nil {
		return fmt.Errorf("status: schema_seq: %w", err)
	}
	fmt.Printf("schema_seq: %d\n", seq)

	front, err := meta.Frontier()
	if err != nil {
		return fmt.Errorf("status: frontier: %w", err)
	}
	origins := make([]crdt.Origin, 0, len(front))
	for o := range front {
		origins = append(origins, o)
	}
	sort.Slice(origins, func(i, j int) bool { return origins[i] < origins[j] })
	fmt.Printf("frontier (%d origins):\n", len(origins))
	for _, o := range origins {
		f := front[o]
		fmt.Printf("  origin %d: applied seq %d (hlc %v)\n", o, f.LastSeq, f.LastHLC)
	}

	if entries, err := meta.ListQuarantine(); err == nil && len(entries) > 0 {
		fmt.Printf("quarantine: %d entries (oldest origin %d seq %d, %d attempts)\n",
			len(entries), entries[0].Origin, uint64(entries[0].Seq), entries[0].Attempts)
	} else if err == nil {
		fmt.Println("quarantine: empty")
	}

	// Journal footprints from the filesystem only — opening the live daemon's
	// journals would contend for their write locks.
	for _, dir := range []string{"selflog", "mirror"} {
		if n, b := dirFootprint(filepath.Join(*dataDir, dir)); n > 0 {
			fmt.Printf("%s: %d files, %d bytes\n", dir, n, b)
		}
	}
	return nil
}

// dirFootprint counts files and bytes under root (recursive, best-effort).
func dirFootprint(root string) (files int, bytes int64) {
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			files++
			bytes += info.Size()
		}
		return nil
	})
	return files, bytes
}
