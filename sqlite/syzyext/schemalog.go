package syzyext

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/wjordan/syzy/internal/clusterurl"
	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/schemalog"
	"github.com/wjordan/syzy/wake/vsock"
)

// OpenSchemaLog selects a schema-log backend for the producer attached
// to dbPath. Resolution order matches the daemon's defaults so the
// producer and daemon converge on the same backend:
//
//  1. SYZY_SCHEMA_LOG{,_DIAL,_S3} explicit
//  2. SYZY_CLUSTER → derived schema sibling
//  3. SYZY_OBJECT_BACKEND set: literal mode, no auto-derived schema log
//  4. otherwise: file://<metadata>/schema.db (matches daemon default)
//
// Returns (nil, nil, nil) when no schema log is desired (case 3): the
// producer's trace_v2 hook then rejects DDL, which is correct for
// deployments that pre-apply schema on every peer.
func OpenSchemaLog(dbPath string) (schemalog.Log, io.Closer, error) {
	if path := strings.TrimSpace(os.Getenv("SYZY_SCHEMA_LOG")); path != "" {
		return schemalog.Dial(path, "", "")
	}
	if dial := strings.TrimSpace(os.Getenv("SYZY_SCHEMA_LOG_DIAL")); dial != "" {
		if strings.HasPrefix(dial, "unix:") || strings.HasPrefix(dial, "vsock:") {
			dialAddr, err := vsock.DialAddr(dial)
			if err != nil {
				return nil, nil, err
			}
			client, err := schemalog.DialFunc(dial, func(ctx context.Context) (net.Conn, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return dialAddr()
			})
			if err != nil {
				return nil, nil, err
			}
			return client, client, nil
		}
		return schemalog.Dial("", dial, "")
	}
	if s3 := strings.TrimSpace(os.Getenv("SYZY_SCHEMA_LOG_S3")); s3 != "" {
		return schemalog.Dial("", "", s3)
	}
	if root := strings.TrimSpace(os.Getenv("SYZY_CLUSTER")); root != "" {
		_, schemaURL := clusterurl.Split(root, false)
		return clusterurl.OpenSchema(context.Background(), schemaURL)
	}
	if strings.TrimSpace(os.Getenv("SYZY_OBJECT_BACKEND")) != "" {
		return nil, nil, nil
	}
	return OpenDefaultSchemaLog(dbPath)
}

// OpenDefaultSchemaLog opens the file schema log a producer attached
// to dbPath resolves when no environment override is present:
// file://<metadata>/schema.db. Embedders that manage configuration
// out-of-band (and must not inherit SYZY_* from their own process
// environment) call this directly to converge on the same backend as
// env-driven producers running with a clean environment.
func OpenDefaultSchemaLog(dbPath string) (schemalog.Log, io.Closer, error) {
	abs, err := filepath.Abs(layout.MetaDir(dbPath))
	if err != nil {
		abs = layout.MetaDir(dbPath)
	}
	// Callers may resolve the schema log before anything else has
	// created the state directory (e.g. a host-side node opening the
	// log ahead of sqlite.Open on a fresh slot).
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, nil, fmt.Errorf("syzyext: create schema log dir: %w", err)
	}
	_, schemaURL := clusterurl.Split("file://"+abs, false)
	return clusterurl.OpenSchema(context.Background(), schemaURL)
}
