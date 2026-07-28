// Package clusterurl resolves the cluster-root URL convention shared by
// the daemon CLI and the loadable extension. A cluster root URL like
// file:///path or s3://bucket/prefix expands into:
//
//   - <root>/objects — the object backend
//   - <root>/schema  — the schema log (file: SQLite file with .db
//     suffix; s3: CAS PutObject prefix)
//
// Keeping this in one place ensures the daemon and the extension agree
// on where each piece lives so they can share the same backing store.
package clusterurl

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/wjordan/objectstore"
	"github.com/wjordan/syzy/schemalog"
)

// Split returns the object-backend and schema-log child URLs derived
// from a cluster root. When literalObjects is true, the root IS the
// object backend (--object-backend mode) and schemaURL is "" — the
// caller skips schema-log auto-derivation.
func Split(rootURL string, literalObjects bool) (objURL, schemaURL string) {
	u, err := url.Parse(rootURL)
	if err != nil || u.Scheme == "" {
		return rootURL, ""
	}
	if literalObjects {
		return rootURL, ""
	}
	withSub := func(sub string) string {
		c := *u
		c.Path = path.Join("/", u.Path, sub)
		return c.String()
	}
	return withSub("objects"), withSub("schema")
}

// OpenSchema opens the schema log at schemaURL. file:// → SQLite-file
// CAS (multi-process safe via SQLite locks; .db is appended to the
// path); s3:// → CAS PutObject through objectstore.
func OpenSchema(ctx context.Context, schemaURL string) (schemalog.Log, io.Closer, error) {
	u, err := url.Parse(schemaURL)
	if err != nil {
		return nil, nil, fmt.Errorf("clusterurl: parse %q: %w", schemaURL, err)
	}
	switch u.Scheme {
	case "file":
		p := strings.TrimRight(u.Path, "/") + ".db"
		f, err := schemalog.OpenFile(p)
		if err != nil {
			return nil, nil, fmt.Errorf("open schema log %s: %w", p, err)
		}
		return f, f, nil
	case "s3":
		be, err := objectstore.Open(ctx, schemaURL)
		if err != nil {
			return nil, nil, fmt.Errorf("open schema backend %s: %w", schemaURL, err)
		}
		s := schemalog.NewS3WithBackend(be)
		return s, s, nil
	default:
		return nil, nil, fmt.Errorf("clusterurl: unsupported scheme %q in %q", u.Scheme, schemaURL)
	}
}
