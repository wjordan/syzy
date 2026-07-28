package transport

import (
	"context"
	"io"
)

// BundleHandler streams a node's clone bundle to w. Returning nil
// closes the connection cleanly; returning an error truncates whatever
// bytes were already written. The wire-format codec lives in
// internal/clone; these shapes only describe the endpoints.
type BundleHandler func(w io.Writer) error

// BundleSource is the transport-side endpoint that accepts inbound
// clone-fetch connections and routes them to the registered handler.
// The transport implementation owns the listener; consumer code
// installs the producer here. Sibling of CatchupRegistrar /
// FrontierRegistrar in the optional-capability roster.
type BundleSource interface {
	// SetBundleHandler installs the producer. Pass nil to refuse
	// incoming requests. Idempotent.
	SetBundleHandler(BundleHandler)
	// Endpoint returns the peer-dialable URL of this transport's
	// scope (for tcpmesh, "tcp://host:port?topic=…" over the mesh's
	// one advertised address), or empty when the transport accepts
	// no inbound connections. Clone URLs and uniqueness-lease
	// records carry this form.
	Endpoint() string
}

// BundleFetcher dials addr (a BundleSource's endpoint) and
// copies the response stream into w until EOF. The built-in
// implementation is tcpmesh.FetchBundle.
type BundleFetcher func(ctx context.Context, addr string, w io.Writer) error
