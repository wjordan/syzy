package pgwire

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// fakeReserver records requests and returns a scripted verdict.
type fakeReserver struct {
	mu   sync.Mutex
	reqs []Request
	err  error
}

func (f *fakeReserver) Reserve(_ context.Context, req Request) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return f.err
}

func (f *fakeReserver) seen() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs
}

func (f *fakeReserver) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// client is a minimal frontend: connect, send one query, read the reply.
type client struct {
	conn net.Conn
	fe   *pgproto3.Frontend
}

func dial(t *testing.T, socket string) *client {
	t.Helper()
	conn, err := net.DialTimeout("unix", socket, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	fe := pgproto3.NewFrontend(conn, conn)
	fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "syzy"},
	})
	if err := fe.Flush(); err != nil {
		t.Fatalf("flush startup: %v", err)
	}
	c := &client{conn: conn, fe: fe}
	c.readUntilReady(t)
	return c
}

// readUntilReady drains messages through ReadyForQuery, returning any
// ErrorResponse and the DataRow values seen.
func (c *client) readUntilReady(t *testing.T) (*pgproto3.ErrorResponse, [][]byte) {
	t.Helper()
	var errResp *pgproto3.ErrorResponse
	var row [][]byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = c.conn.SetReadDeadline(deadline)
		msg, err := c.fe.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			errResp = m
		case *pgproto3.DataRow:
			row = make([][]byte, len(m.Values))
			for i, v := range m.Values {
				row[i] = append([]byte(nil), v...)
			}
		case *pgproto3.ReadyForQuery:
			return errResp, row
		}
	}
}

func (c *client) query(t *testing.T, sql string) (*pgproto3.ErrorResponse, [][]byte) {
	t.Helper()
	c.fe.Send(&pgproto3.Query{String: sql})
	if err := c.fe.Flush(); err != nil {
		t.Fatalf("flush query: %v", err)
	}
	return c.readUntilReady(t)
}

func start(t *testing.T, r Reserver) *Server {
	t.Helper()
	// Unix socket paths are capped near 100 bytes by the OS, below what
	// a long t.TempDir plus name can reach, so keep the name to one char.
	socket := filepath.Join(t.TempDir(), "s")
	srv, err := Listen(Config{Socket: socket, Reserver: r, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func s(v string) *string { return &v }

func requestFixture() Request {
	return Request{Entries: []Entry{
		{
			TableID: strings.Repeat("11", 16),
			KeyID:   strings.Repeat("22", 16),
			Values:  []*string{s("a@example.com")},
			Owner:   []*string{s("1")},
		},
		{
			TableID: strings.Repeat("11", 16),
			KeyID:   strings.Repeat("22", 16),
			Values:  []*string{s("b@example.com")},
			Owner:   []*string{s("2")},
			Prev:    []*string{s("0")},
		},
	}}
}

func reserveSQL(t *testing.T, req Request) string {
	t.Helper()
	q, err := EncodeRequest(req)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	return q
}

func TestReserve_Granted(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	errResp, row := c.query(t, reserveSQL(t, requestFixture()))
	if errResp != nil {
		t.Fatalf("unexpected error response: %+v", errResp)
	}
	if len(row) != 1 || string(row[0]) != "ok" {
		t.Fatalf("row = %q, want [ok]", row)
	}
	seen := r.seen()
	if len(seen) != 1 {
		t.Fatalf("reserver saw %d requests, want 1 (the whole txn reserves once)", len(seen))
	}
	if !reflect.DeepEqual(seen[0], requestFixture()) {
		t.Fatalf("request round-trip mismatch:\n got %+v\nwant %+v", seen[0], requestFixture())
	}
}

func TestReserve_DeniedIsUniqueViolation(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{err: ErrDenied}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	errResp, _ := c.query(t, reserveSQL(t, requestFixture()))
	if errResp == nil {
		t.Fatal("denial produced no error response; the commit would not abort")
	}
	if errResp.Code != sqlStateUniqueViolation {
		t.Fatalf("SQLSTATE = %s, want %s (must look like an ordinary duplicate key)",
			errResp.Code, sqlStateUniqueViolation)
	}
}

func TestReserve_UnavailableIsRetryable(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{err: errors.New("registry unreachable")}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	errResp, _ := c.query(t, reserveSQL(t, requestFixture()))
	if errResp == nil {
		t.Fatal("unavailable registry produced no error; the write would commit unreserved")
	}
	if errResp.Code != sqlStateSerializationFailure {
		t.Fatalf("SQLSTATE = %s, want %s (retryable, not a permanent duplicate)",
			errResp.Code, sqlStateSerializationFailure)
	}
}

// The connection must survive a denial: PL/pgSQL may catch the error and
// the backend keeps its dblink connection cached for the session.
func TestReserve_ConnectionUsableAfterDenial(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{err: ErrDenied}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	if errResp, _ := c.query(t, reserveSQL(t, requestFixture())); errResp == nil {
		t.Fatal("setup: wanted a denial")
	}
	r.setErr(nil)

	errResp, row := c.query(t, reserveSQL(t, requestFixture()))
	if errResp != nil {
		t.Fatalf("second query on the same connection failed: %+v", errResp)
	}
	if len(row) != 1 || string(row[0]) != "ok" {
		t.Fatalf("row = %q, want [ok]", row)
	}
}

func TestReserve_RejectsNonReserveQuery(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	for _, q := range []string{"SELECT 1", "DROP TABLE users", "", "RESERVEX ff"} {
		errResp, _ := c.query(t, q)
		if errResp == nil {
			t.Errorf("query %q was accepted; the endpoint serves RESERVE only", q)
		}
	}
	if n := len(r.seen()); n != 0 {
		t.Errorf("reserver saw %d requests from non-RESERVE queries, want 0", n)
	}
}

func TestReserve_MalformedPayloadRejected(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	valid, err := json.Marshal(requestFixture())
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]string{
		"not hex":        reserveVerb + "zzzz",
		"not json":       reserveVerb + hex.EncodeToString([]byte("{oops")),
		"unknown field":  reserveVerb + hex.EncodeToString([]byte(`{"e":[],"x":1}`)),
		"trailing data":  reserveVerb + hex.EncodeToString(append(valid, '{')),
		"wrong type":     reserveVerb + hex.EncodeToString([]byte(`{"e":"nope"}`)),
		"bare json null": reserveVerb + hex.EncodeToString([]byte(`null`)),
	} {
		errResp, _ := c.query(t, q)
		// A bare `null` decodes to the zero Request, which is a legal
		// empty batch — the reserver decides, not the parser.
		if name == "bare json null" {
			continue
		}
		if errResp == nil {
			t.Errorf("%s: accepted; want error", name)
		}
	}
	for _, req := range r.seen() {
		if len(req.Entries) != 0 {
			t.Errorf("reserver saw a non-empty request from malformed input: %+v", req)
		}
	}
}

// An empty batch is legal and must not fail: a transaction can touch a
// coordinated table without changing any key value.
func TestReserve_EmptyBatchSucceeds(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	errResp, row := c.query(t, reserveSQL(t, Request{}))
	if errResp != nil {
		t.Fatalf("empty batch rejected: %+v", errResp)
	}
	if len(row) != 1 || string(row[0]) != "ok" {
		t.Fatalf("row = %q, want [ok]", row)
	}
}

// NULL key values must survive the round trip distinctly from "": a
// partial-key row that is not participating carries NULLs.
func TestReserve_NullValuesPreserved(t *testing.T) {
	t.Parallel()
	r := &fakeReserver{}
	srv := start(t, r)
	c := dial(t, srv.Addr())

	req := Request{Entries: []Entry{{
		TableID: strings.Repeat("aa", 16),
		KeyID:   strings.Repeat("bb", 16),
		Values:  []*string{nil, s("")},
		Owner:   []*string{s("7")},
	}}}
	if errResp, _ := c.query(t, reserveSQL(t, req)); errResp != nil {
		t.Fatalf("unexpected error: %+v", errResp)
	}
	got := r.seen()[0].Entries[0].Values
	if len(got) != 2 || got[0] != nil || got[1] == nil || *got[1] != "" {
		t.Fatalf("values = %v, want [nil, \"\"] (NULL must stay distinct from empty)", got)
	}
}

// A stale socket from a crashed predecessor must not block startup, but
// a live one must not be stolen.
func TestListen_StaleSocketReclaimed(t *testing.T) {
	t.Parallel()
	socket := filepath.Join(t.TempDir(), "s")

	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("pre-bind: %v", err)
	}
	ln.Close() // leaves the file behind, nothing listening

	srv, err := Listen(Config{Socket: socket, Reserver: &fakeReserver{}})
	if err != nil {
		t.Fatalf("Listen over stale socket: %v", err)
	}
	defer srv.Close()

	_, err = Listen(Config{Socket: socket, Reserver: &fakeReserver{}})
	if err == nil {
		t.Fatal("second Listen stole a live socket; want refusal")
	}
	if !strings.Contains(err.Error(), "already served") {
		t.Fatalf("second Listen err = %v, want an already-served refusal", err)
	}
}
