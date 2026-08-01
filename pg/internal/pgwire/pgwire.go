// Package pgwire is the sidecar's minimal Postgres-wire endpoint for
// in-transaction coordinated-uniqueness reservations.
//
// Why a Postgres server and not an ordinary RPC: Postgres's only
// pre-commit veto point available without a server extension is a
// DEFERRABLE INITIALLY DEFERRED constraint trigger, and the only way
// trigger PL/pgSQL can reach outside its own transaction is a
// connection extension. dblink is the one in wide distribution, and it
// speaks the Postgres wire protocol — so the sidecar answers it by
// being a Postgres server, for exactly one query verb.
//
// The endpoint listens on a unix socket next to the sidecar's data
// directory. It is not a general Postgres: it accepts a startup
// message, answers a single RESERVE verb, and rejects everything else.
// Nothing here parses SQL.
//
// Wire verb (the trigger sends it as the dblink query text):
//
//	RESERVE <hex of JSON Request>
//
// The payload is hex-encoded so it can ride inside a simple-query
// string with no quoting or escaping questions at all.
//
// The request carries key and primary-key values as TEXT, not as
// canonical key bytes: canonical encoding lives in one place (the
// engine's Go encoder, shared with capture), because canonical byte
// equality IS row identity and a second implementation in PL/pgSQL
// could diverge from it. The trigger renders text under pinned
// per-function GUCs so the writer's session settings cannot change the
// bytes.
//
// A denial is an ErrorResponse with SQLSTATE 23505 (unique_violation),
// which surfaces in the backend as an ordinary constraint error and
// aborts the commit — exactly the semantics a UNIQUE index would have
// given, without any node holding one.
package pgwire

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// SQLSTATE codes the endpoint returns. A denial must look like an
// ordinary unique violation so application error handling (and ORM
// retry logic) treats it the way it treats any duplicate key.
const (
	sqlStateUniqueViolation = "23505"
	// 40001 (serialization_failure) marks a retryable condition: the
	// registry could not be reached. Drivers and frameworks already
	// retry this class, and retrying is correct — the reservation was
	// never granted, so no duplicate can result.
	sqlStateSerializationFailure = "40001"
	sqlStateProtocolViolation    = "08P01"
)

// reserveVerb prefixes the only query the endpoint accepts.
const reserveVerb = "RESERVE "

// ErrDenied reports that the batch conflicts with a value another owner
// holds. The endpoint turns it into a 23505 so the commit aborts.
var ErrDenied = errors.New("pgwire: coordinated key already taken")

// Entry is one row's coordinated-key reservation within a transaction.
// Values are the key's column values in declared order, rendered as
// text; Owner and Prev are primary-key column values in PK order.
type Entry struct {
	// TableID and KeyID are 32-character hex of the stable 16-byte ids.
	TableID string `json:"t"`
	KeyID   string `json:"k"`
	// Values holds the key's column values, declared order. A nil
	// element is SQL NULL.
	Values []*string `json:"v"`
	// Owner is the owning row's PK column values, PK order.
	Owner []*string `json:"o"`
	// Prev, when set, is the prior owning row's PK: a PK-changing
	// update that keeps the same key value transfers it rather than
	// conflicting.
	Prev []*string `json:"p,omitempty"`
}

// Request is one transaction's whole batch. The trigger sends exactly
// one of these, at commit.
type Request struct {
	Entries []Entry `json:"e"`
}

// Reserver is the semantic half: it resolves the request's text values
// to canonical key bytes and reserves them in the cluster registry. It
// returns ErrDenied for a genuine conflict (permanent — abort the
// write) and any other error for an unavailable backend (retryable —
// nothing was reserved).
type Reserver interface {
	Reserve(ctx context.Context, req Request) error
}

// Server answers reservation requests from Postgres backends over a
// unix socket. Zero value is not usable; construct with Listen.
type Server struct {
	ln       net.Listener
	reserver Reserver
	log      *slog.Logger
	// timeout bounds one reservation round trip. A backend is blocked
	// in its commit while this runs, holding row locks, so it must not
	// be unbounded.
	timeout time.Duration

	wg     sync.WaitGroup
	closed chan struct{}
	once   sync.Once
}

// Config parameterizes Listen.
type Config struct {
	// Socket is the unix socket path to bind. A stale socket from a
	// crashed predecessor is reclaimed; a live one is refused.
	Socket string
	// Reserver receives the decoded batches. Required.
	Reserver Reserver
	// Timeout bounds one reservation round trip (default 5s).
	Timeout time.Duration
	Logger  *slog.Logger
}

// Listen binds cfg.Socket and starts serving. Close stops the listener
// and waits for in-flight connections.
func Listen(cfg Config) (*Server, error) {
	if cfg.Reserver == nil {
		return nil, errors.New("pgwire: Reserver is required")
	}
	if cfg.Socket == "" {
		return nil, errors.New("pgwire: Socket is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// A crashed predecessor leaves the socket file behind; binding
	// would fail with EADDRINUSE even though nothing is listening. The
	// error here distinguishes that from a live sidecar already holding
	// the socket, which a bare bind failure would not.
	if err := removeStaleSocket(cfg.Socket); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return nil, fmt.Errorf("pgwire: listen %s: %w", cfg.Socket, err)
	}
	s := &Server{
		ln:       ln,
		reserver: cfg.Reserver,
		log:      cfg.Logger.With("component", "pgwire"),
		timeout:  cfg.Timeout,
		closed:   make(chan struct{}),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop()
	}()
	return s, nil
}

// Addr returns the bound socket path.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops accepting and waits for in-flight connections to finish.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.closed) })
	err := s.ln.Close()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			// A transient accept error must not kill the endpoint:
			// coordinated commits abort while it is down.
			s.log.Warn("accept", "err", err)
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.serve(conn); err != nil {
				s.log.Debug("connection ended", "err", err)
			}
		}()
	}
}

// serve runs one connection: startup handshake, then a query loop.
func (s *Server) serve(conn net.Conn) error {
	be := pgproto3.NewBackend(conn, conn)

	startup, err := be.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("receive startup: %w", err)
	}
	switch startup.(type) {
	case *pgproto3.StartupMessage:
	case *pgproto3.SSLRequest:
		// Deny SSL and let the client retry in plaintext. The socket is
		// a filesystem object with unix permissions; TLS over it buys
		// nothing.
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return err
		}
		return s.serve(conn)
	case *pgproto3.CancelRequest:
		return nil
	default:
		return fmt.Errorf("unexpected startup message %T", startup)
	}

	// No authentication: the socket's filesystem permissions are the
	// access control, the same posture Postgres itself uses for local
	// peer connections.
	if err := s.send(conn,
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	); err != nil {
		return err
	}

	for {
		msg, err := be.Receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := s.handleQuery(conn, m.String); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			return nil
		default:
			// Extended-protocol and everything else: this endpoint
			// serves one simple-query verb by design.
			if err := s.fail(conn, sqlStateProtocolViolation,
				fmt.Sprintf("syzy reservation endpoint accepts simple queries only (got %T)", msg)); err != nil {
				return err
			}
		}
	}
}

// handleQuery answers one query. Only the RESERVE verb is accepted.
func (s *Server) handleQuery(conn net.Conn, query string) error {
	q := strings.TrimSpace(query)
	// dblink sends a trailing semicolon depending on how the trigger
	// composes the statement.
	q = strings.TrimSuffix(q, ";")

	if !strings.HasPrefix(strings.ToUpper(q), reserveVerb) {
		return s.fail(conn, sqlStateProtocolViolation,
			"syzy reservation endpoint: expected RESERVE")
	}
	raw, err := hex.DecodeString(strings.TrimSpace(q[len(reserveVerb):]))
	if err != nil {
		return s.fail(conn, sqlStateProtocolViolation,
			fmt.Sprintf("syzy reservation endpoint: malformed payload: %v", err))
	}
	var req Request
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return s.fail(conn, sqlStateProtocolViolation,
			fmt.Sprintf("syzy reservation endpoint: %v", err))
	}
	if dec.More() {
		return s.fail(conn, sqlStateProtocolViolation,
			"syzy reservation endpoint: trailing data after request")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	switch err := s.reserver.Reserve(ctx, req); {
	case errors.Is(err, ErrDenied):
		return s.fail(conn, sqlStateUniqueViolation, "syzy: "+err.Error())
	case err != nil:
		// Fail closed and retryable: the write is refused, so no
		// duplicate can result, and the caller may try again.
		return s.fail(conn, sqlStateSerializationFailure,
			fmt.Sprintf("syzy: coordinated-key reservation unavailable: %v", err))
	}
	return s.send(conn,
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{
			Name:         []byte("reserve"),
			DataTypeOID:  25, // text
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       0,
		}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("ok")}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
}

// fail sends an ErrorResponse with the given SQLSTATE, then returns the
// connection to a ready state. The backend's trigger sees a raised
// error and aborts the transaction.
func (s *Server) fail(conn net.Conn, code, message string) error {
	return s.send(conn,
		&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: message},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
}

func (s *Server) send(conn net.Conn, msgs ...pgproto3.BackendMessage) error {
	var buf []byte
	var err error
	for _, m := range msgs {
		buf, err = m.Encode(buf)
		if err != nil {
			return err
		}
	}
	_, err = conn.Write(buf)
	return err
}

// EncodeRequest renders req the way the trigger does: JSON, hex-encoded.
// Used by tests and by any Go-side client of the endpoint.
func EncodeRequest(req Request) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	return reserveVerb + hex.EncodeToString(b), nil
}
