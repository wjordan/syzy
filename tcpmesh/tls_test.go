package tcpmesh

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// testCA is a throwaway CA that can issue node certs with a
// 127.0.0.1 SAN, suitable for both server and client auth.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// issue returns a node certificate (client+server usable) signed by
// the CA, with a 127.0.0.1 SAN so tls.Dial verification passes.
func (ca *testCA) issue(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("node key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("node cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// nodeTLS builds a mutual-TLS config: present cert, require and
// verify peer certs against trust.
func nodeTLS(cert tls.Certificate, trust *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      trust,
		ClientCAs:    trust,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
}

func newTLSMux(t *testing.T, tlsCfg *tls.Config, nodeID uint64, seeds ...string) *Mesh {
	t.Helper()
	m, err := New(Config{
		Listen:    "127.0.0.1:0",
		Seeds:     seeds,
		DialRetry: 25 * time.Millisecond,
		TLSConfig: tlsCfg,
		NodeID:    nodeID,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// TestTLS_MutualCertConnects: two muxes with certs from the same CA,
// mutual verification required on both ends, mesh successfully.
func TestTLS_MutualCertConnects(t *testing.T) {
	ca := newTestCA(t, "syzy-test-ca")
	a := newTLSMux(t, nodeTLS(ca.issue(t, "node-a"), ca.pool), 1)
	b := newTLSMux(t, nodeTLS(ca.issue(t, "node-b"), ca.pool), 2, a.Addr())
	a.SetSeeds([]string{b.Addr()})
	if !waitForReady(a, 1, 2*time.Second) || !waitForReady(b, 1, 2*time.Second) {
		t.Fatalf("mutual-TLS peers did not connect: a=%d b=%d", peerCount(a), peerCount(b))
	}
}

// TestTLS_WrongCARejected: a node whose cert chains to a different CA
// is rejected by both the dial and accept directions; no ready peer
// forms.
func TestTLS_WrongCARejected(t *testing.T) {
	ca := newTestCA(t, "syzy-test-ca")
	rogueCA := newTestCA(t, "rogue-ca")
	a := newTLSMux(t, nodeTLS(ca.issue(t, "node-a"), ca.pool), 1)
	// b trusts the real CA but presents a rogue-signed cert.
	b := newTLSMux(t, nodeTLS(rogueCA.issue(t, "node-b"), ca.pool), 2, a.Addr())
	a.SetSeeds([]string{b.Addr()})
	time.Sleep(300 * time.Millisecond)
	if n := peerCount(a); n != 0 {
		t.Errorf("a accepted %d peer(s) with wrong-CA cert, want 0", n)
	}
	if n := peerCount(b); n != 0 {
		t.Errorf("b connected %d peer(s) with wrong-CA cert, want 0", n)
	}
}

// TestInsecure_PlaintextBeyondLoopbackRefused: plaintext non-loopback
// TCP requires the explicit Insecure acknowledgement, for listeners
// and seeds alike; loopback and unix need nothing.
func TestInsecure_PlaintextBeyondLoopbackRefused(t *testing.T) {
	if _, err := New(Config{Listen: "0.0.0.0:0"}); err == nil || !strings.Contains(err.Error(), "Insecure") {
		t.Fatalf("New(wildcard, plaintext) err = %v, want refusal naming Insecure", err)
	}
	if _, err := New(Config{Listen: "127.0.0.1:0", Seeds: []string{"10.0.0.9:7000"}}); err == nil || !strings.Contains(err.Error(), "Insecure") {
		t.Fatalf("New(remote seed, plaintext) err = %v, want refusal naming Insecure", err)
	}
	m, err := New(Config{Listen: "0.0.0.0:0", Insecure: true})
	if err != nil {
		t.Fatalf("New(wildcard, Insecure) err = %v", err)
	}
	_ = m.Close()
	m, err = New(Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New(loopback, plaintext) err = %v", err)
	}
	defer m.Close()
	// Runtime seed refresh applies the same rule: the remote seed is
	// dropped with an error log, the loopback seed is kept.
	m.SetSeeds([]string{"10.0.0.9:7000", "127.0.0.1:9"})
	for _, s := range m.ActiveSeeds() {
		if s == "10.0.0.9:7000" {
			t.Fatalf("plaintext mux dials non-loopback seed %q", s)
		}
	}
	if got := m.ActiveSeeds(); len(got) != 1 || got[0] != "127.0.0.1:9" {
		t.Fatalf("ActiveSeeds = %v, want [127.0.0.1:9]", got)
	}
}
