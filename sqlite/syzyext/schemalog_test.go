package syzyext

import (
	"context"
	"net"
	"testing"

	"github.com/wjordan/syzy/schemalog"
)

func TestOpenSchemaLogUnixDial(t *testing.T) {
	ln, err := net.Listen("unix", t.TempDir()+"/schema.sock")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := schemalog.Serve(ln, schemalog.NewLocal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("SYZY_SCHEMA_LOG_DIAL", "unix:"+ln.Addr().String())

	log, closer, err := OpenSchemaLog(t.TempDir() + "/app.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closer.Close() })
	if seq, err := log.Append(context.Background(), 0, []byte("op"), "CREATE TABLE t (id)"); err != nil || seq != 1 {
		t.Fatalf("Append = %d, %v", seq, err)
	}
}
