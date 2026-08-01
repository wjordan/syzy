package main

import (
	"strings"
	"testing"
)

const tConn = "postgres://u:p@h:5432/mydb"

// base returns a minimal valid flag set; callers override/extend via extra.
func base(extra ...string) []string {
	return append([]string{
		"-conn", tConn,
		"-origin", "1",
		"-cluster-id", "ccccccccccccccccccccccccccccccdd",
		"-data-dir", "/tmp/x",
	}, extra...)
}

func TestParseFlags_Valid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []string
	}{
		{"tables", base("-tables", "public.kv")},
		{"ddl file", base("-ddl", "-schema-log", "/tmp/s.db", "-bucket", "file:///tmp/b")},
		{"ddl dial", base("-ddl", "-schema-log-dial", "127.0.0.1:7100", "-bucket", "file:///tmp/b")},
		{"ddl s3", base("-ddl", "-schema-log-s3", "https://b.s3.us-east-1.amazonaws.com?region=us-east-1", "-bucket", "file:///tmp/b")},
		{"ddl file+listen", base("-ddl", "-schema-log", "/tmp/s.db", "-schema-log-listen", ":7100", "-bucket", "file:///tmp/b")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parseFlags(c.args); err != nil {
				t.Fatalf("parseFlags(%v): unexpected error: %v", c.args, err)
			}
		})
	}
}

func TestParseFlags_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []string
		want string // substring of the error
	}{
		{"no conn", []string{"-origin", "1", "-cluster-id", "ccccccccccccccccccccccccccccccdd", "-data-dir", "/tmp/x", "-tables", "public.kv"}, "-conn required"},
		{"origin zero", base("-origin", "0", "-tables", "public.kv"), "-origin must be in 1..65535"},
		{"origin too big", base("-origin", "70000", "-tables", "public.kv"), "-origin must be in 1..65535"},
		{"bad cluster id", append(base("-tables", "public.kv"), "-cluster-id", "nothex"), "32 hex"},
		{"ddl and tables", base("-ddl", "-tables", "public.kv", "-schema-log", "/tmp/s.db"), "mutually exclusive"},
		{"neither tables nor ddl", base(), "either -tables or -ddl"},
		{"ddl no source", base("-ddl"), "one of -schema-log"},
		{"ddl two sources", base("-ddl", "-schema-log", "/tmp/s.db", "-schema-log-s3", "https://b/x"), "at most one"},
		{"listen needs file", base("-ddl", "-schema-log-dial", "127.0.0.1:7100", "-schema-log-listen", ":7100"), "requires -schema-log"},
		{"ddl no bucket", base("-ddl", "-schema-log", "/tmp/s.db"), "requires -bucket"},
		{"schema flag without ddl", base("-tables", "public.kv", "-schema-log", "/tmp/s.db"), "require -ddl"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFlags(c.args)
			if err == nil {
				t.Fatalf("parseFlags(%v): want error containing %q, got nil", c.args, c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("parseFlags error = %q, want substring %q", err, c.want)
			}
		})
	}
}

// TestParseFlags_Defaults checks the dbname-derived identity defaults.
func TestParseFlags_Defaults(t *testing.T) {
	t.Parallel()
	cfg, err := parseFlags(base("-tables", "public.kv"))
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.dbName != "mydb" {
		t.Errorf("dbName = %q, want mydb", cfg.dbName)
	}
	if cfg.slot != "syzy_slot_mydb" {
		t.Errorf("slot = %q, want syzy_slot_mydb", cfg.slot)
	}
	if cfg.originName != "syzy_origin_mydb" {
		t.Errorf("originName = %q, want syzy_origin_mydb", cfg.originName)
	}
	if !strings.Contains(cfg.replConnURL, "replication=database") {
		t.Errorf("replConnURL = %q, want it to contain replication=database", cfg.replConnURL)
	}
}
