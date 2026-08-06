package syzyenv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// ErrMalformed marks a configuration file that could not be parsed.
// Callers classify it as permanent: re-reading an unparseable file
// produces the same result.
var ErrMalformed = errors.New("syzyenv: malformed env file")

// Vars resolves SYZY_* configuration for one database: the process
// environment first, then the database's env file. A nil *Vars is
// usable and resolves from the environment alone.
type Vars struct {
	file map[string]string
}

// Get returns the value of key. Empty counts as unset in the
// environment, so a file value fills a gap rather than being shadowed
// by an exported-but-empty variable.
func (v *Vars) Get(key string) string {
	if val := Getenv(key); val != "" {
		return val
	}
	if v == nil {
		return ""
	}
	return v.file[key]
}

// Load reads path. Whether a missing file is a default to fall through
// or a mistake to report is the caller's policy: the error wraps
// os.ErrNotExist either way.
func Load(path string) (*Vars, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parse(path, f)
}

// parse reads the shell-sourceable subset described in the SQLite
// architecture doc: KEY=VALUE, an optional "export " prefix, #
// comments, blank lines, and optional matching quotes around a value.
// Anything else fails: a knob silently dropped for a typo is the
// failure mode this file exists to avoid.
func parse(path string, r io.Reader) (*Vars, error) {
	vars := &Vars{file: map[string]string{}}
	scan := bufio.NewScanner(r)
	for line := 1; scan.Scan(); line++ {
		text := strings.TrimSpace(scan.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		text = strings.TrimPrefix(text, "export ")
		key, value, found := strings.Cut(text, "=")
		if !found {
			return nil, fmt.Errorf("%w %s:%d: no '=' in %q", ErrMalformed, path, line, text)
		}
		key = strings.TrimSpace(key)
		if !validKey(key) {
			return nil, fmt.Errorf("%w %s:%d: %q is not a SYZY_* key", ErrMalformed, path, line, key)
		}
		if _, dup := vars.file[key]; dup {
			return nil, fmt.Errorf("%w %s:%d: %s set twice", ErrMalformed, path, line, key)
		}
		vars.file[key] = unquote(strings.TrimSpace(value))
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	return vars, nil
}

func validKey(key string) bool {
	if !strings.HasPrefix(key, "SYZY_") || len(key) == len("SYZY_") {
		return false
	}
	for _, c := range key {
		if c != '_' && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// unquote strips one layer of matching quotes. Values are taken
// literally otherwise: no escapes, no variable expansion.
func unquote(value string) string {
	if len(value) < 2 {
		return value
	}
	q := value[0]
	if (q == '"' || q == '\'') && value[len(value)-1] == q {
		return value[1 : len(value)-1]
	}
	return value
}
