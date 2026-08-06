package syzyext

import (
	"errors"
	"os"

	"github.com/wjordan/syzy/internal/layout"
	"github.com/wjordan/syzy/internal/syzyenv"
)

// LoadVars resolves the attach-time configuration view for dbPath: the
// process environment, backed by an env file for anything the
// environment leaves unset. The file is $SYZY_ENV_FILE when the
// operator named one, otherwise the database's own <dbPath>-syzy/env.
// Most databases have neither, which is not an error; a file the
// operator named by hand and misspelled is.
//
// Callers that need a knob before Attach (AutoSpawn) load the view
// once and pass it in Config.Vars, so one attach reads one file.
func LoadVars(dbPath string) (*syzyenv.Vars, error) {
	path := syzyenv.Getenv("SYZY_ENV_FILE")
	named := path != ""
	if !named {
		path = layout.EnvFile(dbPath)
	}
	vars, err := syzyenv.Load(path)
	if !named && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return vars, err
}
