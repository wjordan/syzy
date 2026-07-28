package syzyext

import (
	"errors"
	"fmt"
	"testing"

	"github.com/wjordan/syzy/internal/ctrlsock"
	"github.com/wjordan/syzy/internal/layout"
)

func TestPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"version skew", ctrlsock.ErrVersionMismatch, true},
		{"socket path too long", layout.ErrSocketPathTooLong, true},
		// Wrapped the way the real call chain wraps them: the attach
		// error travels up through several fmt.Errorf %w layers.
		{"wrapped version skew",
			fmt.Errorf("attach daemon: %w", fmt.Errorf("ctrlsock: %w", ctrlsock.ErrVersionMismatch)), true},
		{"wrapped socket path",
			fmt.Errorf("start mesh: %w", layout.CheckUnixSocketPath("/"+string(make([]byte, 200)))), true},
		// The retry loop exists for these; classifying one as permanent
		// would break recovery from a startup race.
		{"database is locked", errors.New("database is locked"), false},
		{"no daemon yet", ctrlsock.ErrNoDaemon, false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := permanent(tc.err); got != tc.want {
				t.Errorf("permanent(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
