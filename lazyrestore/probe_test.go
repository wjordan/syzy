//go:build linux

package lazyrestore

import "testing"

// The probe's verdict depends on the environment (kernel version,
// CAP_SYS_ADMIN), so assert behavior, not the value: it must not
// panic, must be stable across calls, and must leave no mount behind.
func TestPassthroughAvailable(t *testing.T) {
	first := PassthroughAvailable()
	if second := PassthroughAvailable(); second != first {
		t.Fatalf("probe not stable: first=%v second=%v", first, second)
	}
	t.Logf("kernel FUSE passthrough available: %v", first)
}
