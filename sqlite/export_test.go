package sqlite

import "time"

// SetSecondaryIntervalsForTest shrinks the secondary-origin rescan and
// drain-poll cadence so tests gated on the rescan tick don't wait out
// the production 2s/500ms intervals. Returns a restore func; callers
// must set the intervals before Open (they are read at Open time) and
// must not run in parallel with other tests that Open a Transport node.
func SetSecondaryIntervalsForTest(rescan, drainPoll time.Duration) (restore func()) {
	oldRescan, oldPoll := secondaryRescanInterval, secondaryDrainPollInterval
	secondaryRescanInterval, secondaryDrainPollInterval = rescan, drainPoll
	return func() {
		secondaryRescanInterval, secondaryDrainPollInterval = oldRescan, oldPoll
	}
}
