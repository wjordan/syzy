package journal

import (
	"os"
	"testing"
	"time"
)

// makeManySegments appends enough sizeable records at the minimum segment
// size to span several segments, returning the journal and the sorted
// segment numbers on disk.
func makeManySegments(t *testing.T, dir string, records int) (*Journal, []uint32) {
	t.Helper()
	j, err := Open(dir, 1088, SyncOff) // 1088 = journal minimum
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	payload := make([]byte, 400)
	for i := 0; i < records; i++ {
		if _, _, err := j.Append(KindMirror, uint64(i+1), 7, payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	nums, err := listSegmentNumbers(dir)
	if err != nil {
		t.Fatalf("listSegmentNumbers: %v", err)
	}
	if len(nums) < 5 {
		t.Fatalf("only %d segments formed; raise record count", len(nums))
	}
	return j, nums
}

func setMtime(t *testing.T, dir string, num uint32, mt time.Time) {
	t.Helper()
	if err := os.Chtimes(segmentPath(dir, num), mt, mt); err != nil {
		t.Fatalf("chtimes seg %d: %v", num, err)
	}
}

func segExists(dir string, num uint32) bool {
	_, err := os.Stat(segmentPath(dir, num))
	return err == nil
}

// TestRetainAfterAged_PrunesOldKeepsYoung: old sealed segments below the
// cutoff are unlinked; segments younger than the age floor are kept.
func TestRetainAfterAged_PrunesOldKeepsYoung(t *testing.T) {
	dir := t.TempDir()
	j, nums := makeManySegments(t, dir, 30)
	defer j.Close()

	now := time.Now()
	old := now.Add(-2 * time.Hour)
	// First three segments are old; everything else is fresh.
	for i, n := range nums {
		if i < 3 {
			setMtime(t, dir, n, old)
		} else {
			setMtime(t, dir, n, now)
		}
	}

	if err := j.RetainAfterAged(j.Head(), now.Add(-1*time.Hour)); err != nil {
		t.Fatalf("RetainAfterAged: %v", err)
	}

	for i, n := range nums {
		if i < 3 {
			if segExists(dir, n) {
				t.Errorf("old segment %d (idx %d) should be pruned", n, i)
			}
		} else {
			if !segExists(dir, n) {
				t.Errorf("young segment %d (idx %d) should be kept", n, i)
			}
		}
	}
}

// TestRetainAfterAged_ContiguityNoGaps: a young segment sitting below the
// cutoff stops the prune prefix, so an even-older segment after it is kept
// rather than leaving a hole beneath a retained segment.
func TestRetainAfterAged_ContiguityNoGaps(t *testing.T) {
	dir := t.TempDir()
	j, nums := makeManySegments(t, dir, 30)
	defer j.Close()

	now := time.Now()
	old := now.Add(-2 * time.Hour)
	// idx0 old, idx1 young, idx2 old, rest fresh.
	setMtime(t, dir, nums[0], old)
	setMtime(t, dir, nums[1], now)
	setMtime(t, dir, nums[2], old)
	for _, n := range nums[3:] {
		setMtime(t, dir, n, now)
	}

	if err := j.RetainAfterAged(j.Head(), now.Add(-1*time.Hour)); err != nil {
		t.Fatalf("RetainAfterAged: %v", err)
	}

	if segExists(dir, nums[0]) {
		t.Errorf("segment idx0 (%d) should be pruned", nums[0])
	}
	if !segExists(dir, nums[1]) {
		t.Errorf("young segment idx1 (%d) must be kept", nums[1])
	}
	if !segExists(dir, nums[2]) {
		t.Errorf("old segment idx2 (%d) must be kept — pruning stops at the young idx1 to avoid a gap", nums[2])
	}
}

// TestRetainAfterAged_ZeroFloorPrunesAllBelow: a zero olderThan disables the
// age floor — every segment below the cutoff is pruned regardless of mtime,
// matching RetainAfter.
func TestRetainAfterAged_ZeroFloorPrunesAllBelow(t *testing.T) {
	dir := t.TempDir()
	j, nums := makeManySegments(t, dir, 30)
	defer j.Close()

	now := time.Now()
	for _, n := range nums {
		setMtime(t, dir, n, now) // all fresh: age floor would keep everything
	}

	if err := j.RetainAfterAged(j.Head(), time.Time{}); err != nil {
		t.Fatalf("RetainAfterAged: %v", err)
	}

	cutoff := j.Head().seg()
	for _, n := range nums {
		if n < cutoff && segExists(dir, n) {
			t.Errorf("segment %d below cutoff %d should be pruned with zero floor", n, cutoff)
		}
	}
	if !segExists(dir, cutoff) {
		t.Errorf("active segment %d must survive", cutoff)
	}
}
