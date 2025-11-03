package storagetest

import (
	"testing"
	"time"
)

func AssertUTC(t *testing.T, ts time.Time) {
	t.Helper()
	if ts.Location() != time.UTC {
		t.Fatalf("expected time to be in UTC, got %s", ts.Location())
	}
}
