package storage

import (
	"database/sql"
	"testing"
	"time"
)

func newStoreWithNow(t *testing.T, db *sql.DB, nowFn func() time.Time) *Store {
	t.Helper()
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Store{DB: NewDB(db), now: nowFn}
}
