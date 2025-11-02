package storagetest

import (
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type SQLMockRunner struct {
	sqlDB *sql.DB
	Mock  sqlmock.Sqlmock
}

func NewSQLMockRunner() (*SQLMockRunner, error) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		return nil, err
	}
	return &SQLMockRunner{sqlDB: db, Mock: mock}, nil
}

// MustSQLMock creates a new SQLMockRunner and returns its underlying *sql.DB.
// The caller should wrap this with storage.NewDB to get a storage.DB implementation.
// This avoids import cycles between storage and storagetest packages.
func MustSQLMock(t *testing.T) *sql.DB {
	t.Helper()
	runner, err := NewSQLMockRunner()
	if err != nil {
		t.Fatalf("failed to create sqlmock runner: %v", err)
	}
	// Store the runner for later expectation checking
	t.Cleanup(func() {
		runner.ExpectationsWereMet(t)
	})
	return runner.sqlDB
}

// MustSQLMockWithRunner creates a new SQLMockRunner and returns both the *sql.DB
// and the runner itself, allowing tests to set up expectations.
func MustSQLMockWithRunner(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	runner, err := NewSQLMockRunner()
	if err != nil {
		t.Fatalf("failed to create sqlmock runner: %v", err)
	}
	return runner.sqlDB, runner.Mock
}

func (r *SQLMockRunner) ExpectationsWereMet(t *testing.T) {
	t.Helper()
	r.Mock.ExpectClose()
	if err := r.sqlDB.Close(); err != nil {
		t.Fatalf("failed to close sqlmock db: %v", err)
	}
	if err := r.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

func AssertUTC(t *testing.T, ts time.Time) {
	t.Helper()
	if ts.Location() != time.UTC {
		t.Fatalf("expected time to be in UTC, got %s", ts.Location())
	}
}
