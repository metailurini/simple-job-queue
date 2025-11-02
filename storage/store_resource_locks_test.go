package storage

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/storage/storagetest"
)

func TestAcquireResource_Success(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	now := time.Date(2025, 10, 31, 12, 0, 0, 0, time.UTC)
	store := newStoreWithNow(t, db, func() time.Time { return now })

	// Expect INSERT returning 1 row affected
	mock.ExpectExec(`
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_key) DO NOTHING;`).
		WithArgs("order:123", int64(42), "worker-1", now).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := store.AcquireResource(ctx, "order:123", 42, "worker-1")
	require.NoError(t, err)
	storagetest.AssertUTC(t, now)
}

func TestAcquireResource_Conflict(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	now := time.Date(2025, 10, 31, 12, 0, 0, 0, time.UTC)
	store := newStoreWithNow(t, db, func() time.Time { return now })

	// ON CONFLICT DO NOTHING → 0 rows affected
	mock.ExpectExec(`
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_key) DO NOTHING;`).
		WithArgs("order:123", int64(42), "worker-1", now).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	err := store.AcquireResource(ctx, "order:123", 42, "worker-1")
	assert.ErrorIs(t, err, ErrResourceBusy)
}

func TestAcquireResource_EmptyKey(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	store := newStoreWithNow(t, db, time.Now)

	// No SQL should be executed for empty resource key
	ctx := context.Background()
	err := store.AcquireResource(ctx, "", 42, "worker-1")
	require.NoError(t, err)
}

func TestAcquireResource_NormalizesNonUTCTimezone(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	// Provider returns a time in a non-UTC timezone (EST)
	providerNow := time.Date(2025, 10, 31, 12, 0, 0, 0, time.FixedZone("EST", -5*3600))
	expectedUTC := providerNow.UTC()
	store := newStoreWithNow(t, db, func() time.Time { return providerNow })

	// Expect the time to be normalized to UTC in the SQL args
	mock.ExpectExec(`
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_key) DO NOTHING;`).
		WithArgs("order:456", int64(99), "worker-2", expectedUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := store.AcquireResource(ctx, "order:456", 99, "worker-2")
	require.NoError(t, err)
	storagetest.AssertUTC(t, expectedUTC)
}

func TestAcquireResource_ExecutionError(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	now := time.Date(2025, 10, 31, 12, 0, 0, 0, time.UTC)
	store := newStoreWithNow(t, db, func() time.Time { return now })

	// Simulate a database error (e.g., connection failure)
	mock.ExpectExec(`
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_key) DO NOTHING;`).
		WithArgs("order:789", int64(50), "worker-3", now).
		WillReturnError(sqlmock.ErrCancelled)

	ctx := context.Background()
	err := store.AcquireResource(ctx, "order:789", 50, "worker-3")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrResourceBusy, "should return execution error, not ErrResourceBusy")
	assert.Contains(t, err.Error(), "acquire resource")
}

func TestReleaseResource_Success(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	store := newStoreWithNow(t, db, time.Now)

	mock.ExpectExec(`
DELETE FROM queue_resource_locks
WHERE resource_key = $1 AND job_id = $2;`).
		WithArgs("order:123", int64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err := store.ReleaseResource(ctx, "order:123", 42)
	require.NoError(t, err)
}

func TestReleaseResource_NotFound(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	store := newStoreWithNow(t, db, time.Now)

	// Delete returns 0 rows affected when resource not found
	mock.ExpectExec(`
DELETE FROM queue_resource_locks
WHERE resource_key = $1 AND job_id = $2;`).
		WithArgs("order:999", int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	// ReleaseResource should not return an error even if the resource was not found
	err := store.ReleaseResource(ctx, "order:999", 100)
	require.NoError(t, err)
}

func TestReleaseResource_EmptyKey(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	store := newStoreWithNow(t, db, time.Now)

	// No SQL should be executed for empty resource key
	ctx := context.Background()
	err := store.ReleaseResource(ctx, "", 42)
	require.NoError(t, err)
}

func TestReleaseResource_ExecutionError(t *testing.T) {
	db, mock := storagetest.MustSQLMockWithRunner(t)
	defer func() { mock.ExpectClose(); db.Close(); require.NoError(t, mock.ExpectationsWereMet()) }()

	store := newStoreWithNow(t, db, time.Now)

	// Simulate a database error
	mock.ExpectExec(`
DELETE FROM queue_resource_locks
WHERE resource_key = $1 AND job_id = $2;`).
		WithArgs("order:error", int64(200)).
		WillReturnError(sqlmock.ErrCancelled)

	ctx := context.Background()
	err := store.ReleaseResource(ctx, "order:error", 200)
	require.Error(t, err)
}
