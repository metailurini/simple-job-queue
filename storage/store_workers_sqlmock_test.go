package storage

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/storage/storagetest"
)

func TestRegisterWorker_InsertsWithUTCTimestamp(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2025, 11, 16, 10, 30, 0, 0, time.FixedZone("PST", -8*3600))
	expectedUTC := providerNow.UTC()

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO queue_workers (worker_id, meta, last_seen)
VALUES ($1, $2, $3)
ON CONFLICT (worker_id) DO UPDATE
SET meta = EXCLUDED.meta, last_seen = EXCLUDED.last_seen;`)).
		WithArgs("worker-123", []byte(nil), expectedUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.RegisterWorker(ctx, "worker-123", nil)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRegisterWorker_WithMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2025, 11, 16, 10, 30, 0, 0, time.UTC)

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	meta := []byte(`{"version":"1.0"}`)
	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO queue_workers (worker_id, meta, last_seen)
VALUES ($1, $2, $3)
ON CONFLICT (worker_id) DO UPDATE
SET meta = EXCLUDED.meta, last_seen = EXCLUDED.last_seen;`)).
		WithArgs("worker-456", meta, providerNow).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.RegisterWorker(ctx, "worker-456", meta)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRegisterWorker_RequiresWorkerID(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store, err := NewStore(db, time.Now)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.RegisterWorker(ctx, "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "worker id is required")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHeartbeatWorker_UpdatesWithUTCTimestamp(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2025, 11, 16, 10, 35, 0, 0, time.FixedZone("EST", -5*3600))
	expectedUTC := providerNow.UTC()

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(`
UPDATE queue_workers
SET last_seen = $2
WHERE worker_id = $1;`)).
		WithArgs("worker-789", expectedUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.HeartbeatWorker(ctx, "worker-789")
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHeartbeatWorker_RequiresWorkerID(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store, err := NewStore(db, time.Now)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.HeartbeatWorker(ctx, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "worker id is required")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestActiveWorkers_SelectsWithCutoffParameter(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cutoff := time.Date(2025, 11, 16, 10, 25, 0, 0, time.FixedZone("JST", 9*3600))
	cutoffUTC := cutoff.UTC()

	store, err := NewStore(db, time.Now)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"worker_id"}).
		AddRow("worker-1").
		AddRow("worker-2").
		AddRow("worker-3")

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT worker_id
FROM queue_workers
WHERE last_seen >= $1
ORDER BY worker_id;`)).
		WithArgs(cutoffUTC).
		WillReturnRows(rows)

	ctx := context.Background()
	workers, err := store.ActiveWorkers(ctx, cutoff)
	require.NoError(t, err)
	require.Len(t, workers, 3)
	require.Equal(t, []string{"worker-1", "worker-2", "worker-3"}, workers)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestActiveWorkers_ReturnsEmptyWhenNoActiveWorkers(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cutoff := time.Date(2025, 11, 16, 10, 25, 0, 0, time.UTC)

	store, err := NewStore(db, time.Now)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"worker_id"})

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT worker_id
FROM queue_workers
WHERE last_seen >= $1
ORDER BY worker_id;`)).
		WithArgs(cutoff).
		WillReturnRows(rows)

	ctx := context.Background()
	workers, err := store.ActiveWorkers(ctx, cutoff)
	require.NoError(t, err)
	require.Len(t, workers, 0)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueBroadcast_CreatesOriginAndChildJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2025, 11, 16, 10, 40, 0, 0, time.UTC)

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	cutoff := providerNow.UTC().Add(-5 * time.Minute)

	// Expect origin job insert
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, dedupe_key, resource_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;`)).
		WithArgs("default", "broadcast-task", []byte(`{"msg":"hello"}`), 5, providerNow, 20, 10, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(100)))

	// Expect active workers query
	workerRows := sqlmock.NewRows([]string{"worker_id"}).
		AddRow("worker-A").
		AddRow("worker-B")
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT worker_id
FROM queue_workers
WHERE last_seen >= $1
ORDER BY worker_id;`)).
		WithArgs(cutoff).
		WillReturnRows(workerRows)

	// Expect child job inserts for each worker
	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, origin_job_id, target_worker_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`)).
		WithArgs("default", "broadcast-task", []byte(`{"msg":"hello"}`), 5, providerNow, 20, 10, int64(100), "worker-A").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, origin_job_id, target_worker_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`)).
		WithArgs("default", "broadcast-task", []byte(`{"msg":"hello"}`), 5, providerNow, 20, 10, int64(100), "worker-B").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Expect origin job to be marked as dispatched
	mock.ExpectExec(regexp.QuoteMeta(`
UPDATE queue_jobs
SET status = 'dispatched', updated_at = $2
WHERE id = $1;`)).
		WithArgs(int64(100), providerNow).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "broadcast-task",
			Payload:   []byte(`{"msg":"hello"}`),
			Priority:  5,
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, int64(100), ids[0])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueBroadcast_NoActiveWorkers(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2025, 11, 16, 10, 40, 0, 0, time.UTC)

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	cutoff := providerNow.UTC().Add(-5 * time.Minute)

	// Expect origin job insert
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, dedupe_key, resource_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;`)).
		WithArgs("default", "broadcast-task", []byte("{}"), 0, providerNow, 20, 10, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(200)))

	// Expect active workers query returns empty
	workerRows := sqlmock.NewRows([]string{"worker_id"})
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT worker_id
FROM queue_workers
WHERE last_seen >= $1
ORDER BY worker_id;`)).
		WithArgs(cutoff).
		WillReturnRows(workerRows)

	// Expect origin job to be marked as dispatched (no child jobs)
	mock.ExpectExec(regexp.QuoteMeta(`
UPDATE queue_jobs
SET status = 'dispatched', updated_at = $2
WHERE id = $1;`)).
		WithArgs(int64(200), providerNow).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "broadcast-task",
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, int64(200), ids[0])

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanJob_IncludesOriginAndTargetWorker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := &Store{DB: db, now: time.Now}

	nowUTC := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	originID := int64(100)
	targetWorker := "worker-X"

	rows := sqlmock.NewRows([]string{
		"id", "queue", "task_type", "payload", "priority", "run_at", "status", "attempts", "max_attempts", "backoff_sec",
		"lease_until", "worker_id", "dedupe_key", "resource_key", "origin_job_id", "target_worker_id", "created_at", "updated_at",
	}).AddRow(
		int64(101), "default", "task", []byte(`{}`), 0, nowUTC, "queued", 0, 20, 10,
		nil, nil, nil, nil, originID, targetWorker, nowUTC, nowUTC,
	)

	mock.ExpectQuery(regexp.QuoteMeta(selectJobSQL)).
		WithArgs(int64(101)).
		WillReturnRows(rows)

	ctx := context.Background()
	job, err := store.GetJob(ctx, int64(101))
	require.NoError(t, err)
	require.NotNil(t, job.OriginJobID)
	require.Equal(t, originID, *job.OriginJobID)
	require.NotNil(t, job.TargetWorkerID)
	require.Equal(t, targetWorker, *job.TargetWorkerID)

	storagetest.AssertUTC(t, job.RunAt)
	storagetest.AssertUTC(t, job.CreatedAt)
	storagetest.AssertUTC(t, job.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}
