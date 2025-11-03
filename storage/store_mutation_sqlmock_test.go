package storage

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/storage/storagetest"
)

func TestEnqueueJob_DefaultsRunAtAndSerializesTZ(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2024, 12, 1, 9, 0, 0, 0, time.FixedZone("EST", -5*3600))
	expectedUTC := providerNow.UTC()

	store, err := NewStore(db, func() time.Time { return providerNow })
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, dedupe_key, resource_key) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING id;")).
		WithArgs("default", "send-email", []byte("{}"), 0, expectedUTC, 20, 10, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(99)))

	ctx := context.Background()
	_, err = store.EnqueueJob(ctx, EnqueueParams{Queue: "default", TaskType: "send-email"})
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchSchedulesTxSelectsWithoutLocks(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "task_type", "queue", "payload", "cron", "dedupe_key", "last_enqueued_at"}).
		AddRow(int64(1), "sync", "default", []byte(`{"a":1}`), "* * * * *", sql.NullString{}, sql.NullTime{})

	mock.ExpectBegin()
	mock.ExpectQuery(`
SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at
FROM queue_schedules;
`).
		WillReturnRows(rows)

	ctx := context.Background()
	store, err := NewStore(db, time.Now)
	require.NoError(t, err)

	tx, err := store.Begin(ctx)
	require.NoError(t, err)

	_, err = store.FetchSchedulesTx(ctx, tx)
	require.NoError(t, err)
}

func TestHeartbeatJob_TruncatesExtendToSeconds(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	base := time.Date(2030, 3, 4, 5, 6, 7, 9, time.FixedZone("IST", 19800))
	baseUTC := base.UTC()
	extend := 30*time.Second + 512*time.Millisecond
	leaseUntil := baseUTC.Add(30 * time.Second)

	store, err := NewStore(db, func() time.Time { return base })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_jobs\nSET lease_until = $3,\n    updated_at  = $4\nWHERE id=$1 AND worker_id=$2;")).
		WithArgs(int64(77), "wk", leaseUntil, baseUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.HeartbeatJob(ctx, 77, "wk", extend)
	require.NoError(t, err)
	storagetest.AssertUTC(t, leaseUntil)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestHeartbeatJob_ReturnsErrLeaseMismatchOnZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	base := time.Date(2031, 1, 2, 3, 4, 5, 0, time.FixedZone("CET", 1*3600))
	baseUTC := base.UTC()

	store, err := NewStore(db, func() time.Time { return base })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_jobs\nSET lease_until = $3,\n    updated_at  = $4\nWHERE id=$1 AND worker_id=$2;")).
		WithArgs(int64(1), "worker", baseUTC.Add(10*time.Second), baseUTC).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	err = store.HeartbeatJob(ctx, 1, "worker", 10*time.Second)
	assert.ErrorIs(t, err, ErrLeaseMismatch)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequeueJob_DefaultsRunAtToProviderUTC(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.FixedZone("ACDT", 10*3600+1800))
	nowUTC := now.UTC()

	store, err := NewStore(db, func() time.Time { return now })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_jobs\nSET status='queued',\n    run_at=$3,\n    worker_id=NULL,\n    lease_until=NULL,\n    updated_at=$4\nWHERE id=$1 AND worker_id=$2;")).
		WithArgs(int64(55), "worker-1", nowUTC, nowUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.RequeueJob(ctx, 55, "worker-1", time.Time{})
	require.NoError(t, err)
	storagetest.AssertUTC(t, nowUTC)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequeueJob_UsesProvidedRunAtAndNormalizes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.FixedZone("ACDT", 10*3600+1800))
	nowUTC := now.UTC()
	runAt := time.Date(2026, 7, 9, 1, 2, 3, 0, time.FixedZone("PDT", -7*3600))
	runAtUTC := runAt.UTC()

	store, err := NewStore(db, func() time.Time { return now })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_jobs\nSET status='queued',\n    run_at=$3,\n    worker_id=NULL,\n    lease_until=NULL,\n    updated_at=$4\nWHERE id=$1 AND worker_id=$2;")).
		WithArgs(int64(56), "worker-2", runAtUTC, nowUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctx := context.Background()
	err = store.RequeueJob(ctx, 56, "worker-2", runAt)
	require.NoError(t, err)
	storagetest.AssertUTC(t, runAtUTC)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequeueJob_ReturnsErrLeaseMismatchOnZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Date(2026, 7, 8, 9, 10, 11, 0, time.FixedZone("ACDT", 10*3600+1800))
	nowUTC := now.UTC()

	store, err := NewStore(db, func() time.Time { return now })
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_jobs\nSET status='queued',\n    run_at=$3,\n    worker_id=NULL,\n    lease_until=NULL,\n    updated_at=$4\nWHERE id=$1 AND worker_id=$2;")).
		WithArgs(int64(57), "worker-3", nowUTC, nowUTC).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	err = store.RequeueJob(ctx, 57, "worker-3", time.Time{})
	assert.ErrorIs(t, err, ErrLeaseMismatch)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFailJob_RecordsFailureAndCommits(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	base := time.Date(2027, 8, 9, 10, 11, 12, 0, time.FixedZone("JST", 9*3600))
	baseUTC := base.UTC()
	nextRun := time.Date(2027, 8, 10, 1, 2, 3, 0, time.FixedZone("CST", -6*3600))
	nextRunUTC := nextRun.UTC()

	store, err := NewStore(db, func() time.Time { return base })
	require.NoError(t, err)

	updateSQL := regexp.QuoteMeta("UPDATE queue_jobs\nSET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,\n    run_at = CASE WHEN attempts >= max_attempts THEN run_at ELSE $3 END,\n    worker_id=NULL,\n    lease_until=NULL,\n    updated_at=$4\nWHERE id=$1 AND worker_id=$2\nRETURNING attempts, status;")

	mock.ExpectBegin()
	mock.ExpectQuery(updateSQL).
		WithArgs(int64(88), "worker", nextRunUTC, baseUTC).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "status"}).AddRow(2, "queued"))

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO queue_job_failures (job_id, error, attempts, failed_at)\nVALUES ($1, $2, $3, $4);")).
		WithArgs(int64(88), "boom", 2, baseUTC).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx := context.Background()
	dead, err := store.FailJob(ctx, 88, "worker", nextRun, "boom")
	require.NoError(t, err)
	assert.False(t, dead, "expected job to remain queued")
	storagetest.AssertUTC(t, baseUTC)
	storagetest.AssertUTC(t, nextRunUTC)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFailJob_ReturnsErrLeaseMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	base := time.Date(2027, 8, 9, 10, 11, 12, 0, time.FixedZone("JST", 9*3600))
	baseUTC := base.UTC()

	store, err := NewStore(db, func() time.Time { return base })
	require.NoError(t, err)

	updateSQL := regexp.QuoteMeta("UPDATE queue_jobs\nSET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,\n    run_at = CASE WHEN attempts >= max_attempts THEN run_at ELSE $3 END,\n    worker_id=NULL,\n    lease_until=NULL,\n    updated_at=$4\nWHERE id=$1 AND worker_id=$2\nRETURNING attempts, status;")

	mock.ExpectBegin()
	mock.ExpectQuery(updateSQL).
		WithArgs(int64(89), "worker", baseUTC, baseUTC).
		WillReturnRows(sqlmock.NewRows([]string{"attempts", "status"}))
	mock.ExpectRollback()

	ctx := context.Background()
	dead, err := store.FailJob(ctx, 89, "worker", time.Time{}, "boom")
	assert.ErrorIs(t, err, ErrLeaseMismatch)
	assert.False(t, dead)

	require.NoError(t, mock.ExpectationsWereMet())
}
