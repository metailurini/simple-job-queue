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

func TestClaimJobs_UsesProvidedNowAndLeaseOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := &Store{DB: db, now: func() time.Time { panic("unexpected now() call") }}

	ctx := context.Background()
	tokyo := time.Date(2025, 2, 3, 4, 5, 6, 0, time.FixedZone("JST", 9*3600))
	leaseUTC := tokyo.Add(45 * time.Second).UTC()
	nowUTC := tokyo.UTC()

	rows := sqlmock.NewRows([]string{
		"id", "queue", "task_type", "payload", "priority", "run_at", "status", "attempts", "max_attempts", "backoff_sec",
		"lease_until", "worker_id", "dedupe_key", "resource_key", "created_at", "updated_at",
	}).AddRow(
		int64(10), "emails", "send", []byte(`{}`), 0, nowUTC, "running", 1, 20, 10,
		leaseUTC, "worker-1", nil, nil, nowUTC, nowUTC,
	)

	mock.ExpectQuery("WITH candidates AS \\( SELECT id FROM queue_jobs WHERE queue = \\$1 AND \\( \\(status = 'queued' AND run_at <= \\$5\\) OR \\(\\$6 AND status = 'running' AND lease_until < \\$5\\) \\) ORDER BY priority DESC, run_at ASC, id ASC LIMIT \\$3 \\), updated AS \\( UPDATE queue_jobs j SET status = 'running', worker_id = \\$2, attempts = j.attempts \\+ 1, lease_until = \\$4, updated_at = \\$5 FROM candidates c WHERE j.id = c.id AND \\( \\(j.status = 'queued' AND j.run_at <= \\$5\\) OR \\(\\$6 AND j.status = 'running' AND j.lease_until < \\$5\\) \\) RETURNING j.\\* \\) SELECT id, queue, task_type, payload, priority, run_at, status, attempts, max_attempts, backoff_sec, lease_until, worker_id, dedupe_key, resource_key, created_at, updated_at FROM updated ORDER BY priority DESC, run_at ASC, id ASC;").
		WithArgs("emails", "worker-1", 2, leaseUTC, nowUTC, true).
		WillReturnRows(rows)

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue: "emails", WorkerID: "worker-1", Limit: 2,
		LeaseDuration: 45 * time.Second, IncludeLeased: true, Now: tokyo,
	})
	require.NoError(t, err)
	require.Len(t, claim.Jobs, 1)
	assert.False(t, claim.LeaseUntil.IsZero(), "expected batch lease to be set")
	assert.True(t, claim.LeaseUntil.Equal(leaseUTC), "expected claim lease %v, got %v", leaseUTC, claim.LeaseUntil)
	job := claim.Jobs[0]
	require.NotNil(t, job.LeaseUntil, "expected lease until to be set")
	assert.True(t, job.LeaseUntil.Equal(leaseUTC), "expected claimed until %v, got %v", leaseUTC, *job.LeaseUntil)

	storagetest.AssertUTC(t, *job.LeaseUntil)
	storagetest.AssertUTC(t, job.RunAt)
	storagetest.AssertUTC(t, job.CreatedAt)
	storagetest.AssertUTC(t, job.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaimJobs_DefaultsNowAndTruncatesLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	providerNow := time.Date(2032, 6, 7, 8, 9, 10, 500*1e6, time.FixedZone("PDT", -7*3600))
	nowUTC := providerNow.UTC()
	leaseDuration := 30*time.Second + 850*time.Millisecond
	leaseUntil := nowUTC.Add(30 * time.Second)

	store := &Store{DB: db, now: func() time.Time { return providerNow }}

	ctx := context.Background()

	rows := sqlmock.NewRows([]string{
		"id", "queue", "task_type", "payload", "priority", "run_at", "status", "attempts", "max_attempts", "backoff_sec",
		"lease_until", "worker_id", "dedupe_key", "resource_key", "created_at", "updated_at",
	}).AddRow(
		int64(42), "default", "process", []byte(`{"foo":1}`), 1, nowUTC, "running", 2, 25, 15,
		leaseUntil, "wk", nil, nil, nowUTC, nowUTC,
	)

	mock.ExpectQuery("WITH candidates AS \\( SELECT id FROM queue_jobs WHERE queue = \\$1 AND \\( \\(status = 'queued' AND run_at <= \\$5\\) OR \\(\\$6 AND status = 'running' AND lease_until < \\$5\\) \\) ORDER BY priority DESC, run_at ASC, id ASC LIMIT \\$3 \\), updated AS \\( UPDATE queue_jobs j SET status = 'running', worker_id = \\$2, attempts = j.attempts \\+ 1, lease_until = \\$4, updated_at = \\$5 FROM candidates c WHERE j.id = c.id AND \\( \\(j.status = 'queued' AND j.run_at <= \\$5\\) OR \\(\\$6 AND j.status = 'running' AND j.lease_until < \\$5\\) \\) RETURNING j.\\* \\) SELECT id, queue, task_type, payload, priority, run_at, status, attempts, max_attempts, backoff_sec, lease_until, worker_id, dedupe_key, resource_key, created_at, updated_at FROM updated ORDER BY priority DESC, run_at ASC, id ASC;").
		WithArgs("default", "wk", 1, leaseUntil, nowUTC, false).
		WillReturnRows(rows)

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue: "default", WorkerID: "wk", Limit: 1, LeaseDuration: leaseDuration,
	})
	require.NoError(t, err)
	require.Len(t, claim.Jobs, 1)
	assert.False(t, claim.LeaseUntil.IsZero(), "expected batch lease to be set")
	assert.True(t, claim.LeaseUntil.Equal(leaseUntil), "expected claim lease %v, got %v", leaseUntil, claim.LeaseUntil)
	job := claim.Jobs[0]
	require.NotNil(t, job.LeaseUntil, "expected lease until to be set")
	assert.True(t, job.LeaseUntil.Equal(leaseUntil), "expected claimed until %v, got %v", leaseUntil, *job.LeaseUntil)

	storagetest.AssertUTC(t, *job.LeaseUntil)
	storagetest.AssertUTC(t, job.RunAt)
	storagetest.AssertUTC(t, job.CreatedAt)
	storagetest.AssertUTC(t, job.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}
