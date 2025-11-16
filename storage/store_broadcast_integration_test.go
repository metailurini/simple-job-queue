package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBroadcast_FanOut verifies that a broadcast job creates an origin and per-worker children
func TestBroadcast_FanOut(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register 3 workers
	require.NoError(t, store.RegisterWorker(ctx, "worker-A", nil))
	require.NoError(t, store.RegisterWorker(ctx, "worker-B", nil))
	require.NoError(t, store.RegisterWorker(ctx, "worker-C", nil))

	// Enqueue broadcast job
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
	originID := ids[0]

	// Verify origin job exists and is marked as dispatched
	origin, err := store.GetJob(ctx, originID)
	require.NoError(t, err)
	assert.Equal(t, "dispatched", origin.Status)
	assert.Nil(t, origin.TargetWorkerID)
	assert.Nil(t, origin.OriginJobID)

	// Verify 3 child jobs were created
	var childJobs []Job
	rows, err := store.DB.QueryContext(ctx, `
SELECT `+jobColumns+`
FROM queue_jobs
WHERE origin_job_id = $1
ORDER BY target_worker_id;`, originID)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		job, err := scanJob(rows)
		require.NoError(t, err)
		childJobs = append(childJobs, job)
	}
	require.NoError(t, rows.Err())

	require.Len(t, childJobs, 3)

	// Verify child jobs have correct fields
	for i, child := range childJobs {
		assert.Equal(t, "default", child.Queue, "child %d queue", i)
		assert.Equal(t, "broadcast-task", child.TaskType, "child %d task", i)
		assert.Equal(t, "queued", child.Status, "child %d status", i)
		assert.NotNil(t, child.OriginJobID, "child %d origin", i)
		assert.Equal(t, originID, *child.OriginJobID, "child %d origin ID", i)
		assert.NotNil(t, child.TargetWorkerID, "child %d target", i)
	}

	// Verify each worker got one child
	targets := []string{*childJobs[0].TargetWorkerID, *childJobs[1].TargetWorkerID, *childJobs[2].TargetWorkerID}
	assert.Contains(t, targets, "worker-A")
	assert.Contains(t, targets, "worker-B")
	assert.Contains(t, targets, "worker-C")
}

// TestBroadcast_NoActiveWorkers verifies behavior when no workers are active
func TestBroadcast_NoActiveWorkers(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Enqueue broadcast job with no active workers
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "broadcast-task",
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	originID := ids[0]

	// Verify origin exists and is dispatched
	origin, err := store.GetJob(ctx, originID)
	require.NoError(t, err)
	assert.Equal(t, "dispatched", origin.Status)

	// Verify no child jobs were created
	var count int
	err = store.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_jobs WHERE origin_job_id = $1;`, originID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestBroadcast_OfflineWorkerMissesBroadcast verifies that offline workers don't get jobs
func TestBroadcast_OfflineWorkerMissesBroadcast(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register worker A
	require.NoError(t, store.RegisterWorker(ctx, "worker-A", nil))

	// Enqueue broadcast job
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "broadcast-task",
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	originID := ids[0]

	// Register worker B after broadcast
	require.NoError(t, store.RegisterWorker(ctx, "worker-B", nil))

	// Verify only worker-A got a child job
	var childTargets []string
	rows, err := store.DB.QueryContext(ctx, `SELECT target_worker_id FROM queue_jobs WHERE origin_job_id = $1;`, originID)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var target string
		require.NoError(t, rows.Scan(&target))
		childTargets = append(childTargets, target)
	}
	require.NoError(t, rows.Err())

	require.Len(t, childTargets, 1)
	assert.Equal(t, "worker-A", childTargets[0])
}

// TestClaim_TargetedJobOnlyForTargetWorker verifies that targeted jobs are only claimable by their target worker
func TestClaim_TargetedJobOnlyForTargetWorker(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register workers
	require.NoError(t, store.RegisterWorker(ctx, "worker-X", nil))
	require.NoError(t, store.RegisterWorker(ctx, "worker-Y", nil))

	// Enqueue broadcast job
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "test-task",
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	originID := ids[0]

	// Find a child targeted to worker-X
	var targetXJobID int64
	err = store.DB.QueryRowContext(ctx, `SELECT id FROM queue_jobs WHERE origin_job_id = $1 AND target_worker_id = $2;`, originID, "worker-X").Scan(&targetXJobID)
	require.NoError(t, err)

	// Worker Y tries to claim - should not get worker-X's job
	claimY, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker-Y",
		Limit:         10,
		LeaseDuration: 30 * time.Second,
		Now:           now,
	})
	require.NoError(t, err)

	// Verify worker-Y did not claim worker-X's job
	for _, job := range claimY.Jobs {
		assert.NotEqual(t, targetXJobID, job.ID, "worker-Y should not claim worker-X's job")
		if job.TargetWorkerID != nil {
			assert.Equal(t, "worker-Y", *job.TargetWorkerID)
		}
	}

	// Worker X claims - should get its targeted job
	claimX, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker-X",
		Limit:         10,
		LeaseDuration: 30 * time.Second,
		Now:           now,
	})
	require.NoError(t, err)

	// Verify worker-X claimed its job
	found := false
	for _, job := range claimX.Jobs {
		if job.ID == targetXJobID {
			found = true
			assert.NotNil(t, job.TargetWorkerID)
			assert.Equal(t, "worker-X", *job.TargetWorkerID)
		}
	}
	assert.True(t, found, "worker-X should claim its targeted job")
}

// TestBroadcast_FailureIsolation verifies that failures on one worker don't affect others
func TestBroadcast_FailureIsolation(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register 2 workers
	require.NoError(t, store.RegisterWorker(ctx, "worker-A", nil))
	require.NoError(t, store.RegisterWorker(ctx, "worker-B", nil))

	// Enqueue broadcast job
	ids, err := store.EnqueueJobs(ctx, []EnqueueParams{
		{
			Queue:     "default",
			TaskType:  "test-task",
			Broadcast: true,
		},
	})
	require.NoError(t, err)
	originID := ids[0]

	// Get child jobs
	var childA, childB Job
	rows, err := store.DB.QueryContext(ctx, `SELECT `+jobColumns+` FROM queue_jobs WHERE origin_job_id = $1 ORDER BY target_worker_id;`, originID)
	require.NoError(t, err)
	require.True(t, rows.Next())
	childA, err = scanJob(rows)
	require.NoError(t, err)
	require.True(t, rows.Next())
	childB, err = scanJob(rows)
	require.NoError(t, err)
	rows.Close()

	workerA := *childA.TargetWorkerID
	workerB := *childB.TargetWorkerID

	// Worker A claims and completes successfully
	claimA, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      workerA,
		Limit:         10,
		LeaseDuration: 30 * time.Second,
		Now:           now,
	})
	require.NoError(t, err)
	require.Len(t, claimA.Jobs, 1)
	require.NoError(t, store.CompleteJob(ctx, claimA.Jobs[0].ID, workerA))

	// Worker B claims and fails
	claimB, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      workerB,
		Limit:         10,
		LeaseDuration: 30 * time.Second,
		Now:           now,
	})
	require.NoError(t, err)
	require.Len(t, claimB.Jobs, 1)

	nextRun := now.Add(10 * time.Second)
	dead, err := store.FailJob(ctx, claimB.Jobs[0].ID, workerB, nextRun, "test error")
	require.NoError(t, err)
	assert.False(t, dead, "job should not be dead after first attempt")

	// Verify worker A's job succeeded
	jobA, err := store.GetJob(ctx, claimA.Jobs[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", jobA.Status)
	assert.Equal(t, 0, jobA.Attempts)

	// Verify worker B's job is queued for retry
	jobB, err := store.GetJob(ctx, claimB.Jobs[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "queued", jobB.Status)
	assert.Equal(t, 1, jobB.Attempts)
}

// TestHeartbeatWorker_UpdatesTimestamp verifies worker heartbeat updates last_seen
func TestHeartbeatWorker_UpdatesTimestamp(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register worker
	require.NoError(t, store.RegisterWorker(ctx, "worker-1", nil))

	// Get initial last_seen
	var lastSeen1 time.Time
	err := store.DB.QueryRowContext(ctx, `SELECT last_seen FROM queue_workers WHERE worker_id = $1;`, "worker-1").Scan(&lastSeen1)
	require.NoError(t, err)
	assert.True(t, lastSeen1.Equal(now))

	// Advance time and heartbeat
	provider.Advance(30 * time.Second)
	require.NoError(t, store.HeartbeatWorker(ctx, "worker-1"))

	// Verify last_seen updated
	var lastSeen2 time.Time
	err = store.DB.QueryRowContext(ctx, `SELECT last_seen FROM queue_workers WHERE worker_id = $1;`, "worker-1").Scan(&lastSeen2)
	require.NoError(t, err)
	assert.True(t, lastSeen2.Equal(now.Add(30*time.Second)))
}

// TestActiveWorkers_FiltersByLastSeen verifies active workers selection based on cutoff
func TestActiveWorkers_FiltersByLastSeen(t *testing.T) {
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := newMutableProvider(now)
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx := context.Background()

	// Register workers at different times
	require.NoError(t, store.RegisterWorker(ctx, "worker-1", nil))
	provider.Advance(3 * time.Minute)
	require.NoError(t, store.RegisterWorker(ctx, "worker-2", nil))
	provider.Advance(3 * time.Minute)
	require.NoError(t, store.RegisterWorker(ctx, "worker-3", nil))

	// Query with cutoff 5 minutes ago from latest
	currentTime := provider.Now()
	cutoff := currentTime.Add(-5 * time.Minute)
	workers, err := store.ActiveWorkers(ctx, cutoff)
	require.NoError(t, err)

	// Only worker-2 and worker-3 should be active (within 5 minutes)
	require.Len(t, workers, 2)
	assert.Contains(t, workers, "worker-2")
	assert.Contains(t, workers, "worker-3")
	assert.NotContains(t, workers, "worker-1")
}
