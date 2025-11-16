package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/metailurini/simple-job-queue/timeprovider"
)

const testDatabaseEnv = "TEST_DATABASE_URL"

type mutableProvider struct {
	mu sync.Mutex
	t  time.Time
}

func newMutableProvider(start time.Time) *mutableProvider {
	return &mutableProvider{t: start}
}

func (m *mutableProvider) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *mutableProvider) Advance(d time.Duration) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = m.t.Add(d)
	return m.t
}

func (m *mutableProvider) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = t
}

func newTestStore(t *testing.T, fixed time.Time) (*Store, func()) {
	t.Helper()
	return newTestStoreWithProvider(t, timeprovider.FixedProvider{T: fixed})
}

func newTestStoreWithProvider(t *testing.T, provider timeprovider.Provider) (*Store, func()) {
	t.Helper()
	db := mustConnectTestDB(t)
	resetTestTables(t, db)

	store, err := NewStoreWithProvider(db, provider)
	if err != nil {
		t.Fatalf("failed to build store: %v", err)
	}

	cleanup := func() {
		resetTestTables(t, db)
		db.Close()
	}

	return store, cleanup
}

func mustConnectTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv(testDatabaseEnv)
	if dsn == "" {
		t.Skipf("set %s to run storage integration tests", testDatabaseEnv)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("connect test db: %v", err)
	}
	return db
}

func resetTestTables(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "TRUNCATE queue_job_failures, queue_jobs, queue_workers RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func seedRunnableJobs(t *testing.T, store *Store, count int, runAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < count; i++ {
		runAtVal := runAt
		_, err := store.EnqueueJob(ctx, EnqueueParams{
			Queue:          "default",
			TaskType:       fmt.Sprintf("task-%d", i),
			Payload:        []byte("{}"),
			Priority:       1,
			RunAt:          &runAtVal,
			MaxAttempts:    3,
			BackoffSeconds: 5,
		})
		if err != nil {
			t.Fatalf("seed job %d: %v", i, err)
		}
	}
}

func TestClaimJobs_LeaseExpiry_AllowsReclaim(t *testing.T) {
	provider := newMutableProvider(time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:    "default",
		TaskType: "reclaim",
		Payload:  []byte("{}"),
		RunAt:    &runAt,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	leaseDuration := 10 * time.Second
	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "workerA",
		Limit:         1,
		LeaseDuration: leaseDuration,
	})
	if err != nil {
		t.Fatalf("claim1: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(claim.Jobs))
	}

	provider.Advance(leaseDuration + time.Second)

	claim, err = store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "workerB",
		Limit:         1,
		LeaseDuration: leaseDuration,
		IncludeLeased: true,
	})
	if err != nil {
		t.Fatalf("claim2: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job on reclaim, got %d", len(claim.Jobs))
	}
	if claim.Jobs[0].ID != id {
		t.Fatalf("reclaimed job id = %d, want %d", claim.Jobs[0].ID, id)
	}
}

func TestEnqueueJob_RunAtDefaultsToProvider(t *testing.T) {
	provider := newMutableProvider(time.Date(2032, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("DefaultsToNow", func(t *testing.T) {
		id, err := store.EnqueueJob(ctx, EnqueueParams{Queue: "default", TaskType: "default"})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		job, err := store.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !job.RunAt.Equal(provider.Now()) {
			t.Fatalf("run_at = %v, want %v", job.RunAt, provider.Now())
		}
	})

	t.Run("HonorsExplicitPast", func(t *testing.T) {
		runAt := provider.Now().Add(-time.Hour)
		id, err := store.EnqueueJob(ctx, EnqueueParams{Queue: "default", TaskType: "past", RunAt: &runAt})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		job, err := store.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !job.RunAt.Equal(runAt) {
			t.Fatalf("run_at = %v, want %v", job.RunAt, runAt)
		}
	})

	t.Run("HonorsExplicitFuture", func(t *testing.T) {
		runAt := provider.Now().Add(time.Hour)
		id, err := store.EnqueueJob(ctx, EnqueueParams{Queue: "default", TaskType: "future", RunAt: &runAt})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		job, err := store.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !job.RunAt.Equal(runAt) {
			t.Fatalf("run_at = %v, want %v", job.RunAt, runAt)
		}
	})
}

func TestGetJob_NotFoundBehavior(t *testing.T) {
	store, cleanup := newTestStore(t, time.Now())
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := store.GetJob(ctx, 99999)
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

// TestConcurrency_NoDuplicateClaims_WithRace is a stress test for ClaimJobs.
// It's intended to be run with the -race flag to detect race conditions.
func TestConcurrency_NoDuplicateClaims_WithRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	provider := newMutableProvider(time.Date(2033, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	const (
		jobCount    = 50
		workerCount = 10
		batchSize   = 5
	)
	seedRunnableJobs(t, store, jobCount, provider.Now().Add(-time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[int64]int)
		errCh   = make(chan error, workerCount)
	)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				claim, err := store.ClaimJobs(ctx, ClaimOptions{
					Queue:         "default",
					WorkerID:      workerID,
					Limit:         batchSize,
					LeaseDuration: 30 * time.Second,
				})
				if err != nil {
					errCh <- err
					return
				}
				if len(claim.Jobs) == 0 {
					return
				}
				mu.Lock()
				for _, job := range claim.Jobs {
					claimed[job.ID]++
				}
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("worker error: %v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if len(claimed) != jobCount {
		t.Fatalf("expected %d unique jobs, got %d", jobCount, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("job %d was claimed %d times", id, count)
		}
	}
}

func TestClaimJobs_ReturnsExpectedLease(t *testing.T) {
	fixed := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	store, cleanup := newTestStore(t, fixed)
	defer cleanup()

	seedRunnableJobs(t, store, 4, fixed.Add(-time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leaseDuration := 30 * time.Second
	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker",
		Limit:         4,
		LeaseDuration: leaseDuration,
		Now:           fixed,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 4 {
		t.Fatalf("expected 4 claimed jobs, got %d", len(claim.Jobs))
	}

	expectedLease := fixed.Add((leaseDuration / time.Second) * time.Second)
	if claim.LeaseUntil.IsZero() {
		t.Fatalf("claim missing lease timestamp")
	}
	if !claim.LeaseUntil.Equal(expectedLease) {
		t.Fatalf("claim lease = %v, want %v", claim.LeaseUntil, expectedLease)
	}
	for _, job := range claim.Jobs {
		if job.LeaseUntil == nil {
			t.Fatalf("job %d missing lease", job.ID)
		}
		if !job.LeaseUntil.Equal(expectedLease) {
			t.Fatalf("job %d lease = %v, want %v", job.ID, *job.LeaseUntil, expectedLease)
		}
	}
}

func TestClaimJobsConcurrency_NoDuplicates(t *testing.T) {
	fixed := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
	store, cleanup := newTestStore(t, fixed)
	defer cleanup()

	const (
		jobCount    = 18
		workerCount = 5
		batchSize   = 4
	)
	seedRunnableJobs(t, store, jobCount, fixed.Add(-time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leaseDuration := 30 * time.Second
	expectedLease := fixed.Add((leaseDuration / time.Second) * time.Second)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[int64]int)
		total   int
		errCh   = make(chan error, jobCount)
	)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", idx)
			for {
				claim, err := store.ClaimJobs(ctx, ClaimOptions{
					Queue:         "default",
					WorkerID:      workerID,
					Limit:         batchSize,
					LeaseDuration: leaseDuration,
					IncludeLeased: false,
					Now:           fixed,
				})
				if err != nil {
					errCh <- fmt.Errorf("claim: %w", err)
					return
				}
				if len(claim.Jobs) == 0 {
					return
				}
				// compute expected lease locally and assert on jobs' LeaseUntil
				if claim.LeaseUntil.IsZero() || !claim.LeaseUntil.Equal(expectedLease) {
					errCh <- fmt.Errorf("batch lease mismatch: got %v want %v", claim.LeaseUntil, expectedLease)
				}
				mu.Lock()
				for _, job := range claim.Jobs {
					if job.LeaseUntil == nil || !job.LeaseUntil.Equal(expectedLease) {
						errCh <- fmt.Errorf("job %d lease mismatch: got %v want %v", job.ID, job.LeaseUntil, expectedLease)
					}
					claimed[job.ID]++
					total++
				}
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrency test failed: %v", err)
		}
	}

	if total != jobCount {
		t.Fatalf("expected %d claimed jobs, got %d", jobCount, total)
	}
	if len(claimed) != jobCount {
		t.Fatalf("expected %d unique job ids, got %d", jobCount, len(claimed))
	}
	for id, seen := range claimed {
		if seen != 1 {
			t.Fatalf("job %d claimed %d times", id, seen)
		}
	}
}

func TestEnqueueJob_DefaultsRunAtFromProvider(t *testing.T) {
	provider := newMutableProvider(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:    "default",
		TaskType: "noop",
		Payload:  []byte("{}"),
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	expected := provider.Now()

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !job.RunAt.Equal(expected) {
		t.Fatalf("RunAt = %v, want %v", job.RunAt, expected)
	}
}

func TestHeartbeatJob_UsesProviderTimestamps(t *testing.T) {
	provider := newMutableProvider(time.Date(2027, 3, 4, 5, 6, 7, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	runAtVal := runAt
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:          "default",
		TaskType:       "heartbeat",
		Payload:        []byte("{}"),
		Priority:       1,
		RunAt:          &runAtVal,
		MaxAttempts:    5,
		BackoffSeconds: 10,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker",
		Limit:         1,
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(claim.Jobs))
	}
	job := claim.Jobs[0]

	callTime := provider.Advance(10 * time.Second)
	extend := 45 * time.Second
	if err := store.HeartbeatJob(ctx, id, "worker", extend); err != nil {
		t.Fatalf("HeartbeatJob: %v", err)
	}

	job, err = store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.LeaseUntil == nil {
		t.Fatalf("expected lease to be set")
	}
	expectedLease := callTime.Add((extend / time.Second) * time.Second)
	if !job.LeaseUntil.Equal(expectedLease) {
		t.Fatalf("lease_until = %v, want %v", job.LeaseUntil, expectedLease)
	}
	if !job.UpdatedAt.Equal(callTime) {
		t.Fatalf("updated_at = %v, want %v", job.UpdatedAt, callTime)
	}
}

func TestClaimJobs_UsesProvidedNowDeterministically(t *testing.T) {
	provider := newMutableProvider(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	_, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:    "default",
		TaskType: "deterministic",
		Payload:  []byte("{}"),
		RunAt:    &runAt,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimTime := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	leaseDuration := 15 * time.Minute
	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker-deterministic",
		Limit:         1,
		LeaseDuration: leaseDuration,
		Now:           claimTime,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(claim.Jobs))
	}

	expectedLease := claimTime.Add(leaseDuration)
	if !claim.LeaseUntil.Equal(expectedLease) {
		t.Fatalf("claim lease = %v, want %v", claim.LeaseUntil, expectedLease)
	}
	job := claim.Jobs[0]
	if job.LeaseUntil == nil || !job.LeaseUntil.Equal(expectedLease) {
		t.Fatalf("job lease = %v, want %v", job.LeaseUntil, expectedLease)
	}
}

func TestFailJob_DeadLetterOnMaxAttempts(t *testing.T) {
	provider := newMutableProvider(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:       "default",
		TaskType:    "dead-letter",
		Payload:     []byte("{}"),
		RunAt:       &runAt,
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker-dead-letter",
		Limit:         1,
		LeaseDuration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job claimed, got %d", len(claim.Jobs))
	}

	dead, err := store.FailJob(ctx, id, "worker-dead-letter", time.Time{}, "final failure")
	if err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if !dead {
		t.Fatalf("expected dead=true, got false")
	}

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != "dead" {
		t.Fatalf("status = %s, want dead", job.Status)
	}
}

func TestFailJob_InsertsFailureRecordAndIncrementsAttempts(t *testing.T) {
	provider := newMutableProvider(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:       "default",
		TaskType:    "failure-record",
		Payload:     []byte("{}"),
		RunAt:       &runAt,
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker-failure-record",
		Limit:         1,
		LeaseDuration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job claimed, got %d", len(claim.Jobs))
	}

	failTime := provider.Advance(5 * time.Second)
	failureMessage := "something went wrong"
	dead, err := store.FailJob(ctx, id, "worker-failure-record", time.Time{}, failureMessage)
	if err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if dead {
		t.Fatalf("expected dead=false, got true")
	}

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", job.Attempts)
	}

	rows, err := store.DB.QueryContext(ctx, "SELECT error, failed_at FROM queue_job_failures WHERE job_id = $1", id)
	if err != nil {
		t.Fatalf("query failures: %v", err)
	}
	defer rows.Close()

	var (
		foundMessage string
		foundTime    time.Time
	)
	if !rows.Next() {
		t.Fatalf("expected failure record, got none")
	}
	if err := rows.Scan(&foundMessage, &foundTime); err != nil {
		t.Fatalf("scan failure: %v", err)
	}
	if foundMessage != failureMessage {
		t.Fatalf("message = %q, want %q", foundMessage, failureMessage)
	}
	if !foundTime.Equal(failTime) {
		t.Fatalf("failed_at = %v, want %v", foundTime, failTime)
	}
}

func TestHeartbeatJob_OwnerCheckRejectsWrongWorker(t *testing.T) {
	provider := newMutableProvider(time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:    "default",
		TaskType: "owner-check",
		Payload:  []byte("{}"),
		RunAt:    &runAt,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "workerA",
		Limit:         1,
		LeaseDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(claim.Jobs))
	}

	originalJob, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	err = store.HeartbeatJob(ctx, id, "workerB", 10*time.Minute)
	if err == nil {
		t.Fatalf("expected error for wrong worker")
	}

	updatedJob, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob after failed heartbeat: %v", err)
	}
	if !updatedJob.LeaseUntil.Equal(*originalJob.LeaseUntil) {
		t.Fatalf("lease_until changed: got %v, want %v", updatedJob.LeaseUntil, originalJob.LeaseUntil)
	}
	if !updatedJob.UpdatedAt.Equal(originalJob.UpdatedAt) {
		t.Fatalf("updated_at changed: got %v, want %v", updatedJob.UpdatedAt, originalJob.UpdatedAt)
	}
}

func TestCompleteRequeueFail_OwnerCheckRejectsWrongWorker(t *testing.T) {
	provider := newMutableProvider(time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:    "default",
		TaskType: "owner-check-multi",
		Payload:  []byte("{}"),
		RunAt:    &runAt,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "workerA",
		Limit:         1,
		LeaseDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(claim.Jobs))
	}

	originalJob, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	t.Run("CompleteJob", func(t *testing.T) {
		err := store.CompleteJob(ctx, id, "workerB")
		if err == nil {
			t.Fatalf("expected error")
		}
	})
	t.Run("RequeueJob", func(t *testing.T) {
		err := store.RequeueJob(ctx, id, "workerB", provider.Now())
		if err == nil {
			t.Fatalf("expected error")
		}
	})
	t.Run("FailJob", func(t *testing.T) {
		_, err := store.FailJob(ctx, id, "workerB", provider.Now(), "fail")
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	updatedJob, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob after failed ops: %v", err)
	}
	if updatedJob.Status != originalJob.Status {
		t.Fatalf("status changed: got %s, want %s", updatedJob.Status, originalJob.Status)
	}
	if updatedJob.Attempts != originalJob.Attempts {
		t.Fatalf("attempts changed: got %d, want %d", updatedJob.Attempts, originalJob.Attempts)
	}
}

func TestClaimJobs_PriorityAndRunAtOrdering(t *testing.T) {
	provider := newMutableProvider(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := provider.Now()
	runAtPast := now.Add(-time.Minute)
	runAtFuture := now.Add(time.Minute)

	jobs := []EnqueueParams{
		{Queue: "default", Priority: 1, RunAt: &runAtPast, TaskType: "p1-past"},
		{Queue: "default", Priority: 1, RunAt: &now, TaskType: "p1-now"},
		{Queue: "default", Priority: 2, RunAt: &runAtPast, TaskType: "p2-past"},
		{Queue: "default", Priority: 2, RunAt: &now, TaskType: "p2-now"},
		{Queue: "default", Priority: 1, RunAt: &runAtFuture, TaskType: "p1-future"},
	}

	for _, job := range jobs {
		_, err := store.EnqueueJob(ctx, job)
		if err != nil {
			t.Fatalf("enqueue %s: %v", job.TaskType, err)
		}
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue: "default", WorkerID: "worker-ordering", Limit: 10, LeaseDuration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	expectedOrder := []string{"p2-past", "p2-now", "p1-past", "p1-now"}
	if len(claim.Jobs) != len(expectedOrder) {
		t.Fatalf("expected %d jobs, got %d", len(expectedOrder), len(claim.Jobs))
	}
	for i, taskType := range expectedOrder {
		if claim.Jobs[i].TaskType != taskType {
			t.Errorf("job %d: got %s, want %s", i, claim.Jobs[i].TaskType, taskType)
		}
	}
}

func TestCompleteJob_UsesProviderTimestamp(t *testing.T) {
	provider := newMutableProvider(time.Date(2028, 4, 5, 6, 7, 8, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	runAtVal := runAt
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:       "default",
		TaskType:    "complete",
		Payload:     []byte("{}"),
		Priority:    1,
		RunAt:       &runAtVal,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker",
		Limit:         1,
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(claim.Jobs))
	}

	callTime := provider.Advance(5 * time.Second)
	if err := store.CompleteJob(ctx, id, "worker"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != "succeeded" {
		t.Fatalf("status = %s, want succeeded", job.Status)
	}
	if job.WorkerID != nil {
		t.Fatalf("expected worker_id cleared, got %v", job.WorkerID)
	}
	if job.LeaseUntil != nil {
		t.Fatalf("expected lease_until cleared, got %v", job.LeaseUntil)
	}
	if !job.UpdatedAt.Equal(callTime) {
		t.Fatalf("updated_at = %v, want %v", job.UpdatedAt, callTime)
	}
}

func TestRequeueJob_UsesProviderTimestamp(t *testing.T) {
	provider := newMutableProvider(time.Date(2029, 5, 6, 7, 8, 9, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	runAtVal := runAt
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:       "default",
		TaskType:    "requeue",
		Payload:     []byte("{}"),
		Priority:    1,
		RunAt:       &runAtVal,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker",
		Limit:         1,
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(claim.Jobs))
	}

	callTime := provider.Advance(15 * time.Second)
	nextRun := callTime.Add(2 * time.Minute)
	if err := store.RequeueJob(ctx, id, "worker", nextRun); err != nil {
		t.Fatalf("RequeueJob: %v", err)
	}

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != "queued" {
		t.Fatalf("status = %s, want queued", job.Status)
	}
	if job.WorkerID != nil {
		t.Fatalf("expected worker_id cleared, got %v", job.WorkerID)
	}
	if job.LeaseUntil != nil {
		t.Fatalf("expected lease_until cleared, got %v", job.LeaseUntil)
	}
	if !job.RunAt.Equal(nextRun) {
		t.Fatalf("run_at = %v, want %v", job.RunAt, nextRun)
	}
	if !job.UpdatedAt.Equal(callTime) {
		t.Fatalf("updated_at = %v, want %v", job.UpdatedAt, callTime)
	}
}

func TestFailJob_UsesProviderTimestamp(t *testing.T) {
	provider := newMutableProvider(time.Date(2030, 6, 7, 8, 9, 10, 0, time.UTC))
	store, cleanup := newTestStoreWithProvider(t, provider)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runAt := provider.Now().Add(-time.Minute)
	runAtVal := runAt
	id, err := store.EnqueueJob(ctx, EnqueueParams{
		Queue:       "default",
		TaskType:    "fail",
		Payload:     []byte("{}"),
		Priority:    1,
		RunAt:       &runAtVal,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claim, err := store.ClaimJobs(ctx, ClaimOptions{
		Queue:         "default",
		WorkerID:      "worker",
		Limit:         1,
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claim.Jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(claim.Jobs))
	}

	callTime := provider.Advance(20 * time.Second)
	nextRun := callTime.Add(3 * time.Minute)
	dead, err := store.FailJob(ctx, id, "worker", nextRun, "boom")
	if err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if dead {
		t.Fatalf("expected job to remain queued")
	}

	job, err := store.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != "queued" {
		t.Fatalf("status = %s, want queued", job.Status)
	}
	if !job.RunAt.Equal(nextRun) {
		t.Fatalf("run_at = %v, want %v", job.RunAt, nextRun)
	}
	if !job.UpdatedAt.Equal(callTime) {
		t.Fatalf("updated_at = %v, want %v", job.UpdatedAt, callTime)
	}
}
