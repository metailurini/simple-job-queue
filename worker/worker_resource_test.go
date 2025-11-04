package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

// mockJobStore is a test double for the jobStore interface
type mockJobStore struct {
	acquireResourceErr   error
	releaseResourceErr   error
	requeueErr           error
	failErr              error
	completeErr          error
	rescheduleErr        error
	acquireResourceCalls int32
	releaseResourceCalls int32
	requeueCalls         int32
	failCalls            int32
	completeCalls        int32
	rescheduleCalls      int32
	mu                   sync.Mutex
	requeuedJobs         []int64
	failedJobs           []int64
	completedJobs        []int64
}

func (m *mockJobStore) ClaimJobs(ctx context.Context, opts storage.ClaimOptions) (storage.ClaimResult, error) {
	return storage.ClaimResult{}, nil
}

func (m *mockJobStore) HeartbeatJob(ctx context.Context, id int64, workerID string, extend time.Duration) error {
	return nil
}

func (m *mockJobStore) RequeueJob(ctx context.Context, id int64, workerID string, runAt time.Time) error {
	atomic.AddInt32(&m.requeueCalls, 1)
	m.mu.Lock()
	m.requeuedJobs = append(m.requeuedJobs, id)
	m.mu.Unlock()
	return m.requeueErr
}

func (m *mockJobStore) FailJob(ctx context.Context, id int64, workerID string, nextRun time.Time, errText string) (bool, error) {
	atomic.AddInt32(&m.failCalls, 1)
	m.mu.Lock()
	m.failedJobs = append(m.failedJobs, id)
	m.mu.Unlock()
	return false, m.failErr
}

func (m *mockJobStore) CompleteJob(ctx context.Context, id int64, workerID string) error {
	atomic.AddInt32(&m.completeCalls, 1)
	m.mu.Lock()
	m.completedJobs = append(m.completedJobs, id)
	m.mu.Unlock()
	return m.completeErr
}

func (m *mockJobStore) AcquireResource(ctx context.Context, resourceKey string, jobID int64, workerID string) error {
	atomic.AddInt32(&m.acquireResourceCalls, 1)
	return m.acquireResourceErr
}

func (m *mockJobStore) ReleaseResource(ctx context.Context, resourceKey string, jobID int64) error {
	atomic.AddInt32(&m.releaseResourceCalls, 1)
	return m.releaseResourceErr
}

func (m *mockJobStore) RescheduleJob(ctx context.Context, id int64, runAt time.Time) error {
	atomic.AddInt32(&m.rescheduleCalls, 1)
	return m.rescheduleErr
}

func setupTestRunner(t *testing.T, mockStore *mockJobStore, handlers HandlerMap) *Runner {
	t.Helper()
	if handlers == nil {
		handlers = HandlerMap{
			"test": func(ctx context.Context, job storage.Job) error { return nil },
		}
	}
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	cfg := Config{
		WorkerID:              "test-worker",
		Queues:                []QueueConfig{{Name: "default", LeaseDuration: 30 * time.Second, BatchSize: 1}},
		PollInterval:          100 * time.Millisecond,
		HeartbeatInterval:     0, // Disable heartbeat for simpler tests
		MaxInFlight:           2,
		StoreOperationTimeout: 1 * time.Second,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		TimeProvider:          timeprovider.RealProvider{},
	}

	store, err := storage.NewStore(db, time.Now)
	require.NoError(t, err)

	runner, err := NewRunner(store, db, handlers, cfg)
	require.NoError(t, err)
	// Replace the store with our mock
	runner.store = mockStore
	return runner
}

// executeJobSync is a test helper that runs executeJob synchronously.
// It handles the inflight channel semantics that executeJob expects.
func executeJobSync(t *testing.T, runner *Runner, ctx context.Context, job storage.Job) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runner.inflight <- struct{}{}
		runner.executeJob(ctx, job)
	}()
	wg.Wait()
}

// TestExecuteJob_AcquireResource_Success verifies that a job with a resource_key
// successfully acquires the resource, runs the handler, and releases the resource.
func TestExecuteJob_AcquireResource_Success(t *testing.T) {
	mockStore := &mockJobStore{}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:123"
	job := storage.Job{
		ID:          100,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: &resourceKey,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	executeJobSync(t, runner, ctx, job)

	// Verify acquire was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should acquire resource")
	// Verify handler was called
	assert.True(t, handlerCalled, "handler should be called after successful acquire")
	// Verify resource was released
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should release resource")
	// Verify job was completed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.completeCalls), "job should be completed")
	// Verify job was not requeued or failed
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.requeueCalls), "job should not be requeued")
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.failCalls), "job should not fail")
}

// TestExecuteJob_AcquireResource_Busy verifies that when resource acquisition fails
// with ErrResourceBusy, the job is requeued and the handler is NOT invoked.
func TestExecuteJob_AcquireResource_Busy(t *testing.T) {
	mockStore := &mockJobStore{
		acquireResourceErr: storage.ErrResourceBusy,
	}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:456"
	job := storage.Job{
		ID:          200,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was attempted
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should attempt to acquire resource")
	// Verify handler was NOT called
	assert.False(t, handlerCalled, "handler should not be called when resource is busy")
	// Verify resource was NOT released (we never acquired it)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should not release resource we didn't acquire")
	// Verify job was rescheduled
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.rescheduleCalls), "job should be rescheduled")
	// Verify job was not completed or failed
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.requeueCalls), "job should not be requeued")
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.completeCalls), "job should not be completed")
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.failCalls), "job should not fail")
}

// TestExecuteJob_AcquireResource_ExecutionError verifies that when resource acquisition
// fails with a non-busy error (e.g., DB down), the job fails and the handler is NOT invoked.
func TestExecuteJob_AcquireResource_ExecutionError(t *testing.T) {
	mockStore := &mockJobStore{
		acquireResourceErr: errors.New("database connection lost"),
	}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:789"
	job := storage.Job{
		ID:          300,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was attempted
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should attempt to acquire resource")
	// Verify handler was NOT called
	assert.False(t, handlerCalled, "handler should not be called when acquire fails")
	// Verify resource was NOT released
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should not release resource we didn't acquire")
	// Verify job was failed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.failCalls), "job should fail on acquire error")
	// Verify job was not completed or requeued
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.completeCalls), "job should not be completed")
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.requeueCalls), "job should not be requeued")
}

// TestExecuteJob_ReleaseResource_OnPanic verifies that the resource is released
// even when the handler panics.
func TestExecuteJob_ReleaseResource_OnPanic(t *testing.T) {
	mockStore := &mockJobStore{}
	handlers := HandlerMap{
		"panic_task": func(ctx context.Context, job storage.Job) error {
			panic("boom")
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:panic"
	job := storage.Job{
		ID:          400,
		Queue:       "default",
		TaskType:    "panic_task",
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should acquire resource")
	// Verify resource was released after panic
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should release resource after panic")
	// Verify job was failed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.failCalls), "job should fail after panic")
}

// TestExecuteJob_ReleaseResource_OnHandlerError verifies that the resource is released
// when the handler returns an error.
func TestExecuteJob_ReleaseResource_OnHandlerError(t *testing.T) {
	mockStore := &mockJobStore{}
	handlers := HandlerMap{
		"error_task": func(ctx context.Context, job storage.Job) error {
			return errors.New("handler error")
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:error"
	job := storage.Job{
		ID:          500,
		Queue:       "default",
		TaskType:    "error_task",
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should acquire resource")
	// Verify resource was released after error
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should release resource after handler error")
	// Verify job was failed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.failCalls), "job should fail after handler error")
}

// TestExecuteJob_NoResourceKey verifies that jobs without a resource_key execute
// normally without attempting to acquire/release resources.
func TestExecuteJob_NoResourceKey(t *testing.T) {
	mockStore := &mockJobStore{}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	job := storage.Job{
		ID:          600,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: nil, // No resource key
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was NOT called
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should not acquire resource without key")
	// Verify handler was called
	assert.True(t, handlerCalled, "handler should be called for jobs without resource_key")
	// Verify resource was NOT released
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should not release resource without key")
	// Verify job was completed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.completeCalls), "job should be completed")
}

// TestExecuteJob_EmptyResourceKey verifies that jobs with an empty resource_key
// execute normally without attempting to acquire/release resources.
func TestExecuteJob_EmptyResourceKey(t *testing.T) {
	mockStore := &mockJobStore{}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	emptyKey := ""
	job := storage.Job{
		ID:          700,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: &emptyKey, // Empty string resource key
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was NOT called (empty key is skipped)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should not acquire resource with empty key")
	// Verify handler was called
	assert.True(t, handlerCalled, "handler should be called for jobs with empty resource_key")
	// Verify resource was NOT released
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should not release resource with empty key")
	// Verify job was completed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.completeCalls), "job should be completed")
}

// TestExecuteJob_ReleaseResource_ErrorLogged verifies that release errors are logged
// but don't prevent job completion.
func TestExecuteJob_ReleaseResource_ErrorLogged(t *testing.T) {
	mockStore := &mockJobStore{
		releaseResourceErr: errors.New("release failed"),
	}
	var handlerCalled bool
	handlers := HandlerMap{
		"test": func(ctx context.Context, job storage.Job) error {
			handlerCalled = true
			return nil
		},
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:release-error"
	job := storage.Job{
		ID:          800,
		Queue:       "default",
		TaskType:    "test",
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should acquire resource")
	// Verify handler was called
	assert.True(t, handlerCalled, "handler should be called after successful acquire")
	// Verify resource release was attempted
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should attempt to release resource")
	// Verify job was still completed (release error doesn't prevent completion)
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.completeCalls), "job should be completed despite release error")
}

// TestExecuteJob_MissingHandler_WithResourceKey verifies that when a handler is missing,
// the job fails and the resource is released if it was acquired.
func TestExecuteJob_MissingHandler_WithResourceKey(t *testing.T) {
	mockStore := &mockJobStore{}
	handlers := HandlerMap{
		"different_task": func(ctx context.Context, job storage.Job) error { return nil },
	}
	runner := setupTestRunner(t, mockStore, handlers)

	resourceKey := "order:missing-handler"
	job := storage.Job{
		ID:          900,
		Queue:       "default",
		TaskType:    "test", // This handler doesn't exist
		ResourceKey: &resourceKey,
	}

	ctx := context.Background()
	executeJobSync(t, runner, ctx, job)

	// Verify acquire was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.acquireResourceCalls), "should acquire resource")
	// Verify resource was released (even though handler was missing)
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.releaseResourceCalls), "should release resource when handler is missing")
	// Verify job was failed
	assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.failCalls), "job should fail when handler is missing")
}
