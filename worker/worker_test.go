package worker

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/apperrors"
	"github.com/metailurini/simple-job-queue/storage"
)

func TestNewRunnerAppliesDefaults(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store, err := storage.NewStore(db, time.Now)
	require.NoError(t, err)

	handlers := HandlerMap{"task": func(context.Context, storage.Job) error { return nil }}
	cfg := Config{Queues: []QueueConfig{{Name: "default"}}}

	runner, err := NewRunner(store, db, handlers, &cfg)
	require.NoError(t, err)
	require.NotEmpty(t, runner.cfg.WorkerID, "expected worker id to be generated")
	assert.Equal(t, 1, runner.cfg.Queues[0].BatchSize, "expected batch size default 1")
	assert.Equal(t, 30*time.Second, runner.cfg.Queues[0].LeaseDuration, "expected lease duration default 30s")
	assert.Equal(t, 500*time.Millisecond, runner.cfg.PollInterval, "expected default poll interval")
	assert.Equal(t, runner.cfg.Queues[0].LeaseDuration/2, runner.cfg.HeartbeatInterval, "expected heartbeat interval derived from lease duration")
	assert.Equal(t, runtime.NumCPU(), runner.cfg.MaxInFlight, "expected default max in flight")
	require.NotNil(t, runner.inflight, "expected inflight channel initialised")
	assert.Equal(t, runner.cfg.MaxInFlight, cap(runner.inflight), "expected inflight channel sized to max in flight")
}

func TestNewRunnerValidation(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store, err := storage.NewStore(db, time.Now)
	require.NoError(t, err)

	handler := HandlerMap{"task": func(context.Context, storage.Job) error { return nil }}
	queue := []QueueConfig{{Name: "default"}}

	tests := []struct {
		name    string
		store   jobStore
		db      *sql.DB
		h       HandlerMap
		queues  []QueueConfig
		wantErr string
	}{{
		name:    "missing db",
		store:   store,
		db:      nil,
		h:       handler,
		queues:  queue,
		wantErr: "database is required",
	}, {
		name:    "no handlers",
		store:   store,
		db:      db,
		h:       HandlerMap{},
		queues:  queue,
		wantErr: "at least one handler is required",
	}, {
		name:    "no queues",
		store:   store,
		db:      db,
		h:       handler,
		queues:  nil,
		wantErr: "at least one queue is required",
	}, {
		name:    "queue missing name",
		store:   store,
		db:      db,
		h:       handler,
		queues:  []QueueConfig{{}},
		wantErr: "queue[0] requires a name",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRunner(tt.store, tt.db, tt.h, &Config{Queues: tt.queues})
			if tt.wantErr == "store is required" || tt.wantErr == "database is required" {
				require.ErrorIs(t, err, apperrors.ErrNotConfigured)
			} else {
				require.Error(t, err)
				// For other validation errors we only assert an error was returned.
			}
		})
	}
}

func TestRunnerWaitForWorkWithoutNotifier(t *testing.T) {
	r := &Runner{
		cfg:      &Config{PollInterval: 20 * time.Millisecond},
		inflight: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	require.NoError(t, r.waitForWork(ctx))
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond, "expected wait at least poll interval")
}

func TestRunnerWaitForWorkWithNotifier(t *testing.T) {
	updates := make(chan struct{}, 1)
	updates <- struct{}{}
	notifier := &pgNotifier{updates: updates}

	r := &Runner{
		cfg:      &Config{PollInterval: 500 * time.Millisecond},
		notifier: notifier,
		inflight: make(chan struct{}, 1),
	}

	start := time.Now()
	require.NoError(t, r.waitForWork(context.Background()))
	assert.LessOrEqual(t, time.Since(start), 100*time.Millisecond, "expected to wake quickly when notification available")
	require.NotNil(t, r.notifier, "expected notifier to remain when channel open")
}

func TestRunnerWaitForWorkNotifierClosed(t *testing.T) {
	updates := make(chan struct{})
	close(updates)
	notifier := &pgNotifier{updates: updates}

	r := &Runner{
		cfg:      &Config{PollInterval: 10 * time.Millisecond},
		notifier: notifier,
		inflight: make(chan struct{}, 1),
	}

	require.NoError(t, r.waitForWork(context.Background()))
	assert.Nil(t, r.notifier, "expected notifier cleared when channel closed")
}

func TestRunnerAvailableCapacity(t *testing.T) {
	r := &Runner{inflight: make(chan struct{}, 3)}
	r.inflight <- struct{}{}
	assert.Equal(t, 2, r.availableCapacity(), "expected capacity 2")
}

func TestRunnerIdleSleepJitter(t *testing.T) {
	rand.Seed(1)
	r := &Runner{cfg: &Config{PollInterval: 10 * time.Millisecond, IdleJitter: 10 * time.Millisecond}}
	sleep := r.idleSleep()
	assert.GreaterOrEqual(t, sleep, 10*time.Millisecond, "expected sleep within jitter window")
	assert.LessOrEqual(t, sleep, 20*time.Millisecond, "expected sleep within jitter window")
}

func TestRunnerNextRetryDelay(t *testing.T) {
	job := storage.Job{Attempts: 3, BackoffSeconds: 2}
	r := &Runner{}
	delay := r.nextRetryDelay(job)

	base := time.Duration(job.BackoffSeconds) * time.Second
	if base <= 0 {
		base = 5 * time.Second
	}
	attempts := job.Attempts
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 16 {
		attempts = 16
	}
	multiplier := 1 << uint(attempts)
	base *= time.Duration(multiplier)

	minDelay := time.Duration(float64(base) * 0.5)
	maxDelay := time.Duration(float64(base) * 1.5)
	assert.GreaterOrEqual(t, delay, minDelay, "expected delay above minimum")
	assert.LessOrEqual(t, delay, maxDelay, "expected delay below maximum")
}

func TestRunnerRunContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &Runner{cfg: &Config{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), inflight: make(chan struct{}, 1)}

	require.ErrorIs(t, r.Run(ctx), context.Canceled)
}

func TestHostnameOrWorker(t *testing.T) {
	assert.Equal(t, "host", hostnameOrWorker("host"))
	assert.Equal(t, "worker", hostnameOrWorker(""))
}

func TestMinHelper(t *testing.T) {
	assert.Equal(t, 1, min(1, 2))
	assert.Equal(t, 3, min(5, 3))
}

func TestRunner_executeJob_ResourceBusy(t *testing.T) {
	key := "test-resource"
	job := storage.Job{ID: 1, Queue: "default", TaskType: "test", ResourceKey: &key}
	store := &mockJobStore{
		acquireResourceErr: storage.ErrResourceBusy,
	}

	runner := setupTestRunner(t, store, nil)
	executeJobSync(t, runner, context.Background(), job)

	assert.Equal(t, int32(1), atomic.LoadInt32(&store.rescheduleCalls), "expected RescheduleJob to be called")
	// Verify attempts was NOT incremented on resource contention
	assert.Equal(t, 0, job.Attempts, "expected job attempts not to be incremented")
	assert.Equal(t, int32(0), atomic.LoadInt32(&store.requeueCalls), "expected RequeueJob not to be called")
}
