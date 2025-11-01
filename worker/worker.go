package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metailurini/simple-job-queue/apperrors"
	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

// jobStore defines the minimal storage surface the worker requires.
//
// This narrow interface intentionally accepts only the methods used by the
// worker runner so callers can provide either the concrete *storage.Store or
// a lightweight test double. Implementations must behave like the Store
// helpers: timestamps are UTC-normalized and errors follow the exported
// sentinel values on the storage package (e.g. storage.ErrResourceBusy).
type jobStore interface {
	ClaimJobs(ctx context.Context, opts storage.ClaimOptions) (storage.ClaimResult, error)
	HeartbeatJob(ctx context.Context, id int64, workerID string, extend time.Duration) error
	RequeueJob(ctx context.Context, id int64, workerID string, runAt time.Time) error
	FailJob(ctx context.Context, id int64, workerID string, nextRun time.Time, errText string) (bool, error)
	CompleteJob(ctx context.Context, id int64, workerID string) error
	AcquireResource(ctx context.Context, resourceKey string, jobID int64, workerID string) error
	ReleaseResource(ctx context.Context, resourceKey string, jobID int64) error
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// HandlerFunc is invoked for each claimed job. When job.ResourceKey is set,
// handlers are responsible for coordinating exclusivity using application-level
// mechanisms (in-process mutexes, domain row locks, external fencing, etc.).
type HandlerFunc func(ctx context.Context, job storage.Job) error

// HandlerMap maps task types to handler functions.
type HandlerMap map[string]HandlerFunc

// QueueConfig defines polling/lease settings for a queue.
type QueueConfig struct {
	Name          string
	BatchSize     int
	LeaseDuration time.Duration
	IncludeLeased bool
}

// Config controls a worker runner.
type Config struct {
	WorkerID                 string
	Queues                   []QueueConfig
	PollInterval             time.Duration
	IdleJitter               time.Duration
	HeartbeatInterval        time.Duration
	MaxInFlight              int
	StoreOperationTimeout    time.Duration
	ResourceBusyRequeueDelay time.Duration
	Logger                   *slog.Logger
	EnableNotifications      bool
	TimeProvider             timeprovider.Provider
}

// Runner orchestrates claiming, executing, heartbeat, and completion of jobs.
type Runner struct {
	store      jobStore
	pool       *pgxpool.Pool
	handlers   HandlerMap
	cfg        Config
	logger     *slog.Logger
	inflight   chan struct{}
	queueIndex map[string]QueueConfig
	notifier   *pgNotifier
	now        func() time.Time
}

// NewRunner validates configuration and returns a worker runner.
func NewRunner(store jobStore, pool *pgxpool.Pool, handlers HandlerMap, cfg Config) (*Runner, error) {
	if store == nil {
		return nil, apperrors.ErrNotConfigured
	}
	if pool == nil {
		return nil, apperrors.ErrNotConfigured
	}
	if len(handlers) == 0 {
		return nil, fmt.Errorf("at least one handler is required: %w", apperrors.ErrInvalidArgument)
	}
	if len(cfg.Queues) == 0 {
		return nil, fmt.Errorf("at least one queue is required: %w", apperrors.ErrInvalidArgument)
	}
	for i := range cfg.Queues {
		if cfg.Queues[i].Name == "" {
			return nil, fmt.Errorf("queue[%d] requires a name: %w", i, apperrors.ErrInvalidArgument)
		}
		if cfg.Queues[i].BatchSize <= 0 {
			cfg.Queues[i].BatchSize = 1
		}
		if cfg.Queues[i].LeaseDuration <= 0 {
			cfg.Queues[i].LeaseDuration = 30 * time.Second
		}
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d", hostnameOrWorker(host), os.Getpid())
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.IdleJitter < 0 {
		cfg.IdleJitter = 0
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.Queues[0].LeaseDuration / 2
		if cfg.HeartbeatInterval <= 0 {
			cfg.HeartbeatInterval = 5 * time.Second
		}
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = runtime.NumCPU()
	}
	if cfg.StoreOperationTimeout <= 0 {
		cfg.StoreOperationTimeout = 5 * time.Second
	}
	// Default delay for requeueing when a resource is busy.
	if cfg.ResourceBusyRequeueDelay <= 0 {
		cfg.ResourceBusyRequeueDelay = 2 * time.Second
	}
	if cfg.TimeProvider == nil {
		cfg.TimeProvider = timeprovider.RealProvider{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	queueIndex := make(map[string]QueueConfig, len(cfg.Queues))
	for _, q := range cfg.Queues {
		queueIndex[q.Name] = q
	}

	return &Runner{
		store:      store,
		pool:       pool,
		handlers:   handlers,
		cfg:        cfg,
		logger:     logger,
		inflight:   make(chan struct{}, cfg.MaxInFlight),
		queueIndex: queueIndex,
		now:        cfg.TimeProvider.Now,
	}, nil
}

// Run starts the worker loop until the context is canceled.
func (r *Runner) Run(ctx context.Context) error {
	if r.cfg.EnableNotifications && r.notifier == nil {
		n, err := newPGNotifier(ctx, r.pool, r.cfg.Queues, r.logger)
		if err != nil {
			r.logger.Warn("listen/notify disabled", "err", err)
		} else {
			r.notifier = n
			defer r.notifier.Close()
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		worked := r.pollOnce(ctx)
		if !worked {
			if err := r.waitForWork(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				r.logger.Warn("idle wait failed", "err", err)
			}
		}
	}
}

func (r *Runner) pollOnce(ctx context.Context) bool {
	var madeProgress bool
	for _, queue := range r.cfg.Queues {
		if r.availableCapacity() == 0 {
			return madeProgress
		}
		limit := min(queue.BatchSize, r.availableCapacity())
		if limit == 0 {
			continue
		}
		claimAt := r.now()
		claim, err := r.store.ClaimJobs(ctx, storage.ClaimOptions{
			Queue:         queue.Name,
			WorkerID:      r.cfg.WorkerID,
			Limit:         limit,
			LeaseDuration: queue.LeaseDuration,
			IncludeLeased: queue.IncludeLeased,
			Now:           claimAt,
		})
		if err != nil {
			r.logger.Error("claim failed", "queue", queue.Name, "err", err)
			continue
		}
		if len(claim.Jobs) == 0 {
			continue
		}
		madeProgress = true
		for _, job := range claim.Jobs {
			r.startJob(ctx, job)
		}
	}
	return madeProgress
}

func (r *Runner) availableCapacity() int {
	return cap(r.inflight) - len(r.inflight)
}

func (r *Runner) idleSleep() time.Duration {
	if r.cfg.IdleJitter <= 0 {
		return r.cfg.PollInterval
	}
	delta := time.Duration(rand.Int63n(int64(r.cfg.IdleJitter)))
	return r.cfg.PollInterval + delta
}

func (r *Runner) startJob(ctx context.Context, job storage.Job) {
	r.inflight <- struct{}{}
	go r.executeJob(ctx, job)
}

func (r *Runner) executeJob(ctx context.Context, job storage.Job) {
	defer func() {
		<-r.inflight
	}()

	queueCfg := r.queueIndex[job.Queue]
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// --- NEW: Try to acquire resource ownership ---
	acquired := false
	if job.ResourceKey != nil && *job.ResourceKey != "" {
		storeCtx, acqCancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
		defer acqCancel()
		if err := r.store.AcquireResource(storeCtx, *job.ResourceKey, job.ID, r.cfg.WorkerID); err != nil {
			if errors.Is(err, storage.ErrResourceBusy) {
				r.logger.Info("resource busy, requeueing", "job_id", job.ID, "resource_key", *job.ResourceKey)
				// Short backoff before trying again
				r.requeue(ctx, job, r.cfg.ResourceBusyRequeueDelay)
				return
			}
			// Execution error (e.g., DB down); log and fail
			r.logger.Error("acquire resource failed", "job_id", job.ID, "err", err)
			r.fail(jobCtx, job, fmt.Errorf("acquire resource: %w", err))
			return
		}
		acquired = true
	}
	// Ensure release on all exit paths
	if acquired {
		defer func() {
			storeCtx, relCancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
			defer relCancel()
			if err := r.store.ReleaseResource(storeCtx, *job.ResourceKey, job.ID); err != nil {
				r.logger.Warn("failed to release resource lock", "job_id", job.ID, "resource_key", *job.ResourceKey, "err", err)
			}
		}()
	}
	// --- END NEW ---

	var wg sync.WaitGroup
	stopHeartbeat := make(chan struct{})
	if r.cfg.HeartbeatInterval > 0 {
		wg.Add(1)
		go r.heartbeat(jobCtx, &wg, stopHeartbeat, job.ID, queueCfg.LeaseDuration)
	}
	defer func() {
		close(stopHeartbeat)
		wg.Wait()
	}()

	handler, ok := r.handlers[job.TaskType]
	if !ok {
		r.logger.Error("missing handler", "task_type", job.TaskType, "job_id", job.ID)
		r.fail(jobCtx, job, fmt.Errorf("missing handler for task type %q", job.TaskType))
		return
	}

	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("job panic", "job_id", job.ID, "panic", rec)
			r.fail(jobCtx, job, fmt.Errorf("panic: %v", rec))
		}
	}()

	runErr := handler(jobCtx, job)
	if runErr != nil {
		r.logger.Warn("job handler failed", "job_id", job.ID, "task_type", job.TaskType, "err", runErr)
		r.fail(jobCtx, job, runErr)
		return
	}
	r.complete(jobCtx, job)
}

func (r *Runner) heartbeat(ctx context.Context, wg *sync.WaitGroup, stop <-chan struct{}, jobID int64, extend time.Duration) {
	defer wg.Done()
	if extend <= 0 {
		extend = 30 * time.Second
	}
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				storeCtx, cancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
				defer cancel()
				if err := r.store.HeartbeatJob(storeCtx, jobID, r.cfg.WorkerID, extend); err != nil {
					r.logger.Warn("heartbeat failed", "job_id", jobID, "err", err)
				}
			}()
		}
	}
}

func (r *Runner) requeue(ctx context.Context, job storage.Job, delay time.Duration) {
	storeCtx, cancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
	defer cancel()
	next := r.now().Add(delay)
	if err := r.store.RequeueJob(storeCtx, job.ID, r.cfg.WorkerID, next); err != nil {
		r.logger.Error("requeue failed", "job_id", job.ID, "err", err)
		return
	}
}

func (r *Runner) fail(ctx context.Context, job storage.Job, cause error) {
	storeCtx, cancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
	defer cancel()

	var delay time.Duration
	if job.MaxAttempts == 0 || job.Attempts < job.MaxAttempts {
		delay = r.nextRetryDelay(job)
	}
	nextRun := r.now().Add(delay)

	var message string
	if cause != nil {
		message = cause.Error()
	}

	dead, err := r.store.FailJob(storeCtx, job.ID, r.cfg.WorkerID, nextRun, message)
	if err != nil {
		r.logger.Error("fail update failed", "job_id", job.ID, "err", err)
		return
	}
	if dead {
		r.logger.Warn("job moved to dead letter queue", "job_id", job.ID, "task_type", job.TaskType)
	} else {
		r.logger.Info("job scheduled for retry", "job_id", job.ID, "task_type", job.TaskType, "run_at", nextRun)
	}
}

func (r *Runner) complete(ctx context.Context, job storage.Job) {
	storeCtx, cancel := context.WithTimeout(ctx, r.cfg.StoreOperationTimeout)
	defer cancel()
	if err := r.store.CompleteJob(storeCtx, job.ID, r.cfg.WorkerID); err != nil {
		r.logger.Error("complete failed", "job_id", job.ID, "err", err)
		return
	}
}

func (r *Runner) nextRetryDelay(job storage.Job) time.Duration {
	base := time.Duration(job.BackoffSeconds) * time.Second
	if base <= 0 {
		base = 5 * time.Second
	}

	attempts := max(job.Attempts, 0)
	if attempts > 16 {
		attempts = 16
	}

	multiplier := 1 << uint(attempts)
	delay := base * time.Duration(multiplier)
	jitter := 0.5 + rand.Float64()
	return time.Duration(float64(delay) * jitter)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func hostnameOrWorker(host string) string {
	if host == "" {
		return "worker"
	}
	return host
}

func (r *Runner) waitForWork(ctx context.Context) error {
	idle := r.idleSleep()
	if r.notifier == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(idle):
			return nil
		}
	}
	timer := time.NewTimer(idle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case _, ok := <-r.notifier.C():
		if !ok {
			r.notifier = nil
		}
		return nil
	}
}
