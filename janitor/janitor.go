package janitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/metailurini/simple-job-queue/apperrors"
	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

// Config controls janitor behavior.
type Config struct {
	Interval     time.Duration // how often to run cleanup
	MaxAge       time.Duration // delete locks older than this
	WorkerMaxAge time.Duration // delete workers older than this
	GracePeriod  time.Duration // how long to wait for cleanup on shutdown
	Logger       *slog.Logger
	TimeProvider timeprovider.Provider
}

// Runner periodically deletes stale resource locks.
type Runner struct {
	db     storage.DB
	cfg    Config
	logger *slog.Logger
	now    func() time.Time
}

// NewRunner constructs a janitor runner.
func NewRunner(db storage.DB, cfg Config) (*Runner, error) {
	if db == nil {
		return nil, apperrors.ErrNotConfigured
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 1 * time.Minute
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 5 * time.Minute
	}
	if cfg.WorkerMaxAge <= 0 {
		cfg.WorkerMaxAge = 5 * time.Minute
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.TimeProvider == nil {
		cfg.TimeProvider = timeprovider.RealProvider{}
	}
	r := &Runner{
		db:     db,
		cfg:    cfg,
		logger: logger,
		now:    cfg.TimeProvider.Now,
	}
	return r, nil
}

// Run executes cleanup in a loop until the context is canceled.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info(
				"janitor shutdown started",
				"event", "janitor_shutdown",
				"status", "started",
				"grace_period", r.cfg.GracePeriod.String(),
			)

			shutdownCtx, cancel := context.WithTimeout(context.Background(), r.cfg.GracePeriod)
			defer cancel()

			// We call cleanup directly here to attempt one final cleanup before shutting down.
			cleanupSuccess := r.cleanup(shutdownCtx)

			if shutdownCtx.Err() == context.DeadlineExceeded {
				r.logger.Warn(
					"janitor shutdown aborted: grace period exceeded",
					"event", "janitor_shutdown",
					"status", "aborted",
				)
			} else if !cleanupSuccess {
				r.logger.Error(
					"janitor shutdown completed but final cleanup failed",
					"event", "janitor_shutdown",
					"status", "completed_with_error",
				)
			} else {
				r.logger.Info(
					"janitor shutdown completed",
					"event", "janitor_shutdown",
					"status", "completed",
				)
			}

			return ctx.Err()
		case <-ticker.C:
			r.cleanup(ctx)
		}
	}
}

func (r *Runner) executeCleanupQuery(ctx context.Context, description, query string, args ...any) (bool, int64) {
	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		r.logger.Error("janitor cleanup failed", "description", description, "err", err)
		return false, 0
	}

	affected, err := res.RowsAffected()
	if err != nil {
		r.logger.Error("janitor cleanup failed to get rows affected", "description", description, "err", err)
		return false, 0
	}

	return true, affected
}

func (r *Runner) cleanup(ctx context.Context) bool {
	cutoff := r.now().UTC().Add(-r.cfg.MaxAge)
	success := true

	// Clean up stale resource locks
	// Use <= to include locks created exactly at the cutoff boundary.
	ok, affected := r.executeCleanupQuery(ctx, "stale resource locks", `
DELETE FROM queue_resource_locks
WHERE created_at <= $1;`, cutoff)
	if !ok {
		success = false
	} else if affected > 0 {
		r.logger.Info("janitor cleaned stale locks", "count", affected)
	}

	// Clean up dead workers
	workerCutoff := r.now().UTC().Add(-r.cfg.WorkerMaxAge)
	ok, affected = r.executeCleanupQuery(ctx, "dead workers", `
DELETE FROM queue_workers
WHERE last_seen < $1;`, workerCutoff)
	if !ok {
		success = false
	} else if affected > 0 {
		r.logger.Info("janitor cleaned dead workers", "count", affected)
	}

	return success
}
