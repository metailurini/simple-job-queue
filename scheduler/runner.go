package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/metailurini/simple-job-queue/apperrors"
	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

// scheduleStore defines the minimal storage surface needed by the scheduler.
//
// The scheduler requires transactional access to the schedules table and a
// way to enqueue jobs. Accepting this slim interface makes testing easier and
// avoids coupling callers to the full concrete Store type.
// Implementations should ensure FetchSchedulesTx returns rows scanned into the
// storage.ScheduleRow projection (see storage.ScanSchedule).
type scheduleStore interface {
	Begin(ctx context.Context) (storage.Tx, error)
	FetchSchedulesTx(ctx context.Context, tx storage.Tx) ([]storage.ScheduleRow, error)
	EnqueueJobs(ctx context.Context, params []storage.EnqueueParams) ([]int64, error)
}

// Runner periodically inspects the schedules table and enqueues due jobs.
type Runner struct {
	store      scheduleStore
	logger     *slog.Logger
	interval   time.Duration
	catchUpMax int
	now        func() time.Time
}

// Config provides runtime options for the scheduler.
type Config struct {
	Interval     time.Duration
	CatchUpMax   int
	Logger       *slog.Logger
	TimeProvider timeprovider.Provider
}

// NewRunner constructs a scheduler runner.
func NewRunner(store scheduleStore, cfg Config) (*Runner, error) {
	if store == nil {
		return nil, apperrors.ErrNotConfigured
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.CatchUpMax <= 0 {
		cfg.CatchUpMax = 5
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.TimeProvider == nil {
		cfg.TimeProvider = timeprovider.RealProvider{}
	}

	return &Runner{
		store:      store,
		logger:     logger,
		interval:   cfg.Interval,
		catchUpMax: cfg.CatchUpMax,
		now:        cfg.TimeProvider.Now,
	}, nil
}

// Run starts the scheduling loop until the context is canceled.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		if err := r.tick(ctx); err != nil {
			r.logger.Error("schedule tick failed", "err", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runner) tick(ctx context.Context) error {
	tx, err := r.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	schedules, err := r.store.FetchSchedulesTx(ctx, tx)
	if err != nil {
		return err
	}

	for _, schedule := range schedules {
		current := r.now()
		due := r.computeDue(schedule, current)
		if len(due) == 0 {
			continue
		}

		last := due[len(due)-1]

		var previous any
		if schedule.LastEnqueuedAt != nil {
			previous = *schedule.LastEnqueuedAt
		}

		result, err := tx.ExecContext(ctx, `UPDATE queue_schedules SET last_enqueued_at=$2 WHERE id=$1 AND (last_enqueued_at IS NOT DISTINCT FROM $3)`, schedule.ID, last, previous)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			// Another runner updated this schedule first; skip to the next.
			continue
		}

		if err := r.enqueueRuns(ctx, schedule, due); err != nil {
			r.logger.Error("enqueue schedule run failed", "schedule_id", schedule.ID, "err", err)

			if _, revertErr := tx.ExecContext(ctx, `UPDATE queue_schedules SET last_enqueued_at=$2 WHERE id=$1 AND (last_enqueued_at IS NOT DISTINCT FROM $3)`, schedule.ID, previous, last); revertErr != nil {
				return fmt.Errorf("revert schedule %d failed: %w, original enqueue error: %v", schedule.ID, revertErr, err)
			}
			continue
		}
	}

	return tx.Commit()
}

func (r *Runner) computeDue(schedule storage.ScheduleRow, now time.Time) []time.Time {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	spec, err := parser.Parse(schedule.Cron)
	if err != nil {
		r.logger.Error("invalid cron expression", "schedule_id", schedule.ID, "cron", schedule.Cron, "err", err)
		return nil
	}

	loc := now.Location()
	if schedule.LastEnqueuedAt != nil {
		loc = schedule.LastEnqueuedAt.Location()
	}

	nowInLoc := now.In(loc)

	var start time.Time
	if schedule.LastEnqueuedAt != nil {
		start = schedule.LastEnqueuedAt.In(loc)
	} else {
		start = nowInLoc.Add(-24 * time.Hour)
	}

	var runs []time.Time
	next := spec.Next(start)
	for len(runs) < r.catchUpMax && !next.After(nowInLoc) {
		runs = append(runs, next.UTC())
		next = spec.Next(next)
	}
	return runs
}

func (r *Runner) enqueueRuns(ctx context.Context, schedule storage.ScheduleRow, runTimes []time.Time) error {
	params := make([]storage.EnqueueParams, 0, len(runTimes))
	for _, runAt := range runTimes {
		runAtCopy := runAt
		p := storage.EnqueueParams{
			Queue:       schedule.Queue,
			TaskType:    schedule.TaskType,
			Payload:     schedule.Payload,
			Priority:    0,
			RunAt:       &runAtCopy,
			MaxAttempts: 0,
		}
		if schedule.DedupeKey != nil {
			dedupe := fmt.Sprintf("%s:%d", *schedule.DedupeKey, runAt.Unix())
			p.DedupeKey = &dedupe
		}
		params = append(params, p)
	}

	ids, err := r.store.EnqueueJobs(ctx, params)
	if err != nil {
		return err
	}

	// Log deduped runTimes that did not return an id
	if len(ids) != len(params) {
		// Build a set of returned ids count; we cannot map ids to params robustly
		// because RETURNING omits rows that conflicted. Just log how many were
		// deduped for observability.
		r.logger.Debug("some enqueues skipped due to dedupe", "schedule_id", schedule.ID, "requested", len(params), "inserted", len(ids))
	}
	return nil
}
