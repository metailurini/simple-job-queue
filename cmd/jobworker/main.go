package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/metailurini/simple-job-queue/diag"
	"github.com/metailurini/simple-job-queue/janitor"
	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
	"github.com/metailurini/simple-job-queue/worker"
)

func main() {
	var (
		dsn          = flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string")
		queue        = flag.String("queue", "default", "Queue to process")
		batchSize    = flag.Int("batch", 10, "Maximum jobs to claim per poll")
		lease        = flag.Duration("lease", 30*time.Second, "Lease duration for claimed jobs")
		workerID     = flag.String("worker-id", "", "Worker identifier (default hostname:pid)")
		pollInterval = flag.Duration("poll", 750*time.Millisecond, "Base poll interval when idle")
		idleJitter   = flag.Duration("jitter", 500*time.Millisecond, "Random jitter added to idle sleeps")
		heartbeat    = flag.Duration("heartbeat", 0, "Heartbeat interval (defaults to lease/2)")
		concurrency  = flag.Int("concurrency", 8, "Maximum concurrent job handlers")
		stealExpired = flag.Bool("steal-expired", true, "Claim expired leases in addition to queued jobs")
		listenNotify = flag.Bool("listen-notify", false, "Use LISTEN/NOTIFY to wake idle workers")
		// Janitor-specific flags
		janitorGracePeriod = flag.Duration("janitor-grace-period", 30*time.Second, "Janitor shutdown grace period")
		janitorMaxAge      = flag.Duration("janitor-max-age", 0, "Max age for resource locks (defaults to 2× lease if 0)")
	)
	flag.Parse()

	logger := slog.Default()
	if *dsn == "" {
		logger.Error("dsn is required (flag --dsn or env DATABASE_URL)")
		os.Exit(2)
	}
	if *batchSize <= 0 {
		logger.Error("batch must be > 0")
		os.Exit(2)
	}
	if *lease <= 0 {
		logger.Error("lease must be > 0")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		logger.Error("failed to init db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	provider := timeprovider.RealProvider{}
	store, err := storage.NewStoreWithProvider(db, provider)
	if err != nil {
		logger.Error("failed to build store", "err", err)
		os.Exit(1)
	}

	queueCfg := worker.QueueConfig{
		Name:          *queue,
		BatchSize:     *batchSize,
		LeaseDuration: *lease,
		IncludeLeased: *stealExpired,
	}
	workerCfg := worker.Config{
		WorkerID:            *workerID,
		Queues:              []worker.QueueConfig{queueCfg},
		PollInterval:        *pollInterval,
		IdleJitter:          *idleJitter,
		MaxInFlight:         *concurrency,
		Logger:              logger,
		EnableNotifications: *listenNotify,
		TimeProvider:        provider,
	}

	if *heartbeat > 0 {
		workerCfg.HeartbeatInterval = *heartbeat
	}

	handlers := worker.HandlerMap{
		"noop": func(ctx context.Context, job storage.Job) error {
			logger.Info("executing job", "id", job.ID, "task", job.TaskType, "queue", job.Queue)
			time.Sleep(30 * time.Second)
			return nil
		},
	}

	runner, err := worker.NewRunner(store, db, handlers, workerCfg)
	if err != nil {
		logger.Error("failed to build worker", "err", err)
		os.Exit(1)
	}

	// record and log DB/app clock drift at startup
	_, _ = diag.RecordClockDrift(ctx, db, provider, logger)

	// Start janitor in separate goroutine
	// If janitorMaxAge was provided use it; otherwise default to 2× lease.
	maxAge := 2 * (*lease)
	if *janitorMaxAge > 0 {
		maxAge = *janitorMaxAge
	}
	janitorCfg := janitor.Config{
		Interval:     1 * time.Minute,
		MaxAge:       maxAge,
		GracePeriod:  *janitorGracePeriod,
		Logger:       logger,
		TimeProvider: provider,
	}
	j, err := janitor.NewRunner(db, janitorCfg)
	if err != nil {
		logger.Error("failed to build janitor", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := j.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("janitor stopped", "err", err)
		}
	}()

	logger.Info("job worker starting", "queue", *queue, "worker_id", workerCfg.WorkerID)
	err = runner.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker exited", "err", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}
