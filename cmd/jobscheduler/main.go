package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metailurini/simple-job-queue/diag"
	"github.com/metailurini/simple-job-queue/scheduler"
	"github.com/metailurini/simple-job-queue/storage"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

func main() {
	var (
		dsn      = flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string")
		interval = flag.Duration("interval", time.Minute, "Frequency to scan schedules for due jobs")
		catchUp  = flag.Int("catch-up", 5, "Maximum cron intervals to enqueue per tick")
	)
	flag.Parse()

	logger := slog.Default()
	if *dsn == "" {
		logger.Error("dsn is required (flag --dsn or env DATABASE_URL)")
		os.Exit(2)
	}
	if *interval <= 0 {
		logger.Error("interval must be positive")
		os.Exit(2)
	}
	if *catchUp <= 0 {
		logger.Error("catch-up must be positive")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		logger.Error("failed to init pgx pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	provider := timeprovider.RealProvider{}
	store, err := storage.NewStoreWithProvider(pool, provider)
	if err != nil {
		logger.Error("failed to build store", "err", err)
		os.Exit(1)
	}
	runner, err := scheduler.NewRunner(store, scheduler.Config{
		Interval:   *interval,
		CatchUpMax: *catchUp,
		Logger:     logger,
	})
	if err != nil {
		logger.Error("failed to build scheduler", "err", err)
		os.Exit(1)
	}

	// record and log DB/app clock drift at startup
	_, _ = diag.RecordClockDrift(ctx, pool, provider, logger)

	logger.Info("job scheduler starting", "interval", interval.String(), "catch_up", *catchUp)
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("scheduler exited", "err", err)
		os.Exit(1)
	}
	logger.Info("scheduler stopped")
}
