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

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		logger.Error("failed to init db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	// Verify that the DB is reachable and the DSN is valid. sql.Open does not
	// establish connections by itself, so we ping the database to fail fast on
	// misconfiguration.
	if err := db.PingContext(ctx); err != nil {
		logger.Error("failed to connect to db", "err", err)
		os.Exit(1)
	}

	// Configure connection pool limits to sensible defaults. Consider exposing
	// these via flags if callers need to tune them for different environments.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	provider := timeprovider.RealProvider{}
	store, err := storage.NewStoreWithProvider(db, provider)
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
	_, _ = diag.RecordClockDrift(ctx, db, provider, logger)

	logger.Info("job scheduler starting", "interval", interval.String(), "catch_up", *catchUp)
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("scheduler exited", "err", err)
		os.Exit(1)
	}
	logger.Info("scheduler stopped")
}
