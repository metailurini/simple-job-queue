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

	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver for database/sql

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

	// Open database connection using pgx driver for database/sql
	sqlDB, err := sql.Open("pgx", *dsn)
	if err != nil {
		logger.Error("failed to open database", "err", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	// Configure connection pool settings
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)
	sqlDB.SetConnMaxIdleTime(1 * time.Minute)

	// Verify database connection
	if err := sqlDB.PingContext(ctx); err != nil {
		logger.Error("failed to ping database", "err", err)
		os.Exit(1)
	}

	provider := timeprovider.RealProvider{}
	db := storage.NewDB(sqlDB)
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

	// TODO: Migrate diag to use *sql.DB
	// For now, keep using pgxpool for diag package during the transition
	pool, err := storage.NewPgxPoolFromDB(ctx, sqlDB, *dsn)
	if err != nil {
		logger.Error("failed to create pgxpool adapter", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// record and log DB/app clock drift at startup
	_, _ = diag.RecordClockDrift(ctx, pool, provider, logger)

	logger.Info("job scheduler starting", "interval", interval.String(), "catch_up", *catchUp)
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("scheduler exited", "err", err)
		os.Exit(1)
	}
	logger.Info("scheduler stopped")
}
