package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/metailurini/simple-job-queue/db/migrations"
)

type migrateRunner interface {
	Up() error
	Close() (sourceErr, dbErr error)
}

type migrateFactory func(db *sql.DB) (migrateRunner, error)

var newMigrator migrateFactory = func(db *sql.DB) (migrateRunner, error) {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return nil, fmt.Errorf("init postgres migration driver: %w", err)
	}

	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("init migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return nil, fmt.Errorf("init migrator: %w", err)
	}

	return m, nil
}

// Run applies any pending database migrations. It is safe to call multiple times.
func Run(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	m, err := newMigrator(db)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, dbErr := m.Close()
		if sourceErr != nil {
			logger.Warn("failed to close migration source", "err", sourceErr)
		}
		if dbErr != nil {
			logger.Warn("failed to close migration db", "err", dbErr)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	logger.Info("applying migrations")
	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			logger.Info("no migrations to apply")
			return nil
		}
		return fmt.Errorf("apply migrations: %w", err)
	}
	logger.Info("migrations applied")
	return nil
}
