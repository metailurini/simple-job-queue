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

type migrateFactory func(ctx context.Context, db *sql.DB) (migrateRunner, error)

func defaultMigrator(ctx context.Context, db *sql.DB) (migrateRunner, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire db connection for migrations: %w", err)
	}

	driver, err := postgres.WithConnection(ctx, conn, &postgres.Config{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("init postgres migration driver: %w", err)
	}

	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		driver.Close()
		return nil, fmt.Errorf("init migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		driver.Close()
		return nil, fmt.Errorf("init migrator: %w", err)
	}

	return m, nil
}

// Runner executes database migrations using a provided migrator factory.
type Runner struct {
	factory migrateFactory
}

// NewRunner constructs a Runner. When factory is nil, the default migrator
// implementation backed by golang-migrate is used.
func NewRunner(factory migrateFactory) *Runner {
	if factory == nil {
		factory = defaultMigrator
	}

	return &Runner{factory: factory}
}

// Run applies any pending database migrations. It is safe to call multiple times.
func Run(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	return NewRunner(nil).Run(ctx, db, logger)
}

// Run applies any pending database migrations. It is safe to call multiple times.
func (r *Runner) Run(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	m, err := r.factory(ctx, db)
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
