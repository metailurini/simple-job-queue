package diag

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metailurini/simple-job-queue/timeprovider"
)

// RecordClockDrift queries the database for its current time and logs the drift
// between the DB clock and the provided time provider. It returns the measured
// drift and any error encountered while querying the database.
func RecordClockDrift(ctx context.Context, pool *pgxpool.Pool, provider timeprovider.Provider, logger *slog.Logger) (time.Duration, error) {
	driftCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var dbNow time.Time
	if err := pool.QueryRow(driftCtx, "SELECT now()").Scan(&dbNow); err != nil {
		logger.Warn("clock drift measurement failed", "err", err)
		return 0, err
	}

	appNow := provider.Now()
	drift := dbNow.Sub(appNow)
	logger.Info("clock drift measured", "db_now", dbNow, "app_now", appNow, "drift", drift)
	return drift, nil
}
