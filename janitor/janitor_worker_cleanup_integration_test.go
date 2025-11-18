package janitor

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type workerTimeProvider struct {
	t time.Time
}

func (m *workerTimeProvider) Now() time.Time {
	return m.t
}

func (m *workerTimeProvider) Advance(d time.Duration) {
	m.t = m.t.Add(d)
}

func resetWorkerTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "TRUNCATE queue_workers"); err != nil {
		t.Fatalf("truncate queue_workers: %v", err)
	}
}

// TestJanitor_CleanupDeadWorkers verifies that workers with old last_seen are removed
func TestJanitor_CleanupDeadWorkers(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetWorkerTable(t, db)

	ctx := context.Background()
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := &workerTimeProvider{t: now}

	// Insert workers with different last_seen timestamps
	// Active worker (last_seen = now)
	_, err := db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`, "active-worker", now)
	require.NoError(t, err)

	// Recently active worker (last_seen = 4 minutes ago)
	_, err = db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`, "recent-worker", now.Add(-4*time.Minute))
	require.NoError(t, err)

	// Dead worker (last_seen = 6 minutes ago)
	_, err = db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`, "dead-worker", now.Add(-6*time.Minute))
	require.NoError(t, err)

	// Create janitor with 5 minute max age
	cfg := Config{
		Interval:     1 * time.Minute,
		MaxAge:       5 * time.Minute,
		Logger:       slog.New(slog.NewTextHandler(os.Stdout, nil)),
		TimeProvider: provider,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup once
	success := runner.cleanup(ctx)
	assert.True(t, success)

	// Verify active and recent workers still exist
	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_workers WHERE worker_id IN ($1, $2);`, "active-worker", "recent-worker").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "active and recent workers should remain")

	// Verify dead worker was removed
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_workers WHERE worker_id = $1;`, "dead-worker").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "dead worker should be removed")
}

// TestJanitor_WorkerCleanupBoundary verifies boundary conditions for worker cleanup
func TestJanitor_WorkerCleanupBoundary(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetWorkerTable(t, db)

	ctx := context.Background()
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := &workerTimeProvider{t: now}

	// Insert worker exactly at the 5-minute boundary
	exactlyFiveMinutesAgo := now.Add(-5 * time.Minute)
	_, err := db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`, "boundary-worker", exactlyFiveMinutesAgo)
	require.NoError(t, err)

	// Create janitor with 5 minute max age
	cfg := Config{
		Interval:     1 * time.Minute,
		MaxAge:       5 * time.Minute,
		Logger:       slog.New(slog.NewTextHandler(os.Stdout, nil)),
		TimeProvider: provider,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	success := runner.cleanup(ctx)
	assert.True(t, success)

	// Verify worker at exactly 5 minutes ago is kept (cutoff is < not <=)
	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_workers WHERE worker_id = $1;`, "boundary-worker").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "worker at exactly 5 minutes should be kept")
}

// TestJanitor_CleanupMultipleDeadWorkers verifies batch cleanup
func TestJanitor_CleanupMultipleDeadWorkers(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetWorkerTable(t, db)

	ctx := context.Background()
	now := time.Date(2025, 11, 16, 12, 0, 0, 0, time.UTC)
	provider := &workerTimeProvider{t: now}

	// Insert multiple dead workers
	for i := 1; i <= 10; i++ {
		workerID := time.Duration(i)*time.Minute + 5*time.Minute
		lastSeen := now.Add(-workerID)
		_, err := db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`,
			time.Duration(i).String(), lastSeen)
		require.NoError(t, err)
	}

	// Insert one active worker
	_, err := db.ExecContext(ctx, `INSERT INTO queue_workers (worker_id, last_seen) VALUES ($1, $2);`, "active", now)
	require.NoError(t, err)

	// Create janitor
	cfg := Config{
		Interval:     1 * time.Minute,
		MaxAge:       5 * time.Minute,
		Logger:       slog.New(slog.NewTextHandler(os.Stdout, nil)),
		TimeProvider: provider,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	success := runner.cleanup(ctx)
	assert.True(t, success)

	// Verify all dead workers removed, active worker remains
	var count int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_workers;`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "only active worker should remain")

	var activeWorkerID string
	err = db.QueryRowContext(ctx, `SELECT worker_id FROM queue_workers;`).Scan(&activeWorkerID)
	require.NoError(t, err)
	assert.Equal(t, "active", activeWorkerID)
}
