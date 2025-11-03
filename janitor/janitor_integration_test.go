package janitor

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDatabaseEnv = "TEST_DATABASE_URL"

func mustConnectTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv(testDatabaseEnv)
	if dsn == "" {
		t.Skipf("set %s to run janitor integration tests", testDatabaseEnv)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("connect test db: %v", err)
	}
	return db
}

func resetResourceLocks(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "TRUNCATE queue_resource_locks"); err != nil {
		t.Fatalf("truncate resource locks: %v", err)
	}
}

func insertResourceLock(t *testing.T, db *sql.DB, resourceKey string, jobID int64, workerID string, createdAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)`, resourceKey, jobID, workerID, createdAt)
	require.NoError(t, err)
}

func countResourceLocks(t *testing.T, db *sql.DB) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM queue_resource_locks").Scan(&count)
	require.NoError(t, err)
	return count
}

func getResourceKeys(t *testing.T, db *sql.DB) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT resource_key FROM queue_resource_locks ORDER BY resource_key")
	require.NoError(t, err)
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		err := rows.Scan(&key)
		require.NoError(t, err)
		keys = append(keys, key)
	}
	require.NoError(t, rows.Err())
	return keys
}

// TestJanitor_CleanupStale verifies that the janitor deletes stale locks
// while preserving fresh ones.
func TestJanitor_CleanupStale(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetResourceLocks(t, db)

	now := time.Now().UTC()

	// Insert stale lock (5 minutes ago)
	staleTime := now.Add(-5 * time.Minute)
	insertResourceLock(t, db, "stale:123", 999, "dead-worker", staleTime)

	// Insert fresh lock (30 seconds ago)
	freshTime := now.Add(-30 * time.Second)
	insertResourceLock(t, db, "fresh:456", 888, "live-worker", freshTime)

	// Verify both locks are present
	assert.Equal(t, 2, countResourceLocks(t, db))

	// Create janitor with maxAge of 2 minutes
	cfg := Config{
		Interval: 1 * time.Hour, // not used in single cleanup call
		MaxAge:   2 * time.Minute,
		Logger:   slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	j, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup once
	ctx := context.Background()
	_ = j.cleanup(ctx)

	// Verify stale deleted, fresh retained
	count := countResourceLocks(t, db)
	assert.Equal(t, 1, count, "only fresh lock should remain")

	keys := getResourceKeys(t, db)
	require.Len(t, keys, 1)
	assert.Equal(t, "fresh:456", keys[0])

	// Cleanup test data
	resetResourceLocks(t, db)
}

// TestJanitor_CleanupMultipleStale verifies that multiple stale locks are deleted.
func TestJanitor_CleanupMultipleStale(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetResourceLocks(t, db)

	now := time.Now().UTC()

	// Insert multiple stale locks (10 minutes ago)
	staleTime := now.Add(-10 * time.Minute)
	insertResourceLock(t, db, "stale:1", 100, "worker-1", staleTime)
	insertResourceLock(t, db, "stale:2", 200, "worker-2", staleTime)
	insertResourceLock(t, db, "stale:3", 300, "worker-3", staleTime)

	// Insert fresh lock (1 minute ago)
	freshTime := now.Add(-1 * time.Minute)
	insertResourceLock(t, db, "fresh:1", 400, "worker-4", freshTime)

	assert.Equal(t, 4, countResourceLocks(t, db))

	// Create janitor with maxAge of 5 minutes
	cfg := Config{
		MaxAge: 5 * time.Minute,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	j, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	ctx := context.Background()
	_ = j.cleanup(ctx)

	// Verify only the fresh lock remains
	count := countResourceLocks(t, db)
	assert.Equal(t, 1, count, "only fresh lock should remain")

	keys := getResourceKeys(t, db)
	require.Len(t, keys, 1)
	assert.Equal(t, "fresh:1", keys[0])

	resetResourceLocks(t, db)
}

// TestJanitor_CleanupNoStale verifies cleanup succeeds when no locks are stale.
func TestJanitor_CleanupNoStale(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetResourceLocks(t, db)

	now := time.Now().UTC()

	// Insert only fresh locks (30 seconds ago)
	freshTime := now.Add(-30 * time.Second)
	insertResourceLock(t, db, "fresh:1", 100, "worker-1", freshTime)
	insertResourceLock(t, db, "fresh:2", 200, "worker-2", freshTime)

	assert.Equal(t, 2, countResourceLocks(t, db))

	// Create janitor with maxAge of 2 minutes
	cfg := Config{
		MaxAge: 2 * time.Minute,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	j, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	ctx := context.Background()
	_ = j.cleanup(ctx)

	// Verify all locks remain
	count := countResourceLocks(t, db)
	assert.Equal(t, 2, count, "all locks should remain")

	resetResourceLocks(t, db)
}

// TestJanitor_CleanupEmptyTable verifies cleanup succeeds when table is empty.
func TestJanitor_CleanupEmptyTable(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetResourceLocks(t, db)

	// Table is empty
	assert.Equal(t, 0, countResourceLocks(t, db))

	// Create janitor
	cfg := Config{
		MaxAge: 2 * time.Minute,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	j, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	ctx := context.Background()
	_ = j.cleanup(ctx)

	// Verify table is still empty
	count := countResourceLocks(t, db)
	assert.Equal(t, 0, count)
}

// TestJanitor_CleanupBoundary verifies cleanup respects the exact MaxAge boundary.
func TestJanitor_CleanupBoundary(t *testing.T) {
	db := mustConnectTestDB(t)
	defer db.Close()
	resetResourceLocks(t, db)

	now := time.Now().UTC()
	maxAge := 2 * time.Minute

	// Insert lock exactly at maxAge boundary (should be deleted)
	exactlyMaxAge := now.Add(-maxAge)
	insertResourceLock(t, db, "boundary:exact", 100, "worker-1", exactlyMaxAge)

	// Insert lock just over maxAge boundary (should be deleted)
	justOverMaxAge := now.Add(-maxAge - 1*time.Second)
	insertResourceLock(t, db, "boundary:over", 200, "worker-2", justOverMaxAge)

	// Insert lock just under maxAge boundary (should be retained)
	justUnderMaxAge := now.Add(-maxAge + 1*time.Second)
	insertResourceLock(t, db, "boundary:under", 300, "worker-3", justUnderMaxAge)

	assert.Equal(t, 3, countResourceLocks(t, db))

	// Create janitor
	cfg := Config{
		MaxAge: maxAge,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	j, err := NewRunner(db, cfg)
	require.NoError(t, err)

	// Run cleanup
	ctx := context.Background()
	_ = j.cleanup(ctx)

	// Verify only the lock just under maxAge remains
	keys := getResourceKeys(t, db)
	require.Len(t, keys, 1)
	assert.Equal(t, "boundary:under", keys[0])

	resetResourceLocks(t, db)
}
