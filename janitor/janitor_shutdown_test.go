package janitor

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJanitor_Shutdown_Unit verifies graceful shutdown behavior with a unit test.
func TestJanitor_Shutdown_Unit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	gracePeriod := 5 * time.Second

	cfg := Config{
		Interval:    10 * time.Second, // Long interval to prevent cleanup
		GracePeriod: gracePeriod,
		Logger:      logger,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	mock.ExpectExec("DELETE FROM queue_resource_locks").WillReturnResult(sqlmock.NewResult(0, 0))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- runner.Run(ctx)
	}()

	// Allow the runner to start
	time.Sleep(100 * time.Millisecond)

	// Trigger shutdown
	cancel()

	// Wait for the runner to exit
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled, "expected context.Canceled error")
	case <-time.After(1 * time.Second):
		t.Fatal("runner did not shut down within the expected time")
	}

	// Verify log output
	t.Log(buf.String())
	dec := json.NewDecoder(&buf)
	var log map[string]interface{}

	// Check for "shutdown started" log
	require.NoError(t, dec.Decode(&log))
	assert.Equal(t, "janitor shutdown started", log["msg"])
	assert.Equal(t, "janitor_shutdown", log["event"])
	assert.Equal(t, "started", log["status"])
	assert.Equal(t, gracePeriod.String(), log["grace_period"])

	// Check for "shutdown completed" log
	require.NoError(t, dec.Decode(&log))
	assert.Equal(t, "janitor shutdown completed", log["msg"])
	assert.Equal(t, "janitor_shutdown", log["event"])
	assert.Equal(t, "completed", log["status"])
}

// TestJanitor_Shutdown_Integration verifies graceful shutdown against a real database.
func TestJanitor_Shutdown_Integration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping integration test; TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	gracePeriod := 3 * time.Second

	cfg := Config{
		Interval:    1 * time.Second,
		MaxAge:      1 * time.Minute,
		GracePeriod: gracePeriod,
		Logger:      logger,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	runCtx, runCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- runner.Run(runCtx)
	}()

	// Let the janitor run at least once
	time.Sleep(cfg.Interval + 500*time.Millisecond)

	// Trigger shutdown
	runCancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled, "expected context.Canceled error")
	case <-time.After(gracePeriod + 1*time.Second):
		t.Fatal("runner did not shut down within the grace period")
	}

	// Verify log output for shutdown sequence
	t.Log(buf.String())
	logStr := buf.String()
	assert.Contains(t, logStr, `"msg":"janitor shutdown started"`)
	assert.Contains(t, logStr, `"status":"started"`)
	assert.Contains(t, logStr, `"grace_period":"3s"`)
	assert.Contains(t, logStr, `"msg":"janitor shutdown completed"`)
	assert.Contains(t, logStr, `"status":"completed"`)
}

// TestJanitor_Shutdown_AbortsOnLongCleanup verifies that shutdown aborts
// when cleanup exceeds the grace period.
func TestJanitor_Shutdown_AbortsOnLongCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	gracePeriod := 100 * time.Millisecond
	cleanupDuration := 200 * time.Millisecond

	cfg := Config{
		Interval:    10 * time.Second,
		GracePeriod: gracePeriod,
		Logger:      logger,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)
	mock.ExpectExec("DELETE FROM queue_resource_locks").WillReturnResult(sqlmock.NewResult(0, 0)).WillDelayFor(cleanupDuration)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- runner.Run(ctx)
	}()

	// Allow the runner to start
	time.Sleep(50 * time.Millisecond)

	// Trigger shutdown
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(gracePeriod + cleanupDuration):
		t.Fatal("runner did not shut down as expected")
	}

	// Verify log output
	t.Log(buf.String())
	logStr := buf.String()
	assert.Contains(t, logStr, `"msg":"janitor shutdown started"`)
	assert.Contains(t, logStr, `"status":"started"`)
	assert.Contains(t, logStr, `"msg":"janitor shutdown aborted: grace period exceeded"`)
	assert.Contains(t, logStr, `"status":"aborted"`)
}

// TestJanitor_Shutdown_DBErrorOnCleanup verifies that a DB error during the
// final cleanup does not prevent shutdown.
func TestJanitor_Shutdown_DBErrorOnCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	gracePeriod := 5 * time.Second

	cfg := Config{
		Interval:    10 * time.Second,
		GracePeriod: gracePeriod,
		Logger:      logger,
	}
	runner, err := NewRunner(db, cfg)
	require.NoError(t, err)

	mock.ExpectExec("DELETE FROM queue_resource_locks").WillReturnError(assert.AnError)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- runner.Run(ctx)
	}()

	// Allow the runner to start
	time.Sleep(100 * time.Millisecond)

	// Trigger shutdown
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("runner did not shut down as expected")
	}

	// Verify log output
	t.Log(buf.String())
	logStr := buf.String()
	assert.Contains(t, logStr, `"msg":"janitor shutdown started"`)
	assert.Contains(t, logStr, `"msg":"janitor cleanup failed"`)
	assert.Contains(t, logStr, `"err":"assert.AnError general error for testing"`)
	// Final log should indicate the shutdown completed but with an error from cleanup
	assert.Contains(t, logStr, `"msg":"janitor shutdown completed but final cleanup failed"`)
	assert.Contains(t, logStr, `"status":"completed_with_error"`)
}
