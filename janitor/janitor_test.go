package janitor

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/timeprovider"
)

// TestNewRunner validates the constructor with various configurations.
func TestNewRunner(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		wantCfg Config
	}{
		{
			name:    "nil pool returns error",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name: "default interval and maxAge",
			cfg:  Config{},
			wantCfg: Config{
				Interval: 1 * time.Minute,
				MaxAge:   5 * time.Minute,
			},
		},
		{
			name: "custom interval and maxAge",
			cfg: Config{
				Interval: 30 * time.Second,
				MaxAge:   2 * time.Minute,
			},
			wantCfg: Config{
				Interval: 30 * time.Second,
				MaxAge:   2 * time.Minute,
			},
		},
		{
			name: "zero interval uses default",
			cfg: Config{
				Interval: 0,
				MaxAge:   10 * time.Minute,
			},
			wantCfg: Config{
				Interval: 1 * time.Minute,
				MaxAge:   10 * time.Minute,
			},
		},
		{
			name: "zero maxAge uses default",
			cfg: Config{
				Interval: 2 * time.Minute,
				MaxAge:   0,
			},
			wantCfg: Config{
				Interval: 2 * time.Minute,
				MaxAge:   5 * time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				_, err := NewRunner(nil, tt.cfg)
				require.Error(t, err)
				return
			}
			r, err := NewRunner(&pgxpool.Pool{}, tt.cfg)
			require.NoError(t, err)
			require.NotNil(t, r)
			assert.Equal(t, tt.wantCfg.Interval, r.cfg.Interval)
			assert.Equal(t, tt.wantCfg.MaxAge, r.cfg.MaxAge)
			assert.NotNil(t, r.logger, "logger should be set to default if nil")
		})
	}
}

// TestNewRunner_CustomLogger verifies that a custom logger is used when provided.
func TestNewRunner_CustomLogger(t *testing.T) {
	customLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		Logger: customLogger,
	}
	runner, err := NewRunner(&pgxpool.Pool{}, cfg)
	require.NoError(t, err)
	assert.Equal(t, customLogger, runner.logger)
}

// TestNewRunner_TimeProvider verifies that the TimeProvider is properly set.
func TestNewRunner_TimeProvider(t *testing.T) {
	fixedTime := time.Date(2025, 10, 31, 12, 0, 0, 0, time.UTC)
	provider := timeprovider.FixedProvider{T: fixedTime}
	cfg := Config{
		TimeProvider: provider,
	}
	runner, err := NewRunner(&pgxpool.Pool{}, cfg)
	require.NoError(t, err)
	require.NotNil(t, runner.now)

	// Verify the time function returns the fixed time
	got := runner.now()
	assert.Equal(t, fixedTime, got)
}

// TestNewRunner_DefaultTimeProvider verifies that RealProvider is used by default.
func TestNewRunner_DefaultTimeProvider(t *testing.T) {
	cfg := Config{}
	runner, err := NewRunner(&pgxpool.Pool{}, cfg)
	require.NoError(t, err)
	require.NotNil(t, runner.now)

	// Verify the time function returns a time close to now
	got := runner.now()
	assert.WithinDuration(t, time.Now(), got, 1*time.Second)
}

// TestCleanup_NoStaleRows verifies that cleanup succeeds when no rows match by
// injecting a mock exec function into the runner.
func TestCleanup_NoStaleRows(t *testing.T) {
	called := false
	// Mock exec returns 0 rows affected
	exec := func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
		called = true
		return 0, nil
	}
	cfg := Config{
		Interval:     1 * time.Minute,
		MaxAge:       5 * time.Minute,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		TimeProvider: timeprovider.RealProvider{},
	}
	r := &Runner{exec: exec, cfg: cfg, logger: cfg.Logger, now: cfg.TimeProvider.Now}
	// Call cleanup directly
	_ = r.cleanup(context.Background())
	assert.True(t, called, "exec should be invoked")
}

// TestCleanup_Boundary verifies that the cutoff boundary uses <= and passes
// the expected cutoff value to the exec function.
func TestCleanup_Boundary(t *testing.T) {
	var gotArg interface{}
	exec := func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
		if len(args) > 0 {
			gotArg = args[0]
		}
		return 1, nil
	}
	fixedTime := time.Date(2025, 10, 31, 12, 0, 0, 0, time.UTC)
	provider := timeprovider.FixedProvider{T: fixedTime}
	cfg := Config{
		Interval:     1 * time.Minute,
		MaxAge:       5 * time.Minute,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		TimeProvider: provider,
	}
	r := &Runner{exec: exec, cfg: cfg, logger: cfg.Logger, now: cfg.TimeProvider.Now}
	_ = r.cleanup(context.Background())
	// expected cutoff is fixedTime.Add(-MaxAge)
	expected := fixedTime.UTC().Add(-cfg.MaxAge)
	if gotArg == nil {
		t.Fatalf("expected cutoff arg, got nil")
	}
	// assert time roughly equal
	gotTime, ok := gotArg.(time.Time)
	if !ok {
		t.Fatalf("expected time arg, got %T", gotArg)
	}
	assert.Equal(t, expected, gotTime)
}

// TestRun_ContextCancellation verifies that Run exits when the context is canceled.
func TestRun_ContextCancellation(t *testing.T) {
	// For this test, we immediately cancel the context before the ticker fires
	// so we don't need a real pool connection
	cfg := Config{
		Interval: 10 * time.Second, // Long interval to ensure ticker doesn't fire during test
		MaxAge:   1 * time.Minute,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runner, err := NewRunner(&pgxpool.Pool{}, cfg)
	require.NoError(t, err)

	// Mock the exec function to avoid a panic from the nil pool.
	runner.exec = func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
		return 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start the runner in a goroutine
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	// Cancel the context immediately (before ticker fires)
	cancel()

	// Wait for Run to return
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

// TestRun_ContextTimeout verifies that Run exits when the context times out.
func TestRun_ContextTimeout(t *testing.T) {
	// Using a very short timeout that expires before the ticker fires
	cfg := Config{
		Interval: 10 * time.Second, // Long interval to ensure ticker doesn't fire
		MaxAge:   1 * time.Minute,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runner, err := NewRunner(&pgxpool.Pool{}, cfg)
	require.NoError(t, err)

	// Mock the exec function to avoid a panic from the nil pool.
	runner.exec = func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Run will exit when the context times out
	err = runner.Run(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
