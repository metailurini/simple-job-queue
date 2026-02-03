package migrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

// These tests use a fake migrator; no external database is required.

type fakeMigrator struct {
	upErr      error
	closeErr   error
	closeDBErr error
	closed     bool
}

func (f *fakeMigrator) Up() error {
	return f.upErr
}

func (f *fakeMigrator) Close() (error, error) {
	f.closed = true
	return f.closeErr, f.closeDBErr
}

func TestRun(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name      string
		upErr     error
		wantErr   bool
		wantClose bool
	}{
		{
			name:      "success",
			wantErr:   false,
			wantClose: true,
		},
		{
			name:      "no changes",
			upErr:     migrate.ErrNoChange,
			wantErr:   false,
			wantClose: true,
		},
		{
			name:      "failure",
			upErr:     errors.New("boom"),
			wantErr:   true,
			wantClose: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMigrator{upErr: tc.upErr}
			runner := NewRunner(func(_ *sql.DB) (migrateRunner, error) {
				return fake, nil
			})

			err := runner.Run(ctx, nil, nil)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.wantClose && !fake.closed {
				t.Fatalf("expected migrator to close")
			}
		})
	}
}

func TestRunContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runner := NewRunner(func(_ *sql.DB) (migrateRunner, error) {
		return &fakeMigrator{}, nil
	})

	err := runner.Run(ctx, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}
