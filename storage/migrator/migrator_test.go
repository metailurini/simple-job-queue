package migrator

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/golang-migrate/migrate/v4"
)

type stubRunner struct {
	upCalled    bool
	closeCalled bool
	upErr       error
	sourceClose error
	dbClose     error
}

func (s *stubRunner) Up() error {
	s.upCalled = true
	return s.upErr
}

func (s *stubRunner) Close() (sourceErr, dbErr error) {
	s.closeCalled = true
	return s.sourceClose, s.dbClose
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunInvokesUpAndClose(t *testing.T) {
	stub := &stubRunner{}
	factory := func(_ context.Context, _ *sql.DB) (migrateRunner, error) { return stub, nil }

	err := NewRunner(factory).Run(context.Background(), &sql.DB{}, discardLogger())
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if !stub.upCalled {
		t.Fatalf("expected Up to be called")
	}
	if !stub.closeCalled {
		t.Fatalf("expected Close to be called")
	}
}

func TestRunTreatsNoChangeAsSuccess(t *testing.T) {
	stub := &stubRunner{upErr: migrate.ErrNoChange}
	factory := func(_ context.Context, _ *sql.DB) (migrateRunner, error) { return stub, nil }

	if err := NewRunner(factory).Run(context.Background(), &sql.DB{}, discardLogger()); err != nil {
		t.Fatalf("Run() unexpected error for ErrNoChange: %v", err)
	}
	if !stub.upCalled || !stub.closeCalled {
		t.Fatalf("expected Up and Close to be called")
	}
}

func TestRunPropagatesUpError(t *testing.T) {
	wantErr := errors.New("boom")
	stub := &stubRunner{upErr: wantErr}
	factory := func(_ context.Context, _ *sql.DB) (migrateRunner, error) { return stub, nil }

	err := NewRunner(factory).Run(context.Background(), &sql.DB{}, discardLogger())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want %v", err, wantErr)
	}
}
