package storage

import (
	"context"
	"database/sql"
)

// DB is a minimal interface capturing the subset of *sql.DB methods used by
// this project. The interface is intentionally small and focuses on the query
// execution methods. Since *sql.DB.BeginTx returns *sql.Tx (not an interface),
// we provide a thin adapter wrapper to convert between concrete and interface
// types while preserving testability.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Close() error
}

// Tx mirrors the subset of *sql.Tx methods used across the storage layer and
// worker/scheduler packages. This interface enables testing with fakes and
// sqlmock while production code uses the concrete *sql.Tx.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Commit() error
	Rollback() error
}

// dbAdapter wraps *sql.DB to implement the storage.DB interface. The only
// adaptation needed is BeginTx, which converts *sql.Tx to storage.Tx.
type dbAdapter struct {
	*sql.DB
}

// NewDB creates a storage.DB adapter wrapping the provided *sql.DB.
// This is the preferred way to construct a DB instance for production use.
func NewDB(db *sql.DB) DB {
	return &dbAdapter{DB: db}
}

// BeginTx wraps the underlying *sql.DB.BeginTx and returns the *sql.Tx
// wrapped in our txAdapter which implements storage.Tx.
func (a *dbAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := a.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &txAdapter{Tx: tx}, nil
}

// txAdapter wraps *sql.Tx to implement the storage.Tx interface.
// Since all methods except the type signature match, this is a simple wrapper.
type txAdapter struct {
	*sql.Tx
}

// Ensure txAdapter implements storage.Tx by embedding *sql.Tx, which already
// has all the required methods (ExecContext, QueryRowContext, QueryContext,
// Commit, Rollback).
