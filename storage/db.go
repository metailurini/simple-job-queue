package storage

import (
	"context"
	"database/sql"
)

// DB is a minimal interface capturing the subset of database methods used by
// this project. The interface is intentionally small and focuses on the query
// execution methods needed by the storage layer.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Close() error
}

// Tx mirrors the subset of transaction methods used across the storage layer.
// This interface enables testing with fakes and sqlmock while production code
// uses the concrete *sql.Tx.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Commit() error
	Rollback() error
}

// Row is a minimal interface for scanning a single row result. This allows
// both *sql.Row and pgx.Row to be used interchangeably during the migration.
type Row interface {
	Scan(dest ...any) error
}

// Rows is a minimal interface for iterating over query results. This allows
// both *sql.Rows and pgx.Rows to be used interchangeably during the migration.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
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
