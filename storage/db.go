package storage

import (
	"context"
	"database/sql"
)

// DB is intentionally small so *sql.DB already satisfies it.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// BeginTx's signature is intentionally modified to return a concrete *sql.Tx
	// because the Go standard library's *sql.DB returns this concrete type.
	// This allows a bare *sql.DB to satisfy the interface without a wrapper.
	// The returned *sql.Tx satisfies the Tx interface.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

// Tx mirrors the subset of *sql.Tx we use.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Commit() error
	Rollback() error
}
