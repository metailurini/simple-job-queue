package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxPoolAdapter wraps *pgxpool.Pool to implement the storage.DB interface.
// This is a temporary adapter used during the transition from pgx to database/sql.
// It allows integration tests to continue using pgxpool while the rest of the
// codebase migrates to database/sql.
type pgxPoolAdapter struct {
	pool *pgxpool.Pool
}

// NewPgxPoolAdapter wraps a pgxpool.Pool to implement storage.DB.
// This is temporary and will be removed once migration to database/sql is complete.
func NewPgxPoolAdapter(pool *pgxpool.Pool) DB {
	return &pgxPoolAdapter{pool: pool}
}

// NewPgxPoolFromDB creates a new pgxpool.Pool from a *sql.DB connection.
// This is a temporary helper to support packages that still require pgxpool
// during the migration. It creates a separate pgxpool connection using the
// same DSN. This will be removed once all packages are migrated to database/sql.
func NewPgxPoolFromDB(ctx context.Context, _ *sql.DB, dsn string) (*pgxpool.Pool, error) {
	// Note: We create a new pool here rather than trying to share the sql.DB connection
	// This is inefficient but temporary during the migration phase
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	return pool, nil
}

func (a *pgxPoolAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tag, err := a.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxResult{tag: tag}, nil
}

func (a *pgxPoolAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// pgx.Row doesn't directly implement *sql.Row, so we need to handle this differently
	// For now, we'll panic if this is called since it's not used in integration tests
	panic("QueryRowContext not implemented for pgxPoolAdapter")
}

func (a *pgxPoolAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Similar issue - pgx.Rows doesn't directly map to *sql.Rows
	panic("QueryContext not implemented for pgxPoolAdapter")
}

func (a *pgxPoolAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &pgxTxAdapter{tx: tx}, nil
}

func (a *pgxPoolAdapter) Close() error {
	a.pool.Close()
	return nil
}

// Query is a helper for integration tests that need to use pgx.Rows directly
func (a *pgxPoolAdapter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return a.pool.Query(ctx, sql, args...)
}

// Exec is a helper for integration tests
func (a *pgxPoolAdapter) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := a.pool.Exec(ctx, sql, args...)
	return err
}

type pgxTxAdapter struct {
	tx pgx.Tx
}

func (t *pgxTxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tag, err := t.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxResult{tag: tag}, nil
}

func (t *pgxTxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	panic("QueryRowContext not implemented for pgxTxAdapter")
}

func (t *pgxTxAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("QueryContext not implemented for pgxTxAdapter")
}

func (t *pgxTxAdapter) Commit() error {
	return t.tx.Commit(context.Background())
}

func (t *pgxTxAdapter) Rollback() error {
	return t.tx.Rollback(context.Background())
}

type pgxResult struct {
	tag pgconn.CommandTag
}

func (r *pgxResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId not supported")
}

func (r *pgxResult) RowsAffected() (int64, error) {
	return r.tag.RowsAffected(), nil
}
