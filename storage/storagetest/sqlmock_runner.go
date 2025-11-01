package storagetest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type SQLMockRunner struct {
	DB   *sql.DB
	Mock sqlmock.Sqlmock
}

func NewSQLMockRunner() (*SQLMockRunner, error) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		return nil, err
	}
	return &SQLMockRunner{DB: db, Mock: mock}, nil
}

func MustSQLMock(t *testing.T) *SQLMockRunner {
	t.Helper()
	runner, err := NewSQLMockRunner()
	if err != nil {
		t.Fatalf("failed to create sqlmock runner: %v", err)
	}
	return runner
}

func (r *SQLMockRunner) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	result, err := r.DB.ExecContext(ctx, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	rowsAffected, raErr := result.RowsAffected()
	if raErr != nil {
		return pgconn.CommandTag{}, raErr
	}
	command := "EXEC"
	if fields := strings.Fields(sql); len(fields) > 0 {
		command = strings.ToUpper(fields[0])
	}
	tag := fmt.Sprintf("%s %d", command, rowsAffected)
	return pgconn.NewCommandTag(tag), nil
}

func (r *SQLMockRunner) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := r.DB.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &rowsWrapper{rows: rows}, nil
}

func (r *SQLMockRunner) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := r.Query(ctx, sql, args...)
	return &rowWrapper{rows: rows, err: err}
}

func (r *SQLMockRunner) Begin(ctx context.Context) (pgx.Tx, error) {
	return &sqlRunnerTx{runner: r}, nil
}

func (r *SQLMockRunner) ExpectationsWereMet(t *testing.T) {
	t.Helper()
	r.Mock.ExpectClose()
	if err := r.DB.Close(); err != nil {
		t.Fatalf("failed to close sqlmock db: %v", err)
	}
	if err := r.Mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// sqlRunnerTx adapts the SQLMockRunner to a pgx.Tx used in tests.
type sqlRunnerTx struct {
	runner    *SQLMockRunner
	committed bool
	rolled    bool
}

func (tx *sqlRunnerTx) Begin(ctx context.Context) (pgx.Tx, error) { return tx, nil }
func (tx *sqlRunnerTx) Commit(ctx context.Context) error          { tx.committed = true; return nil }
func (tx *sqlRunnerTx) Rollback(ctx context.Context) error        { tx.rolled = true; return nil }
func (tx *sqlRunnerTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom not implemented")
}
func (tx *sqlRunnerTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("SendBatch not implemented")
}
func (tx *sqlRunnerTx) LargeObjects() pgx.LargeObjects { panic("LargeObjects not implemented") }
func (tx *sqlRunnerTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("Prepare not implemented")
}
func (tx *sqlRunnerTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.runner.Exec(ctx, sql, args...)
}
func (tx *sqlRunnerTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.runner.Query(ctx, sql, args...)
}
func (tx *sqlRunnerTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.runner.QueryRow(ctx, sql, args...)
}
func (tx *sqlRunnerTx) Conn() *pgx.Conn { return nil }

type rowsWrapper struct {
	rows *sql.Rows
}

func (r *rowsWrapper) Close() {
	_ = r.rows.Close()
}

func (r *rowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *rowsWrapper) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *rowsWrapper) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *rowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r *rowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *rowsWrapper) Values() ([]any, error) {
	return nil, errors.New("Values not supported")
}

func (r *rowsWrapper) RawValues() [][]byte {
	return nil
}

func (r *rowsWrapper) Conn() *pgx.Conn {
	return nil
}

type rowWrapper struct {
	rows pgx.Rows
	err  error
}

func (r *rowWrapper) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return errors.New("nil rows")
	}
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}

func AssertUTC(t *testing.T, ts time.Time) {
	t.Helper()
	if ts.Location() != time.UTC {
		t.Fatalf("expected time to be in UTC, got %s", ts.Location())
	}
}
