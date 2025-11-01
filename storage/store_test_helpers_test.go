package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/metailurini/simple-job-queue/storage/storagetest"
)

type sqlMockPool struct {
	runner *storagetest.SQLMockRunner
	last   *sqlMockTx
}

var (
	mockPoolsMu sync.Mutex
	mockPools   = make(map[*storagetest.SQLMockRunner]*sqlMockPool)
)

func newSQLMockPool(runner *storagetest.SQLMockRunner) *sqlMockPool {
	mockPoolsMu.Lock()
	defer mockPoolsMu.Unlock()
	if p, ok := mockPools[runner]; ok {
		return p
	}
	p := &sqlMockPool{runner: runner}
	mockPools[runner] = p
	return p
}

func (p *sqlMockPool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx := &sqlMockTx{runner: p.runner}
	p.last = tx
	return tx, nil
}

func (p *sqlMockPool) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.BeginTx(ctx, pgx.TxOptions{})
}

func (p *sqlMockPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.runner.Exec(ctx, sql, args...)
}

func (p *sqlMockPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.runner.Query(ctx, sql, args...)
}

func (p *sqlMockPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.runner.QueryRow(ctx, sql, args...)
}

type sqlMockTx struct {
	runner    *storagetest.SQLMockRunner
	committed bool
	rolled    bool
}

func (tx *sqlMockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *sqlMockTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *sqlMockTx) Rollback(context.Context) error {
	tx.rolled = true
	return nil
}

func (tx *sqlMockTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom not implemented")
}

func (tx *sqlMockTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("SendBatch not implemented")
}

func (tx *sqlMockTx) LargeObjects() pgx.LargeObjects {
	panic("LargeObjects not implemented")
}

func (tx *sqlMockTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("Prepare not implemented")
}

func (tx *sqlMockTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.runner.Exec(ctx, sql, args...)
}

func (tx *sqlMockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.runner.Query(ctx, sql, args...)
}

func (tx *sqlMockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.runner.QueryRow(ctx, sql, args...)
}

func (tx *sqlMockTx) Conn() *pgx.Conn {
	return nil
}

func newStoreWithNow(t *testing.T, runner *storagetest.SQLMockRunner, nowFn func() time.Time) *Store {
	t.Helper()
	if nowFn == nil {
		nowFn = time.Now
	}
	pool := newSQLMockPool(runner)
	return &Store{DB: pool, now: nowFn}
}
