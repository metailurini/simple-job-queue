package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type listenAcquire interface {
	Acquire(ctx context.Context) (listenConn, error)
}

type listenConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Release()
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
}

type poolListenAcquire struct {
	pool *pgxpool.Pool
}

func (p poolListenAcquire) Acquire(ctx context.Context) (listenConn, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &poolListenConn{conn: conn}, nil
}

type poolListenConn struct {
	conn *pgxpool.Conn
}

func (p *poolListenConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.conn.Exec(ctx, sql, args...)
}

func (p *poolListenConn) Release() {
	p.conn.Release()
}

func (p *poolListenConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	return p.conn.Conn().WaitForNotification(ctx)
}

type pgNotifier struct {
	conn    listenConn
	cancel  context.CancelFunc
	logger  *slog.Logger
	updates chan struct{}
}

func newPGNotifier(ctx context.Context, pool *pgxpool.Pool, queues []QueueConfig, logger *slog.Logger) (*pgNotifier, error) {
	return newPGNotifierWithAcquire(ctx, poolListenAcquire{pool: pool}, queues, logger)
}

func newPGNotifierWithAcquire(ctx context.Context, pool listenAcquire, queues []QueueConfig, logger *slog.Logger) (*pgNotifier, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire listen connection: %w", err)
	}

	childCtx, cancel := context.WithCancel(ctx)
	n := &pgNotifier{
		conn:    conn,
		cancel:  cancel,
		logger:  logger,
		updates: make(chan struct{}, 1),
	}

	for _, q := range queues {
		channel := pgx.Identifier{fmt.Sprintf("queue_jobs_%s", q.Name)}.Sanitize()
		if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
			conn.Release()
			cancel()
			return nil, fmt.Errorf("listen %s: %w", channel, err)
		}
	}

	go n.loop(childCtx)
	return n, nil
}

func (n *pgNotifier) loop(ctx context.Context) {
	defer close(n.updates)
	defer n.conn.Release()

	for {
		notification, err := n.conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.logger.Warn("notification wait failed", "err", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		if notification == nil {
			continue
		}
		select {
		case n.updates <- struct{}{}:
		default:
		}
	}
}

func (n *pgNotifier) C() <-chan struct{} {
	return n.updates
}

func (n *pgNotifier) Close() {
	n.cancel()
}
