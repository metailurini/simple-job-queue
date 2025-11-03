package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrUnsupportedDriver = errors.New("unexpected driver connection type")

type listenAcquire interface {
	Acquire(ctx context.Context) (listenConn, error)
}

type listenConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Release()
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
}

type poolListenAcquire struct {
	db *sql.DB
}

func (p poolListenAcquire) Acquire(ctx context.Context) (listenConn, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &poolListenConn{conn: conn}, nil
}

type poolListenConn struct {
	conn *sql.Conn
}

func extractPgxConn(driverConn any) (*pgx.Conn, error) {
	// Prefer direct *pgx.Conn
	if c, ok := driverConn.(*pgx.Conn); ok {
		return c, nil
	}
	// Support types that expose Conn() *pgx.Conn (e.g. pgx stdlib wrapper)
	if c, ok := driverConn.(interface{ Conn() *pgx.Conn }); ok {
		return c.Conn(), nil
	}
	// Unexpected type
	slog.Warn("unexpected driver connection type; ensure using pgx stdlib driver", "type", fmt.Sprintf("%T", driverConn))
	return nil, fmt.Errorf("%w: %T", ErrUnsupportedDriver, driverConn)
}

func (p *poolListenConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := p.conn.Raw(func(driverConn any) error {
		pgxConn, err := extractPgxConn(driverConn)
		if err != nil {
			return err
		}
		var execErr error
		tag, execErr = pgxConn.Exec(ctx, sql, args...)
		return execErr
	})
	return tag, err
}

func (p *poolListenConn) Release() {
	_ = p.conn.Close()
}

func (p *poolListenConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	var notification *pgconn.Notification
	err := p.conn.Raw(func(driverConn any) error {
		pgxConn, err := extractPgxConn(driverConn)
		if err != nil {
			return err
		}
		var waitErr error
		notification, waitErr = pgxConn.WaitForNotification(ctx)
		return waitErr
	})
	return notification, err
}

type pgNotifier struct {
	conn    listenConn
	cancel  context.CancelFunc
	logger  *slog.Logger
	updates chan struct{}
}

func newPGNotifier(ctx context.Context, db *sql.DB, queues []QueueConfig, logger *slog.Logger) (*pgNotifier, error) {
	return newPGNotifierWithAcquire(ctx, poolListenAcquire{db: db}, queues, logger)
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
