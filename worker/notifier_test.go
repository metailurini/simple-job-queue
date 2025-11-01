package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeListenAcquire struct {
	acquire func(context.Context) (listenConn, error)
}

func (f fakeListenAcquire) Acquire(ctx context.Context) (listenConn, error) {
	return f.acquire(ctx)
}

type fakeListenConn struct {
	exec    func(context.Context, string, ...any) (pgconn.CommandTag, error)
	wait    func(context.Context) (*pgconn.Notification, error)
	release func()
}

func (f *fakeListenConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if f.exec != nil {
		return f.exec(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeListenConn) Release() {
	if f.release != nil {
		f.release()
	}
}

func (f *fakeListenConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	if f.wait != nil {
		return f.wait(ctx)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestNewPGNotifierAcquireError(t *testing.T) {
	_, err := newPGNotifierWithAcquire(context.Background(), fakeListenAcquire{acquire: func(context.Context) (listenConn, error) {
		return nil, errors.New("boom")
	}}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.EqualError(t, err, "acquire listen connection: boom")
}

func TestNewPGNotifierListenFailureReleasesConn(t *testing.T) {
	released := false
	_, err := newPGNotifierWithAcquire(context.Background(), fakeListenAcquire{acquire: func(context.Context) (listenConn, error) {
		return &fakeListenConn{
			exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, errors.New("listen failed")
			},
			release: func() { released = true },
		}, nil
	}}, []QueueConfig{{Name: "default"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.EqualError(t, err, "listen \"queue_jobs_default\": listen failed")
	assert.True(t, released, "expected release on listen failure")
}

func TestPGNotifierLoopEmitsUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	notifications := make(chan struct{}, 1)
	released := make(chan struct{}, 1)

	conn := &fakeListenConn{
		exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("LISTEN"), nil
		},
		wait: func(ctx context.Context) (*pgconn.Notification, error) {
			select {
			case <-notifications:
				return &pgconn.Notification{Channel: "queue_jobs_default"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		release: func() { released <- struct{}{} },
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	notifier, err := newPGNotifierWithAcquire(ctx, fakeListenAcquire{acquire: func(context.Context) (listenConn, error) {
		return conn, nil
	}}, []QueueConfig{{Name: "default"}}, logger)
	require.NoError(t, err)

	notifications <- struct{}{}

	select {
	case <-notifier.C():
	case <-time.After(time.Second):
		require.FailNow(t, "expected notification delivery")
	}

	cancel()
	notifier.Close()

	select {
	case <-released:
	case <-time.After(time.Second):
		require.FailNow(t, "expected release on close")
	}

	select {
	case _, ok := <-notifier.C():
		assert.False(t, ok, "expected channel closed")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for channel close")
	}
}
