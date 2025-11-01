package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/metailurini/simple-job-queue/storage"
)

func newTestRunner(catchUpMax int) *Runner {
	return &Runner{
		catchUpMax: catchUpMax,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestNewRunnerDefaults(t *testing.T) {
	store := newStoreWithNow(func() time.Time { return time.Unix(0, 0) })
	cfg := Config{}

	runner, err := NewRunner(store, cfg)
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, runner.interval, "expected default interval 30s")
	assert.Equal(t, 5, runner.catchUpMax, "expected default catchUpMax 5")
	require.NotNil(t, runner.logger, "expected logger to be set")

	now := runner.now()
	assert.False(t, now.IsZero(), "expected now function to be initialized")
}

func TestNewRunnerRequiresDependencies(t *testing.T) {
	_, err := NewRunner(nil, Config{})
	assert.Error(t, err, "expected error when store is nil")
}

func TestRunnerRunStopsOnContextCancel(t *testing.T) {
	tx := &stubTx{}
	pool := &pgxpoolStub{tx: tx, began: make(chan struct{}, 1)}
	store := storage.NewTestStore(storage.TestStoreDependencies{DB: pool, Now: func() time.Time { return time.Now().UTC() }})

	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Nanosecond,
		catchUpMax: 1,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	select {
	case <-pool.began:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for BeginTx")
	}
	cancel()
	wg.Wait()

	assert.True(t, tx.rollbackCalled, "expected rollback to be called")
}

func TestTickEnqueuesDueRunsAndUpdatesSchedule(t *testing.T) {
	nowLoc := time.FixedZone("America/Los_Angeles", -8*3600)
	initialLast := time.Date(2024, 11, 5, 7, 0, 0, 0, nowLoc)
	scheduleRowValues := []stubRow{{
		values: []any{
			int64(10),
			"send-email",
			"critical",
			[]byte(`{"foo":"bar"}`),
			"* * * * *",
			sql.NullString{Valid: true, String: "dedupe"},
			sql.NullTime{Valid: true, Time: initialLast},
		},
	}}

	enqueueTimes := []time.Time{
		time.Date(2024, 11, 5, 7, 1, 0, 0, nowLoc).UTC(),
		time.Date(2024, 11, 5, 7, 2, 0, 0, nowLoc).UTC(),
	}

	tx := &stubTx{
		scheduleRows:     scheduleRowValues,
		enqueueResponses: []stubRow{{values: []any{int64(1)}}, {values: []any{int64(2)}}},
	}

	runNow := time.Date(2024, 11, 5, 7, 2, 30, 0, nowLoc)
	store := storage.NewTestStore(storage.TestStoreDependencies{DB: tx, Now: func() time.Time { return runNow }})
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 5,
		now: func() time.Time {
			return runNow
		},
	}

	err := runner.tick(context.Background())
	require.NoError(t, err)

	assert.True(t, tx.commitCalled, "expected commit to be called")

	// Expect a single bulk insert call
	require.Len(t, tx.enqueueCalls, 1, "expected 1 enqueue call")
	call := tx.enqueueCalls[0]
	expectedArgs := 9 * len(enqueueTimes)
	require.Len(t, call.args, expectedArgs, "expected %d args", expectedArgs)
	// Validate each run_at and dedupe embedded in the args. Each row has 9 args, run_at is at index 4 of each row block.
	for i := range enqueueTimes {
		offset := i * 9
		runAt, ok := call.args[offset+4].(time.Time)
		require.True(t, ok, "expected runAt time argument for row %d", i)
		assert.True(t, runAt.Equal(enqueueTimes[i]), "runAt %d mismatch: got %s want %s", i, runAt, enqueueTimes[i])
		assert.Equal(t, time.UTC, runAt.Location(), "expected runAt to be UTC")

		dedupe, _ := call.args[offset+7].(*string)
		expectedDedupe := "dedupe:" + fmt.Sprintf("%d", runAt.Unix())
		require.NotNil(t, dedupe, "dedupe should not be nil for row %d", i)
		assert.Equal(t, expectedDedupe, *dedupe, "dedupe mismatch for row %d", i)
	}

	require.Len(t, tx.execCalls, 1, "expected one exec call")
	execCall := tx.execCalls[0]
	expectedSQL := "UPDATE queue_schedules SET last_enqueued_at=$2 WHERE id=$1 AND (last_enqueued_at IS NOT DISTINCT FROM $3)"
	assert.Equal(t, expectedSQL, execCall.sql)
	lastRun, ok := execCall.args[1].(time.Time)
	require.True(t, ok, "expected last run time argument")
	expectedLast := enqueueTimes[len(enqueueTimes)-1]
	assert.True(t, lastRun.Equal(expectedLast), "last enqueued mismatch: got %s want %s", lastRun, expectedLast)
	prevRun, ok := execCall.args[2].(time.Time)
	require.True(t, ok, "expected previous last enqueued argument")
	assert.True(t, prevRun.Equal(initialLast), "previous last enqueued mismatch: got %s want %s", prevRun, initialLast)
}

func TestTickSkipsWhenScheduleAlreadyClaimed(t *testing.T) {
	now := time.Date(2024, 11, 5, 7, 5, 0, 0, time.UTC)
	scheduleRowValues := []stubRow{{
		values: []any{
			int64(42),
			"send-email",
			"default",
			[]byte(`{"foo":true}`),
			"* * * * *",
			sql.NullString{},
			sql.NullTime{},
		},
	}}

	tx := &stubTx{
		scheduleRows: scheduleRowValues,
		execTags:     []string{"UPDATE 0"},
	}

	store := storage.NewTestStore(storage.TestStoreDependencies{DB: tx, Now: func() time.Time { return now }})
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 5,
		now: func() time.Time {
			return now
		},
	}

	err := runner.tick(context.Background())
	require.NoError(t, err)

	require.Empty(t, tx.enqueueCalls, "expected no enqueue calls when claim fails")
	require.Len(t, tx.execCalls, 1, "expected one exec attempt")
	assert.Nil(t, tx.execCalls[0].args[2], "expected previous value argument to be nil")
}

func TestTickRevertsScheduleWhenEnqueueFails(t *testing.T) {
	now := time.Date(2024, 11, 5, 9, 0, 0, 0, time.UTC)
	scheduleRowValues := []stubRow{{
		values: []any{
			int64(51),
			"sync-data",
			"critical",
			[]byte(`{"bar":true}`),
			"* * * * *",
			sql.NullString{},
			sql.NullTime{},
		},
	}}

	tx := &stubTx{
		scheduleRows:     scheduleRowValues,
		enqueueInsertErr: errors.New("insert boom"),
		enqueueResponses: []stubRow{},
	}

	store := storage.NewTestStore(storage.TestStoreDependencies{DB: tx, Now: func() time.Time { return now }})
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 2,
		now: func() time.Time {
			return now
		},
	}

	err := runner.tick(context.Background())
	require.NoError(t, err)

	require.Len(t, tx.execCalls, 2, "expected claim and revert exec calls")

	claimArgs := tx.execCalls[0].args
	assert.Nil(t, claimArgs[2], "expected claim previous arg nil")

	revertArgs := tx.execCalls[1].args
	assert.Nil(t, revertArgs[1], "expected revert to restore nil previous")
	_, ok := revertArgs[2].(time.Time)
	assert.True(t, ok, "expected revert guard to include claimed timestamp")
}

func TestTickReturnsErrorWhenQueryFails(t *testing.T) {
	tx := &stubTx{queryErr: errors.New("boom")}
	store := storage.NewTestStore(storage.TestStoreDependencies{DB: tx, Now: time.Now})
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 1,
		now:        time.Now,
	}

	err := runner.tick(context.Background())
	assert.ErrorIs(t, err, tx.queryErr)
	assert.False(t, tx.commitCalled, "did not expect commit on failure")
}

func TestTickContinuesAfterScanError(t *testing.T) {
	validRun := time.Date(2024, 2, 2, 10, 0, 0, 0, time.UTC)
	scheduleRows := []stubRow{
		{err: errors.New("scan failure")},
		{values: []any{
			int64(22),
			"task",
			"queue",
			[]byte("{}"),
			"0 * * * *",
			sql.NullString{},
			sql.NullTime{},
		}},
	}
	tx := &stubTx{
		scheduleRows:     scheduleRows,
		enqueueResponses: []stubRow{{values: []any{int64(99)}}},
	}

	store := storage.NewTestStore(storage.TestStoreDependencies{DB: tx, Now: func() time.Time { return validRun }})
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 1,
		now: func() time.Time {
			return validRun
		},
	}

	err := runner.tick(context.Background())
	require.NoError(t, err)

	require.Len(t, tx.enqueueCalls, 1, "expected one enqueue call")
	assert.True(t, tx.commitCalled, "expected commit to be called")
}

func TestScanScheduleCopiesPayloadAndSetsPointers(t *testing.T) {
	payload := []byte{1, 2, 3}
	dedupe := sql.NullString{Valid: true, String: "dk"}
	last := sql.NullTime{Valid: true, Time: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	row := &stubSingleRow{row: stubRow{values: []any{
		int64(5),
		"send",
		"queue",
		payload,
		"*/5 * * * *",
		dedupe,
		last,
	}}}

	schedule, err := storage.ScanSchedule(row)
	require.NoError(t, err)

	assert.Equal(t, int64(5), schedule.ID)
	assert.Equal(t, "send", schedule.TaskType)
	assert.Equal(t, "queue", schedule.Queue)

	payload[0] = 9
	expectedPayload := []byte{1, 2, 3}
	assert.Equal(t, expectedPayload, schedule.Payload, "expected payload copy")
	require.NotNil(t, schedule.DedupeKey, "expected dedupe key to be set")
	assert.Equal(t, "dk", *schedule.DedupeKey)
	require.NotNil(t, schedule.LastEnqueuedAt, "expected last enqueued at to be set")
	assert.True(t, schedule.LastEnqueuedAt.Equal(last.Time))
}

func TestEnqueueRunsBuildsParamsAndSkipsDuplicates(t *testing.T) {
	now := time.Date(2024, 5, 6, 7, 0, 0, 0, time.UTC)
	db := &enqueueDBStub{
		results: []stubRow{
			{values: []any{int64(1)}},
			{values: []any{int64(3)}},
		},
	}
	store := storage.NewTestStore(storage.TestStoreDependencies{
		DB:  db,
		Now: func() time.Time { return now },
	})

	runner := &Runner{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		catchUpMax: 3,
		store:      store,
	}

	dedupe := "dedupe-key"
	schedule := storage.ScheduleRow{
		ID:        44,
		TaskType:  "send",
		Queue:     "emails",
		Payload:   []byte(`{"ok":true}`),
		DedupeKey: &dedupe,
	}
	runTimes := []time.Time{
		time.Date(2024, 5, 6, 7, 1, 0, 0, time.UTC),
		time.Date(2024, 5, 6, 7, 2, 0, 0, time.UTC),
		time.Date(2024, 5, 6, 7, 3, 0, 0, time.UTC),
	}

	err := runner.enqueueRuns(context.Background(), schedule, runTimes)
	require.NoError(t, err)

	// Expect a single bulk insert call
	require.Len(t, db.calls, 1, "expected 1 call")
	call := db.calls[0]
	expectedArgs := 9 * len(runTimes)
	require.Len(t, call.args, expectedArgs, "expected %d args", expectedArgs)
	for i := range runTimes {
		offset := i * 9
		runAt, ok := call.args[offset+4].(time.Time)
		require.True(t, ok, "expected runAt argument at offset %d", offset+4)
		assert.Equal(t, time.UTC, runAt.Location(), "runAt should be UTC")
		assert.True(t, runAt.Equal(runTimes[i]), "runAt mismatch at %d: got %s want %s", i, runAt, runTimes[i])
		assert.Equal(t, schedule.Queue, call.args[offset], "queue mismatch at %d", i)
		assert.Equal(t, schedule.TaskType, call.args[offset+1], "task type mismatch at %d", i)
		dedupeArg, _ := call.args[offset+7].(*string)
		expected := fmt.Sprintf("%s:%d", dedupe, runTimes[i].Unix())
		require.NotNil(t, dedupeArg, "dedupe should not be nil at %d", i)
		assert.Equal(t, expected, *dedupeArg, "dedupe mismatch at %d", i)
	}
}

func TestComputeDue(t *testing.T) {
	locPST := time.FixedZone("America/Los_Angeles", -8*3600)
	locEST := time.FixedZone("America/New_York", -5*3600)
	lastEnqueuedAt := time.Date(2024, 11, 5, 7, 0, 0, 0, locPST)

	testCases := []struct {
		name         string
		catchUpMax   int
		schedule     storage.ScheduleRow
		now          time.Time
		expectedRuns []time.Time
	}{
		{
			name:       "respects schedule location",
			catchUpMax: 4,
			schedule: storage.ScheduleRow{
				Cron:           "*/30 * * * *",
				LastEnqueuedAt: &lastEnqueuedAt,
			},
			now: time.Date(2024, 11, 5, 8, 5, 0, 0, locPST),
			expectedRuns: []time.Time{
				time.Date(2024, 11, 5, 7, 30, 0, 0, locPST).UTC(),
				time.Date(2024, 11, 5, 8, 0, 0, 0, locPST).UTC(),
			},
		},
		{
			name:       "defaults from now location",
			catchUpMax: 3,
			schedule:   storage.ScheduleRow{Cron: "0 * * * *"},
			now:        time.Date(2024, 2, 1, 10, 15, 0, 0, locEST),
			expectedRuns: []time.Time{
				time.Date(2024, 1, 31, 11, 0, 0, 0, locEST).UTC(),
				time.Date(2024, 1, 31, 12, 0, 0, 0, locEST).UTC(),
				time.Date(2024, 1, 31, 13, 0, 0, 0, locEST).UTC(),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runner := newTestRunner(tc.catchUpMax)
			due := runner.computeDue(tc.schedule, tc.now)

			require.Len(t, due, len(tc.expectedRuns), "expected %d runs", len(tc.expectedRuns))

			for i, ts := range due {
				assert.True(t, ts.Equal(tc.expectedRuns[i]), "due[%d]=%s, expected %s", i, ts, tc.expectedRuns[i])
				assert.Equal(t, time.UTC, ts.Location(), "due[%d] location should be UTC", i)
			}
		})
	}
}

type pgxpoolStub struct {
	tx         *stubTx
	beginErr   error
	beginCalls atomic.Int32
	began      chan struct{}
}

func (p *pgxpoolStub) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	p.beginCalls.Add(1)
	if p.began != nil {
		select {
		case p.began <- struct{}{}:
		default:
		}
	}
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	if p.tx == nil {
		return nil, errors.New("no transaction configured")
	}
	p.tx.reset()
	return p.tx, nil
}

// Begin allows the pool stub to satisfy the store's dbRunner interface by
// delegating to BeginTx. This mirrors pgxpool.Pool's behavior.
func (p *pgxpoolStub) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.BeginTx(ctx, pgx.TxOptions{})
}

// Exec delegates to the underlying stubTx Exec implementation.
func (p *pgxpoolStub) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.tx == nil {
		return pgconn.CommandTag{}, errors.New("no transaction configured")
	}
	return p.tx.Exec(ctx, sql, args...)
}

// Query delegates to the underlying stubTx Query implementation.
func (p *pgxpoolStub) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.tx == nil {
		return nil, errors.New("no transaction configured")
	}
	return p.tx.Query(ctx, sql, args...)
}

// QueryRow delegates to the underlying stubTx QueryRow implementation.
func (p *pgxpoolStub) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.tx == nil {
		return &stubSingleRow{row: stubRow{err: errors.New("no transaction configured")}}
	}
	return p.tx.QueryRow(ctx, sql, args...)
}

type stubTx struct {
	scheduleRows     []stubRow
	enqueueResponses []stubRow
	enqueueCalls     []enqueueCall
	execCalls        []execCall
	execTags         []string
	queryErr         error
	enqueueInsertErr error
	execErr          error
	commitCalled     bool
	rollbackCalled   bool
}

func (tx *stubTx) reset() {
	tx.enqueueCalls = nil
	tx.execCalls = nil
	tx.commitCalled = false
	tx.rollbackCalled = false
}

func (tx *stubTx) Begin(ctx context.Context) (pgx.Tx, error) { return tx, nil }
func (tx *stubTx) Commit(context.Context) error {
	tx.commitCalled = true
	return nil
}
func (tx *stubTx) Rollback(context.Context) error {
	tx.rollbackCalled = true
	return nil
}
func (tx *stubTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("not implemented")
}
func (tx *stubTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("SendBatch not implemented")
}
func (tx *stubTx) LargeObjects() pgx.LargeObjects {
	panic("LargeObjects not implemented")
}
func (tx *stubTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("Prepare not implemented")
}
func (tx *stubTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execCalls = append(tx.execCalls, execCall{sql: sql, args: cloneArgs(args)})
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	tag := "UPDATE 1"
	if len(tx.execTags) > 0 {
		tag = tx.execTags[0]
		tx.execTags = tx.execTags[1:]
	}
	return pgconn.NewCommandTag(tag), nil
}
func (tx *stubTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if tx.queryErr != nil {
		return nil, tx.queryErr
	}
	// If this is an INSERT returning ids from EnqueueJobs, return enqueueResponses
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upper, "INSERT INTO QUEUE_JOBS") {
		if tx.enqueueInsertErr != nil {
			return nil, tx.enqueueInsertErr
		}
		tx.enqueueCalls = append(tx.enqueueCalls, enqueueCall{sql: sql, args: cloneArgs(args)})
		rowsCopy := make([]stubRow, len(tx.enqueueResponses))
		copy(rowsCopy, tx.enqueueResponses)
		return &stubRows{rows: rowsCopy}, nil
	}
	rowsCopy := make([]stubRow, len(tx.scheduleRows))
	copy(rowsCopy, tx.scheduleRows)
	return &stubRows{rows: rowsCopy}, nil
}
func (tx *stubTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	tx.enqueueCalls = append(tx.enqueueCalls, enqueueCall{sql: sql, args: cloneArgs(args)})
	if len(tx.enqueueResponses) == 0 {
		return &stubSingleRow{row: stubRow{err: errors.New("unexpected query")}}
	}
	row := tx.enqueueResponses[0]
	tx.enqueueResponses = tx.enqueueResponses[1:]
	return &stubSingleRow{row: row}
}
func (tx *stubTx) Conn() *pgx.Conn { return nil }

type stubRows struct {
	rows []stubRow
	idx  int
	err  error
}

func (r *stubRows) Close()                                       {}
func (r *stubRows) Err() error                                   { return r.err }
func (r *stubRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *stubRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}
func (r *stubRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("invalid scan index")
	}
	row := r.rows[r.idx-1]
	return row.scanInto(dest...)
}
func (r *stubRows) Values() ([]any, error) { return nil, errors.New("Values not supported") }
func (r *stubRows) RawValues() [][]byte    { return nil }
func (r *stubRows) Conn() *pgx.Conn        { return nil }

type stubSingleRow struct {
	row stubRow
}

func (r *stubSingleRow) Scan(dest ...any) error {
	return r.row.scanInto(dest...)
}

type stubRow struct {
	values []any
	err    error
}

func (r stubRow) scanInto(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan length mismatch: dest=%d values=%d", len(dest), len(r.values))
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *int64:
			*ptr = r.values[i].(int64)
		case *int:
			*ptr = r.values[i].(int)
		case *string:
			*ptr = r.values[i].(string)
		case *[]byte:
			if r.values[i] == nil {
				*ptr = nil
			} else {
				val := r.values[i].([]byte)
				*ptr = append((*ptr)[:0], val...)
			}
		case *sql.NullString:
			*ptr = r.values[i].(sql.NullString)
		case *sql.NullTime:
			*ptr = r.values[i].(sql.NullTime)
		default:
			rv := reflect.ValueOf(ptr)
			if rv.Kind() != reflect.Pointer || rv.IsNil() {
				return fmt.Errorf("unsupported scan dest %T", d)
			}
			rv.Elem().Set(reflect.ValueOf(r.values[i]))
		}
	}
	return nil
}

type enqueueCall struct {
	sql  string
	args []any
}

type execCall struct {
	sql  string
	args []any
}

type enqueueDBStub struct {
	results []stubRow
	calls   []enqueueCall
}

// Begin implements storage.TestDBRunner.
func (s *enqueueDBStub) Begin(ctx context.Context) (pgx.Tx, error) {
	panic("unimplemented")
}

var _ storage.TestDBRunner = (*enqueueDBStub)(nil)

func (s *enqueueDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("Exec not implemented")
}
func (s *enqueueDBStub) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.calls = append(s.calls, enqueueCall{sql: sql, args: cloneArgs(args)})
	if len(s.results) == 0 {
		return nil, errors.New("unexpected query")
	}
	rowsCopy := make([]stubRow, len(s.results))
	copy(rowsCopy, s.results)
	return &stubRows{rows: rowsCopy}, nil
}
func (s *enqueueDBStub) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	panic("QueryRow not implemented")
}

func cloneArgs(args []any) []any {
	cp := make([]any, len(args))
	copy(cp, args)
	return cp
}

func newStoreWithNow(now func() time.Time) *storage.Store {
	return storage.NewTestStore(storage.TestStoreDependencies{Now: now})
}
