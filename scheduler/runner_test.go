package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store, err := storage.NewStore(db, func() time.Time { return time.Unix(0, 0) })
	require.NoError(t, err)

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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store, err := storage.NewStore(db, time.Now)
	require.NoError(t, err)

	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Nanosecond,
		catchUpMax: 1,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	mock.ExpectBegin()
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := runner.Run(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTickEnqueuesDueRunsAndUpdatesSchedule(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	nowLoc := time.FixedZone("America/Los_Angeles", -8*3600)
	initialLast := time.Date(2024, 11, 5, 7, 0, 0, 0, nowLoc)
	runNow := time.Date(2024, 11, 5, 7, 2, 30, 0, nowLoc)

	store, err := storage.NewStore(db, func() time.Time { return runNow })
	require.NoError(t, err)

	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 5,
		now: func() time.Time {
			return runNow
		},
	}
	rows := sqlmock.NewRows([]string{"id", "task_type", "queue", "payload", "cron", "dedupe_key", "last_enqueued_at"}).
		AddRow(int64(10), "send-email", "critical", []byte(`{"foo":"bar"}`), "* * * * *", sql.NullString{Valid: true, String: "dedupe"}, sql.NullTime{Valid: true, Time: initialLast})

	mock.ExpectBegin()
	mock.ExpectQuery(`
SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at
FROM queue_schedules;
`).WillReturnRows(rows)

	enqueueTimes := []time.Time{
		time.Date(2024, 11, 5, 7, 1, 0, 0, nowLoc).UTC(),
		time.Date(2024, 11, 5, 7, 2, 0, 0, nowLoc).UTC(),
	}
	expectedLast := enqueueTimes[len(enqueueTimes)-1]

	mock.ExpectExec("UPDATE queue_schedules SET last_enqueued_at=\\$2 WHERE id=\\$1 AND \\(last_enqueued_at IS NOT DISTINCT FROM \\$3\\)").
		WithArgs(int64(10), expectedLast, initialLast).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery("INSERT INTO queue_jobs").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(2)))
	mock.ExpectCommit()

	err = runner.tick(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTickSkipsWhenScheduleAlreadyClaimed(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	now := time.Date(2024, 11, 5, 7, 5, 0, 0, time.UTC)
	store, err := storage.NewStore(db, func() time.Time { return now })
	require.NoError(t, err)
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 5,
		now: func() time.Time {
			return now
		},
	}
	rows := sqlmock.NewRows([]string{"id", "task_type", "queue", "payload", "cron", "dedupe_key", "last_enqueued_at"}).
		AddRow(int64(42), "send-email", "default", []byte(`{"foo":true}`), "* * * * *", sql.NullString{}, sql.NullTime{})
	mock.ExpectBegin()
	mock.ExpectQuery(`
SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at
FROM queue_schedules;
`).WillReturnRows(rows)
	mock.ExpectExec("UPDATE queue_schedules").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	err = runner.tick(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTickRevertsScheduleWhenEnqueueFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Date(2024, 11, 5, 9, 0, 0, 0, time.UTC)
	store, err := storage.NewStore(db, func() time.Time { return now })
	require.NoError(t, err)

	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 2,
		now: func() time.Time {
			return now
		},
	}

	rows := sqlmock.NewRows([]string{"id", "task_type", "queue", "payload", "cron", "dedupe_key", "last_enqueued_at"}).
		AddRow(int64(51), "sync-data", "critical", []byte(`{"bar":true}`), "* * * * *", sql.NullString{}, sql.NullTime{})
	mock.ExpectBegin()
	mock.ExpectQuery(`
SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at
FROM queue_schedules;
`).WillReturnRows(rows)
	mock.ExpectExec("UPDATE queue_schedules").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("INSERT INTO queue_jobs").WillReturnError(errors.New("insert boom"))
	mock.ExpectExec("UPDATE queue_schedules").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = runner.tick(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTickReturnsErrorWhenQueryFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store, err := storage.NewStore(db, time.Now)
	require.NoError(t, err)

	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 1,
		now:        time.Now,
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at").WillReturnError(errors.New("boom"))
	mock.ExpectRollback()

	err = runner.tick(context.Background())
	assert.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTickContinuesAfterScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	validRun := time.Date(2024, 2, 2, 10, 0, 0, 0, time.UTC)
	store, err := storage.NewStore(db, func() time.Time { return validRun })
	require.NoError(t, err)
	runner := &Runner{
		store:      store,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval:   time.Second,
		catchUpMax: 1,
		now: func() time.Time {
			return validRun
		},
	}
	rows := sqlmock.NewRows([]string{"id"}).AddRow("not-a-number")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at").WillReturnRows(rows)
	mock.ExpectCommit()
	err = runner.tick(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// func TestScanScheduleCopiesPayloadAndSetsPointers(t *testing.T) {
// 	payload := []byte{1, 2, 3}
// 	dedupe := sql.NullString{Valid: true, String: "dk"}
// 	last := sql.NullTime{Valid: true, Time: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}

// 	rows := sqlmock.NewRows([]string{"id", "task_type", "queue", "payload", "cron", "dedupe_key", "last_enqueued_at"}).
// 		AddRow(int64(5), "send", "queue", payload, "*/5 * * * *", dedupe, last)
// 	rows.Next()
// 	schedule, err := storage.ScanSchedule(rows)
// 	require.NoError(t, err)

// 	assert.Equal(t, int64(5), schedule.ID)
// 	assert.Equal(t, "send", schedule.TaskType)
// 	assert.Equal(t, "queue", schedule.Queue)

// 	payload[0] = 9
// 	expectedPayload := []byte{1, 2, 3}
// 	assert.Equal(t, expectedPayload, schedule.Payload, "expected payload copy")
// 	require.NotNil(t, schedule.DedupeKey, "expected dedupe key to be set")
// 	assert.Equal(t, "dk", *schedule.DedupeKey)
// 	require.NotNil(t, schedule.LastEnqueuedAt, "expected last enqueued at to be set")
// 	assert.True(t, schedule.LastEnqueuedAt.Equal(last.Time))
// }

func TestEnqueueRunsBuildsParamsAndSkipsDuplicates(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	now := time.Date(2024, 5, 6, 7, 0, 0, 0, time.UTC)
	store, err := storage.NewStore(db, func() time.Time { return now })
	require.NoError(t, err)

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

	mock.ExpectQuery("INSERT INTO queue_jobs").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).AddRow(int64(3)))

	err = runner.enqueueRuns(context.Background(), schedule, runTimes)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
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
