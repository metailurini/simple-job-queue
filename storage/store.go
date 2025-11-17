package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/metailurini/simple-job-queue/apperrors"
	"github.com/metailurini/simple-job-queue/timeprovider"
)

// ErrDuplicateJob indicates the dedupe key rejected a new enqueue request.
var ErrDuplicateJob = errors.New("storage: duplicate job")

// ErrJobNotFound indicates a row with the provided identifier does not exist.
var ErrJobNotFound = errors.New("storage: job not found")

// ErrLeaseMismatch is returned when a worker tries to mutate a job it does not own.
var ErrLeaseMismatch = errors.New("storage: job lease mismatch")

// ErrResourceBusy indicates another worker holds the resource token.
var ErrResourceBusy = errors.New("storage: resource busy")

// Store wraps a pgx connection helper and exposes job-centric helpers.
type Store struct {
	DB                      DB
	now                     func() time.Time
	workerActiveGracePeriod time.Duration
}

// NewStore builds a Store.
func NewStore(db DB, nowFn func() time.Time) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required: %w", apperrors.ErrNotConfigured)
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Store{
		DB:                      db,
		now:                     nowFn,
		workerActiveGracePeriod: 5 * time.Minute,
	}, nil
}

// NewStoreWithProvider builds a Store using the supplied time provider.
func NewStoreWithProvider(pool DB, provider timeprovider.Provider) (*Store, error) {
	if provider == nil {
		provider = timeprovider.RealProvider{}
	}
	return NewStore(pool, provider.Now)
}

// Begin delegates to the underlying DB runner's Begin if it supports it.
func (s *Store) Begin(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
}

// Job represents the public projection returned by storage helpers.
type Job struct {
	ID             int64
	Queue          string
	TaskType       string
	Payload        []byte
	Priority       int
	RunAt          time.Time
	Status         string
	Attempts       int
	MaxAttempts    int
	BackoffSeconds int
	LeaseUntil     *time.Time
	WorkerID       *string
	DedupeKey      *string
	ResourceKey    *string
	OriginJobID    *int64
	TargetWorkerID *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ClaimOptions configures the selection window for ClaimJobs.
type ClaimOptions struct {
	Queue         string
	WorkerID      string
	Limit         int
	LeaseDuration time.Duration
	IncludeLeased bool // set true to steal expired running jobs
	Now           time.Time
}

// ClaimResult wraps the jobs returned by a claim attempt alongside the
// lease expiration timestamp applied to every job in the batch.
type ClaimResult struct {
	Jobs       []Job
	LeaseUntil time.Time
}

// EnqueueParams describes a job insert.
type EnqueueParams struct {
	Queue          string
	TaskType       string
	Payload        []byte
	Priority       int
	RunAt          *time.Time
	MaxAttempts    int
	BackoffSeconds int
	DedupeKey      *string
	ResourceKey    *string
	Broadcast      bool
}

const jobColumns = `
  id,
  queue,
  task_type,
  payload,
  priority,
  run_at,
  status,
  attempts,
  max_attempts,
  backoff_sec,
  lease_until,
  worker_id,
  dedupe_key,
  resource_key,
  origin_job_id,
  target_worker_id,
  created_at,
  updated_at
`

const (
	// claimArg* constants intentionally start at 1 (iota+1) so their values
	// match Postgres positional parameter indexes ($1, $2, ...). This makes
	// it easy to reason about which SQL placeholder each constant represents
	// when reading the SQL below. The args slice used to pass values to pgx
	// is zero-based, so callers subtract 1 when assigning into the slice
	// (e.g. args[claimArgQueue-1] = ...). Starting the constants at 1 also
	// preserves the zero value as an invalid/sentinel value.
	claimArgQueue = iota + 1
	claimArgWorker
	claimArgLimit
	claimArgLeaseUntil
	claimArgNow
	claimArgIncludeLeased
)

const claimSQL = `
WITH candidates AS (
	SELECT id
	FROM queue_jobs
	WHERE queue = $1
		AND (
			(target_worker_id IS NULL AND ((status = 'queued' AND run_at <= $5) OR ($6 AND status = 'running' AND lease_until < $5)))
			OR (target_worker_id = $2 AND status = 'queued' AND run_at <= $5)
		)
	ORDER BY priority DESC, run_at ASC, id ASC
	LIMIT $3
),
updated AS (
	UPDATE queue_jobs j
	SET
		status      = 'running',
		worker_id   = $2,
		lease_until = $4,
		updated_at  = $5
	FROM candidates c
	WHERE j.id = c.id
	RETURNING j.*
)
SELECT` + jobColumns + `
FROM updated
ORDER BY priority DESC, run_at ASC, id ASC;`

const selectJobSQL = "SELECT " + jobColumns + `
FROM queue_jobs
WHERE id = $1;
`

// ClaimJobs atomically selects and leases the next runnable jobs for a queue
// using an optimistic, lock-free update. All returned jobs share the same
// lease expiration timestamp computed from the provided options.
func (s *Store) ClaimJobs(ctx context.Context, opts ClaimOptions) (ClaimResult, error) {
	if opts.Queue == "" {
		return ClaimResult{}, fmt.Errorf("queue is required: %w", apperrors.ErrInvalidArgument)
	}
	if opts.WorkerID == "" {
		return ClaimResult{}, fmt.Errorf("worker id is required: %w", apperrors.ErrInvalidArgument)
	}
	if opts.Limit <= 0 {
		return ClaimResult{}, fmt.Errorf("limit must be > 0: %w", apperrors.ErrInvalidArgument)
	}
	if opts.LeaseDuration <= 0 {
		return ClaimResult{}, fmt.Errorf("lease duration must be positive: %w", apperrors.ErrInvalidArgument)
	}

	nowTS := opts.Now
	if nowTS.IsZero() {
		nowTS = s.now()
	}
	nowTS = nowTS.UTC()
	leaseDuration := (opts.LeaseDuration / time.Second) * time.Second
	leaseUntil := nowTS.Add(leaseDuration)

	args := make([]any, claimArgIncludeLeased)
	args[claimArgQueue-1] = opts.Queue
	args[claimArgWorker-1] = opts.WorkerID
	args[claimArgLimit-1] = opts.Limit
	args[claimArgLeaseUntil-1] = leaseUntil
	args[claimArgNow-1] = nowTS
	args[claimArgIncludeLeased-1] = opts.IncludeLeased

	rows, err := s.DB.QueryContext(ctx, claimSQL, args...)
	if err != nil {
		return ClaimResult{}, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			return ClaimResult{}, scanErr
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return ClaimResult{}, err
	}
	if len(jobs) == 0 {
		return ClaimResult{}, nil
	}
	return ClaimResult{Jobs: jobs, LeaseUntil: leaseUntil}, nil
}

// EnqueueJob inserts a job while honoring the active dedupe constraint.
// It now delegates to EnqueueJobs for single-row convenience.
func (s *Store) EnqueueJob(ctx context.Context, params EnqueueParams) (int64, error) {
	ids, err := s.EnqueueJobs(ctx, []EnqueueParams{params})
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, ErrDuplicateJob
	}
	return ids[0], nil
}

// EnqueueJobs inserts multiple jobs in a single multi-row INSERT while
// honoring the active dedupe constraint. It returns the inserted ids in
// the order returned by PostgreSQL (which includes only successfully
// inserted rows). If none were inserted the returned slice will be empty.
// When a job has Broadcast=true, it creates an origin job and per-worker child jobs.
func (s *Store) EnqueueJobs(ctx context.Context, params []EnqueueParams) ([]int64, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("no params provided: %w", apperrors.ErrInvalidArgument)
	}

	// Separate broadcast from non-broadcast jobs
	var nonBroadcast []EnqueueParams
	var allIDs []int64

	for i := range params {
		if params[i].Broadcast {
			// Process any accumulated non-broadcast jobs first
			if len(nonBroadcast) > 0 {
				ids, err := s.enqueueRegular(ctx, nonBroadcast)
				if err != nil {
					return nil, err
				}
				allIDs = append(allIDs, ids...)
				nonBroadcast = nil
			}
			// Process broadcast job
			originID, err := s.enqueueBroadcast(ctx, params[i])
			if err != nil {
				return nil, err
			}
			allIDs = append(allIDs, originID)
		} else {
			nonBroadcast = append(nonBroadcast, params[i])
		}
	}

	// Process any remaining non-broadcast jobs
	if len(nonBroadcast) > 0 {
		ids, err := s.enqueueRegular(ctx, nonBroadcast)
		if err != nil {
			return nil, err
		}
		allIDs = append(allIDs, ids...)
	}

	return allIDs, nil
}

// enqueueRegular handles non-broadcast job insertion
func (s *Store) enqueueRegular(ctx context.Context, params []EnqueueParams) ([]int64, error) {
	// Normalize parameters and build args
	args := make([]any, 0, len(params)*9)
	for i := range params {
		p := params[i]
		if p.Queue == "" {
			return nil, fmt.Errorf("queue is required: %w", apperrors.ErrInvalidArgument)
		}
		if p.TaskType == "" {
			return nil, fmt.Errorf("task type is required: %w", apperrors.ErrInvalidArgument)
		}
		if p.Payload == nil {
			p.Payload = []byte("{}")
		}
		var runAt time.Time
		if p.RunAt == nil || p.RunAt.IsZero() {
			runAt = s.now()
		} else {
			runAt = *p.RunAt
		}
		runAt = runAt.UTC()
		if p.MaxAttempts == 0 {
			p.MaxAttempts = 20
		}
		if p.BackoffSeconds == 0 {
			p.BackoffSeconds = 10
		}

		args = append(args,
			p.Queue,
			p.TaskType,
			p.Payload,
			p.Priority,
			runAt,
			p.MaxAttempts,
			p.BackoffSeconds,
			p.DedupeKey,
			p.ResourceKey,
		)
	}

	// Build placeholder groups: ($1,$2,...,$9),($10,...)
	var b strings.Builder
	b.WriteString("INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, dedupe_key, resource_key) VALUES ")
	placeCount := 9
	totalRows := len(params)
	for i := range totalRows {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for j := 0; j < placeCount; j++ {
			idx := i*placeCount + j + 1
			if j > 0 {
				b.WriteString(",")
			}
			fmtStr := fmt.Sprintf("$%d", idx)
			b.WriteString(fmtStr)
		}
		b.WriteString(")")
	}
	b.WriteString(" ON CONFLICT DO NOTHING RETURNING id;")

	sql := b.String()

	rows, err := s.DB.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// enqueueBroadcast handles broadcast job insertion:
// 1. Insert origin job in 'queued' status
// 2. Get active workers snapshot
// 3. Insert per-worker child jobs with origin_job_id and target_worker_id
// 4. Mark origin job as 'dispatched'
// Returns the origin job ID
func (s *Store) enqueueBroadcast(ctx context.Context, p EnqueueParams) (int64, error) {
	if p.Queue == "" {
		return 0, fmt.Errorf("queue is required: %w", apperrors.ErrInvalidArgument)
	}
	if p.TaskType == "" {
		return 0, fmt.Errorf("task type is required: %w", apperrors.ErrInvalidArgument)
	}
	if p.Payload == nil {
		p.Payload = []byte("{}")
	}
	var runAt time.Time
	if p.RunAt == nil || p.RunAt.IsZero() {
		runAt = s.now()
	} else {
		runAt = *p.RunAt
	}
	runAt = runAt.UTC()
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 20
	}
	if p.BackoffSeconds == 0 {
		p.BackoffSeconds = 10
	}

	var originID int64
	// Wrap the broadcast flow in a transaction so the origin row is not visible
	// in 'queued' state to claimers until child rows are inserted and the
	// origin is marked 'dispatched' at commit time.
	if err := s.withTx(ctx, func(tx Tx) error {
		// Step 1: Insert origin job (inside tx)
		if err := tx.QueryRowContext(ctx, `
INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, dedupe_key, resource_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;`,
			p.Queue, p.TaskType, p.Payload, p.Priority, runAt, p.MaxAttempts, p.BackoffSeconds, p.DedupeKey, p.ResourceKey,
		).Scan(&originID); err != nil {
			return err
		}

		// Step 2: Get active workers
		// ActiveWorkers reads from the workers table; it's fine to call outside
		// the transaction snapshot since worker liveness is independent of this tx.
		cutoff := s.now().UTC().Add(-s.workerActiveGracePeriod)
		workers, err := s.ActiveWorkers(ctx, cutoff)
		if err != nil {
			return fmt.Errorf("get active workers: %w", err)
		}

		// Step 3: Insert per-worker child jobs (inside tx)
		if len(workers) > 0 {
			args := make([]any, 0, len(workers)*9)
			var b strings.Builder
			b.WriteString("INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, max_attempts, backoff_sec, origin_job_id, target_worker_id) VALUES ")

			for i, workerID := range workers {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9))

				args = append(args, p.Queue, p.TaskType, p.Payload, p.Priority, runAt, p.MaxAttempts, p.BackoffSeconds, originID, workerID)
			}
			b.WriteString(";")

			if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
				return fmt.Errorf("bulk insert child jobs: %w", err)
			}
		}

		// Step 4: Mark origin job as dispatched (inside tx)
		nowTS := s.now().UTC()
		if _, err := tx.ExecContext(ctx, `
UPDATE queue_jobs
SET status = 'dispatched', updated_at = $2
WHERE id = $1;`, originID, nowTS); err != nil {
			return fmt.Errorf("mark origin dispatched: %w", err)
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return originID, nil
}

// GetJob loads a job by identifier.
func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	row := s.DB.QueryRowContext(ctx, selectJobSQL, id)
	job, err := scanJob(row)
	if err != nil {
		if IsNoRows(err) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, err
	}
	return job, nil
}

// ScheduleRow represents a row from the queue_schedules table.
type ScheduleRow struct {
	ID             int64
	TaskType       string
	Queue          string
	Payload        []byte
	Cron           string
	DedupeKey      *string
	LastEnqueuedAt *time.Time
}

type scanner interface {
	Scan(dest ...any) error
}

func ScanSchedule(row scanner) (ScheduleRow, error) {
	var (
		s         ScheduleRow
		payload   []byte
		dedupeKey sql.NullString
		lastRun   sql.NullTime
	)
	if err := row.Scan(&s.ID, &s.TaskType, &s.Queue, &payload, &s.Cron, &dedupeKey, &lastRun); err != nil {
		return ScheduleRow{}, err
	}
	// Copy the scanned payload into a new slice so the returned ScheduleRow
	// owns its data. Database drivers (including pgx) may reuse internal
	// buffers while iterating rows; assigning the driver-provided slice
	// directly would let that backing memory be mutated later. Make an
	// explicit copy to avoid subtle aliasing bugs.
	s.Payload = make([]byte, len(payload))
	copy(s.Payload, payload)
	if dedupeKey.Valid {
		val := dedupeKey.String
		s.DedupeKey = &val
	}
	if lastRun.Valid {
		t := lastRun.Time
		s.LastEnqueuedAt = &t
	}
	return s, nil
}

// FetchSchedulesTx selects schedule rows inside the provided transaction
// without locking the rows and returns the scanned rows.
// Individual row scan failures are skipped to allow the scheduler to
// continue processing other schedules in the same transaction.
func (s *Store) FetchSchedulesTx(ctx context.Context, tx Tx) ([]ScheduleRow, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT id, task_type, queue, payload, cron, dedupe_key, last_enqueued_at
FROM queue_schedules;
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ScheduleRow
	for rows.Next() {
		r, err := ScanSchedule(rows)
		if err != nil {
			// Skip rows that fail to scan (corrupt data) and continue
			// The scheduler's tests expect a single bad row not to abort
			// the entire selection. Ignore and continue.
			continue
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// CompleteJob marks a job as succeeded and clears the lease/worker
// ownership. It enforces that the caller owns the job by matching
// the provided worker id; otherwise ErrLeaseMismatch is returned.
func (s *Store) CompleteJob(ctx context.Context, id int64, workerID string) error {
	nowTS := s.now().UTC()

	res, err := s.DB.ExecContext(ctx, `
UPDATE queue_jobs
SET status='succeeded',
    worker_id=NULL,
    lease_until=NULL,
    updated_at=$3
WHERE id=$1 AND worker_id=$2;`, id, workerID, nowTS)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// HeartbeatJob extends the lease for a running job to prevent premature requeue.
func (s *Store) HeartbeatJob(ctx context.Context, id int64, workerID string, extend time.Duration) error {
	if extend <= 0 {
		return fmt.Errorf("extend duration must be positive: %w", apperrors.ErrInvalidArgument)
	}
	nowTS := s.now().UTC()
	extendDur := (extend / time.Second) * time.Second
	leaseUntil := nowTS.Add(extendDur)

	res, err := s.DB.ExecContext(ctx, `
UPDATE queue_jobs
SET lease_until = $3,
    updated_at  = $4
WHERE id=$1 AND worker_id=$2;`, id, workerID, leaseUntil, nowTS)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// RequeueJob releases the current lease and places the job back into the queued
// state so another worker can attempt it after the provided timestamp.
func (s *Store) RequeueJob(ctx context.Context, id int64, workerID string, runAt time.Time) error {
	nowTS := s.now().UTC()
	if runAt.IsZero() {
		runAt = nowTS
	}
	runAt = runAt.UTC()
	res, err := s.DB.ExecContext(ctx, `
UPDATE queue_jobs
SET status='queued',
    run_at=$3,
    worker_id=NULL,
    lease_until=NULL,
    updated_at=$4
WHERE id=$1 AND worker_id=$2;`, id, workerID, runAt, nowTS)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return ErrLeaseMismatch
	}
	return nil
}

// RescheduleJob places the job back into the queued state without incrementing
// the attempts counter. This is used for transient, non-failure conditions
// like resource contention.
func (s *Store) RescheduleJob(ctx context.Context, id int64, runAt time.Time) error {
	nowTS := s.now().UTC()
	if runAt.IsZero() {
		runAt = nowTS
	}
	runAt = runAt.UTC()
	_, err := s.DB.ExecContext(ctx, `
UPDATE queue_jobs
SET status      = 'queued',
    run_at      = $2,
    worker_id   = NULL,
    lease_until = NULL,
    updated_at  = $3
WHERE id = $1;`, id, runAt, nowTS)
	if err != nil {
		return fmt.Errorf("reschedule job: %w", err)
	}
	return nil
}

// FailJob records a job failure, optionally scheduling a retry or marking the job dead.
// The method inserts a row into job_failures for inspection and enforces max_attempts.
// When the job exhausts its attempts the return value "dead" is true.
func (s *Store) FailJob(ctx context.Context, id int64, workerID string, nextRun time.Time, errText string) (dead bool, err error) {
	nowTS := s.now().UTC()
	if nextRun.IsZero() {
		nextRun = nowTS
	}
	nextRun = nextRun.UTC()
	if errText == "" {
		errText = "unknown error"
	}

	var (
		attempts int
		status   string
	)

	execErr := s.withTx(ctx, func(tx Tx) error {
		row := tx.QueryRowContext(ctx, `
UPDATE queue_jobs
SET attempts = attempts + 1,
    status = CASE WHEN attempts + 1 >= max_attempts THEN 'dead' ELSE 'queued' END,
    run_at = CASE WHEN attempts + 1 >= max_attempts THEN run_at ELSE $3 END,
    worker_id=NULL,
    lease_until=NULL,
    updated_at=$4
WHERE id=$1 AND worker_id=$2
RETURNING attempts, status;`, id, workerID, nextRun, nowTS)

		if err := row.Scan(&attempts, &status); err != nil {
			if IsNoRows(err) {
				return ErrLeaseMismatch
			}
			return err
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO queue_job_failures (job_id, error, attempts, failed_at)
VALUES ($1, $2, $3, $4);`, id, errText, attempts, nowTS); err != nil {
			return err
		}

		return nil
	})
	if execErr != nil {
		return false, execErr
	}

	return status == "dead", nil
}

// AcquireResource attempts to claim ownership of a resource token.
// Returns nil on success, ErrResourceBusy if another worker holds it,
// or another error on execution failure.
func (s *Store) AcquireResource(ctx context.Context, resourceKey string, jobID int64, workerID string) error {
	if resourceKey == "" {
		return nil
	}
	nowTS := s.now().UTC()
	res, err := s.DB.ExecContext(ctx, `
INSERT INTO queue_resource_locks (resource_key, job_id, worker_id, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_key) DO NOTHING;`, resourceKey, jobID, workerID, nowTS)
	if err != nil {
		return fmt.Errorf("acquire resource: %w", err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return ErrResourceBusy
	}
	return nil
}

// ReleaseResource deletes the ownership token for the given resource.
// Any database error during deletion is returned to the caller.
func (s *Store) ReleaseResource(ctx context.Context, resourceKey string, jobID int64) error {
	if resourceKey == "" {
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `
DELETE FROM queue_resource_locks
WHERE resource_key = $1 AND job_id = $2;`, resourceKey, jobID)
	return err
}

// RegisterWorker inserts or updates a worker in the registry with the current timestamp.
// This method should be called at worker startup to register the worker.
func (s *Store) RegisterWorker(ctx context.Context, workerID string, meta []byte) error {
	if workerID == "" {
		return fmt.Errorf("worker id is required: %w", apperrors.ErrInvalidArgument)
	}
	nowTS := s.now().UTC()
	_, err := s.DB.ExecContext(ctx, `
INSERT INTO queue_workers (worker_id, meta, last_seen)
VALUES ($1, $2, $3)
ON CONFLICT (worker_id) DO UPDATE
SET meta = EXCLUDED.meta, last_seen = EXCLUDED.last_seen;`, workerID, meta, nowTS)
	return err
}

// HeartbeatWorker updates the last_seen timestamp for a worker to indicate it is still active.
// This method should be called periodically by workers to maintain their active status.
func (s *Store) HeartbeatWorker(ctx context.Context, workerID string) error {
	if workerID == "" {
		return fmt.Errorf("worker id is required: %w", apperrors.ErrInvalidArgument)
	}
	nowTS := s.now().UTC()
	_, err := s.DB.ExecContext(ctx, `
UPDATE queue_workers
SET last_seen = $2
WHERE worker_id = $1;`, workerID, nowTS)
	return err
}

// ActiveWorkers returns a list of worker IDs that have a last_seen timestamp
// at or after the provided cutoff time. This is used to determine which workers
// should receive broadcast jobs.
func (s *Store) ActiveWorkers(ctx context.Context, cutoff time.Time) ([]string, error) {
	cutoffUTC := cutoff.UTC()
	rows, err := s.DB.QueryContext(ctx, `
SELECT worker_id
FROM queue_workers
WHERE last_seen >= $1
ORDER BY worker_id;`, cutoffUTC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []string
	for rows.Next() {
		var workerID string
		if err := rows.Scan(&workerID); err != nil {
			return nil, err
		}
		workers = append(workers, workerID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workers, nil
}

func (s *Store) withTx(ctx context.Context, fn func(Tx) error) (err error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	panicked := true
	defer func() {
		if panicked || err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	err = fn(tx)
	panicked = false
	return err
}

func scanJob(row scanner) (Job, error) {
	var (
		job            Job
		leaseUntil     sql.NullTime
		workerID       sql.NullString
		dedupeKey      sql.NullString
		resourceKey    sql.NullString
		originJobID    sql.NullInt64
		targetWorkerID sql.NullString
	)

	err := row.Scan(
		&job.ID,
		&job.Queue,
		&job.TaskType,
		&job.Payload,
		&job.Priority,
		&job.RunAt,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.BackoffSeconds,
		&leaseUntil,
		&workerID,
		&dedupeKey,
		&resourceKey,
		&originJobID,
		&targetWorkerID,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return Job{}, err
	}

	if leaseUntil.Valid {
		t := leaseUntil.Time
		job.LeaseUntil = &t
	}
	if workerID.Valid {
		val := workerID.String
		job.WorkerID = &val
	}
	if dedupeKey.Valid {
		val := dedupeKey.String
		job.DedupeKey = &val
	}
	if resourceKey.Valid {
		val := resourceKey.String
		job.ResourceKey = &val
	}
	if originJobID.Valid {
		val := originJobID.Int64
		job.OriginJobID = &val
	}
	if targetWorkerID.Valid {
		val := targetWorkerID.String
		job.TargetWorkerID = &val
	}

	return job, nil
}
