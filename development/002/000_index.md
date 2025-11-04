## Goals

- Make the worker avoid incrementing a job's "attempts"/retry counter when the only reason for postponement is that a resource key is busy. Instead, the worker should reschedule the job (update run_at/queued state) while preserving the attempts count.
- Keep behaviour unchanged for true execution failures: they should still increment attempts or move to failed as today.
- Add a clear, testable code path and DB operation (`RescheduleJob`) to support the above.

## Context

This repository implements a simple job queue with a Postgres-backed store. Currently when a worker tries to acquire a resource key for a job and the store responds with `ErrResourceBusy`, the worker follows the same requeue/failure path used for other transient errors — which increments the job's attempt counter. For use-cases where resource contention is expected (e.g., only one job may hold a DB-level lock on a given resource key at a time), we want contention to only postpone the job (bump `run_at`) without counting as an attempt.

Key files and packages to inspect and update:
- `worker/worker.go` — runner and job execution path; where `AcquireResource` is called and where requeue/fail logic lives.
- `storage/store.go` (or DB-backed store file) — add `RescheduleJob(ctx,id,runAt)` that updates `run_at`, `state`, clears lease fields, but does not touch attempts.
- `worker/*_test.go` — update unit tests and test doubles (mocks) to include `RescheduleJob` and assert on it when resource is busy.

Repository conventions and constraints (from AGENTS.md):
- Use snake_case for SQL columns.
- Add unit tests for all new behavior with sqlmock-backed tests for storage.
- Target Go 1.25; run `go fmt` and `go test ./...` before merging.

## Implementation Steps

Each step below is detailed, actionable, and includes rationale, example diffs/snippets, rollout notes, and a Complexity score.

1) Add `RescheduleJob` to the store interface (Complexity: 2)

Rationale: introduce a minimal store contract that expresses "postpone job without touching attempts." Keep it intentionally small to limit DB-level risk.

Code (illustrative diff):

*** Update File: /Users/shanenoi/workspace/jobqueue/worker/worker.go

@@
 type jobStore interface {
		 ClaimJobs(ctx context.Context, opts storage.ClaimOptions) (storage.ClaimResult, error)
		 HeartbeatJob(ctx context.Context, id int64, workerID string, extend time.Duration) error
		 RequeueJob(ctx context.Context, id int64, workerID string, runAt time.Time) error
		 FailJob(ctx context.Context, id int64, workerID string, nextRun time.Time, errText string) (bool, error)
		 CompleteJob(ctx context.Context, id int64, workerID string) error
		 AcquireResource(ctx context.Context, resourceKey string, jobID int64, workerID string) error
		 ReleaseResource(ctx context.Context, resourceKey string, jobID int64) error
+    // Reschedule without incrementing attempts
+    RescheduleJob(ctx context.Context, id int64, runAt time.Time) error
 }

Rollout notes: This is a non-breaking additive change to an internal interface; follow up by implementing it on production store.

2) Implement `RescheduleJob` in the DB-backed store (Complexity: 4)

Rationale: Concrete DB operation to set `run_at` and return job to `queued` state while clearing lease/worker fields. Avoid touching `attempts`, `last_error`, or `failed_at`.

Illustrative SQL snippet:

```sql
UPDATE jobs
SET run_at = $1,
		state = 'queued',
		lease_until = NULL,
		worker_id = NULL
WHERE id = $2;
```

Diff (conceptual change in `storage/store.go`):

*** Update File: /Users/shanenoi/workspace/jobqueue/storage/store.go

@@
 func (s *Store) RescheduleJob(ctx context.Context, id int64, runAt time.Time) error {
-    // new impl
+    _, err := s.DB.ExecContext(ctx, `
+UPDATE jobs
+SET run_at = $1,
+    state = 'queued',
+    lease_until = NULL,
+    worker_id = NULL
+WHERE id = $2;`, runAt.UTC(), id)
+    if err != nil {
+        return fmt.Errorf("reschedule job: %w", err)
+    }
+    return nil
 }

Rollout notes: Add unit tests for this DB method using `sqlmock` per the project's testing conventions (see `storage/storagetest`). Validate SQL text, parameter order, and UTC normalization.

3) Update worker execution flow to call `RescheduleJob` for resource-busy (Complexity: 3)

Rationale: Only change the path that handles ErrResourceBusy from the store. Preserve existing behavior for other errors.

Conceptual diff in `worker/worker.go` inside job execution/AcquireResource handling:

@@
-    if err := r.store.AcquireResource(storeCtx, *job.ResourceKey, job.ID, r.cfg.WorkerID); err != nil {
-        if errors.Is(err, storage.ErrResourceBusy) {
-            // existing code currently does a Requeue which increments attempts
-            r.store.RequeueJob(storeCtx, job.ID, r.cfg.WorkerID, nextRun)
-            return
-        }
+    if err := r.store.AcquireResource(storeCtx, *job.ResourceKey, job.ID, r.cfg.WorkerID); err != nil {
+        if errors.Is(err, storage.ErrResourceBusy) {
+            // New behaviour: reschedule without incrementing attempts
+            nextRun := r.now().Add(r.cfg.ResourceBusyRequeueDelay)
+            if err := r.store.RescheduleJob(storeCtx, job.ID, nextRun); err != nil {
+                r.logger.Error("reschedule failed", "job_id", job.ID, "err", err)
+            }
+            return
+        }

Rollout notes: Add a short configurable delay `ResourceBusyRequeueDelay` to the Runner config (default e.g., 1s-5s) and ensure store operation timeouts are used as elsewhere.

4) Update unit tests and mocks to exercise and assert on reschedule (Complexity: 3)

Rationale: Tests must ensure the new path is taken and `attempts` is unchanged.

Illustrative mock update (in `worker` tests):

```go
type mockJobStore struct { ... 
		rescheduleCalls int32
}

func (m *mockJobStore) RescheduleJob(ctx context.Context, id int64, runAt time.Time) error {
		atomic.AddInt32(&m.rescheduleCalls, 1)
		return m.rescheduleErr
}
```

Test assertion snippet:

```go
// When AcquireResource returns ErrResourceBusy
assert.Equal(t, int32(1), atomic.LoadInt32(&mockStore.rescheduleCalls))
// attempts count should be unchanged (e.g. still 0)
jobFromStore := mockStore.lastQueriedJob
assert.Equal(t, 0, jobFromStore.Attempts)
```

Rollout notes: Update storage sqlmock tests to assert that `RescheduleJob` SQL matches expectations; update worker tests that previously asserted `RequeueJob` was called.

5) Backwards compatibility & migration plan (Complexity: 2)

Rationale: The change is additive on the interface and opt-out-free. Older binaries won't call the new method; new binaries will call it but SQL is straightforward.

Rollout notes:
- Deploy worker changes first (new workers will prefer RescheduleJob when resource busy). No DB schema migration required.
- Monitor logs for reschedule errors (e.g., permission problems), and have a rollback by re-deploying previous worker if needed.

6) Observability & metrics (Complexity: 4)

Rationale: Add a metric and logs for the reschedule path to ensure it's used and to debug issues.

Code snippet:

```go
r.metrics.Inc("jobs.rescheduled.resource_busy")
r.logger.Info("job rescheduled due to resource busy", "job_id", job.ID)
```

Rollout notes: Add metrics to monitoring dashboards; alert if reschedules spike (could indicate resource starvation or misconfiguration).

## Pitfalls & Validation

Known risks and mitigations:

- Risk: Mistakenly leaving other retry semantics untouched (we must only skip attempt increment for resource-busy).
	- Mitigation: Keep `RescheduleJob` focused to only update run_at/lease/worker; do not touch `attempts` or `last_error`.
	- Validation: Add unit tests that check `attempts` value before/after reschedule.

- Risk: Race conditions where two workers both think the resource is free and both proceed.
	- Mitigation: `AcquireResource` must remain the single source-of-truth; `RescheduleJob` only postpones claiming. Keep lease semantics unchanged.
	- Validation: Integration tests with two parallel workers in a real Postgres instance asserting only one proceeds.

- Risk: DB permissions or SQL mismatch causing `RescheduleJob` to fail and fallback to old behaviour.
	- Mitigation: Implement sqlmock unit tests asserting exact SQL and parameter order. Log and metric on `RescheduleJob` failures.
	- Validation: Run `go test ./...` including `storage` sqlmock tests.

Suggested tests (concrete):

Unit tests (fast, table-driven):

1) TestWorker_ResourceBusy_ReschedulesWithoutIncrement
- Setup: mock store where `AcquireResource` returns `ErrResourceBusy`.
- Expect: `RescheduleJob` called once; handler not called; job attempts unchanged.
- Guards: prevents accidental attempt increments on resource busy.

2) TestStore_RescheduleJob_SQL
- Setup: sqlmock expecting the exact `UPDATE jobs SET run_at = $1, state = 'queued', lease_until = NULL, worker_id = NULL WHERE id = $2;` with UTC-normalized time param.
- Expect: Exec called once with correct args.
- Guards: ensures SQL correctness and order of binds.

3) TestWorker_OtherErrors_StillRequeueOrFail
- Setup: mock store where `AcquireResource` returns a different error (e.g., DB connectivity). The existing `RequeueJob` or `FailJob` path should be used and attempts increment accordingly.
- Expect: `RequeueJob` called and attempts incremented as before.
- Guards: ensures we didn't accidentally change other error handling.

Integration tests (requires Postgres):

1) TwoWorkers_ResourceContention_OnlyOneExecutes
- Setup: two real worker processes claim the same queued job resource keys; first acquires resource and runs; second receives resource-busy and is rescheduled (attempts unchanged).
- Expect: second job's attempts field unchanged after reschedule; job eventually runs once resource freed.
- Guards: real-world race-safety and DB semantics.

How each test prevents failure:
- Unit tests assert the behaviour at the call boundary so we know the worker logic chooses RescheduleJob vs RequeueJob.
- SQL mocks lock the exact DB contract and protect against accidental schema/param changes.
- Integration tests exercise timing and real-locking races that unit tests can't.

Final rollout checklist

- [ ] Implement code & tests in a feature branch
- [ ] Run `gofmt` and `go vet` and `go test ./...` (including sqlmock tests)
- [ ] Add metric for reschedules and instrument logging
- [ ] Deploy worker binaries, monitor reschedule metric and errors
- [ ] If any `RescheduleJob` errors occur, investigate and roll back if necessary

References

- Repo guidelines: AGENTS.md (testing/storage/sqlmock conventions)
- Files to edit: `worker/worker.go`, `storage/store.go`, `worker/*_test.go`, `storage/storagetest` helpers

