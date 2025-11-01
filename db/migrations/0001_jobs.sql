-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS queue_jobs (
    id            BIGSERIAL PRIMARY KEY,
    queue         TEXT        NOT NULL,
    task_type     TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    priority      INT         NOT NULL DEFAULT 0,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT        NOT NULL DEFAULT 'queued',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 20,
    backoff_sec   INT         NOT NULL DEFAULT 10,
    lease_until   TIMESTAMPTZ,
    worker_id     TEXT,
    dedupe_key    TEXT,
    resource_key  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queue_jobs_queue_sched_idx
  ON queue_jobs (queue, status, run_at, priority DESC);

CREATE INDEX IF NOT EXISTS queue_jobs_requeue_idx
  ON queue_jobs (status, lease_until);

CREATE UNIQUE INDEX IF NOT EXISTS queue_jobs_dedupe_active_uniq
  ON queue_jobs (queue, task_type, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS queue_jobs_dedupe_active_uniq;
DROP INDEX IF EXISTS queue_jobs_requeue_idx;
DROP INDEX IF EXISTS queue_jobs_queue_sched_idx;
DROP TABLE IF EXISTS queue_jobs;
-- +goose StatementEnd
