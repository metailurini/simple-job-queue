-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS queue_job_failures (
    id         BIGSERIAL PRIMARY KEY,
    job_id     BIGINT      NOT NULL REFERENCES queue_jobs(id) ON DELETE CASCADE,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    error      TEXT        NOT NULL,
    attempts   INT         NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_schedules (
    id               BIGSERIAL PRIMARY KEY,
    task_type        TEXT        NOT NULL,
    queue            TEXT        NOT NULL DEFAULT 'default',
    payload          JSONB       NOT NULL,
    cron             TEXT        NOT NULL,
    dedupe_key       TEXT,
    last_enqueued_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS queue_schedules_task_queue_uniq
  ON queue_schedules (task_type, queue);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS queue_schedules_task_queue_uniq;
DROP TABLE IF EXISTS queue_schedules;
DROP TABLE IF EXISTS queue_job_failures;
-- +goose StatementEnd
