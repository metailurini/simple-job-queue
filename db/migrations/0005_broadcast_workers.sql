-- +goose Up
-- +goose StatementBegin
ALTER TABLE queue_jobs
  ADD COLUMN origin_job_id BIGINT REFERENCES queue_jobs(id),
  ADD COLUMN target_worker_id TEXT;

CREATE TABLE IF NOT EXISTS queue_workers (
  worker_id TEXT PRIMARY KEY,
  meta JSONB,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queue_workers_last_seen_idx ON queue_workers (last_seen);
CREATE INDEX IF NOT EXISTS queue_jobs_target_worker_idx ON queue_jobs (target_worker_id, status, run_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS queue_jobs_target_worker_idx;
DROP INDEX IF EXISTS queue_workers_last_seen_idx;
DROP TABLE IF EXISTS queue_workers;
ALTER TABLE queue_jobs
  DROP COLUMN IF EXISTS target_worker_id,
  DROP COLUMN IF EXISTS origin_job_id;
-- +goose StatementEnd
