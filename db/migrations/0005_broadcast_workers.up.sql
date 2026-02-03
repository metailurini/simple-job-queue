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

ALTER TABLE queue_schedules
  ADD COLUMN broadcast BOOLEAN NOT NULL DEFAULT FALSE;
