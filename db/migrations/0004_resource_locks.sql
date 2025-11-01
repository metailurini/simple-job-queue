-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS queue_resource_locks (
  resource_key TEXT PRIMARY KEY,
  job_id       BIGINT NOT NULL,
  worker_id    TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for janitor cleanup queries (age-based deletion)
CREATE INDEX IF NOT EXISTS queue_resource_locks_created_at_idx
  ON queue_resource_locks(created_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS queue_resource_locks_created_at_idx;
DROP TABLE IF EXISTS queue_resource_locks;
-- +goose StatementEnd
