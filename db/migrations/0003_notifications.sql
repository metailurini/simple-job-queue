-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION notify_job_available() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('queue_jobs_' || NEW.queue, '');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS queue_jobs_notify ON queue_jobs;
CREATE TRIGGER queue_jobs_notify
AFTER INSERT ON queue_jobs
FOR EACH ROW
EXECUTE FUNCTION notify_job_available();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS queue_jobs_notify ON queue_jobs;
DROP FUNCTION IF EXISTS notify_job_available();
-- +goose StatementEnd
