INSERT INTO queue_schedules (task_type, queue, payload, cron, dedupe_key, last_enqueued_at)
VALUES (
    'demo:heartbeat',
    'default',
    '{}'::jsonb,
    '*/5 * * * *',
    'demo:heartbeat',
    NULL
)
ON CONFLICT ON CONSTRAINT queue_schedules_task_queue_uniq
DO UPDATE SET
    cron = EXCLUDED.cron,
    payload = EXCLUDED.payload,
    dedupe_key = EXCLUDED.dedupe_key;

INSERT INTO queue_jobs (queue, task_type, payload, priority, run_at, backoff_sec, dedupe_key, resource_key)
VALUES (
    'default',
    'demo:print',
    jsonb_build_object('message', 'hello jobqueue'),
    10,
    now(),
    5,
    'demo:print:hello',
    NULL
)
ON CONFLICT ON CONSTRAINT queue_jobs_dedupe_active_uniq DO NOTHING;
