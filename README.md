# simple-job-queue

jobqueue is a Postgres-backed job queue and worker framework implemented in Go. It is designed for reliability, observability, and testability and is focused on the core responsibilities needed to run background work in production.

Functionalities

- Durable job persistence and reliable retries: jobs are stored durably and can be retried with configurable backoff and failure tracking.
- Lease-based job claiming: workers acquire leases and use advisory-lock semantics to ensure a job is processed by a single worker at a time.
- Worker lifecycle management: heartbeats, lease renewal, and graceful shutdown handling to minimize duplicate work and support clean restarts.
- Scheduling support: a scheduler component enables cron-like or scheduled jobs alongside ad-hoc queue processing.
- Concurrency and batching controls: runtime knobs to tune parallelism, batch sizes, and lease durations for different workloads.
- Extensible storage layer: database migrations and a storage abstraction make it straightforward to adapt or extend persistence behavior.
- Test-first storage testing: sqlmock-backed tests for the storage layer to assert SQL text, bind order, and timestamp normalization without requiring a live database.
- Clear specifications and documentation: design docs and specs capture expected behaviors, operational considerations, and upgrade paths.

Intended uses

- Background job processing for web services and data pipelines.
- Scheduled task runners and cron replacement where reliability and observability matter.
- As a reference implementation for building Postgres-backed worker systems with strong testability guarantees.

Project notes

The project emphasizes small, focused binaries and packages so operators can run only the components they need. Storage and worker behavior is intentionally explicit to make testing and auditing straightforward.

Migrations

Database migrations live in `db/migrations` and are applied automatically at worker startup by default. You can disable this with `--auto-migrate=false` if you prefer to run migrations as an explicit deployment step. The worker uses embedded SQL migrations, so no external tooling is required at runtime.
