# Seed Data

The SQL snippets in this directory are optional helpers to bootstrap lower
environments with representative queues, schedules, and demo jobs. Apply them
after running the migrations, e.g.

```sh
psql "$DATABASE_URL" -f db/migrations/0001_jobs.sql
psql "$DATABASE_URL" -f db/migrations/0002_failures_and_schedules.sql
psql "$DATABASE_URL" -f db/seeds/sample_data.sql
```

The provided sample inserts a recurring heartbeat job and an example ad-hoc
payload so workers can be validated without crafting custom SQL.
