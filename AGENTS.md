# Repository Guidelines

## Project Structure & Module Organization
`cmd/jobworker` holds the runnable worker entrypoint; use it as the template for any additional binaries. Shared logic lives under `storage` (Postgres data access) and `worker` (queue orchestration, advisory locks, heartbeats). SQL migrations and seeds are versioned in `db/migrations` and `db/seeds`, and high-level behavior specs are documented in `specs.md`. Keep new packages inside `…` unless they must be imported by external repos.

## Build, Test, and Development Commands
- `go run ./cmd/jobworker --dsn $DATABASE_URL --queue default` spins up the worker locally; pass `--batch`, `--lease`, or `--concurrency` to mirror production knobs.
- `go build ./cmd/jobworker` verifies the binary compiles with the current module graph.
- `go test ./...` runs all package tests (add `-run TestName` for focused debugging).
- `go fmt ./... && go vet ./...` is the minimum pre-commit hygiene; run `golangci-lint run` if you introduce the config.

## Coding Style & Naming Conventions
Target Go 1.25 features only. Always `gofmt` before committing; keep imports grouped stdlib/third-party/internal. Prefer explicit struct literals and context-aware logger keys (see `cmd/jobworker/main.go`). Package names stay lowercase, short, and match their directory (e.g., `storage`, not `Storage`). Public types/functions should read naturally from other packages, while helpers that should not leak stay unexported. Use `snake_case` for SQL columns to match the existing migrations.

## Testing Guidelines
- All new code and behavior changes MUST include unit tests. Tests should live alongside the package under test in `_test.go` files (for example, `worker/worker_test.go`) and cover the public behavior introduced or modified.
- Prefer table-driven tests for coverage of input/output permutations and edge cases.
- Storage-layer tests should follow the conventions in `docs/003.0.md`: use `DATA-DOG/go-sqlmock` via a test-only harness (kept under `storage/storagetest`) to assert SQL text, bind order, and exact arguments without requiring a live Postgres instance; include helpers that assert UTC+00:00 normalization for all persisted/returned timestamps.
- Use unit tests (sqlmock-backed) to assert SQL arguments and timestamp normalization; reserve integration tests (real Postgres) for end-to-end verification only.
- When changing concurrency- or goroutine-heavy code, run `go test -race ./...` locally and include race-sensitive tests where appropriate.
- Document any required Postgres extensions, environment variables, or test fixtures in the test file header comment.
- Pull requests must include the relevant tests for the change; CI will require tests to pass before merging. Tests missing for new or modified behavior will be returned for revision.

## Commit & Pull Request Guidelines
Follow the existing conventional commits style (`feat:`, `fix:`, `chore:`, etc.) as seen in `git log`. Write present-tense, imperative subject lines capped at ~72 chars; add body paragraphs only when extra context is needed. Every PR should describe the motivation, summarize visible behavior changes, list verification steps (`go test`, `go run`, SQL migrations applied), and link the relevant issue or spec section. Include screenshots or logs when altering operational output (e.g., new slog fields) and mention any config flags that operators must toggle.

## Database & Configuration Tips
Use the migrations in `db/migrations` as the canonical schema; never hand-edit production tables. Point `DATABASE_URL` at a dedicated dev database so leases and advisory locks can be exercised safely. When adding columns, update both migrations and the `storage.Job` struct in lockstep, and document intentional breaking changes in `specs.md`.
