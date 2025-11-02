## Goals

This document describes a careful plan to replace direct usage of github.com/jackc/pgx throughout the codebase with a traditional database/sql-backed adapter while preserving an interface boundary that keeps production code testable with lightweight mocks.

High-level goals:
- Replace pgx-specific usage with a small, well-documented DB interface.
- Provide a concrete adapter using database/sql (the "stdlib" adapter).
- Keep the change incremental and compile-time safe so we can roll out package-by-package.
- Improve unit-test ergonomics by relying on interface-based mocking (eg. sqlmock or hand-written fakes).


## Context

The repository currently uses pgx idioms (pgx.Tx, pgx.Conn, pgxpool) across storage and runner code. That couples the code to the driver API and makes testing require pgx-specific mocks. We want to:

- Stop depending on pgx types in package public APIs.
- Introduce a small, focused interface (DB / Tx) capturing operations used by this project.
- Implement a database/sql adapter that calls into whichever sql driver (lib/pq, pgx/stdlib, etc.) is selected by the service's runtime configuration.

Why this approach?

- database/sql is widely supported and plays nicer with many testing tools (sqlmock) and hosting environments.
- Keeping a thin interface preserves testability and keeps the production/driver details behind an adapter.


## Implementation Steps

Each step below is actionable and includes rationale, illustrative code/diffs, and rollout notes. Complexity scores guide where to add subplans. Steps that need substantial design detail reference a dedicated subplan file.

STEP 1 — Define a minimal DB / Tx interface and adapt store constructors
Complexity: 4

Rationale
- Keep the interface intentionally small: only expose what the codebase needs (Exec, QueryRow, Query, BeginTx, Close). Narrow interfaces are easier to mock and reason about.

Illustrative diff (conceptual):

```
*** Update File: storage/store.go (concept)
@@
 import (
     "context"
     // old: "github.com/jackc/pgx/v4"
     "database/sql"
 )

 // Add a small DB interface used across packages
 type DB interface {
     ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
     QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
     QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
     BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
     Close() error
 }

 type Tx interface {
     ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
     QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
     QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
     Commit() error
     Rollback() error
 }

 // NewStore now takes the DB interface instead of *pgxpool.Pool
 func NewStore(db DB) *Store { ... }
```

Rollout notes
- Start by adding the interfaces in `storage/store.go` (or a new file `storage/db.go`).
- Keep the existing pgx imports until you wire the adapter to avoid cascade breakage.
- Update `NewStore` and any constructors to accept `DB` rather than concrete pgx types.


STEP 2 — Define interfaces and rely on built-in *sql.DB / *sql.Tx
Complexity: 6

Rationale
- Instead of adding an adapter wrapper type, define a small `DB` and `Tx` interface in `storage` and let the built-in `*sql.DB` and `*sql.Tx` satisfy them directly. This avoids an extra wrapper while keeping testability.

This step is important and needs a dedicated subplan. See: `development/001/001_adapter_subplan.md`.

Rollout notes
- Add the `DB` and `Tx` interfaces in `storage` (suggested file: `storage/db.go`). Do not create a separate wrapper type — `*sql.DB` already implements the required methods (ExecContext, QueryContext, QueryRowContext, BeginTx, Close).
- This minimizes churn: callers will continue to pass a concrete object (the `*sql.DB` instance) into `storage.NewStore` but tests can still substitute fakes or sqlmock's `*sql.DB`.



STEP 3 — Wire the adapter into the CLI/server programs
Complexity: 5

Rationale
- Only the top-level `main` packages should construct concrete adapters. Other packages receive the `DB` interface, keeping them testable.

Illustrative snippet (cmd/jobscheduler/main.go):

```
@@
 // old: pool, err := pgxpool.Connect(context.Background(), cfg.DatabaseURL)
 // old: store := storage.NewStore(pool)

 // stdlib usage (new)
 sqlDb, err := sql.Open("pgx", cfg.DatabaseURL) // or "postgres" for lib/pq
 if err != nil { log.Fatal(err) }
 sqlDb.SetMaxOpenConns(cfg.DBMaxOpenConns)
 // pass the *sql.DB directly; it implements the storage.DB interface
 app.Store = storage.NewStore(sqlDb)
```

Rollout notes
- Update `cmd/jobscheduler` and `cmd/jobworker` to construct `*sql.DB` and pass it into storage constructors.
- Important policy: top-level code (main packages) will construct the concrete `*sql.DB` value. However, every internal function and constructor must accept the narrow `storage.DB` interface in their signatures. This keeps call sites flexible for tests while allowing the program to use the concrete `*sql.DB` at runtime.

Illustrative snippet:

```go
// cmd/jobscheduler/main.go (top-level)
db, _ := sql.Open("pgx", cfg.DatabaseURL)
app.Store = storage.NewStore(db) // db is *sql.DB, but NewStore accepts storage.DB

// storage package
func NewStore(db storage.DB) *Store { ... }
```

Rollout guidance:
- You may pass the concrete `*sql.DB` through to lower-level constructors as an argument value, but parameter types must stay as the `storage.DB` interface.
- Tests should pass fakes or sqlmock-backed `*sql.DB` assigned to the `storage.DB` interface (or hand-written fakes) rather than depending on `*sql.DB` methods directly.


STEP 4 — Update unit tests to use interface-based mocks (sqlmock / fakes)
Complexity: 6

Rationale
- When code only depends on the `DB` interface, test suites can use sqlmock for integration-style verification or small hand-written fakes for unit tests.

Illustrative snippet (store_test.go):

```go
// Using github.com/DATA-DOG/go-sqlmock for adapter-level tests
db, mock, _ := sqlmock.New()
defer db.Close()
adapter := sqladapter.New(db)
store := storage.NewStore(adapter)

mock.ExpectExec("INSERT INTO jobs").WithArgs(123).WillReturnResult(sqlmock.NewResult(1, 1))
err := store.InsertJob(ctx, job)
// assertions...
```

Rollout notes
- Start converting one package's tests (eg. `storage`) to the new interface before moving on.
- Add a small `testhelpers` fake DB for fast unit tests that avoids sqlmock overhead when appropriate.


STEP 5 — Incremental migration and CI validation
Complexity: 3

Rationale
- Keep changes incremental. Migrate package-by-package and run CI on each step.

Rollout notes
- Add compile-time checks (small unit tests) that construct stores with the new `DB` interface.
- Reserve a feature branch and open a pull request per major package migration (storage, worker, scheduler).


## Pitfalls & Validation

Known risks and mitigations

- Risk: Subtle semantic differences between pgx transaction behaviour and database/sql (e.g., `Conn` vs pooled `*sql.DB`).
  - Mitigation: Keep transaction handling inside the adapter and expose a Tx interface with clear semantics matching current expectations. Add tests for commit/rollback edge cases.

- Risk: Error types differ (pgx.ErrNoRows vs sql.ErrNoRows) and may be checked in callers.
  - Mitigation: Standardize on `errors.Is(err, sql.ErrNoRows)` and update call sites. Provide adapter helpers to translate common errors if needed.

- Risk: Performance/regression due to different pooling settings.
  - Mitigation: Benchmark critical paths in a staging environment with realistic load. Tune `SetMaxOpenConns`, `SetConnMaxIdleTime`, etc.


Proposed tests (concrete)

- Unit (happy path): Use a fake `DB` that records queries. Verify `Store` calls the expected SQL with correct args. Guards against interface mismatches.

- Unit (error propagation): Fake `DB` returns an error on Exec; assert the higher-level method returns the same error. Guards against swallowing/transforming errors.

- Integration (sqlmock): Adapter-level tests using sqlmock to assert SQL executed exactly and transactions committed/rolled back.

- Integration (smoke): A lightweight integration test using a real Postgres instance (CI job or Docker-based test) to validate migrations, connection pooling, and common queries.

Example test cases and how they protect the system:

1) InsertJob success
   - Setup: sqlmock expects INSERT with given args and returns LastInsertID
   - Verify: store.InsertJob returns nil and the job ID is as expected
   - Guards: ensures SQL, args, and scanning are correct

2) InsertJob unique constraint violation
   - Setup: sqlmock returns a driver-specific constraint error
   - Verify: store.InsertJob returns an error which the caller can classify (e.g., via errors.Is or by type)
   - Guards: prevents misclassification of DB errors

3) Transaction rollback on failure
   - Setup: mock tx.Exec for second statement returns error
   - Verify: mock expects tx.Rollback() and no Commit(); higher-level method returns error
   - Guards: prevents data corruption by ensuring rollbacks occur on errors


Requirements coverage

- Goals: Done — plan defines interface and an adapter approach.
- Context: Done — explains why and where pgx is coupled.
- Implementation Steps: Done — 5 steps with diffs and rollout notes; adapter has a subplan.
- Pitfalls & Validation: Done — risks, mitigations, and concrete tests listed.

Next steps

- Implement the adapter according to `development/001/001_adapter_subplan.md` and convert `storage` package constructors to accept the new `DB` interface.
- Run the unit and integration tests described above.
