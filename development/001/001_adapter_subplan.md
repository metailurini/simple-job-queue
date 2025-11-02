# Adapter subplan — database/sql "stdlib" adapter

This subplan expands STEP 2 from `development/001/000_index.md`. It focuses on designing and implementing a small, test-friendly adapter that implements the `DB` and `Tx` interfaces and delegates to `database/sql`.

## Goals

- Define a minimal `DB` and `Tx` interface inside the `storage` package (suggested file: `storage/db.go`) whose method set matches the subset of `*sql.DB`/`*sql.Tx` used in the repo. Do not add an extra wrapper type — let `*sql.DB` and `*sql.Tx` be passed directly where appropriate.
- Provide helper functions for common translations (error normalization, scanning helpers) and a small transaction wrapper only if semantics demand it.


## Design contract (inputs / outputs / error modes)

- Inputs: context.Context, SQL statements and args, optional TxOptions.
- Outputs: for Exec: (sql.Result, error); for Query: (*sql.Rows, error); for QueryRow: *sql.Row. For BeginTx: (Tx, error).
- Error modes: adopt `errors.Is(err, sql.ErrNoRows)` and return underlying driver errors otherwise.


## Files to add (minimal)

- `storage/db.go` — define the `DB` and `Tx` interfaces and small helpers
- `storage/errors.go` — optional error helpers and translations (IsNoRows)
- `storage/db_test.go` — sqlmock-based tests showing `*sql.DB`/`*sql.Tx` usage


## Key snippets (illustrative)

interface definition (inside storage) (concept):

```
package storage

import (
    "context"
    "database/sql"
)

// DB is intentionally small so *sql.DB already satisfies it.
type DB interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
    Close() error
}

// Tx mirrors the subset of *sql.Tx we use.
type Tx interface {
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    Commit() error
    Rollback() error
}

// Important: *sql.DB implements ExecContext, QueryContext, QueryRowContext, BeginTx, Close.
// *sql.Tx implements ExecContext, QueryContext, QueryRowContext, Commit, Rollback.
// That means you can pass *sql.DB to functions requiring storage.DB without adding a wrapper.
```

// Optional: a small transaction wrapper can be added only if you need to normalize behavior
// between drivers. In most cases *sql.Tx will be used directly and already satisfies Tx.


## Error handling and helper translations

Where the codebase previously checked `pgx.ErrNoRows`, update to use `errors.Is(err, sql.ErrNoRows)`.
Optionally provide a small helper `IsNoRows(err error) bool` in `storage/errors.go` for clarity.

errors.go (concept):

```
package storage

import (
    "database/sql"
    "errors"
)

func IsNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
```


## Tests (sqlmock + unit fakes)

Adapter tests: use `sqlmock` to assert that `NewStdlibDB(...).BeginTx` calls `db.Begin` and the returned Tx implements Commit/Rollback.
Store-level tests: reuse the storage adapter and sqlmock to assert SQL executed by `storage` methods.

Example test case outline (storage/db_test.go):

```
db, mock, _ := sqlmock.New()
defer db.Close()
// *sql.DB already satisfies storage.DB as long as the interface is the small subset.
var s storage.DB = db

mock.ExpectBegin()
mock.ExpectExec("INSERT INTO jobs").WillReturnResult(sqlmock.NewResult(1,1))
mock.ExpectCommit()

tx, err := s.BeginTx(ctx, nil)
_, err = tx.ExecContext(ctx, "INSERT INTO jobs (...) VALUES (...)", args...)
err = tx.Commit()
// assertions and mock expectations
```


## Rollout notes

- Implement changes in a feature branch inside the `storage` package (files like `storage/db.go`). Keep changes incremental so other packages can be migrated step-by-step.

- Important policy: top-level binaries (for example `cmd/jobworker` and `cmd/jobscheduler`) will construct the concrete `*sql.DB` value. All internal functions, methods, and constructors must accept the `storage.DB` interface in their signatures. You may pass the concrete `*sql.DB` as the argument value when calling those functions, but the parameter type must be `storage.DB`.

Illustrative wiring (cmd + storage):

```go
// cmd/jobworker/main.go
db, err := sql.Open("pgx", cfg.DatabaseURL)
if err != nil { log.Fatal(err) }
db.SetMaxOpenConns(cfg.DBMaxOpenConns)
workerStore := storage.NewStore(db) // pass *sql.DB as storage.DB

// storage package
func NewStore(db storage.DB) *Store { ... }
```

- Migrate packages one-by-one: storage → worker → scheduler. For each package, switch constructors to accept `storage.DB` and update tests to use sqlmock or hand-written fakes assigned to the `storage.DB` interface.


## Edge cases and concurrency considerations

- `database/sql` connections are pooled at the driver level; ensure you don't accidentally Hold long-lived connections in `storage` code. Use `context` timeouts for long queries.
- Be explicit about `SetMaxOpenConns`/`SetMaxIdleConns` and document recommended values in README.


## Complexity and subplans

- Complexity: 8 — we created this subplan to contain detailed design and tests.


## Validation checklist

- Unit: adapter methods call underlying `*sql.DB` and `*sql.Tx` methods (sqlmock).
- Unit: Store methods work with fake `DB` implementing `storage.DB`.
- Integration: lightweight Postgres test to exercise migrations and pooled connections.
