package storage

import "time"

// TestStoreDependencies describes the dependencies used to build a Store for
// tests. It allows tests in other packages to construct stores with mocked
// database implementations without relying on unsafe access to the Store
// internals.
type TestStoreDependencies struct {
	DB  TestDBRunner
	Now func() time.Time
}

// TestDBRunner matches the Store's internal database runner contract. It is
// exported solely for use in tests.
type TestDBRunner interface {
	dbRunner
}

// NewTestStore constructs a Store using the supplied test dependencies. Nil
// dependencies are permitted; callers should only provide the fields they rely
// on in a given test.
func NewTestStore(deps TestStoreDependencies) *Store {
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &Store{
		DB:  deps.DB,
		now: now,
	}
}
