package storage

import "time"

// TestStoreDependencies describes the dependencies used to build a Store for
// tests. It allows tests in other packages to construct stores with mocked
// database implementations without relying on unsafe access to the Store
// internals.
type TestStoreDependencies struct {
	DB  DB
	Now func() time.Time
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
