package apperrors

import "errors"

// Package apperrors defines a small set of exported sentinel errors used for
// programmatic checks across packages. Only use these sentinels for errors
// callers may reasonably check with errors.Is. For other validation or
// operational errors prefer context-wrapped errors.

var (
	// ErrInvalidArgument indicates the caller provided invalid input.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrNotConfigured indicates a required runtime dependency was not provided.
	ErrNotConfigured = errors.New("not configured")

	// ErrNotSupported indicates the operation is not supported by the underlying backend.
	ErrNotSupported = errors.New("not supported")

	// ErrNotImplemented indicates a stub or test helper is not implemented.
	ErrNotImplemented = errors.New("not implemented")
)
