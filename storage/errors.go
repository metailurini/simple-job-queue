package storage

import (
	"database/sql"
	"errors"
)

// IsNoRows returns true if err indicates a query returned zero rows.
// This helper provides a consistent way to check for "no rows" errors across
// the codebase, supporting both database/sql.ErrNoRows and any wrapped forms.
func IsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
