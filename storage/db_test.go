package storage

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDB_Interface verifies that the dbAdapter wrapping *sql.DB satisfies
// the storage.DB interface and correctly delegates to the underlying database.
func TestDB_Interface(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Wrap *sql.DB with our adapter
	adapter := NewDB(db)

	ctx := context.Background()

	// Test ExecContext
	mock.ExpectExec("INSERT INTO test").WillReturnResult(sqlmock.NewResult(1, 1))
	result, err := adapter.ExecContext(ctx, "INSERT INTO test VALUES ($1)", "value")
	require.NoError(t, err)
	affected, _ := result.RowsAffected()
	assert.Equal(t, int64(1), affected)

	// Test QueryRowContext
	mock.ExpectQuery("SELECT id FROM test").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(42))
	var id int
	err = adapter.QueryRowContext(ctx, "SELECT id FROM test WHERE id = $1", 42).Scan(&id)
	require.NoError(t, err)
	assert.Equal(t, 42, id)

	// Test QueryContext
	mock.ExpectQuery("SELECT id FROM test").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))
	rows, err := adapter.QueryContext(ctx, "SELECT id FROM test")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	assert.Equal(t, 2, count)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestTx_Interface verifies that the txAdapter wrapping *sql.Tx satisfies
// the storage.Tx interface.
func TestTx_Interface(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewDB(db)
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := adapter.BeginTx(ctx, nil)
	require.NoError(t, err)

	result, err := tx.ExecContext(ctx, "INSERT INTO test VALUES ($1)", "value")
	require.NoError(t, err)
	affected, _ := result.RowsAffected()
	assert.Equal(t, int64(1), affected)

	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestBeginTx_ReturnsValidTx verifies BeginTx returns a Tx that implements
// the required methods.
func TestBeginTx_ReturnsValidTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT count").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectRollback()

	// Use the DB interface with our adapter
	dbInterface := NewDB(db)

	tx, err := dbInterface.BeginTx(ctx, nil)
	require.NoError(t, err)

	var count int
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	err = tx.Rollback()
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestIsNoRows verifies the IsNoRows helper correctly identifies no-rows errors.
func TestIsNoRows(t *testing.T) {
	assert.True(t, IsNoRows(sql.ErrNoRows))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Test with actual query that returns no rows
	mock.ExpectQuery("SELECT id FROM test").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	ctx := context.Background()
	var id int
	err = db.QueryRowContext(ctx, "SELECT id FROM test WHERE id = $1", 999).Scan(&id)
	assert.True(t, IsNoRows(err))

	require.NoError(t, mock.ExpectationsWereMet())
}
