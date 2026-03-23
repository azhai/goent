package goent_test

import (
	"context"
	"testing"

	"github.com/azhai/goent/model"
)

// TestQueryWrapQueryNilRows tests that WrapQuery returns nil rows on error
func TestQueryWrapQueryNilRows(t *testing.T) {
	// Create a mock connection that returns an error
	mockConn := &mockConnection{
		queryError: model.ErrBadRequest,
	}

	cfg := &model.DatabaseConfig{}
	query := model.CreateQuery("SELECT * FROM non_existent_table", nil)

	rows, err := query.WrapQuery(context.Background(), mockConn, cfg)

	// Should return error
	if err == nil {
		t.Error("Expected error, got nil")
	}

	// Should return nil rows to prevent nil pointer dereference
	if rows != nil {
		t.Error("Expected rows to be nil on error, got non-nil")
	}
}

// TestQueryWrapQueryRowNilRow tests that WrapQueryRow handles nil row correctly
func TestQueryWrapQueryRowNilRow(t *testing.T) {
	// Create a mock connection that returns nil row
	mockConn := &mockConnection{
		returnNilRow: true,
	}

	cfg := &model.DatabaseConfig{}
	query := model.CreateQuery("SELECT * FROM non_existent_table", nil)

	row, err := query.WrapQueryRow(context.Background(), mockConn, cfg)

	// Should return error when row is nil
	if err == nil {
		t.Error("Expected error for nil row, got nil")
	}

	// Row should be nil
	if row != nil {
		t.Error("Expected row to be nil, got non-nil")
	}
}

// mockConnection is a mock implementation of model.Connection for testing
type mockConnection struct {
	queryError   error
	returnNilRow bool
}

func (m *mockConnection) ExecContext(ctx context.Context, query *model.Query) error {
	return m.queryError
}

func (m *mockConnection) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	if m.returnNilRow {
		return nil
	}
	return &mockRow{}
}

func (m *mockConnection) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	if m.queryError != nil {
		return nil, m.queryError
	}
	return &mockRows{}, nil
}

// mockRow is a mock implementation of model.Row
type mockRow struct{}

func (m *mockRow) Scan(dest ...any) error {
	return nil
}

// mockRows is a mock implementation of model.Rows
type mockRows struct{}

func (m *mockRows) Close() error {
	return nil
}

func (m *mockRows) Next() bool {
	return false
}

func (m *mockRows) Scan(dest ...any) error {
	return nil
}
