package model

import (
	"context"
	"database/sql"
)

// Driver represents a database driver
// It defines the interface that all database drivers must implement

type Driver interface {
	// MigrateContext migrates the database schema
	MigrateContext(context.Context, *Migrator) error

	// DropTable drops a table from the database schema
	DropTable(schema, table string) error
	// DropColumn drops a column from a table in the database schema
	DropColumn(schema, table, column string) error
	// RenameColumn renames a column in a table in the database schema
	RenameColumn(schema, table, oldColumn, newName string) error
	// RenameTable renames a table in the database schema
	RenameTable(schema, table, newName string) error

	// AddLogger adds a logger to the database driver
	AddLogger(Logger, error) error
	// ErrorTranslator translates database errors
	ErrorTranslator() func(err error) error
	// KeywordHandler handles SQL keywords in the database driver
	KeywordHandler(string) string
	// FormatTableName formats the table name in the database driver
	FormatTableName(schema, table string) string
	// SupportsReturning checks if the database driver supports RETURNING clause
	SupportsReturning() bool

	// NewConnection creates a new database connection
	NewConnection() Connection
	// NewTransaction creates a new database transaction
	NewTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error)

	// Init initializes the database schema
	Init() error
	// Close closes the database driver
	Close() error
	// Stats returns the database statistics
	Stats() sql.DBStats
	// Config represents a database configuration
	Config
}

// Config represents a database configuration
// It defines the interface for database configuration

type Config interface {
	// Name returns the name of the database
	Name() string
	// GetDatabaseConfig returns the database configuration
	GetDatabaseConfig() *DatabaseConfig
}

// Logger represents a logger for database operations
// It defines the interface for logging database events

type Logger interface {
	// InfoContext logs information messages
	InfoContext(ctx context.Context, msg string, kv ...any)
	// WarnContext logs warning messages
	WarnContext(ctx context.Context, msg string, kv ...any)
	// ErrorContext logs error messages
	ErrorContext(ctx context.Context, msg string, kv ...any)
}

// Connection represents a database connection
// It defines the interface for database connection operations

type Connection interface {
	// ExecContext executes a SQL statement without returning rows
	ExecContext(ctx context.Context, query *Query) error
	// QueryRowContext executes a SQL query and returns a single row
	QueryRowContext(ctx context.Context, query *Query) Row
	// QueryContext executes a SQL query and returns multiple rows
	QueryContext(ctx context.Context, query *Query) (Rows, error)
}

// Transaction represents a database transaction
// It extends Connection with transaction-specific methods

type Transaction interface {
	// Connection represents the underlying database connection
	Connection
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
	// SavePoint creates a savepoint in the transaction
	SavePoint() (SavePoint, error)
}

// SavePoint represents a savepoint in a database transaction
// It defines the interface for savepoint operations

type SavePoint interface {
	// Commit commits the savepoint
	Commit() error
	// Rollback rolls back the savepoint
	Rollback() error
}

// Rows represents a result set of a SQL query
// It defines the interface for iterating over query results

type Rows interface {
	// Close closes the result set
	Close() error
	// Next advances to the next row
	Next() bool
	// Row represents a single row in the result set
	Row
}

// Row represents a single row of a SQL query result
// It defines the interface for scanning row values

type Row interface {
	// Scan scans the row values into the provided destinations
	Scan(dest ...any) error
}
