package model

import (
	"context"
	"database/sql"
)

// Driver represents a database driver.
type Driver interface {
	MigrateContext(context.Context, *Migrator) error // MigrateContext migrates the database schema.

	DropTable(schema, table string) error                        // DropTable drops a table from the database schema.
	DropColumn(schema, table, column string) error               // DropColumn drops a column from a table in the database schema.
	RenameColumn(schema, table, oldColumn, newName string) error // RenameColumn renames a column in a table in the database schema.
	RenameTable(schema, table, newName string) error             // RenameTable renames a table in the database schema.

	AddLogger(Logger, error) error               // AddLogger adds a logger to the database driver.
	ErrorTranslator() func(err error) error      // ErrorTranslator translates database errors.
	KeywordHandler(string) string                // KeywordHandler handles SQL keywords in the database driver.
	FormatTableName(schema, table string) string // FormatTableName formats the table name in the database driver.
	SupportsReturning() bool                     // SupportsReturning checks if the database driver supports RETURNING clause.

	NewConnection() Connection                                                    // NewConnection creates a new database connection.
	NewTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error) // NewTransaction creates a new database transaction.

	Init() error        // Init initializes the database schema.
	Close() error       // Close closes the database driver.
	Stats() sql.DBStats // Stats returns the database statistics.
	Config              // Config represents a database configuration.
}

// Config represents a database configuration.
type Config interface {
	Name() string
	GetDatabaseConfig() *DatabaseConfig
}

// Logger represents a logger for database operations.
type Logger interface {
	InfoContext(ctx context.Context, msg string, kv ...any)
	WarnContext(ctx context.Context, msg string, kv ...any)
	ErrorContext(ctx context.Context, msg string, kv ...any)
}

// Connection represents a database connection.
type Connection interface {
	ExecContext(ctx context.Context, query *Query) error
	QueryRowContext(ctx context.Context, query *Query) Row
	QueryContext(ctx context.Context, query *Query) (Rows, error)
}

// Transaction represents a database transaction.
type Transaction interface {
	Connection
	Commit() error
	Rollback() error
	SavePoint() (SavePoint, error)
}

// SavePoint represents a savepoint in a database transaction.
type SavePoint interface {
	Commit() error
	Rollback() error
}

// Rows represents a result set of a SQL query.
type Rows interface {
	Close() error
	Next() bool
	Row
}

// Row represents a single row of a SQL query result.
type Row interface {
	Scan(dest ...any) error
}
