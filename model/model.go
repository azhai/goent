package model

import (
	"context"
	"time"
)

// Attribute represents a column in a table
// It contains the table name and column name
type Attribute struct {
	Table string // Table name
	Name  string // Column name
}

// Query represents a SQL query with its arguments and duration
// It contains information about the SQL statement, its arguments, execution time, and any error

type Query struct {
	RawSql        string        // Raw SQL statement
	Arguments     []any         // Query arguments
	QueryDuration time.Duration // Execution duration
	Err           error         // Execution error
}

// CreateQuery creates a new Query with the given raw SQL and arguments
// It initializes a Query struct with the provided SQL and arguments
func CreateQuery(rawSql string, args []any) Query {
	return Query{RawSql: rawSql, Arguments: args}
}

func (q *Query) Finish(ctx context.Context, dc *DatabaseConfig, start time.Time) error {
	q.QueryDuration = time.Since(start)
	if q.Err != nil {
		return dc.ErrorQueryHandler(ctx, *q)
	}
	dc.InfoHandler(ctx, *q)
	return nil
}

func (q *Query) WrapExec(ctx context.Context, conn Connection, dc *DatabaseConfig) error {
	startTime := time.Now()
	q.Err = conn.ExecContext(ctx, q)
	err := q.Finish(ctx, dc, startTime)
	return err
}

func (q *Query) WrapQuery(ctx context.Context, conn Connection, dc *DatabaseConfig) (Rows, error) {
	var rows Rows
	startTime := time.Now()
	rows, q.Err = conn.QueryContext(ctx, q)
	err := q.Finish(ctx, dc, startTime)
	return rows, err
}

func (q *Query) WrapQueryRow(ctx context.Context, conn Connection, dc *DatabaseConfig) (Row, error) {
	startTime := time.Now()
	row := conn.QueryRowContext(ctx, q)
	if row == nil {
		q.Err = ErrNoRows
	}
	err := q.Finish(ctx, dc, startTime)
	return row, err
}

// Table represents a database table with its schema and name
// It contains the table name and optional schema name

type Table struct {
	Schema *string // Schema name (optional)
	Name   string  // Table name
}

// String returns the table name with schema if available
// It formats the table name as schema.table if a schema is provided
func (t Table) String() string {
	if t.Schema != nil {
		return *t.Schema + "." + t.Name
	}
	return t.Name
}

// Migrator represents a database migrator with its tables and schemas
// It manages the migration process for multiple tables and schemas

type Migrator struct {
	Tables  map[string]*TableMigrate // Tables to migrate
	Schemas []string                 // Schemas to use
	Error   error                    // Migration error
}

// TableMigrate represents a table to be migrated with its columns, indexes, and relationships
// It contains all the necessary information to create or update a database table

type TableMigrate struct {
	Name         string              // Table name
	EscapingName string              // Escaped table name
	Schema       *string             // Schema name
	Migrated     bool                // Whether the table has been migrated
	PrimaryKeys  []PrimaryKeyMigrate // Primary key columns
	Indexes      []IndexMigrate      // Indexes
	Attributes   []AttributeMigrate  // Columns
	ManyToSomes  []ManyToSomeMigrate // Many-to-one/many-to-many relationships
	OneToSomes   []OneToSomeMigrate  // One-to-one/one-to-many relationships
}

// EscapingTableName returns the escaped table name with schema if available
// It formats the escaped table name as schema.table if a schema is provided
func (t TableMigrate) EscapingTableName() string {
	if t.Schema != nil {
		return *t.Schema + "." + t.EscapingName
	}
	return t.EscapingName
}

// IndexMigrate represents an index to be created during migration
// It contains all the necessary information to create an index

type IndexMigrate struct {
	Name         string             // Index name
	EscapingName string             // Escaped index name
	Unique       bool               // Whether the index is unique
	Func         string             // Index function (e.g., "UPPER")
	Attributes   []AttributeMigrate // Attributes included in the index
}

// PrimaryKeyMigrate represents a primary key column with auto-increment flag
// It extends AttributeMigrate with auto-increment information

type PrimaryKeyMigrate struct {
	AutoIncrement    bool // Whether the primary key is auto-incrementing
	AttributeMigrate      // Embedded attribute information
}

// AttributeMigrate represents a column to be created during migration
// It contains all the necessary information to create a database column

type AttributeMigrate struct {
	Nullable     bool   // Whether the column allows null values
	FieldName    string // Go struct field name
	Name         string // Column name
	EscapingName string // Escaped column name
	DataType     string // Data type
	Default      string // Default value
}

// OneToSomeMigrate represents a one-to-one or one-to-many relationship for migration
// It contains information about the relationship target and type

type OneToSomeMigrate struct {
	IsOneToMany          bool    // Whether the relationship is one-to-many
	TargetTable          string  // Target table name
	TargetColumn         string  // Target column name
	EscapingTargetTable  string  // Escaped target table name
	EscapingTargetColumn string  // Escaped target column name
	TargetSchema         *string // Target schema name
	AttributeMigrate             // Embedded attribute information
}

// EscapingTargetTableName returns the escaped target table name with schema if available
// It formats the escaped target table name as schema.table if a schema is provided
func (o OneToSomeMigrate) EscapingTargetTableName() string {
	if o.TargetSchema != nil && *o.TargetSchema != "" {
		return *o.TargetSchema + "." + o.EscapingTargetTable
	}
	return o.EscapingTargetTable
}

// ManyToSomeMigrate represents a many-to-one or many-to-many relationship for migration
// It contains information about the relationship target

type ManyToSomeMigrate struct {
	TargetTable          string  // Target table name
	TargetColumn         string  // Target column name
	EscapingTargetTable  string  // Escaped target table name
	EscapingTargetColumn string  // Escaped target column name
	TargetSchema         *string // Target schema name
	AttributeMigrate             // Embedded attribute information
}

// EscapingTargetTableName returns the escaped target table name with schema if available
// It formats the escaped target table name as schema.table if a schema is provided
func (m ManyToSomeMigrate) EscapingTargetTableName() string {
	if m.TargetSchema != nil && *m.TargetSchema != "" {
		return *m.TargetSchema + "." + m.EscapingTargetTable
	}
	return m.EscapingTargetTable
}

// DatabaseConfig contains database configuration including logging and error handling settings
// It controls how the database driver logs queries and handles errors

type DatabaseConfig struct {
	Logger           Logger                // Logger interface for logging
	IncludeArguments bool                  // Whether to include arguments in logs
	QueryThreshold   time.Duration         // Threshold for slow query warnings
	databaseName     string                // Database name
	errorTranslator  func(err error) error // Error translator function
	schemas          []string              // List of schemas
	initCallback     func() error          // Initialization callback function
}

// ErrorHandler logs the database error using the configured logger
// It logs the error and returns it
func (c *DatabaseConfig) ErrorHandler(ctx context.Context, err error) error {
	if c.Logger != nil {
		c.Logger.ErrorContext(ctx, "error", "database", c.databaseName, "err", err)
	}
	return err
}

// ErrorQueryHandler logs the query error using the configured logger
// It translates the error and logs it with query details
func (c *DatabaseConfig) ErrorQueryHandler(ctx context.Context, query Query) error {
	query.Err = c.errorTranslator(query.Err)
	if c.Logger == nil {
		return query.Err
	}
	logs := make([]any, 0, 8)
	logs = append(logs, "database", c.databaseName)
	logs = append(logs, "sql", query.RawSql)
	if c.IncludeArguments {
		logs = append(logs, "arguments", query.Arguments)
	}
	logs = append(logs, "err", query.Err)

	c.Logger.ErrorContext(ctx, "error", logs...)
	return query.Err
}

// InfoHandler logs the query information using the configured logger
// It logs query details including duration and SQL
func (c *DatabaseConfig) InfoHandler(ctx context.Context, query Query) {
	if c.Logger == nil {
		return
	}
	dur := query.QueryDuration

	logs := make([]any, 0, 10)
	logs = append(logs, "database", c.databaseName)
	logs = append(logs, "query_duration", dur.String())
	logs = append(logs, "sql", query.RawSql)
	if c.IncludeArguments {
		logs = append(logs, "arguments", query.Arguments)
	}
	if c.QueryThreshold != 0 && dur > c.QueryThreshold {
		c.Logger.WarnContext(ctx, "query_threshold", logs...)
		return
	}

	c.Logger.InfoContext(ctx, "query_runned", logs...)
}

// Schemas returns the list of schemas configured for the database
// It returns the currently configured schemas
func (c *DatabaseConfig) Schemas() []string {
	return c.schemas
}

// SetSchemas sets the list of schemas for the database
// It replaces the current schemas with the provided list
func (c *DatabaseConfig) SetSchemas(s []string) {
	c.schemas = s
}

// AddSchema adds a schema to the list of schemas for the database
// It appends the provided schema to the current list
func (c *DatabaseConfig) AddSchema(s string) {
	c.schemas = append(c.schemas, s)
}

// SetInitCallback sets the initialization callback function for the database
// It sets the function to be called during initialization
func (c *DatabaseConfig) SetInitCallback(f func() error) {
	c.initCallback = f
}

// InitCallback returns the initialization callback function for the database
// It returns the currently set initialization callback
func (c *DatabaseConfig) InitCallback() func() error {
	return c.initCallback
}

// Init initializes the database configuration with the given driver name and error translator
// It resets and initializes the configuration with the provided values
func (c *DatabaseConfig) Init(driverName string, errorTranslator func(err error) error) {
	c.schemas = nil
	c.initCallback = nil
	c.databaseName = driverName
	c.errorTranslator = errorTranslator
}
