package model

import (
	"context"
	"time"
)

type Attribute struct {
	Table string
	Name  string
}

type Query struct {
	RawSql        string
	Arguments     []any
	QueryDuration time.Duration
	Err           error
}

func CreateQuery(rawSql string, args []any) Query {
	return Query{RawSql: rawSql, Arguments: args}
}

type Table struct {
	Schema *string
	Name   string
}

func (t Table) String() string {
	if t.Schema != nil {
		return *t.Schema + "." + t.Name
	}
	return t.Name
}

type Migrator struct {
	Tables  map[string]*TableMigrate
	Schemas []string
	Error   error
}

// TableMigrate represents a table to be migrated with its columns, indexes, and relationships.
type TableMigrate struct {
	Name         string
	EscapingName string
	Schema       *string
	Migrated     bool
	PrimaryKeys  []PrimaryKeyMigrate
	Indexes      []IndexMigrate
	Attributes   []AttributeMigrate
	ManyToSomes  []ManyToSomeMigrate
	OneToSomes   []OneToSomeMigrate
}

// EscapingTableName returns the table and the schema.
func (t TableMigrate) EscapingTableName() string {
	if t.Schema != nil {
		return *t.Schema + "." + t.EscapingName
	}
	return t.EscapingName
}

// IndexMigrate represents an index to be created during migration.
type IndexMigrate struct {
	Name         string
	EscapingName string
	Unique       bool
	Func         string
	Attributes   []AttributeMigrate
}

// PrimaryKeyMigrate represents a primary key column with auto-increment flag.
type PrimaryKeyMigrate struct {
	AutoIncrement bool
	AttributeMigrate
}

// AttributeMigrate represents a column to be created during migration.
type AttributeMigrate struct {
	Nullable     bool
	FieldName    string
	Name         string
	EscapingName string
	DataType     string
	Default      string
}

// OneToSomeMigrate represents a one-to-one or one-to-many relationship for migration.
type OneToSomeMigrate struct {
	IsOneToMany          bool
	TargetTable          string
	TargetColumn         string
	EscapingTargetTable  string
	EscapingTargetColumn string
	TargetSchema         *string
	AttributeMigrate
}

// EscapingTargetTableName returns the target table and the schema.
func (o OneToSomeMigrate) EscapingTargetTableName() string {
	if o.TargetSchema != nil {
		return *o.TargetSchema + "." + o.EscapingTargetTable
	}
	return o.EscapingTargetTable
}

// ManyToSomeMigrate represents a many-to-one or many-to-many relationship for migration.
type ManyToSomeMigrate struct {
	TargetTable          string
	TargetColumn         string
	EscapingTargetTable  string
	EscapingTargetColumn string
	TargetSchema         *string
	AttributeMigrate
}

// EscapingTargetTableName returns the target table and the schema.
func (m ManyToSomeMigrate) EscapingTargetTableName() string {
	if m.TargetSchema != nil {
		return *m.TargetSchema + "." + m.EscapingTargetTable
	}
	return m.EscapingTargetTable
}

// DatabaseConfig contains database configuration including logging and error handling settings.
type DatabaseConfig struct {
	Logger           Logger
	IncludeArguments bool          // include all arguments used on query
	QueryThreshold   time.Duration // query threshold to warning on slow queries
	databaseName     string
	errorTranslator  func(err error) error
	schemas          []string
	initCallback     func() error
}

func (c *DatabaseConfig) ErrorHandler(ctx context.Context, err error) error {
	if c.Logger != nil {
		c.Logger.ErrorContext(ctx, "error", "database", c.databaseName, "err", err)
	}
	return err
}

func (c *DatabaseConfig) ErrorQueryHandler(ctx context.Context, query Query) error {
	query.Err = c.errorTranslator(query.Err)
	if c.Logger == nil {
		return query.Err
	}
	logs := make([]any, 0)
	logs = append(logs, "database", c.databaseName)
	logs = append(logs, "sql", query.RawSql)
	if c.IncludeArguments {
		logs = append(logs, "arguments", query.Arguments)
	}
	logs = append(logs, "err", query.Err)

	c.Logger.ErrorContext(ctx, "error", logs...)
	return query.Err
}

func (c *DatabaseConfig) InfoHandler(ctx context.Context, query Query) {
	if c.Logger == nil {
		return
	}
	qr := query.QueryDuration

	logs := make([]any, 0)
	logs = append(logs, "database", c.databaseName)
	logs = append(logs, "query_duration", qr.String())
	logs = append(logs, "sql", query.RawSql)
	if c.IncludeArguments {
		logs = append(logs, "arguments", query.Arguments)
	}

	if c.QueryThreshold != 0 && qr > c.QueryThreshold {
		c.Logger.WarnContext(ctx, "query_threshold", logs...)
		return
	}

	c.Logger.InfoContext(ctx, "query_runned", logs...)
}

func (c *DatabaseConfig) Schemas() []string {
	return c.schemas
}

func (c *DatabaseConfig) SetSchemas(s []string) {
	c.schemas = s
}

func (c *DatabaseConfig) AddSchema(s string) {
	c.schemas = append(c.schemas, s)
}

func (c *DatabaseConfig) SetInitCallback(f func() error) {
	c.initCallback = f
}

func (c *DatabaseConfig) InitCallback() func() error {
	return c.initCallback
}

func (c *DatabaseConfig) Init(driverName string, errorTranslator func(err error) error) {
	c.schemas = nil
	c.initCallback = nil
	c.databaseName = driverName
	c.errorTranslator = errorTranslator
}
