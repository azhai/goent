# GoEnt Development Guide

GoEnt is a type-safe ORM library for Go. This document provides guidance for developers working on this codebase.

## Project Overview

GoEnt provides a type-safe ORM with the following key features:
- Non-string query building using Go code
- Type safety with compile-time errors
- Auto migrations from struct definitions
- SQL-like queries using Go functions
- Iterator support for row iteration
- Pagination support
- Support for PostgreSQL and SQLite

## Architecture

```
goent/
├── builder.go          # SQL query builder core
├── where.go            # WHERE clause construction
├── select.go           # SELECT query operations
├── insert.go           # INSERT query operations
├── update.go           # UPDATE query operations
├── delete.go           # DELETE query operations
├── table.go            # Table metadata and field mapping
├── handler.go          # Query execution and result handling
├── foreign.go          # Foreign key relationship handling (O2O, O2M, M2O, M2M)
├── attribute.go        # Attribute/field definitions
├── column.go           # Column metadata
├── goent.go            # Main entry point and Open function
├── database.go         # Database connection and transaction management
├── migrate.go          # Migration orchestration
├── migrator.go         # Migration implementation
├── aggregate.go        # Aggregate functions (Count, Sum, Avg, etc.)
├── model/              # Core model definitions
│   ├── model.go        # Data structures for queries and migrations
│   ├── interface.go    # Driver, Connection, Transaction interfaces
│   └── enum.go         # Query type and join type enumerations
├── drivers/            # Database drivers
│   ├── pgsql/          # PostgreSQL driver
│   └── sqlite/         # SQLite driver
├── utils/              # Utility functions
└── example/            # Example usage
```

## Key Concepts

### Builder Pattern

The `Builder` struct in `builder.go` is the core of query construction:
- `Type`: Query type (SELECT, INSERT, UPDATE, DELETE)
- `Table`: Target table info
- `Columns`: Selected columns
- `Where`: WHERE conditions
- `Changes`: SET clause for UPDATE
- `Joins`: JOIN clauses
- `Orders`: ORDERBY clauses
- `Groups`: GROUPBY clauses
- `Limit`, `Offset`: Pagination
- `Returning`: RETURNING clause for INSERT

### Table and Field Mapping

Tables are defined using structs with `goe` tags:
```go
type Product struct {
    ID         int64
    CategoryID int64  `goe:"m2o"`           // Many-to-one foreign key
    Name       string `goe:"unique"`        // Unique index
    Price      float64
    Category   *Category `goe:"-"`           // Excluded from mapping
}

type Default struct {
    ID   string `goe:"default:'Default'"` // Primary key with default value
    Name string
}
```

### Column Metadata

The `Column` struct in `column.go` stores column metadata:
- `FieldName`: Go struct field name
- `ColumnName`: Database column name (snake_case)
- `ColumnType`: Database column type
- `AllowNull`: Whether the column allows NULL
- `HasDefault`: Whether the column has a default value
- `DefaultValue`: The default value from struct tag
- `isAutoIncr`: Whether the column is auto-increment

### Foreign Key Relationships

Four types of relationships are supported:
- `O2O` (One-to-One): Single record association
- `O2M` (One-to-Many): One record has many children
- `M2O` (Many-to-One): Many records belong to one parent
- `M2M` (Many-to-Many): Through a junction table

### Query Building

Use the fluent API:
```go
// Select with conditions
animals, err := db.Animal.Select().
    Where(goent.Equals(db.Animal.Field("id"), 2)).
    All()

// Insert with returning
err := db.Animal.Insert().One(&animal)

// Update with conditions
err := db.Animal.Update().
    Set(goent.Pair{Key: "name", Value: "Cat"}).
    Filter(goent.Equals(db.Animal.Field("id"), 1)).
    Exec()

// Delete with conditions
err := db.Animal.Delete().
    Filter(goent.Like(db.Animal.Field("name"), "%Cat%")).
    Exec()
```

## Code Conventions

### Naming
- Use English comments for public functions
- Preserve example code in comments
- Use descriptive variable names

### Error Handling
- Return errors as the last return value
- Use `model.Query.Err` for query errors
- Log errors using the `Logger` interface

### Testing
- Tests are in the `tests/` directory
- Use table-driven tests
- Clean up test data after each test

## Database Drivers

Drivers must implement the `model.Driver` interface:
- `MigrateContext`: Run migrations
- `DropTable`, `DropColumn`, `RenameColumn`, `RenameTable`: Schema modifications
- `KeywordHandler`: Escape reserved keywords
- `NewConnection`, `NewTransaction`: Connection management

## Migration System

Tables are auto-migrated from struct definitions:
1. `AutoMigrate` scans registered tables
2. `migrateFrom` builds `TableMigrate` structures
3. Driver's `MigrateContext` generates and executes SQL

Foreign keys are detected from:
- `m2o` tag for many-to-one relationships
- Field naming convention (`CategoryID` -> `Category`)
- Table registry lookup for tables nested under schemas

### Two-Phase Migration

The migration process is split into two phases to handle table dependencies:

1. **Phase 1 - Create Tables**: All tables are created without foreign key constraints. This ensures tables can be created in any order, even with circular references.

2. **Phase 2 - Add Foreign Keys**: After all tables exist, foreign key constraints are added via `ALTER TABLE` statements.

This approach:
- Prevents "relation does not exist" errors
- Preserves original field order in table definitions
- Handles complex dependency graphs between tables

### Schema Organization

The database structure follows a strict hierarchy:
- **Database** -> **Schema** -> **Table**
- Tables must be nested under schema structs
- SQLite ignores schema names (tables are created directly)

```go
type Database struct {
    Authentication AuthenticationSchema
    Inventory     InventorySchema
}

type AuthenticationSchema struct {
    User *goent.Table[User]
    Role *goent.Table[Role]
}

type InventorySchema struct {
    Product *goent.Table[Product]
}
```

## Insert Operations

### Auto-Increment Primary Keys

For tables with auto-increment primary keys:
- The primary key column is excluded from INSERT
- `last_insert_rowid()` is called to retrieve the generated ID
- The ID is set back on the struct

### Primary Keys with Default Values

For tables with non-auto-increment primary keys that have default values:
- The default value is extracted from the struct tag
- The value is set on the struct before INSERT
- The primary key column is included in the INSERT statement
- `last_insert_rowid()` is NOT called (only works for auto-increment)

```go
type Default struct {
    ID   string `goe:"default:'Default'"` // Default value 'Default'
    Name string
}

d := Default{Name: "Test"}
db.Default.Insert().One(&d)
// d.ID == "Default" (set from default value)
```

## Important Notes

1. **Do not use `Query` struct directly** - Use the `Builder` pattern instead
2. **Do not use `Attribute` type directly** - Use `Field` and `Condition` instead
3. **Always use pointers for Insert/Update/Save** - `One(obj *T)` not `One(obj T)`
4. **Foreign key columns must maintain original order** - Don't remove from Attributes
5. **`last_insert_rowid()` only works for auto-increment columns** - Not for default values

## Common Tasks

### Adding a new query type
1. Add the type to `model.QueryType`
2. Create a new state struct (e.g., `StateSelect`)
3. Implement builder methods
4. Add driver support for the new query

### Adding a new database driver
1. Create a new package under `drivers/`
2. Implement `model.Driver` interface
3. Implement `Connection` and `Transaction` interfaces
4. Add migration support

### Adding a new WHERE condition
1. Add the function to `where.go`
2. Return a `Condition` struct with template and fields
3. Update builder to handle the condition

## Build and Test Commands

```bash
# Build the project
go build ./...

# Run tests
go test ./tests/...

# Run example
cd example && go run main.go
```
