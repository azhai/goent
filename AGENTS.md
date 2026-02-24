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

### Table Name Resolution

Table names are resolved in the following order:
1. **`TableName()` method** - If the struct implements `TableName() string`, that value is used directly
2. **Struct name in snake_case with prefix** - If no `TableName()` method, the struct name is converted to snake_case, with optional `prefix:` from schema tag prepended

The schema tag format is: `goe:"schema_name;prefix:table_prefix"`

```go
// Method 1: TableName() method (returns exact table name, no prefix added)
type OrderDetail struct {
    OrderID   int64 `goe:"pk;not_incr"`
    ProductID int64 `goe:"pk;not_incr"`
}

func (*OrderDetail) TableName() string {
    return "t_order_detail"  // Exact table name, prefix is ignored
}

// Method 2: Auto-generated from struct name with prefix
type PublicSchema struct {
    User        *goent.Table[User]        // Table: t_user (with prefix t_)
    Category    *goent.Table[Category]    // Table: t_category
}

type Database struct {
    PublicSchema `goe:"public;prefix:t_"`  // schema=public, prefix=t_
    *goent.DB
}

// Without prefix
type AuthSchema struct {
    Role *goent.Table[Role]  // Table: role (no prefix)
}

type Database struct {
    AuthSchema `goe:"auth"`  // schema=auth, no prefix
    *goent.DB
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

### LeftJoin Helper Method

The `LeftJoin` method provides a convenient way to perform LEFT JOIN operations:

```go
// LeftJoin automatically selects columns from the joined table
orderDetails, err := db.OrderDetail.Select().
    LeftJoin("product_id", db.Product.Field("id")).
    Filter(goent.Equals(db.OrderDetail.Field("order_id"), orderID)).
    All()
```

Key behaviors:
- Automatically adds joined table's columns to SELECT list
- Supports chaining multiple joins
- Only populates non-slice foreign fields (skips slice relationships like `Jobs []JobTitle`)
- For columns that cannot be mapped to struct fields, uses dummy variables to receive values

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

### Foreign Key Auto-Generation

Foreign keys are automatically generated for relationship fields:
- `o2o` tag: Creates a one-to-one relationship with unique constraint
- `m2o` tag: Creates a foreign key column pointing to the parent table
- `o2m` tag: Creates a foreign key column in the child table
- `m2m` tag: Creates a junction table with foreign keys to both sides
- Field naming convention: `CategoryID` automatically links to `Category` table
- The foreign key column is added in the correct position to preserve field order

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
- `last_insert_rowid()` (SQLite) or `RETURNING` clause (PostgreSQL) is used to retrieve the generated ID
- The ID is set back on the struct

### Non-Auto-Increment Primary Keys (`not_incr` tag)

Use the `not_incr` tag to prevent auto-increment behavior:
- The primary key column is included in the INSERT statement
- No `last_insert_rowid()` or `RETURNING` clause is used
- Useful for UUID, string, or composite primary keys

```go
type User struct {
    ID   string `goe:"pk;not_incr;default:uuid_generate_v4()"`
    Name string
}
```

### Composite Primary Keys

Multiple fields with `pk` tag form a composite primary key:
- All `pk` fields must have `not_incr` tag to prevent auto-increment
- Batch inserts with `All(true, ...)` will not return generated IDs (composite keys are known)

```go
type OrderDetail struct {
    OrderID   int `goe:"pk;not_incr"`
    ProductID int `goe:"pk;not_incr"`
    Quantity  int
}
```

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
6. **Use `not_incr` tag for non-auto-increment primary keys** - UUID, string, or composite keys
7. **`[]byte` is recognized as a valid slice type** - Not treated as a relationship field
8. **PostgreSQL uses `RETURNING` clause** - SQLite uses `last_insert_rowid()` for auto-increment
9. **LeftJoin auto-selects joined table columns** - No need to manually specify columns
10. **Slice foreign fields are skipped in LeftJoin** - Use standard `Join` for slice relationships

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
