package goent

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/azhai/gobus"
	"github.com/azhai/goent/model"
)

var (
	schemaRegistry = make(map[string]*string)
	tableRegistry  = make(map[uintptr]*TableInfo)
	tableRegLock   sync.RWMutex
)

// ResetRegistry clears all registered schemas and tables
// This is useful for testing purposes
func ResetRegistry() {
	tableRegLock.Lock()
	defer tableRegLock.Unlock()
	schemaRegistry = make(map[string]*string)
	tableRegistry = make(map[uintptr]*TableInfo)
}

// GetTableInfo returns the table info for a given table address
// It looks up the table information from the registry
func GetTableInfo(addr uintptr) *TableInfo {
	if addr == 0 {
		return nil
	}
	tableRegLock.RLock()
	defer tableRegLock.RUnlock()
	if info, ok := tableRegistry[addr]; ok {
		return info
	}
	return nil
}

// GetTableColumn returns the column info for a given table address and column name
// It looks up the column information from the table registry
func GetTableColumn(addr uintptr, name string) (*Column, string) {
	if addr == 0 {
		return nil, ""
	}
	if info := GetTableInfo(addr); info != nil {
		return info.ColumnInfo(name), info.TableName
	}
	return nil, ""
}

// GetTableFieldName returns the qualified field name (table.column) for a given table address and column name
// If the address is 0, it returns just the column name
func GetTableFieldName(addr uintptr, name string) (string, error) {
	if addr == 0 {
		return name, nil
	}
	if info, table := GetTableColumn(addr, name); info != nil {
		return fmt.Sprintf("%s.%s", table, name), nil
	}
	return "", model.NewFieldNotFoundError(name)
}

// DB represents a database connection with its driver
// It provides methods for executing queries and managing transactions
type DB struct {
	bus    *gobus.EventBus
	driver model.Driver // Database driver implementation
}

// SetDriver sets the database driver
// It replaces the current driver with the provided one
func (db *DB) SetDriver(driver model.Driver) {
	db.driver = driver
}

// DriverName returns the database name (SQLite, PostgreSQL, etc.)
// It returns the name of the underlying driver
func (db *DB) DriverName() string {
	return db.driver.Name()
}

// Driver returns the underlying database driver
func (db *DB) Driver() model.Driver {
	return db.driver
}

// Stats returns the database stats as [sql.DBStats]
// It returns statistics about the underlying database connection
func (db *DB) Stats() sql.DBStats {
	return db.driver.Stats()
}

// Watch subscribes the provided tables to the event bus for modification events.
// It marks the tables as watched, enabling event notifications on INSERT, UPDATE, and DELETE.
//
// Only single-table modification operations emit events:
//   - INSERT (One/All): emits ent:insert-one / ent:insert-bulk
//   - UPDATE (Exec/ByPK/UpdateByID): emits ent:update / ent:update-bypk / ent:update-byid
//   - DELETE (Exec/ByPK/DeleteByID): emits ent:delete / ent:delete-bypk / ent:delete-byid
//
// The following operations are excluded from event notifications:
//   - JOIN updates (multi-table updates via Join/LeftJoin)
//   - Subquery updates
//   - Clear-all deletes (DELETE without a WHERE clause)
//
// Event data fields:
//   - model:     Go struct type name (e.g. "Animal")
//   - table:     database table name (e.g. "animals")
//   - where:     WHERE clause template string (empty for insert/bypk)
//   - ids:       affected primary key IDs
//   - changes:   column changes map (only for single insert and updates)
//   - affecteds: number of affected rows
//   - trans_no:  transaction identifier string (empty if not in a transaction)
//
// Example:
//
//	bus := gobus.NewEventBus(1024)
//	db.Watching(bus, db.Animal.TableInfo, db.User.TableInfo)
func (db *DB) Watching(bus *gobus.EventBus, tables ...*TableInfo) {
	if db.bus = bus; db.bus == nil {
		return
	}
	for _, table := range tables {
		table.isWatched = true
	}
}

// RawExecContext executes a raw SQL statement without returning rows
// It executes the provided SQL with the given arguments
//
// Example:
//
//	err := db.RawExecContext(ctx, "UPDATE users SET name = ? WHERE id = ?", "John", 1)
func (db *DB) RawExecContext(ctx context.Context, rawSql string, args ...any) error {
	conn := db.driver.NewConnection()
	dc := db.driver.GetDatabaseConfig()
	qr := model.CreateQuery(db.driver.NormalizeSql(rawSql), args)
	return qr.WrapExec(ctx, conn, dc)
}

// RawQueryContext executes a raw SQL query and returns rows
// It executes the provided SQL with the given arguments and returns the result set
//
// Example:
//
//	rows, err := db.RawQueryContext(ctx, "SELECT * FROM users WHERE id = ?", 1)
//	if err != nil {
//		return err
//	}
//	defer rows.Close()
//	for rows.Next() {
//		// scan rows
//	}
func (db *DB) RawQueryContext(ctx context.Context, rawSql string, args ...any) (model.Rows, error) {
	conn := db.driver.NewConnection()
	dc := db.driver.GetDatabaseConfig()
	qr := model.CreateQuery(db.driver.NormalizeSql(rawSql), args)
	return qr.WrapQuery(ctx, conn, dc)
}

// RawQueryRowContext executes a raw SQL query and returns a single row.
// It returns a non-nil Row whose Scan will report the underlying error
// (including sql.ErrNoRows when no row matches), so callers can safely
// chain .Scan(...) without a nil check.
func (db *DB) RawQueryRowContext(ctx context.Context, rawSql string, args ...any) model.Row {
	conn := db.driver.NewConnection()
	dc := db.driver.GetDatabaseConfig()
	qr := model.CreateQuery(db.driver.NormalizeSql(rawSql), args)
	rows, err := qr.WrapQuery(ctx, conn, dc)
	if err != nil {
		return errRow{err: err}
	}
	if rows.Next() {
		return rows
	}
	rows.Close()
	return errRow{err: sql.ErrNoRows}
}

// errRow is a Row implementation that always returns the same error from Scan.
// It is returned by RawQueryRowContext when the query fails or yields no rows,
// so callers do not need to nil-check the returned Row.
type errRow struct {
	err error
}

func (r errRow) Scan(_ ...any) error {
	return r.err
}

// NewTransaction creates a new Transaction on the database using the default level
// It uses [context.Background] internally
// To specify the context and the isolation level, use [NewTransactionContext]
func (db *DB) NewTransaction() (model.Transaction, error) {
	return db.NewTransactionContext(context.Background(), sql.LevelDefault)
}

func (db *DB) NewTransactionContext(ctx context.Context, isolation sql.IsolationLevel) (model.Transaction, error) {
	tx, err := db.driver.NewTransaction(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		dc := db.driver.GetDatabaseConfig()
		return nil, dc.ErrorHandler(ctx, err)
	}
	return tx, nil
}

// BeginTransaction begins a Transaction with the database default level
// Any panic or error will trigger a rollback
// It uses [context.Background] internally
// To specify the context and the isolation level, use [BeginTransactionContext]
//
// Example:
//
//	err = db.BeginTransaction(func(tx model.Transaction) error {
//		cat := &Animal{Name: "Cat"}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(cat); err != nil {
//			return err // triggers rollback
//		}
//
//		dog := &Animal{Name: "Dog"}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(dog); err != nil {
//			return err // triggers rollback
//		}
//		return nil // commits transaction
//	})
func (db *DB) BeginTransaction(exec ExecuteTx) error {
	return db.BeginTransactionContext(context.Background(), sql.LevelDefault, exec)
}

// BeginTransactionContext begins a Transaction with the specified context and isolation level
// Any panic or error will trigger a rollback
//
// Example:
//
//	err = db.BeginTransactionContext(context.Background(), sql.LevelSerializable, func(tx model.Transaction) error {
//		cat := &Animal{Name: "Cat"}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(cat); err != nil {
//			return err // triggers rollback
//		}
//
//		dog := &Animal{Name: "Dog"}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(dog); err != nil {
//			return err // triggers rollback
//		}
//		return nil // commits transaction
//	})
func (db *DB) BeginTransactionContext(ctx context.Context, isolation sql.IsolationLevel, exec ExecuteTx) (err error) {
	var tx model.Transaction
	if tx, err = db.NewTransactionContext(ctx, isolation); err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	if err = exec(tx); err != nil {
		_ = tx.Rollback()
		return
	}
	return tx.Commit()
}

// DropTables drops all registered tables from the database
// It generates and executes DROP TABLE statements for all registered tables
// For PostgreSQL, it adds CASCADE to the DROP TABLE statement
func (db *DB) DropTables() error {
	var tables []string
	tableRegLock.RLock()
	defer tableRegLock.RUnlock()
	for _, info := range tableRegistry {
		tables = append(tables, info.TableName)
	}
	if len(tables) == 0 {
		return nil
	}
	return NewSchemaOps(db).DropTables(context.Background(), tables)
}

// TruncateTable truncates the specified table.
// For PostgreSQL it uses TRUNCATE ... RESTART IDENTITY CASCADE,
// for SQLite it falls back to DELETE FROM plus sqlite_sequence cleanup.
func (db *DB) TruncateTable(schema, table string) error {
	if db == nil || db.driver == nil {
		return model.ErrDBNotFound
	}
	return db.driver.TruncateTable(schema, table)
}

// Upsert inserts a single row or updates it when the conflict columns already exist.
// The table name may include a schema prefix. For PostgreSQL it uses ON CONFLICT,
// for SQLite it uses INSERT OR REPLACE.
func (db *DB) Upsert(table string, columns, conflictCols []string, values ...any) error {
	if db == nil || db.driver == nil {
		return model.ErrDBNotFound
	}
	return db.driver.Upsert(table, columns, conflictCols, values)
}

// Close closes the database connection and cleans up the table registry
// It closes the underlying driver connection and resets the registry
func Close(ent any) error {
	goeDb := getDatabase(ent)
	err := goeDb.driver.Close()
	if err != nil {
		dc := goeDb.driver.GetDatabaseConfig()
		return dc.ErrorHandler(context.TODO(), err)
	}

	ResetRegistry()
	return nil
}

func getDatabase(ent any) *DB {
	valueOf := reflect.ValueOf(ent).Elem()
	return valueOf.Field(valueOf.NumField() - 1).Interface().(*DB)
}

type ExecuteTx func(model.Transaction) error

func RunTransaction(tx model.Transaction, exec ExecuteTx) (err error) {
	var sp model.SavePoint
	if sp, err = tx.SavePoint(); err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			sp.Rollback()
		}
	}()
	if err = exec(tx); err != nil {
		sp.Rollback()
		return
	}
	return sp.Commit()
}
