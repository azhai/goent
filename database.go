package goent

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/azhai/goent/model"
)

var (
	schemaRegistry = make(map[string]*string)
	tableRegistry  = make(map[uintptr]*TableInfo)
	tableRegLock   sync.RWMutex
)

// ResetRegistry clears all registered schemas and tables.
// This is useful for testing purposes.
func ResetRegistry() {
	tableRegLock.Lock()
	defer tableRegLock.Unlock()
	schemaRegistry = make(map[string]*string)
	tableRegistry = make(map[uintptr]*TableInfo)
}

// GetTableInfo returns the table info for a given table address.
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

// GetTableColumn returns the column info for a given table address and column name.
func GetTableColumn(addr uintptr, name string) *Column {
	if info := GetTableInfo(addr); info != nil {
		return info.ColumnInfo(name)
	}
	return nil
}

// GetFieldName returns the qualified field name (table.column) for a given table address and column name.
// If the address is 0, it returns just the column name.
func GetFieldName(addr uintptr, name string) (string, error) {
	if addr == 0 {
		return name, nil
	}
	if info := GetTableInfo(addr); info != nil {
		if _, ok := info.Check(name); ok {
			return fmt.Sprintf("%s.%s", info.String(), name), nil
		}
	}
	return "", NewFieldNotFoundError(name)
}

// DB represents a database connection with its driver.
type DB struct {
	driver model.Driver
}

// SetDriver sets the database driver.
func (db *DB) SetDriver(driver model.Driver) {
	db.driver = driver
}

// DriverName returns the database name (SQLite, PostgreSQL, etc.).
func (db *DB) DriverName() string {
	return db.driver.Name()
}

// Stats returns the database stats as [sql.DBStats].
func (db *DB) Stats() sql.DBStats {
	return db.driver.Stats()
}

// RawQueryContext executes a raw SQL query and returns rows.
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
	cfg := db.driver.GetDatabaseConfig()
	hd := NewHandler(ctx, conn, cfg)
	qr := model.CreateQuery(rawSql, args)
	return hd.QueryResult(qr)
}

// RawExecContext executes a raw SQL statement without returning rows.
//
// Example:
//
//	err := db.RawExecContext(ctx, "UPDATE users SET name = ? WHERE id = ?", "John", 1)
func (db *DB) RawExecContext(ctx context.Context, rawSql string, args ...any) error {
	conn := db.driver.NewConnection()
	cfg := db.driver.GetDatabaseConfig()
	hd := NewHandler(ctx, conn, cfg)
	qr := model.CreateQuery(rawSql, args)
	return hd.ExecuteNoReturn(qr)
}

// NewTransaction creates a new Transaction on the database using the default level.
//
// NewTransaction uses [context.Background] internally;
// to specify the context and the isolation level, use [NewTransactionContext].
func (db *DB) NewTransaction() (model.Transaction, error) {
	return db.NewTransactionContext(context.Background(), sql.LevelDefault)
}

// NewTransactionContext creates a new Transaction with the specified context and isolation level.
func (db *DB) NewTransactionContext(ctx context.Context, isolation sql.IsolationLevel) (model.Transaction, error) {
	t, err := db.driver.NewTransaction(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		dc := db.driver.GetDatabaseConfig()
		return nil, dc.ErrorHandler(ctx, err)
	}
	return t, nil
}

// BeginTransaction begins a Transaction with the database default level.
// Any panic or error will trigger a rollback.
//
// BeginTransaction uses [context.Background] internally;
// to specify the context and the isolation level, use [BeginTransactionContext].
//
// Example:
//
//	err = db.BeginTransaction(func(tx goent.Transaction) error {
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
func (db *DB) BeginTransaction(txFunc func(Transaction) error) error {
	return db.BeginTransactionContext(context.Background(), sql.LevelDefault, txFunc)
}

// BeginTransactionContext begins a Transaction with the specified context and isolation level.
// Any panic or error will trigger a rollback.
//
// Example:
//
//	err = db.BeginTransactionContext(context.Background(), sql.LevelSerializable, func(tx goent.Transaction) error {
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
func (db *DB) BeginTransactionContext(ctx context.Context, isolation sql.IsolationLevel, txFunc func(Transaction) error) (err error) {
	var t model.Transaction
	if t, err = db.NewTransactionContext(ctx, isolation); err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			t.Rollback()
		}
	}()
	if err = txFunc(Transaction{t}); err != nil {
		t.Rollback()
		return
	}
	return t.Commit()
}

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
	sql := "DROP TABLE IF EXISTS %s"
	if db.DriverName() == "PostgreSQL" {
		sql += " CASCADE"
	}
	sql = fmt.Sprintf(sql, strings.Join(tables, ", "))
	return db.RawExecContext(context.Background(), sql)
}

// Close closes the database connection and cleans up the table registry.
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
