package goent

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/azhai/goent/model"
)

var (
	// addrMap       = &goeMap{mapField: make(map[uintptr]field)}
	schemaRegistry = make(map[string]*string)
	tableRegistry  = make(map[uintptr]*TableInfo)
)

// GetTableInfo returns the table info for a given table address.
func GetTableInfo(addr uintptr) *TableInfo {
	if addr == 0 {
		return nil
	}
	if info, ok := tableRegistry[addr]; ok {
		return info
	}
	return nil
}

// GetTableColumn returns the column info for a given table address and column name.
func GetTableColumn(addr uintptr, name string) *Column {
	if info := GetTableInfo(addr); info != nil {
		if col, ok := info.Columns[name]; ok {
			return col
		}
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
	return "", fmt.Errorf("field %s or table not found", name)
}

type goeMap struct {
	mu       sync.Mutex
	mapField map[uintptr]field
}

func (am *goeMap) get(key uintptr) field {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.mapField[key]
}

func (am *goeMap) set(key uintptr, value field) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.mapField[key] = value
}

func (am *goeMap) delete(key uintptr) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.mapField, key)
}

// DB represents a database connection with its driver.
type DB struct {
	driver model.Driver
}

// SetDriver Sets the database driver.
func (db *DB) SetDriver(driver model.Driver) {
	db.driver = driver
}

// Name Get the database name; SQLite, PostgreSQL...
func (db *DB) Name() string {
	return db.driver.Name()
}

// Stats Return the database stats as [sql.DBStats].
func (db *DB) Stats() sql.DBStats {
	return db.driver.Stats()
}

func (db *DB) RawQueryContext(ctx context.Context, rawSql string, args ...any) (model.Rows, error) {
	conn := db.driver.NewConnection()
	cfg := db.driver.GetDatabaseConfig()
	hd := NewHandler(ctx, conn, cfg)
	qr := model.CreateQuery(rawSql, args)
	return hd.QueryResult(qr)
}

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
// to specify the context and the isolation level, use [NewTransactionContext]
func (db *DB) NewTransaction() (model.Transaction, error) {
	return db.NewTransactionContext(context.Background(), sql.LevelDefault)
}

func (db *DB) NewTransactionContext(ctx context.Context, isolation sql.IsolationLevel) (model.Transaction, error) {
	t, err := db.driver.NewTransaction(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		dc := db.driver.GetDatabaseConfig()
		return nil, dc.ErrorHandler(ctx, err)
	}
	return t, nil
}

// BeginTransaction Begin a Transaction with the database default level, any panic or error will trigger a rollback.
//
// BeginTransaction uses [context.Background] internally;
// to specify the context and the isolation level, use [BeginTransactionContext]
//
// # Example
//
//	err = db.BeginTransaction(func(tx goent.Transaction) error {
//		cat := Animal{
//			Name: "Cat",
//		}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(&cat); err != nil {
//			return err // try a rollback
//		}
//
//		dog := Animal{
//			Name: "Dog",
//		}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(&dog); err != nil {
//			return err // try a rollback
//		}
//		return nil // try a commit
//	})
//
//	if err != nil {
//		//begin transaction error...
//	}
func (db *DB) BeginTransaction(txFunc func(Transaction) error) error {
	return db.BeginTransactionContext(context.Background(), sql.LevelDefault, txFunc)
}

// BeginTransactionContext Begin a Transaction, any panic or error will trigger a rollback.
//
// # Example
//
//	err = db.BeginTransactionContext(context.Background(), sql.LevelSerializable, func(tx goent.Transaction) error {
//		cat := Animal{
//			Name: "Cat",
//		}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(&cat); err != nil {
//			return err // try a rollback
//		}
//
//		dog := Animal{
//			Name: "Dog",
//		}
//		if err = goent.Insert(db.Animal).OnTransaction(tx).One(&dog); err != nil {
//			return err // try a rollback
//		}
//		return nil // try a commit
//	})
//
//	if err != nil {
//		//begin transaction error...
//	}
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

// Close closes the database connection and cleans up the table registry.
func Close(ent any) error {
	goeDb := getDatabase(ent)
	err := goeDb.driver.Close()
	if err != nil {
		dc := goeDb.driver.GetDatabaseConfig()
		return dc.ErrorHandler(context.TODO(), err)
	}

	for _, table := range tableRegistry {
		delete(tableRegistry, table.TableAddr)
	}

	// valueOf := reflect.ValueOf(ent).Elem()

	// for i := range valueOf.NumField() - 1 {
	// 	fieldOf := valueOf.Field(i)
	// 	if fieldOf.Kind() == reflect.Ptr {
	// 		if fieldOf.IsNil() {
	// 			continue
	// 		}
	// 		fieldOf = fieldOf.Elem()
	// 	}
	// 	for fieldId := range fieldOf.NumField() {
	// 		addrMap.delete(uintptr(fieldOf.Field(fieldId).Addr().UnsafePointer()))
	// 	}
	// }

	return nil
}

func getDatabase(ent any) *DB {
	valueOf := reflect.ValueOf(ent).Elem()
	return valueOf.Field(valueOf.NumField() - 1).Interface().(*DB)
}
