package goent

import (
	"context"
	"database/sql"
	"reflect"
	"sync"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

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

var addrMap *goeMap

type DB struct {
	driver model.Driver
}

// Stats Return the database stats as [sql.DBStats].
func (db *DB) Stats() sql.DBStats {
	return db.driver.Stats()
}

// Name Get the database name; SQLite, PostgreSQL...
func (db *DB) Name() string {
	return db.driver.Name()
}

func (db *DB) RawQueryContext(ctx context.Context, rawSql string, args ...any) (model.Rows, error) {
	query := model.Query{Type: enum.RawQuery, RawSql: rawSql, Arguments: args}
	var rows model.Rows
	rows, query.Header.Err = wrapperQuery(ctx, db.driver.NewConnection(), &query)
	dc := db.driver.GetDatabaseConfig()
	if query.Header.Err != nil {
		return nil, dc.ErrorQueryHandler(ctx, query)
	}
	dc.InfoHandler(ctx, query)
	return rows, nil
}

func (db *DB) RawExecContext(ctx context.Context, rawSql string, args ...any) error {
	query := model.Query{Type: enum.RawQuery, RawSql: rawSql, Arguments: args}
	query.Header.Err = wrapperExec(ctx, db.driver.NewConnection(), &query)
	dc := db.driver.GetDatabaseConfig()
	if query.Header.Err != nil {
		return dc.ErrorQueryHandler(ctx, query)
	}
	dc.InfoHandler(ctx, query)
	return nil
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

// Close Closes the database connection.
func Close(dbTarget any) error {
	goeDb := getDatabase(dbTarget)
	err := goeDb.driver.Close()
	if err != nil {
		dc := goeDb.driver.GetDatabaseConfig()
		return dc.ErrorHandler(context.TODO(), err)
	}

	valueOf := reflect.ValueOf(dbTarget).Elem()

	for i := range valueOf.NumField() - 1 {
		fieldOf := valueOf.Field(i).Elem()
		for fieldId := range fieldOf.NumField() {
			addrMap.delete(uintptr(fieldOf.Field(fieldId).Addr().UnsafePointer()))
		}
	}

	return nil
}

func getDatabase(dbTarget any) *DB {
	valueOf := reflect.ValueOf(dbTarget).Elem()
	return valueOf.Field(valueOf.NumField() - 1).Interface().(*DB)
}
