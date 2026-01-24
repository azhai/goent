package goent

import (
	"context"
	"errors"
	"reflect"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type StateInsert[T any] struct {
	conn    model.Connection
	table   *Table[T]
	builder builder
	ctx     context.Context
}

// Insert inserts a new record into the given table.
//
// Insert can return [ErrUniqueValue, ErrForeignKey and ErrBadRequest];
// use ErrBadRequest as a generic error for any user interaction.
//
// Insert uses [context.Background] internally;
// to specify the context, use [InsertContext].
//
// # Examples
//
//	// insert one record
//	err = goent.Insert(db.Person).One(&Person{Name: "John"})
//	// insert a list of records
//
//	persons := []Person{{Name: "John"}, {Name: "Mary"}}
//	err = goent.Insert(db.Person).All(persons)
func Insert[T any](table *T) StateInsert[T] {
	return InsertContext(context.Background(), table)
}

// InsertContext inserts a new record into the given table.
//
// See [Insert] for examples.
func InsertContext[T any](ctx context.Context, table *T) StateInsert[T] {
	return createInsertState(ctx, NewTableModel(table))
}

// InsertTable inserts a new record into the given table.
func InsertTable[T any](ctx context.Context, table *Table[T]) StateInsert[T] {
	return InsertTableContext(context.Background(), table)
}

// InsertTableContext inserts a new record into the given table.
func InsertTableContext[T any](ctx context.Context, table *Table[T]) StateInsert[T] {
	return createInsertState(ctx, table)
}

// OnTransaction sets a transaction on the query.
//
// # Example
//
//	tx, err = db.NewTransaction()
//	if err != nil {
//		// handler error
//	}
//	defer tx.Rollback()
//
//	a := Animal{Name: "Cat"}
//	err = goent.Insert(db.Animal).OnTransaction(tx).One(&a)
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (s StateInsert[T]) OnTransaction(tx model.Transaction) StateInsert[T] {
	s.conn = tx
	return s
}

func (s StateInsert[T]) One(value *T) error {
	if value == nil {
		return errors.New("goent: invalid insert value. try sending a pointer to a struct as value")
	}
	valueOf := reflect.ValueOf(value).Elem()

	s.builder.fields = getArgsTable(addrMap.mapField, s.table.Model, valueOf)

	pkFieldId := s.builder.buildSqlInsert(valueOf)

	driver := s.builder.fields[0].getDb().driver
	if s.conn == nil {
		s.conn = driver.NewConnection()
	}

	dc := driver.GetDatabaseConfig()
	if s.builder.query.ReturningID != nil {
		return handlerValuesReturning(s.ctx, s.conn, s.builder.query, valueOf, pkFieldId, dc)
	}
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

func (s StateInsert[T]) All(value []T) error {
	if len(value) == 0 {
		return errors.New("goent: can't insert a empty batch value")
	}
	valueOf := reflect.ValueOf(value)

	s.builder.fields = getArgsTable(addrMap.mapField, s.table.Model, valueOf)

	pkFieldId := s.builder.buildSqlInsertBatch(valueOf)

	driver := s.builder.fields[0].getDb().driver
	if s.conn == nil {
		s.conn = driver.NewConnection()
	}

	dc := driver.GetDatabaseConfig()
	return handlerValuesReturningBatch(s.ctx, s.conn, s.builder.query, valueOf, pkFieldId, dc)
}

func createInsertState[T any](ctx context.Context, t *Table[T]) StateInsert[T] {
	return StateInsert[T]{builder: createBuilder(enum.InsertQuery), ctx: ctx, table: t}
}

func getArgsTable(addrMap map[uintptr]field, table any, valueOf reflect.Value) []field {
	if table == nil {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}
	fields := make([]field, 0)

	tableValueOf := reflect.ValueOf(table).Elem()
	if tableValueOf.Kind() != reflect.Struct {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	var fieldOf reflect.Value
	for i := 0; i < tableValueOf.NumField(); i++ {
		fieldOf = tableValueOf.Field(i)
		if fieldOf.Kind() == reflect.Slice && fieldOf.Type().Elem().Kind() == reflect.Struct {
			continue
		}
		field := addrMap[uintptr(fieldOf.Addr().UnsafePointer())]
		if field != nil {
			if field.getDefault() && valueOf.Field(field.getFieldId()).IsZero() {
				continue
			}
			fields = append(fields, field)
		}
	}

	if len(fields) == 0 {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}
	return fields
}
