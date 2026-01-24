package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type StateDelete struct {
	conn    model.Connection
	builder builder
	ctx     context.Context
}

type StateRemove[T any] struct {
	table  *T
	delete StateDelete
}

// Remove is a wrapper over [Delete] for more simple deletes,
// uses the value for create a where matching the primary keys.
//
// Remove uses [context.Background] internally;
// to specify the context, use [RemoveContext].
//
// # Examples
//
//	// StateRemove animal of id 2
//	err = goent.Remove(db.Animal).ByValue(Animal{Id: 2})
func Remove[T any](table *T) StateRemove[T] {
	return RemoveContext(context.Background(), table)
}

// RemoveContext is a wrapper over [Delete] for more simple deletes,
// uses the value for create a where matching the primary keys.
//
// See [Remove] for examples
func RemoveContext[T any](ctx context.Context, table *T) StateRemove[T] {
	return StateRemove[T]{table: table, delete: DeleteContext(ctx, table)}
}

// RemoveTable is a wrapper over [Delete] for more simple deletes
func RemoveTable[T any](table *Table[T]) StateRemove[T] {
	return RemoveTableContext(context.Background(), table)
}

// RemoveTableContext is a wrapper over [Delete] for more simple deletes
func RemoveTableContext[T any](ctx context.Context, table *Table[T]) StateRemove[T] {
	return StateRemove[T]{table: table.Model, delete: DeleteContext(ctx, table)}
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
//	err = err = goent.Remove(db.Animal).OnTransaction(tx).ByValue(Animal{ID: 2})
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (r StateRemove[T]) OnTransaction(tx model.Transaction) StateRemove[T] {
	r.delete.conn = tx
	return r
}

// ByValue removes the record by non-zero values
func (r StateRemove[T]) ByValue(value T) error {
	args, valuesArgs, skip := getNonZeroFields(getArgs{
		addrMap:   addrMap.mapField,
		tableArgs: getRemoveTableArgs(r.table),
		value:     value})

	if skip {
		return nil
	}

	return r.delete.Where(operations(args, valuesArgs))
}

// Delete StateRemove records in the given table
//
// Delete uses [context.Background] internally;
// to specify the context, use [DeleteContext].
//
// # Examples
//
//	// delete all records
//	err = goent.Delete(db.UserRole).All()
//	// delete one record
//	err = goent.Delete(db.Animal).Where(where.Equals(&db.Animal.ID, 2))
func Delete[T any](table *T) StateDelete {
	return DeleteContext(context.Background(), table)
}

// DeleteContext StateRemove records in the given table
//
// See [Delete] for examples
func DeleteContext[T any](ctx context.Context, table *T) StateDelete {
	var state = createDeleteState(ctx)
	state.builder.fields = append(state.builder.fields, getArgDelete(table, addrMap.mapField))
	return state
}

// DeleteTable StateRemove records in the given table
func DeleteTable[T any](table *Table[T]) StateDelete {
	return DeleteTableContext(context.Background(), table)
}

// DeleteTableContext StateRemove records in the given table
func DeleteTableContext[T any](ctx context.Context, table *Table[T]) StateDelete {
	var state = createDeleteState(ctx)
	state.builder.fields = append(state.builder.fields, getArgDelete(table.Model, addrMap.mapField))
	return state
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
//	err = goent.Delete(db.Animal).OnTransaction(tx).All()
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (s StateDelete) OnTransaction(tx model.Transaction) StateDelete {
	s.conn = tx
	return s
}

// All delete all records
func (s StateDelete) All() error {
	return s.Where(model.Operation{})
}

// Where receives [model.Operation] as where operations from where sub package
func (s StateDelete) Where(o model.Operation) error {
	helperWhere(&s.builder, addrMap.mapField, o)

	s.builder.buildSqlDelete()

	driver := s.builder.fields[0].getDb().driver
	if s.conn == nil {
		s.conn = driver.NewConnection()
	}

	dc := driver.GetDatabaseConfig()
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

func createDeleteState(ctx context.Context) StateDelete {
	return StateDelete{builder: createBuilder(enum.DeleteQuery), ctx: ctx}
}

func getArgDelete(arg any, addrMap map[uintptr]field) field {
	v := reflect.ValueOf(arg)
	if v.Kind() != reflect.Pointer {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	addr := uintptr(v.UnsafePointer())
	if addrMap[addr] != nil {
		return addrMap[addr]
	}

	return nil
}

func getRemoveTableArgs(table any) []any {
	valueOf := reflect.ValueOf(table).Elem()

	if valueOf.Kind() != reflect.Struct {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}
	args := make([]any, 0, valueOf.NumField())
	var fieldOf reflect.Value
	for i := 0; i < valueOf.NumField(); i++ {
		fieldOf = valueOf.Field(i)
		if fieldOf.Kind() == reflect.Slice && fieldOf.Type().Elem().Kind() == reflect.Struct {
			continue
		}

		args = append(args, fieldOf.Addr().Interface())
	}

	return args
}
