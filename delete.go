package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/query/where"
)

type StateWhere struct {
	builder builder
	conn    model.Connection
	ctx     context.Context
}

func NewStateWhere(ctx context.Context) *StateWhere {
	return &StateWhere{ctx: ctx, builder: createBuilder(0)}
}

func MatchWhere[T any](s *StateWhere, table *Table[T], obj T) *StateWhere {
	if et, ok := any(obj).(Entity); ok {
		pkey, id := table.PrimaryKeys[0], et.GetID()
		return s.Filter(where.EqualsTable(table, pkey.ColumnName, id))
	}
	data, valueOf := make(dict), reflect.ValueOf(obj)
	for _, key := range table.PrimaryKeys {
		data[key.ColumnName] = valueOf.FieldByName(key.FieldName).Interface()
	}
	return s.Filter(where.EqualsMap(table, data))
}

func (s *StateWhere) Filter(args ...model.Operation) *StateWhere {
	for _, arg := range args {
		helperWhere(&s.builder, addrMap.mapField, arg)
	}
	return s
}

func (s *StateWhere) OnTransaction(tx model.Transaction) *StateWhere {
	s.conn = tx
	return s
}

func (s *StateWhere) prepare(drv model.Driver) *model.DatabaseConfig {
	if s.conn == nil {
		s.conn = drv.NewConnection()
	}
	return drv.GetDatabaseConfig()
}

func (s *StateWhere) exec(drv model.Driver) error {
	// TODO: add table to builder
	dc := s.prepare(drv)
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

type StateDelete[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateDelete[T]) Exec() error {
	// TODO: add table to builder
	return s.exec(s.table.db.driver)
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
//	err = db.Animal.Delete().OnTransaction(tx).All()
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (s *StateDelete[T]) OnTransaction(tx model.Transaction) *StateDelete[T] {
	s.StateWhere.conn = tx
	return s
}

func (s *StateDelete[T]) Filter(args ...model.Operation) *StateDelete[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}
