package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/model"
)

type StateWhere struct {
	builder *Builder
	conn    model.Connection
	ctx     context.Context
}

func NewStateWhere(ctx context.Context) *StateWhere {
	return &StateWhere{ctx: ctx, builder: GetBuilder()}
}

func MatchWhere[T any](s *StateWhere, table *Table[T], obj T) *StateWhere {
	if et, ok := any(obj).(Entity); ok {
		pkey, id := table.PrimaryKeys[0], et.GetID()
		col := table.TableInfo.Field(pkey.ColumnName)
		return s.Filter(Equals(col, id))
	}
	data, valueOf := make(dict), reflect.ValueOf(obj)
	for _, key := range table.PrimaryKeys {
		data[key.ColumnName] = valueOf.FieldByName(key.FieldName).Interface()
	}
	col := &Field{Table: table.TableAddr}
	return s.Filter(EqualsMap(col, data))
}

func (s *StateWhere) Filter(args ...Condition) *StateWhere {
	cond := And(args...)
	if s.builder.Where.IsEmpty() {
		s.builder.Where = cond
	} else {
		s.builder.Where = And(s.builder.Where, cond)
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
	return handlerValues(s.ctx, s.conn, s.builder, dc)
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

func (s *StateDelete[T]) Filter(args ...Condition) *StateDelete[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}
