package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

// StateWhere represents a query state with WHERE clause building capabilities.
type StateWhere struct {
	builder *Builder
	conn    model.Connection
	ctx     context.Context
}

// NewStateWhere creates a new StateWhere with the given context.
func NewStateWhere(ctx context.Context) *StateWhere {
	return &StateWhere{ctx: ctx, builder: GetBuilder()}
}

// MatchWhere creates a StateWhere with conditions matching the non-zero fields of the given object.
func MatchWhere[T any](s *StateWhere, table *Table[T], obj T) *StateWhere {
	valueOf := reflect.Indirect(reflect.ValueOf(obj))
	data := make(Dict)
	for _, col := range table.Columns {
		fieldOf := valueOf.FieldByName(col.FieldName)
		if fieldOf.IsZero() || fieldOf.Kind() == reflect.Ptr && fieldOf.IsNil() {
			continue
		}
		data[col.ColumnName] = fieldOf.Interface()
	}
	if len(data) == 0 {
		return nil
	}
	col := &Field{Table: table.TableAddr}
	return s.Filter(EqualsMap(col, data))
}

func (s *StateWhere) Filter(conds ...Condition) *StateWhere {
	if !s.builder.Where.IsEmpty() {
		conds = append(conds, s.builder.Where)
	}
	s.builder.Where = And(conds...)
	return s
}

func (s *StateWhere) Where(where string, args ...any) *StateWhere {
	cond := Condition{Template: where, Values: make([]*Value, len(args))}
	for i, arg := range args {
		cond.Values[i] = NewValue(arg)
	}
	if !s.builder.Where.IsEmpty() {
		cond = And(s.builder.Where, cond)
	}
	s.builder.Where = cond
	return s
}

func (s *StateWhere) OnTransaction(tx model.Transaction) *StateWhere {
	s.conn = tx
	return s
}

func (s *StateWhere) Prepare(drv model.Driver) *Handler {
	if s.conn == nil {
		s.conn = drv.NewConnection()
	}
	cfg := drv.GetDatabaseConfig()
	return NewHandler(s.ctx, s.conn, cfg)
}

// StateDelete represents a DELETE query state for removing records from a table.
type StateDelete[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateDelete[T]) Exec() error {
	s.builder.Type = enum.DeleteQuery
	s.builder.SetTable(s.table.TableInfo)
	qr := model.CreateQuery(s.builder.Build())
	hd := s.Prepare(s.table.db.driver)
	return hd.ExecuteNoReturn(qr)
}

func (s *StateDelete[T]) OnTransaction(tx model.Transaction) *StateDelete[T] {
	s.StateWhere.conn = tx
	return s
}

func (s *StateDelete[T]) Filter(args ...Condition) *StateDelete[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}
