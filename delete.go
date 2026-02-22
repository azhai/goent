package goent

import (
	"context"
	"reflect"

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

// MatchData matches the non-zero fields of the given object to a dictionary of column names and values.
func MatchData[T any](table *Table[T], obj T) Dict {
	data := make(Dict)
	valueOf := reflect.Indirect(reflect.ValueOf(obj))
	for _, col := range table.Columns {
		fieldOf := valueOf.FieldByName(col.FieldName)
		if fieldOf.IsZero() || fieldOf.Kind() == reflect.Pointer && fieldOf.IsNil() {
			continue
		}
		data[col.ColumnName] = fieldOf.Interface()
	}
	return data
}

// MatchWhere creates a StateWhere with conditions matching the non-zero fields of the given object.
func MatchWhere[T any](s *StateWhere, table *Table[T], obj T) *StateWhere {
	data := MatchData(table, obj)
	if len(data) == 0 {
		return s
	}
	col := &Field{TableAddr: table.TableAddr}
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
	cond := Expr(where, args...)
	if !s.builder.Where.IsEmpty() {
		cond = And(s.builder.Where, cond)
	} else {
		s.builder.Where = cond
	}
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

func (s *StateDelete[T]) Match(obj T) *StateDelete[T] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

func (s *StateDelete[T]) Exec() error {
	s.builder.Type = model.DeleteQuery
	s.builder.SetTable(s.table.TableInfo)
	qr := model.CreateQuery(s.builder.Build(true))
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

func (s *StateDelete[T]) Where(where string, args ...any) *StateDelete[T] {
	s.StateWhere = s.StateWhere.Where(where, args...)
	return s
}
