package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/model"
)

// NilMarker is a special type to indicate a nil pointer field for IS NULL conditions.
type NilMarker struct{}

// StateDeleteWhere represents a query state with WHERE clause building capabilities for DELETE queries.
type StateDeleteWhere struct {
	builder *DeleteBuilder
	conn    model.Connection
	ctx     context.Context
}

// NewStateDeleteWhere creates a new StateDeleteWhere with the given context.
func NewStateDeleteWhere(ctx context.Context) *StateDeleteWhere {
	return &StateDeleteWhere{ctx: ctx, builder: NewDeleteBuilder()}
}

func (s *StateDeleteWhere) Filter(conds ...Condition) *StateDeleteWhere {
	if !s.builder.Where.IsEmpty() {
		conds = append(conds, s.builder.Where)
	}
	s.builder.Where = And(conds...)
	return s
}

func (s *StateDeleteWhere) Where(where string, args ...any) *StateDeleteWhere {
	cond := Expr(where, args...)
	if !s.builder.Where.IsEmpty() {
		cond = And(s.builder.Where, cond)
	} else {
		s.builder.Where = cond
	}
	return s
}

func (s *StateDeleteWhere) OnTransaction(tx model.Transaction) *StateDeleteWhere {
	s.conn = tx
	return s
}

func (s *StateDeleteWhere) Prepare(drv model.Driver) *Handler {
	if s.conn == nil {
		s.conn = drv.NewConnection()
	}
	cfg := drv.GetDatabaseConfig()
	return NewHandler(s.ctx, s.conn, cfg)
}

// StateWhere represents a query state with WHERE clause building capabilities.
type StateWhere struct {
	builder *Builder
	conn    model.Connection
	ctx     context.Context
}

// NewStateWhere creates a new StateWhere with the given context.
func NewStateWhere(ctx context.Context) *StateWhere {
	return &StateWhere{ctx: ctx, builder: NewBuilder()}
}

// MatchData matches the non-zero fields of the given object to a dictionary of column names and values.
// Nil pointer fields are skipped (not included in the result).
func MatchData[T any](table *Table[T], obj T) Dict {
	data := make(Dict)
	valueOf := reflect.Indirect(reflect.ValueOf(obj))
	for _, col := range table.Columns {
		fieldOf := valueOf.FieldByName(col.FieldName)
		if fieldOf.Kind() == reflect.Pointer && fieldOf.IsNil() {
			continue
		}
		if fieldOf.IsZero() {
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

// MatchDeleteWhere creates a StateDeleteWhere with conditions matching the non-zero fields of the given object.
func MatchDeleteWhere[T any](s *StateDeleteWhere, table *Table[T], obj T) *StateDeleteWhere {
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
	*StateDeleteWhere
}

// Match sets the WHERE conditions based on the non-zero fields of the given object.
func (s *StateDelete[T]) Match(obj T) *StateDelete[T] {
	s.StateDeleteWhere = MatchDeleteWhere(s.StateDeleteWhere, s.table, obj)
	return s
}

// Exec executes the DELETE query.
func (s *StateDelete[T]) Exec() error {
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	qr := model.CreateQuery(s.builder.Build())
	hd := s.Prepare(s.table.db.driver)
	return hd.ExecuteNoReturn(qr)
}

func (s *StateDelete[T]) OnTransaction(tx model.Transaction) *StateDelete[T] {
	s.StateDeleteWhere.conn = tx
	return s
}

// Filter adds filter conditions to the DELETE query.
func (s *StateDelete[T]) Filter(args ...Condition) *StateDelete[T] {
	s.StateDeleteWhere = s.StateDeleteWhere.Filter(args...)
	return s
}

// Where adds a WHERE clause to the DELETE query.
func (s *StateDelete[T]) Where(where string, args ...any) *StateDelete[T] {
	s.StateDeleteWhere = s.StateDeleteWhere.Where(where, args...)
	return s
}

// Take limits the number of records to delete.
//
// Example:
//
//	change := Pair{Key:"status", Value:"archived"}
//	err := db.Book.Delete().Set(change).Take(1).Exec() // deletes only 1 record
func (s *StateDelete[T]) Take(i int) *StateDelete[T] {
	if s.table.db.DriverName() == "PostgreSQL" {
		return s // PostgreSQL does not support LIMIT in DELETE
	}
	if i >= TakeNoLimit {
		s.builder.Limit = i
	}
	return s
}
