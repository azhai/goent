package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/model"
)

// NilMarker is a special type to indicate a nil pointer field for IS NULL conditions
// It is used to generate IS NULL conditions in WHERE clauses
type NilMarker struct{}

// StateDelete represents a DELETE query state for removing records from a table
// It provides methods for building and executing DELETE queries with various options
type StateDelete[T any] struct {
	table             *Table[T] // The table to delete records from
	*StateDeleteWhere           // Embedded StateDeleteWhere for WHERE clause construction
}

// Match sets the WHERE conditions based on the non-zero fields of the given object
// It automatically generates conditions for fields with non-zero values
func (s *StateDelete[T]) Match(obj T) *StateDelete[T] {
	s.StateDeleteWhere = MatchDeleteWhere(s.StateDeleteWhere, s.table, obj)
	return s
}

// Exec executes the DELETE query
// It builds and runs the DELETE statement with the specified conditions
func (s *StateDelete[T]) Exec() error {
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	qr := model.CreateQuery(s.builder.Build())
	defer PutDeleteBuilder(s.builder)
	conn, cfg := s.Prepare(s.table.db.driver)
	return qr.WrapExec(s.ctx, conn, cfg)
}

// OnTransaction sets the transaction for the DELETE operation
// It ensures the delete runs within the specified transaction
func (s *StateDelete[T]) OnTransaction(tx model.Transaction) *StateDelete[T] {
	s.StateDeleteWhere.conn = tx
	return s
}

// Filter adds filter conditions to the DELETE query
// It appends the specified conditions to the WHERE clause
func (s *StateDelete[T]) Filter(args ...Condition) *StateDelete[T] {
	s.StateDeleteWhere = s.StateDeleteWhere.Filter(args...)
	return s
}

// Where adds a WHERE clause to the DELETE query
// It accepts a raw SQL WHERE clause with optional arguments
func (s *StateDelete[T]) Where(where string, args ...any) *StateDelete[T] {
	s.StateDeleteWhere = s.StateDeleteWhere.Where(where, args...)
	return s
}

// Take limits the number of records to delete
// Note: PostgreSQL does not support LIMIT in DELETE statements
//
// Example:
//
//	change := Pair{Key:"status", Value:"archived"}
//	err := db.Book.Delete().Take(1).Exec() // deletes only 1 record
func (s *StateDelete[T]) Take(i int) *StateDelete[T] {
	if s.table.db.DriverName() == "PostgreSQL" {
		return s // PostgreSQL does not support LIMIT in DELETE
	}
	if i >= TakeNoLimit {
		s.builder.Limit = i
	}
	return s
}

// StateDeleteWhere represents a query state with WHERE clause building capabilities for DELETE queries
// It provides methods for constructing WHERE clauses specifically for DELETE operations
type StateDeleteWhere struct {
	builder *DeleteBuilder   // The delete query builder
	conn    model.Connection // The database connection
	ctx     context.Context  // The context for the query
}

// NewStateDeleteWhere creates a new StateDeleteWhere with the given context
// It initializes the delete query builder and sets up the context
func NewStateDeleteWhere(ctx context.Context) *StateDeleteWhere {
	return &StateDeleteWhere{ctx: ctx, builder: GetDeleteBuilder()}
}

// MatchDeleteWhere creates a StateDeleteWhere with conditions matching the non-zero fields of the given object
// It generates conditions based on the object's non-zero fields
func MatchDeleteWhere[T any](s *StateDeleteWhere, table *Table[T], obj T) *StateDeleteWhere {
	return s.Filter(MatchFilter(table, obj))
}

func (s *StateDeleteWhere) Filter(conds ...Condition) *StateDeleteWhere {
	if len(conds) == 0 || len(conds) == 1 && conds[0].IsEmpty() {
		return s
	}
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

func (s *StateDeleteWhere) Prepare(drv model.Driver) (model.Connection, *model.DatabaseConfig) {
	if s.conn == nil {
		s.conn = drv.NewConnection()
	}
	return s.conn, drv.GetDatabaseConfig()
}

// StateWhere represents a query state with WHERE clause building capabilities
// It provides methods for constructing WHERE clauses for various query types
type StateWhere struct {
	builder *Builder         // The query builder
	conn    model.Connection // The database connection
	ctx     context.Context  // The context for the query
}

// NewStateWhere creates a new StateWhere with the given context
// It initializes the query builder and sets up the context
func NewStateWhere(ctx context.Context) *StateWhere {
	return &StateWhere{ctx: ctx, builder: GetBuilder()}
}

// MatchWhere creates a StateWhere with conditions matching the non-zero fields of the given object
// It generates conditions based on the object's non-zero fields
func MatchWhere[T any](s *StateWhere, table *Table[T], obj T) *StateWhere {
	return s.Filter(MatchFilter(table, obj))
}

func (s *StateWhere) Filter(conds ...Condition) *StateWhere {
	if len(conds) == 0 || len(conds) == 1 && conds[0].IsEmpty() {
		return s
	}
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

func (s *StateWhere) Prepare(drv model.Driver) (model.Connection, *model.DatabaseConfig) {
	if s.conn == nil {
		s.conn = drv.NewConnection()
	}
	return s.conn, drv.GetDatabaseConfig()
}

// MatchData matches the non-zero fields of the given object to a dictionary of column names and values
// Nil pointer fields are skipped (not included in the result)
func MatchData[T any](table *Table[T], obj T) Dict {
	data := make(Dict, len(table.Columns))
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

// MatchFilter generates a condition based on the non-zero fields of the given object
// It creates an EqualsMap condition from the object's non-zero fields
func MatchFilter[T any](table *Table[T], obj T) Condition {
	data := MatchData(table, obj)
	if len(data) == 0 {
		return Condition{}
	}
	col := &Field{TableAddr: table.TableAddr}
	return EqualsMap(col, data)
}
