package goent

import (
	"context"
	"fmt"
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
	skipEvent         bool      // Internal: skip event publishing (used by StateDeleteByID)
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
	s.builder.SetTable(s.table.TableInfo)
	sql, args := s.builder.Build()
	if sql == "" {
		defer PutDeleteBuilder(s.builder)
		return fmt.Errorf("goent: StateDelete.Exec built empty SQL (fullName=%q, Where=%v, args=%v)",
			s.builder.core.fullName, !s.builder.core.Where.IsEmpty(), args)
	}
	qr := model.CreateQuery(sql, args)
	defer PutDeleteBuilder(s.builder)
	conn, cfg := s.Prepare(s.table.TableInfo)
	if err := qr.WrapExec(s.ctx, conn, cfg); err != nil {
		return err
	}
	// Skip event for clear-all operations (no WHERE) and internal byid calls
	if !s.skipEvent && !s.builder.core.Where.IsEmpty() {
		info := s.table.TableInfo
		publishEvent(info.db.bus, info, conn, EventTopicDelete,
			s.builder.core.Where.Template, nil, nil, qr.RowsAffected)
	}
	return nil
}

// ByPK deletes a single row by primary key using cached SQL.
// This is an optimized path that bypasses query building for simple primary key deletions.
// Only works for tables with a single primary key column.
func (s *StateDelete[T]) ByPK(id int64) error {
	sql := s.table.GetDeleteByPKSql()
	if sql == "" {
		return model.ErrNoPrimaryKey
	}
	conn, cfg := s.Prepare(s.table.TableInfo)
	qr := model.CreateQuery(sql, []any{id})
	if err := qr.WrapExec(s.ctx, conn, cfg); err != nil {
		return err
	}
	info := s.table.TableInfo
	publishEvent(info.db.bus, info, conn, EventTopicDeleteByPK, "", []int64{id}, nil, 1)
	return nil
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
		s.builder.core.Limit = i
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
	s.builder.core.Where = applyFilter(&s.builder.core.Where, conds...)
	return s
}

func (s *StateDeleteWhere) Where(where string, args ...any) *StateDeleteWhere {
	s.builder.core.Where = applyWhere(&s.builder.core.Where, where, args...)
	return s
}

func (s *StateDeleteWhere) OnTransaction(tx model.Transaction) *StateDeleteWhere {
	s.conn = tx
	return s
}

// Prepare returns a connection and config for query execution.
// When no transaction is set, it uses cached Connection from TableInfo to avoid allocation.
func (s *StateDeleteWhere) Prepare(info *TableInfo) (model.Connection, *model.DatabaseConfig) {
	if s.conn != nil {
		return s.conn, info.GetConfig()
	}
	return info.GetConnection(), info.GetConfig()
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
	s.builder.core.Where = applyFilter(&s.builder.core.Where, conds...)
	return s
}

func (s *StateWhere) Where(where string, args ...any) *StateWhere {
	s.builder.core.Where = applyWhere(&s.builder.core.Where, where, args...)
	return s
}

func (s *StateWhere) OnTransaction(tx model.Transaction) *StateWhere {
	s.conn = tx
	return s
}

// Prepare returns a connection and config for query execution.
// When no transaction is set, it uses cached Connection from TableInfo to avoid allocation.
func (s *StateWhere) Prepare(info *TableInfo) (model.Connection, *model.DatabaseConfig) {
	if s.conn != nil {
		return s.conn, info.GetConfig()
	}
	return info.GetConnection(), info.GetConfig()
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

// StateDeleteByID represents a two-phase DELETE query: first queries matching
// primary key IDs, then deletes rows by ID using an IN clause.
//
// This approach:
//   - Works around PostgreSQL's lack of LIMIT in DELETE statements (via Take)
//   - Provides the list of deleted IDs for auditing
//   - Uses InBatch to handle large ID sets within parameter limits
//
// Only works for tables with a single integer primary key.
//
// Example:
//
//	ids, err := db.User.Filter(goent.Equals(db.User.Field("status"), "deleted")).
//	    DeleteByID().Exec()
type StateDeleteByID[T any] struct {
	byIDBase[T] // Shared two-phase fields and methods (BatchSize, Take, OnTransaction)
}

// BatchSize sets the IN clause batch size for Phase 2.
// Wrapper around byIDBase.BatchSize that returns the concrete type for chaining.
func (s *StateDeleteByID[T]) BatchSize(size int) *StateDeleteByID[T] {
	s.byIDBase.BatchSize(size)
	return s
}

// Take limits the number of IDs queried in Phase 1.
// Wrapper around byIDBase.Take that returns the concrete type for chaining.
// Unlike StateDelete.Take, this works on PostgreSQL because the LIMIT
// is applied to the SELECT query, not the DELETE statement.
func (s *StateDeleteByID[T]) Take(i int) *StateDeleteByID[T] {
	s.byIDBase.Take(i)
	return s
}

// OnTransaction sets the transaction for both Phase 1 (SELECT) and Phase 2 (DELETE).
// Wrapper around byIDBase.OnTransaction that returns the concrete type for chaining.
func (s *StateDeleteByID[T]) OnTransaction(tx model.Transaction) *StateDeleteByID[T] {
	s.byIDBase.OnTransaction(tx)
	return s
}

// Exec executes the two-phase delete.
// Phase 1: SELECT pk FROM table WHERE <conditions> [LIMIT n]
// Phase 2: DELETE FROM table WHERE pk IN (ids)  (batched via InBatch)
// Returns the list of deleted IDs.
func (s *StateDeleteByID[T]) Exec() ([]int64, error) {
	ids, cond, err := s.buildInBatchCond()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return ids, nil
	}

	sd := NewStateDeleteWhere(s.ctx)
	sd.builder.core.Where = cond
	sd.conn = s.conn

	del := &StateDelete[T]{table: s.table, StateDeleteWhere: sd, skipEvent: true}
	if err := del.Exec(); err != nil {
		return ids, err
	}
	// Send byid event with the queried IDs
	info := s.table.TableInfo
	publishEvent(info.db.bus, info, s.conn, EventTopicDeleteByID,
		s.where.Template, ids, nil, int64(len(ids)))
	return ids, nil
}
