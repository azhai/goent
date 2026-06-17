package goent

import (
	"fmt"

	"github.com/azhai/goent/model"
)

// StateUpdate represents an UPDATE query state for modifying records in a table
// It provides methods for building and executing UPDATE queries with various options
type StateUpdate[T any] struct {
	table       *Table[T]   // The table to update records in
	others      []*Table[T] // Other tables involved in JOIN operations
	*StateWhere             // Embedded StateWhere for WHERE clause construction
}

// Exec executes the UPDATE query and returns any error
// It builds and runs the UPDATE statement with the specified changes and conditions
//
// Example:
//
//	change := Pair{Key:"name", Value:"John"}
//	err := db.User.Where("id = ?", 1).Update().Set(change).Exec()
func (s *StateUpdate[T]) Exec() error {
	defer PutBuilder(s.builder)
	s.builder.SetTable(&s.table.TableInfo)
	sql, args := s.builder.Build(true)
	if sql == "" {
		return fmt.Errorf("goent: StateUpdate.Exec built empty SQL (Type=%d, Changes=%d, Where=%v, args=%v)",
			s.builder.Type, len(s.builder.Changes), !s.builder.core.Where.IsEmpty(), args)
	}
	qr := model.CreateQuery(sql, args)
	conn, cfg := s.Prepare(&s.table.TableInfo)
	return qr.WrapExec(s.ctx, conn, cfg)
}

// ByPK updates a single row by primary key.
// This is an optimized path for simple primary key updates.
// Only works for tables with a single primary key column.
func (s *StateUpdate[T]) ByPK(id int64) error {
	if len(s.builder.Changes) == 0 {
		return nil
	}
	pkField := s.table.GetPKField()
	if pkField == nil {
		return model.ErrNoPrimaryKey
	}
	s.builder.core.Where = Equals(pkField, id)
	return s.Exec()
}

// OnTransaction sets the transaction for the UPDATE operation
// It ensures the update runs within the specified transaction
func (s *StateUpdate[T]) OnTransaction(tx model.Transaction) *StateUpdate[T] {
	s.StateWhere.conn = tx
	return s
}

// Set sets the column values to update
// Each pair is a key-value map where key is the column name and value is the new value
//
// Example:
//
//	 changes := []Pair{
//			{Key:"name", Value:"John"},
//			{Key:"email", Value:"john@example.com"},
//		}
//	 err := db.User.Where("id = ?", 1).Update().Set(changes...).Exec()
func (s *StateUpdate[T]) Set(pairs ...Pair) *StateUpdate[T] {
	for _, pair := range pairs {
		fld := s.table.Field(pair.Key)
		s.builder.Changes[fld] = pair.Value
	}
	return s
}

// SetMap sets multiple column values using a map
// It updates columns based on the key-value pairs in the map
//
// Example:
//
//	changes := map[string]any{"name": "John", "email": "john@example.com"}
//	err := db.User.Where("id = ?", 1).Update().SetMap(changes).Exec()
func (s *StateUpdate[T]) SetMap(changes Dict) *StateUpdate[T] {
	for key, val := range changes {
		fld := s.table.Field(key)
		s.builder.Changes[fld] = val
	}
	return s
}

// Filter adds filter conditions to the UPDATE query
// It appends the specified conditions to the WHERE clause
func (s *StateUpdate[T]) Filter(args ...Condition) *StateUpdate[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

// Where adds a WHERE clause to the UPDATE query
// It accepts a raw SQL WHERE clause with optional arguments
func (s *StateUpdate[T]) Where(where string, args ...any) *StateUpdate[T] {
	s.StateWhere = s.StateWhere.Where(where, args...)
	return s
}

// Match sets the WHERE conditions based on the primary key and unique indexes of the given object
// It automatically generates conditions for fields with non-zero values
func (s *StateUpdate[T]) Match(obj T) *StateUpdate[T] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// Take limits the number of records to update
// Note: PostgreSQL does not support LIMIT in UPDATE statements
//
// Example:
//
//	change := Pair{Key:"status", Value:"archived"}
//	err := db.User.Update().Set(change).Take(100).Exec() // updates only 100 records
func (s *StateUpdate[T]) Take(i int) *StateUpdate[T] {
	if s.table.db.DriverName() == "PostgreSQL" {
		return s // PostgreSQL does not support LIMIT in UPDATE
	}
	if i >= TakeNoLimit {
		s.builder.core.Limit = i
	}
	return s
}

// Join joins another table with a condition for UPDATE operations
// It adds a JOIN clause to the UPDATE statement
//
// Example:
//
//	info := GetTableInfo(referAddr)
//	err := db.User.Update().Join(model.InnerJoin, *info, EqualsField(...)).Set(...).Exec()
func (s *StateUpdate[T]) Join(joinType model.JoinType, info TableInfo, on Condition) *StateUpdate[T] {
	s.builder.Type = model.UpdateJoinQuery
	s.builder.Joins = append(s.builder.Joins, &JoinTable{
		JoinType: joinType, Table: info.Table(), On: Condition{},
	})
	return s.Filter(on)
}

// LeftJoin performs a LEFT JOIN with another table using a foreign key relationship
// It automatically creates the join condition based on the foreign key
//
// Example:
//
//	refer := userTable.Field("role_id")
//	change := Pair{Key:"role_name", Value:"admin"}
//	err := db.User.Update().LeftJoin("role_id", refer).Set(change).Exec()
func (s *StateUpdate[T]) LeftJoin(fkey string, refer *Field) *StateUpdate[T] {
	info := GetTableInfo(refer.TableAddr)

	// Check if the column exists in the main table
	col := s.table.ColumnInfo(fkey)
	if col == nil {
		panic("column " + fkey + " not found in table " + s.table.TableName)
	}

	leftField := s.table.sortedFields[col.FieldId]
	return s.Join(model.LeftJoin, *info, EqualsField(leftField, refer))
}

// StateUpdateByID represents a two-phase UPDATE query: first queries matching
// primary key IDs, then updates rows by ID using an IN clause.
//
// This approach:
//   - Works around PostgreSQL's lack of LIMIT in UPDATE statements (via Take)
//   - Provides the list of affected IDs for auditing
//   - Uses InBatch to handle large ID sets within parameter limits
//
// Only works for tables with a single integer primary key.
//
// Example:
//
//	ids, err := db.User.Filter(goent.Equals(db.User.Field("status"), "active")).
//	    UpdateByID().
//	    Set(goent.Pair{Key: "status", Value: "inactive"}).
//	    BatchSize(500).
//	    Take(100).
//	    Exec()
type StateUpdateByID[T any] struct {
	byIDBase[T]      // Shared two-phase fields and methods (BatchSize, Take, OnTransaction)
	changes     Dict // Column changes to apply
}

// Set specifies the column values to update.
// Each pair's Key is the column name, Value is the new value.
func (s *StateUpdateByID[T]) Set(pairs ...Pair) *StateUpdateByID[T] {
	if s.changes == nil {
		s.changes = make(Dict)
	}
	for _, pair := range pairs {
		s.changes[pair.Key] = pair.Value
	}
	return s
}

// SetMap specifies column values to update using a map.
func (s *StateUpdateByID[T]) SetMap(changes Dict) *StateUpdateByID[T] {
	if s.changes == nil {
		s.changes = make(Dict)
	}
	for key, val := range changes {
		s.changes[key] = val
	}
	return s
}

// BatchSize sets the IN clause batch size for Phase 2.
// Wrapper around byIDBase.BatchSize that returns the concrete type for chaining.
func (s *StateUpdateByID[T]) BatchSize(size int) *StateUpdateByID[T] {
	s.byIDBase.BatchSize(size)
	return s
}

// Take limits the number of IDs queried in Phase 1.
// Wrapper around byIDBase.Take that returns the concrete type for chaining.
// Unlike StateUpdate.Take, this works on PostgreSQL because the LIMIT
// is applied to the SELECT query, not the UPDATE statement.
func (s *StateUpdateByID[T]) Take(i int) *StateUpdateByID[T] {
	s.byIDBase.Take(i)
	return s
}

// OnTransaction sets the transaction for both Phase 1 (SELECT) and Phase 2 (UPDATE).
// Wrapper around byIDBase.OnTransaction that returns the concrete type for chaining.
func (s *StateUpdateByID[T]) OnTransaction(tx model.Transaction) *StateUpdateByID[T] {
	s.byIDBase.OnTransaction(tx)
	return s
}

// Exec executes the two-phase update.
// Phase 1: SELECT pk FROM table WHERE <conditions> [LIMIT n]
// Phase 2: UPDATE table SET ... WHERE pk IN (ids)  (batched via InBatch)
// Returns the list of affected IDs.
func (s *StateUpdateByID[T]) Exec() ([]int64, error) {
	if len(s.changes) == 0 {
		return nil, fmt.Errorf("goent: StateUpdateByID.Exec has no changes, use Set() or SetMap()")
	}

	ids, cond, err := s.buildInBatchCond()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return ids, nil
	}

	state := NewStateWhere(s.ctx)
	state.builder.Type = model.UpdateQuery
	state.builder.core.Where = cond
	state.conn = s.conn

	upd := &StateUpdate[T]{table: s.table, StateWhere: state}
	upd.SetMap(s.changes)
	return ids, upd.Exec()
}
