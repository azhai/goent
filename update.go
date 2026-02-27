package goent

import (
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
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	qr := model.CreateQuery(s.builder.Build(true))
	conn, cfg := s.Prepare(s.table.db.driver)
	return qr.WrapExec(s.ctx, conn, cfg)
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
		s.builder.Limit = i
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
	return s.Join(model.LeftJoin, *info, EqualsField(s.table.Field(fkey), refer))
}
