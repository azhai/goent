package goent

import (
	"github.com/azhai/goent/model"
)

// StateUpdate represents an UPDATE query state for modifying records in a table.
type StateUpdate[T any] struct {
	table  *Table[T]
	others []*Table[T]
	*StateWhere
}

func (s *StateUpdate[T]) Exec() error {
	s.builder.SetTable(s.table.TableInfo)
	qr := model.CreateQuery(s.builder.Build(true))
	hd := s.Prepare(s.table.db.driver)
	return hd.ExecuteNoReturn(qr)
}

func (s *StateUpdate[T]) OnTransaction(tx model.Transaction) *StateUpdate[T] {
	s.StateWhere.conn = tx
	return s
}

func (s *StateUpdate[T]) Set(pairs ...Pair) *StateUpdate[T] {
	for _, pair := range pairs {
		fld := s.table.Field(pair.Key)
		s.builder.Changes[fld] = pair.Value
	}
	return s
}

func (s *StateUpdate[T]) SetMap(changes Dict) *StateUpdate[T] {
	for key, val := range changes {
		fld := s.table.Field(key)
		s.builder.Changes[fld] = val
	}
	return s
}

func (s *StateUpdate[T]) Filter(args ...Condition) *StateUpdate[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

func (s *StateUpdate[T]) Where(where string, args ...any) *StateUpdate[T] {
	s.StateWhere = s.StateWhere.Where(where, args...)
	return s
}

func (s *StateUpdate[T]) Match(obj T) *StateUpdate[T] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// Take takes i elements
func (s *StateUpdate[T]) Take(i int) *StateUpdate[T] {
	if i >= 0 {
		s.builder.Limit = i
	}
	return s
}

// Join joins another table with a condition
func (s *StateUpdate[T]) Join(joinType model.JoinType, info TableInfo, on Condition) *StateUpdate[T] {
	s.builder.Type = model.UpdateJoinQuery
	s.builder.Joins = append(s.builder.Joins, &JoinTable{
		JoinType: joinType, Table: info.Table(), On: Condition{},
	})
	return s.Filter(on)
}

// LeftJoin joins another table with a condition on left table
func (s *StateUpdate[T]) LeftJoin(fkey string, refer *Field) *StateUpdate[T] {
	info := GetTableInfo(refer.TableAddr)
	return s.Join(model.LeftJoin, *info, EqualsField(s.table.Field(fkey), refer))
}
