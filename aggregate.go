package goent

import (
	"context"

	"github.com/azhai/goent/model"
)

func NewSelectFunc[T, R any](state *StateWhere, table *Table[T], col, fun string) *StateSelect[T, R] {
	var ctx = context.Background()
	if state != nil {
		ctx = state.ctx
	}
	s := NewStateSelectFrom[T, R](NewStateWhere(ctx), table)
	if state != nil {
		s.builder.core.Where = state.builder.core.Where
		s.builder.Joins = state.builder.Joins
		s.builder.Orders = state.builder.Orders
		s.builder.Groups = state.builder.Groups
		s.builder.core.Limit = state.builder.core.Limit
		s.builder.Offset = state.builder.Offset
		s.builder.RollUp = state.builder.RollUp
		s.conn = state.conn
	}
	s.builder.VisitFields = []*Field{
		{TableAddr: table.TableAddr, ColumnName: col, Function: fun},
	}
	return s
}

func aggInt[T any](state *StateWhere, table *Table[T], col, fun string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](state, table, col, fun)
	return FetchSingleResult(query)
}

func aggFloat[T any](state *StateWhere, table *Table[T], col, fun string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](state, table, col, fun)
	return FetchSingleResult(query)
}

func aggStr[T any](state *StateWhere, table *Table[T], col, fun string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](state, table, col, fun)
	return FetchArrayResult(query)
}

// queryIDsByPK queries primary key IDs matching the given condition.
// It creates a SELECT query for just the PK column and returns the IDs.
// Returns ErrNoPrimaryKey for tables without a single integer primary key.
// When limit > 0, the ID query is limited to that many rows.
func queryIDsByPK[T any](table *Table[T], where Condition, ctx context.Context,
	conn model.Connection, limit int) ([]int64, error) {
	pkField := table.GetPKField()
	if pkField == nil {
		return nil, model.ErrNoPrimaryKey
	}
	state := NewStateWhere(ctx)
	state.builder.core.Where = where
	state.conn = conn
	if limit > 0 {
		state.builder.core.Limit = limit
	}
	sel := NewStateSelectFrom[T, ResultLong](state, table)
	sel.builder.VisitFields = []*Field{pkField}
	return FetchArrayResult(sel)
}

func (s *StateSelect[T, R]) Count(col string) (int64, error) {
	return aggInt(s.StateWhere, s.table, col, "COUNT(%s)")
}

func (t *Table[T]) Count(col string) (int64, error) {
	return aggInt(nil, t, col, "COUNT(%s)")
}

func (s *StateSelect[T, R]) Max(col string) (int64, error) {
	return aggInt(s.StateWhere, s.table, col, "MAX(%s)")
}

func (t *Table[T]) Max(col string) (int64, error) {
	return aggInt(nil, t, col, "MAX(%s)")
}

func (s *StateSelect[T, R]) Min(col string) (int64, error) {
	return aggInt(s.StateWhere, s.table, col, "MIN(%s)")
}

func (t *Table[T]) Min(col string) (int64, error) {
	return aggInt(nil, t, col, "MIN(%s)")
}

func (s *StateSelect[T, R]) Sum(col string) (int64, error) {
	return aggInt(s.StateWhere, s.table, col, "SUM(%s)")
}

func (t *Table[T]) Sum(col string) (int64, error) {
	return aggInt(nil, t, col, "SUM(%s)")
}

func (s *StateSelect[T, R]) Avg(col string) (int64, error) {
	return aggInt(s.StateWhere, s.table, col, "AVG(%s)")
}

func (t *Table[T]) Avg(col string) (int64, error) {
	return aggInt(nil, t, col, "AVG(%s)")
}

func (s *StateSelect[T, R]) MaxFloat(col string) (float64, error) {
	return aggFloat(s.StateWhere, s.table, col, "MAX(%s)")
}

func (t *Table[T]) MaxFloat(col string) (float64, error) {
	return aggFloat(nil, t, col, "MAX(%s)")
}

func (s *StateSelect[T, R]) MinFloat(col string) (float64, error) {
	return aggFloat(s.StateWhere, s.table, col, "MIN(%s)")
}

func (t *Table[T]) MinFloat(col string) (float64, error) {
	return aggFloat(nil, t, col, "MIN(%s)")
}

func (s *StateSelect[T, R]) SumFloat(col string) (float64, error) {
	return aggFloat(s.StateWhere, s.table, col, "SUM(%s)")
}

func (t *Table[T]) SumFloat(col string) (float64, error) {
	return aggFloat(nil, t, col, "SUM(%s)")
}

func (s *StateSelect[T, R]) AvgFloat(col string) (float64, error) {
	return aggFloat(s.StateWhere, s.table, col, "AVG(%s)")
}

func (t *Table[T]) AvgFloat(col string) (float64, error) {
	return aggFloat(nil, t, col, "AVG(%s)")
}

func (s *StateSelect[T, R]) ToUpper(col string) ([]string, error) {
	return aggStr(s.StateWhere, s.table, col, "UPPER(%s)")
}

func (t *Table[T]) ToUpper(col string) ([]string, error) {
	return aggStr(nil, t, col, "UPPER(%s)")
}

func (s *StateSelect[T, R]) ToLower(col string) ([]string, error) {
	return aggStr(s.StateWhere, s.table, col, "LOWER(%s)")
}

func (t *Table[T]) ToLower(col string) ([]string, error) {
	return aggStr(nil, t, col, "LOWER(%s)")
}
