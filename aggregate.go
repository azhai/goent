package goent

func NewSelectFunc[T, R any](state *StateWhere, table *Table[T], col, fun string) *StateSelect[T, R] {
	fld := &Field{TableAddr: table.TableAddr, ColumnName: col, Function: fun}
	s := NewStateSelectFrom[T, R](state, table)
	s.builder.Selects = []*Field{fld}
	return s
}

// ------------------------------
// Count ...
// ------------------------------

func (s *StateSelect[T, R]) Count(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "COUNT(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) Count(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "COUNT(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// Max/Min/Sum/Avg ...
// ------------------------------

func (s *StateSelect[T, R]) Max(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "MAX(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) Max(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "MAX(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) Min(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "MIN(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) Min(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "MIN(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) Sum(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "SUM(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) Sum(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "SUM(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) Avg(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "AVG(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) Avg(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// MaxFloat/MinFloat/SumFloat/AvgFloat ...
// ------------------------------

func (s *StateSelect[T, R]) MaxFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "MAX(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) MaxFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "MAX(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) MinFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "MIN(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) MinFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "MIN(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) SumFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "SUM(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) SumFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "SUM(%s)")
	return FetchSingleResult(query)
}

func (s *StateSelect[T, R]) AvgFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "AVG(%s)")
	return FetchSingleResult(query)
}

func (t *Table[T]) AvgFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// ToUpper/ToLower ...
// ------------------------------

func (s *StateSelect[T, R]) ToUpper(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](s.StateWhere, s.table, col, "UPPER(%s)")
	return FetchArrayResult(query)
}

func (t *Table[T]) ToUpper(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](t.State, t, col, "UPPER(%s)")
	return FetchArrayResult(query)
}

func (s *StateSelect[T, R]) ToLower(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](s.StateWhere, s.table, col, "LOWER(%s)")
	return FetchArrayResult(query)
}

func (t *Table[T]) ToLower(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](t.State, t, col, "LOWER(%s)")
	return FetchArrayResult(query)
}
