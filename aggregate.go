package goent

// NewSelectFunc creates a new StateSelect with a function applied to the specified column.
// T is the model type, R is the result type.
func NewSelectFunc[T, R any](state *StateWhere, table *Table[T], col, fun string) *StateSelect[T, R] {
	fld := &Field{TableAddr: table.TableAddr, ColumnName: col, Function: fun}
	s := NewStateSelectFrom[T, R](state, table)
	s.builder.Selects = []*Field{fld}
	return s
}

// ------------------------------
// Count ...
// ------------------------------

// Count returns the count of rows matching the query.
// Example: count, err := db.Animal.Count("id")
func (s *StateSelect[T, R]) Count(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "COUNT(%s)")
	return FetchSingleResult(query)
}

// Count returns the count of all rows in the table.
// Example: count, err := db.Animal.Count("id")
func (t *Table[T]) Count(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "COUNT(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// Max/Min/Sum/Avg ...
// ------------------------------

// Max returns the maximum value of the specified column.
// Example: max, err := db.Animal.Select().Max("id")
func (s *StateSelect[T, R]) Max(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "MAX(%s)")
	return FetchSingleResult(query)
}

// Max returns the maximum value of the specified column across all rows.
// Example: max, err := db.Animal.Max("id")
func (t *Table[T]) Max(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "MAX(%s)")
	return FetchSingleResult(query)
}

// Min returns the minimum value of the specified column.
// Example: min, err := db.Animal.Select().Min("id")
func (s *StateSelect[T, R]) Min(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "MIN(%s)")
	return FetchSingleResult(query)
}

// Min returns the minimum value of the specified column across all rows.
// Example: min, err := db.Animal.Min("id")
func (t *Table[T]) Min(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "MIN(%s)")
	return FetchSingleResult(query)
}

// Sum returns the sum of values in the specified column.
// Example: sum, err := db.Animal.Select().Sum("price")
func (s *StateSelect[T, R]) Sum(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "SUM(%s)")
	return FetchSingleResult(query)
}

// Sum returns the sum of values in the specified column across all rows.
// Example: sum, err := db.Animal.Sum("price")
func (t *Table[T]) Sum(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "SUM(%s)")
	return FetchSingleResult(query)
}

// Avg returns the average value of the specified column.
// Example: avg, err := db.Animal.Select().Avg("price")
func (s *StateSelect[T, R]) Avg(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](s.StateWhere, s.table, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// Avg returns the average value of the specified column across all rows.
// Example: avg, err := db.Animal.Avg("price")
func (t *Table[T]) Avg(col string) (int64, error) {
	query := NewSelectFunc[T, ResultLong](t.State, t, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// MaxFloat/MinFloat/SumFloat/AvgFloat ...
// ------------------------------

// MaxFloat returns the maximum value of the specified column as float64.
// Example: max, err := db.Animal.Select().MaxFloat("price")
func (s *StateSelect[T, R]) MaxFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "MAX(%s)")
	return FetchSingleResult(query)
}

// MaxFloat returns the maximum value of the specified column across all rows as float64.
// Example: max, err := db.Animal.MaxFloat("price")
func (t *Table[T]) MaxFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "MAX(%s)")
	return FetchSingleResult(query)
}

// MinFloat returns the minimum value of the specified column as float64.
// Example: min, err := db.Animal.Select().MinFloat("price")
func (s *StateSelect[T, R]) MinFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "MIN(%s)")
	return FetchSingleResult(query)
}

// MinFloat returns the minimum value of the specified column across all rows as float64.
// Example: min, err := db.Animal.MinFloat("price")
func (t *Table[T]) MinFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "MIN(%s)")
	return FetchSingleResult(query)
}

// SumFloat returns the sum of values in the specified column as float64.
// Example: sum, err := db.Animal.Select().SumFloat("price")
func (s *StateSelect[T, R]) SumFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "SUM(%s)")
	return FetchSingleResult(query)
}

// SumFloat returns the sum of values in the specified column across all rows as float64.
// Example: sum, err := db.Animal.SumFloat("price")
func (t *Table[T]) SumFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "SUM(%s)")
	return FetchSingleResult(query)
}

// AvgFloat returns the average value of the specified column as float64.
// Example: avg, err := db.Animal.Select().AvgFloat("price")
func (s *StateSelect[T, R]) AvgFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](s.StateWhere, s.table, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// AvgFloat returns the average value of the specified column across all rows as float64.
// Example: avg, err := db.Animal.AvgFloat("price")
func (t *Table[T]) AvgFloat(col string) (float64, error) {
	query := NewSelectFunc[T, ResultFloat](t.State, t, col, "AVG(%s)")
	return FetchSingleResult(query)
}

// ------------------------------
// ToUpper/ToLower ...
// ------------------------------

// ToUpper returns the uppercase version of the specified column values.
// Example: names, err := db.Animal.Select().ToUpper("name")
func (s *StateSelect[T, R]) ToUpper(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](s.StateWhere, s.table, col, "UPPER(%s)")
	return FetchArrayResult(query)
}

// ToUpper returns the uppercase version of the specified column values across all rows.
// Example: names, err := db.Animal.ToUpper("name")
func (t *Table[T]) ToUpper(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](t.State, t, col, "UPPER(%s)")
	return FetchArrayResult(query)
}

// ToLower returns the lowercase version of the specified column values.
// Example: names, err := db.Animal.Select().ToLower("name")
func (s *StateSelect[T, R]) ToLower(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](s.StateWhere, s.table, col, "LOWER(%s)")
	return FetchArrayResult(query)
}

// ToLower returns the lowercase version of the specified column values across all rows.
// Example: names, err := db.Animal.ToLower("name")
func (t *Table[T]) ToLower(col string) ([]string, error) {
	query := NewSelectFunc[T, ResultStr](t.State, t, col, "LOWER(%s)")
	return FetchArrayResult(query)
}
