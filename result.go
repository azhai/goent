package goent

import "github.com/azhai/goent/model"

// ResultFunc holds the result of a function query
// It is used to return single values from aggregate functions or scalar queries
// Example: ResultStr = ResultFunc[string]
type ResultFunc[T any] struct {
	Value T // The result value
}

type ResultStr = ResultFunc[string]    // String result type
type ResultInt = ResultFunc[int]       // Int result type
type ResultLong = ResultFunc[int64]    // Int64 result type
type ResultFloat = ResultFunc[float64] // Float64 result type

// FetchSingleResult executes a single-row query and returns the result
// It fetches a single value from the query result
// Example: count, err := FetchSingleResult[int64](query)
func FetchSingleResult[T, V any](state *StateSelect[T, ResultFunc[V]]) (V, error) {
	defer PutBuilder(state.builder)
	qr := model.CreateQuery(state.builder.Build(true))
	obj, err := state.FetchRow(qr, FetchValue)
	if obj == nil {
		obj = new(ResultFunc[V])
	}
	return obj.Value, err
}

// FetchArrayResult executes a multi-row query and returns the result array
// It fetches multiple values from the query result
// Example: names, err := FetchArrayResult[string](query)
func FetchArrayResult[T, V any](state *StateSelect[T, ResultFunc[V]]) ([]V, error) {
	var res []V
	for row, err := range state.IterRows(FetchValue) {
		if err != nil {
			return nil, err
		}
		res = append(res, row.Value)
	}
	return res, nil
}
