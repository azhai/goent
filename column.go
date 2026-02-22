package goent

import (
	"reflect"
)

// Column represents a database column with its metadata and field information.
type Column struct {
	FieldName  string
	FieldId    int
	ColumnName string
	ColumnType string
	AllowNull  bool
	HasDefault bool
	tableName  string
	schemaName *string
	isAutoIncr bool
}

func (c *Column) GetInt64(obj any) (int64, bool) {
	if c.ColumnType != "int" && c.ColumnType != "int64" {
		return 0, false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).Int(), true
}

func (c *Column) GetString(obj any) (string, bool) {
	if c.ColumnType != "string" && c.ColumnType != "[]byte" {
		return "", false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).String(), true
}

// Index represents a database index with uniqueness and auto-increment flags.
type Index struct {
	IsUnique   bool
	IsAutoIncr bool
	*Column
}

// ResultFunc holds the result of a function query.
type ResultFunc[T any] struct {
	Value T
}

type ResultStr = ResultFunc[string]
type ResultInt = ResultFunc[int]
type ResultLong = ResultFunc[int64]
type ResultFloat = ResultFunc[float64]

// FetchSingleResult executes a single-row query and returns the result.
func FetchSingleResult[T, V any](query *StateSelect[T, ResultFunc[V]]) (V, error) {
	var (
		row *ResultFunc[V]
		err error
	)
	fet, qr := query.Query(CreateFetchOne)
	for row, err = range fet.FetchResult(qr) {
		break
	}
	if row == nil {
		row = new(ResultFunc[V])
	}
	return row.Value, err
}

// FetchArrayResult executes a multi-row query and returns the result array.
func FetchArrayResult[T, V any](query *StateSelect[T, ResultFunc[V]]) ([]V, error) {
	var res []V
	fet, qr := query.Query(CreateFetchOne)
	for row, err := range fet.FetchResult(qr) {
		if err != nil {
			return nil, err
		}
		res = append(res, row.Value)
	}
	return res, nil
}
