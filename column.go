package goent

import (
	"reflect"
)

// Column represents a database column with its metadata and field information.
type Column struct {
	FieldName    string  // Go struct field name
	FieldId      int     // Index of the field in the struct
	ColumnName   string  // Database column name
	ColumnType   string  // Database column type (e.g., "int", "varchar")
	AllowNull    bool    // Whether the column allows NULL values
	HasDefault   bool    // Whether the column has a default value
	DefaultValue string  // The default value from struct tag
	tableName    string  // Table name (internal)
	schemaName   *string // Schema name (internal)
	isAutoIncr   bool    // Whether the column is auto-increment
}

// GetInt64 returns the int64 value of the column from the given object.
// Returns (0, false) if the column is not an int type.
func (c *Column) GetInt64(obj any) (int64, bool) {
	if c.ColumnType != "int" && c.ColumnType != "int64" {
		return 0, false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).Int(), true
}

// GetString returns the string value of the column from the given object.
// Returns ("", false) if the column is not a string type.
func (c *Column) GetString(obj any) (string, bool) {
	if c.ColumnType != "string" && c.ColumnType != "[]byte" {
		return "", false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).String(), true
}

// Index represents a database index with uniqueness and auto-increment flags.
type Index struct {
	IsUnique   bool // Whether the index is unique
	IsAutoIncr bool // Whether the column is auto-increment
	*Column         // Embedded column information
}

// ResultFunc holds the result of a function query.
// Example: ResultStr = ResultFunc[string]
type ResultFunc[T any] struct {
	Value T // The result value
}

type ResultStr = ResultFunc[string]    // String result type
type ResultInt = ResultFunc[int]       // Int result type
type ResultLong = ResultFunc[int64]    // Int64 result type
type ResultFloat = ResultFunc[float64] // Float64 result type

// FetchSingleResult executes a single-row query and returns the result.
// Example: count, err := FetchSingleResult[int64](query)
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
// Example: names, err := FetchArrayResult[string](query)
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
