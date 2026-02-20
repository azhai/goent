package goent

import (
	"reflect"

	"github.com/azhai/goent/model"
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
	db         *DB
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

func (c *Column) Field(name string) field {
	return c
}

func (c *Column) isPrimaryKey() bool {
	return false
}

func (c *Column) getFieldId() int {
	return c.FieldId
}

func (c *Column) getDefault() bool {
	return c.HasDefault
}

func (c *Column) getAttributeName() string {
	return c.ColumnName
}

func (c *Column) getTableId() int {
	return 0
}

func (c *Column) getDb() *DB {
	return c.db
}

func (c *Column) table() string {
	return c.tableName
}

func (c *Column) schema() *string {
	return c.schemaName
}

func (c *Column) buildAttributeSelect(attrs []model.Attribute, i int) {
	attrs[i] = model.Attribute{
		Table: c.table(),
		Name:  c.getAttributeName(),
	}
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
	row, err := query.One()
	if err != nil {
		row = new(ResultFunc[V])
	}
	return row.Value, err
}

// FetchArrayResult executes a multi-row query and returns the result array.
func FetchArrayResult[T, V any](query *StateSelect[T, ResultFunc[V]]) ([]V, error) {
	var res []V
	for row, err := range query.Rows() {
		if err != nil {
			return nil, err
		}
		res = append(res, row.Value)
	}
	return res, nil
}

// // FetchCountResult executes a count query and returns the count value.
// func FetchCountResult[T any](query *StateSelect[T, ResultLong]) (int64, error) {
// 	row, err := query.One()
// 	if err != nil {
// 		return 0, err
// 	}
// 	return row.Value, nil
// }
//
// // FetchAggrResult executes an aggregate query and returns the aggregate value.
// func FetchAggrResult[T any](query *StateSelect[T, ResultFloat]) (float64, error) {
// 	row, err := query.One()
// 	if err != nil {
// 		return 0.0, err
// 	}
// 	return row.Value, nil
// }
//
// // FetchFuncResult executes a function query and returns the string results.
// func FetchFuncResult[T any](query *StateSelect[T, ResultStr]) ([]string, error) {
// 	var res []string
// 	for row, err := range query.Rows() {
// 		if err != nil {
// 			return nil, err
// 		}
// 		res = append(res, row.Value)
// 	}
// 	return res, nil
// }
