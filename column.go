package goent

import (
	"github.com/azhai/goent/model"
)

// Column represents a database column with its metadata and field information.
type Column struct {
	FieldAddr  uintptr
	FieldName  string
	FieldId    int
	ColumnName string
	ColumnType string
	AllowNull  bool
	HasDefault bool
	tableName  string
	schemaName *string
	db         *DB
	isAutoIncr bool
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

// ResultCount holds the result of a COUNT query.
type ResultCount struct {
	Count int64
}

// FetchCountResult executes a count query and returns the count value.
func FetchCountResult[T any](query *StateSelect[T, ResultCount]) (int64, error) {
	row, err := query.One()
	if err != nil {
		return 0, err
	}
	return row.Count, nil
}

// Aggregate represents an aggregate function applied to a column.
type Aggregate struct {
	AggrType string
	*Column
}

// ResultAggr holds the result of an aggregate function query (SUM, AVG, etc.).
type ResultAggr struct {
	Aggr float64
}

// FetchAggrResult executes an aggregate query and returns the aggregate value.
func FetchAggrResult[T any](query *StateSelect[T, ResultAggr]) (float64, error) {
	row, err := query.One()
	if err != nil {
		return 0.0, err
	}
	return row.Aggr, nil
}

// Function represents a SQL function applied to a column.
// type Function struct {
// 	FuncType string
// 	*Column
// }

type ResultFunc[T any] struct {
	Value T
}

type FuncStr = ResultFunc[string]
type FuncInt = ResultFunc[int]
type FuncLong = ResultFunc[int64]
type FuncFloat = ResultFunc[float64]

// FetchFuncResult executes a function query and returns the string results.
func FetchFuncResult[T any](query *StateSelect[T, FuncStr]) ([]string, error) {
	var res []string
	for row, err := range query.Rows() {
		if err != nil {
			return nil, err
		}
		res = append(res, row.Value)
	}
	return res, nil
}
