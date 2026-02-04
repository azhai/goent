package goent

import (
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/query/function"
)

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

func (c *Column) buildAttributeInsert(b *builder) {
	if c.isAutoIncr {
		b.query.ReturningID = &model.Attribute{Name: c.getAttributeName()}
		b.pkFieldId = c.FieldId
		return
	}
	b.fieldIds = append(b.fieldIds, c.FieldId)
	b.query.Attributes = append(b.query.Attributes, model.Attribute{Name: c.getAttributeName()})
}

type Index struct {
	IsUnique   bool
	IsAutoIncr bool
	*Column
}

type ResultCount struct {
	Count int64
}

func FetchCountResult[T any](query *StateSelect[T, ResultCount]) (int64, error) {
	result, err := query.One()
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

type Aggregate struct {
	AggrType string
	*Column
}

type ResultAggr struct {
	Aggr float64
}

func FetchAggrResult[T any](query *StateSelect[T, ResultAggr]) (float64, error) {
	result, err := query.One()
	if err != nil {
		return 0.0, err
	}
	return result.Aggr, nil
}

type FuncString *function.Function[string]

type Function struct {
	FuncType string
	*Column
}

func FetchFuncResult[T any](query *StateSelect[T, FuncString]) (res []string, err error) {
	var row FuncString
	for row, err = range query.Rows() {
		if err != nil {
			return
		}
		res = append(res, row.Value)
	}
	return
}
