package goent

import (
	"github.com/azhai/goent/model"
)

type field interface {
	fieldSelect
	fieldDb
	isPrimaryKey() bool
	getTableId() int
	getFieldId() int
	getDefault() bool
	getAttributeName() string
	buildAttributeInsert(*builder)
}

type fieldDb interface {
	getDb() *DB
}

type fieldSelect interface {
	fieldDb
	buildAttributeSelect([]model.Attribute, int)
	table() string
	schema() *string
	getTableId() int
}
