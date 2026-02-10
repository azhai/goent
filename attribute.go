package goent

import (
	"reflect"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

type attributeStrings struct {
	db            *DB
	schemaName    *string
	tableId       int
	tableName     string
	attributeName string
	fieldId       int
}

type pk struct {
	autoIncrement bool
	attributeStrings
}

func (p pk) schema() *string {
	return p.schemaName
}

func (p pk) getDb() *DB {
	return p.db
}

func (p pk) isPrimaryKey() bool {
	return true
}

func (p pk) getTableId() int {
	return p.tableId
}

func (p pk) table() string {
	return p.tableName
}

func (p pk) getAttributeName() string {
	return p.attributeName
}

func (p pk) getFieldId() int {
	return p.fieldId
}

func (p pk) getDefault() bool {
	return false
}

func (p pk) buildAttributeSelect(attrs []model.Attribute, i int) {
	attrs[i] = model.Attribute{
		Table: p.tableName,
		Name:  p.attributeName,
	}
}

type att struct {
	isDefault bool
	attributeStrings
}

func (a att) schema() *string {
	return a.schemaName
}

func (a att) getDb() *DB {
	return a.db
}

func (a att) isPrimaryKey() bool {
	return false
}

func (a att) getTableId() int {
	return a.tableId
}

func (a att) table() string {
	return a.tableName
}

func (a att) getAttributeName() string {
	return a.attributeName
}

func (a att) getFieldId() int {
	return a.fieldId
}

func (a att) getDefault() bool {
	return a.isDefault
}

func (a att) buildAttributeSelect(attrs []model.Attribute, i int) {
	attrs[i] = model.Attribute{
		Table: a.tableName,
		Name:  a.attributeName,
	}
}

// ManyToSomeRelation represents a many-to-one or many-to-many relationship between tables.
type ManyToSomeRelation struct {
	IsDefault bool
	attributeStrings
}

func (r ManyToSomeRelation) schema() *string {
	return r.schemaName
}

func (r ManyToSomeRelation) getDb() *DB {
	return r.db
}

func (r ManyToSomeRelation) isPrimaryKey() bool {
	return false
}

func (r ManyToSomeRelation) getTableId() int {
	return r.tableId
}

func (r ManyToSomeRelation) table() string {
	return r.tableName
}

func (r ManyToSomeRelation) getAttributeName() string {
	return r.attributeName
}

func (r ManyToSomeRelation) getFieldId() int {
	return r.fieldId
}

func (r ManyToSomeRelation) getDefault() bool {
	return r.IsDefault
}

func (r ManyToSomeRelation) buildAttributeSelect(attrs []model.Attribute, i int) {
	attrs[i] = model.Attribute{
		Table: r.tableName,
		Name:  r.attributeName,
	}
}

// OneToSomeRelation represents a one-to-one or one-to-many relationship between tables.
type OneToSomeRelation struct {
	IsOneToMany bool
	attributeStrings
}

func (r OneToSomeRelation) schema() *string {
	return r.schemaName
}

func (r OneToSomeRelation) getDb() *DB {
	return r.db
}

func (r OneToSomeRelation) isPrimaryKey() bool {
	return false
}

func (r OneToSomeRelation) getTableId() int {
	return r.tableId
}

func (r OneToSomeRelation) table() string {
	return r.tableName
}

func (r OneToSomeRelation) getAttributeName() string {
	return r.attributeName
}

func (r OneToSomeRelation) getFieldId() int {
	return r.fieldId
}

func (r OneToSomeRelation) getDefault() bool {
	return false
}

func (r OneToSomeRelation) buildAttributeSelect(attrs []model.Attribute, i int) {
	attrs[i] = model.Attribute{
		Table: r.tableName,
		Name:  r.attributeName,
	}
}

// type aggregateResult struct {
// 	attributeName string
// 	tableName     string
// 	schemaName    *string
// 	aggregateType enum.AggregateType
// 	tableId       int
// 	db            *DB
// }

// func (a aggregateResult) getDb() *DB {
// 	return a.db
// }
//
// func (a aggregateResult) buildAttributeSelect(attrs []model.Attribute, i int) {
// 	attrs[i] = model.Attribute{
// 		Table: a.tableName,
// 		Name:  a.attributeName,
// 	}
// }
//
// func (a aggregateResult) table() string {
// 	return a.tableName
// }
//
// func (a aggregateResult) schema() *string {
// 	return a.schemaName
// }
//
// func (a aggregateResult) getTableId() int {
// 	return a.tableId
// }

// type functionResult struct {
// 	attributeName string
// 	tableName     string
// 	schemaName    *string
// 	functionType  enum.FunctionType
// 	tableId       int
// 	db            *DB
// }

func createAttributeStrings(db *DB, schema *string, table string, attributeName string, tableId, fieldId int, Driver model.Driver) attributeStrings {
	name := Driver.KeywordHandler(utils.ToSnakeCase(attributeName))
	return attributeStrings{
		db:            db,
		tableName:     table,
		tableId:       tableId,
		fieldId:       fieldId,
		schemaName:    schema,
		attributeName: name,
	}
}

// func createPkFromColumn(db *DB, col *Column, tableId int, isAutoIncr bool) pk {
// 	attStr := createAttributeStrings(db, col.schemaName, col.tableName, col.FieldName, tableId, col.FieldId, db.driver)
// 	return pk{attributeStrings: attStr, autoIncrement: isAutoIncr}
// }

func createAttFromColumn(db *DB, col *Column, tableId int) att {
	attStr := createAttributeStrings(db, col.schemaName, col.tableName, col.FieldName, tableId, col.FieldId, db.driver)
	return att{attributeStrings: attStr, isDefault: col.HasDefault}
}

func createManyToSome(b body, typeOf reflect.Type) any {
	rel := ManyToSomeRelation{}
	targetPks := getPksFromType(typeOf)
	count := 0
	for i := range targetPks {
		if targetPks[i].Name == b.prefixName {
			count++
		}
	}

	if count == 0 {
		return nil
	}
	rel.IsDefault = getTagValue(b.valueOf.Type().Field(b.fieldId).Tag.Get("goe"), "default:") != ""
	rel.attributeStrings = createAttributeStrings(
		b.mapp.db,
		b.schema,
		b.mapp.pks[0].tableName,
		b.fieldName,
		b.mapp.tableId,
		b.fieldId,
		b.driver,
	)
	return rel
}

func createOneToSome(b body, typeOf reflect.Type) any {
	rel := OneToSomeRelation{}
	targetPks := getPksFromType(typeOf)
	count := 0
	for i := range targetPks {
		if targetPks[i].Name == b.prefixName {
			count++
		}
	}

	if count == 0 {
		return nil
	}

	rel.attributeStrings = createAttributeStrings(
		b.mapp.db,
		b.schema,
		b.mapp.pks[0].tableName,
		b.fieldName,
		b.mapp.tableId,
		b.fieldId,
		b.driver,
	)
	return rel
}

func newAttr(b body) error {
	createAttFromColumn(b.mapp.db, &Column{
		ColumnName: utils.ToSnakeCase(b.fieldName),
		FieldName:  b.fieldName,
		HasDefault: b.nullable,
	}, b.mapp.tableId)
	// addrMap.set(b.mapp.addr, att)
	return nil
}

// func getPkFromBody(b body) pk {
// 	attStr := createAttributeStrings(b.mapp.db, b.schema, b.tableName, b.fieldName, b.mapp.tableId, b.fieldId, b.driver)
// 	return pk{attributeStrings: attStr, autoIncrement: false}
// }
//
// func getPksFromBody(b body) []pk {
// 	pks := make([]pk, 0, 1)
// 	attStr := createAttributeStrings(b.mapp.db, b.schema, b.tableName, b.fieldName, b.mapp.tableId, b.fieldId, b.driver)
// 	pks = append(pks, pk{attributeStrings: attStr, autoIncrement: false})
// 	return pks
// }

func getPksFromType(typeOf reflect.Type) []reflect.StructField {
	field, exists := utils.GetTableID(typeOf)
	if exists {
		pks := make([]reflect.StructField, 1)
		pks[0] = field
		return pks
	}
	return fieldsByTags("pk", typeOf)
}
