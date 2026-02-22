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

type att struct {
	isDefault bool
	attributeStrings
}

// ManyToSomeRelation represents a many-to-one or many-to-many relationship between tables.
type ManyToSomeRelation struct {
	IsDefault bool
	attributeStrings
}

// OneToSomeRelation represents a one-to-one or one-to-many relationship between tables.
type OneToSomeRelation struct {
	IsOneToMany bool
	attributeStrings
}

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
	return nil
}

func getPksFromType(typeOf reflect.Type) []reflect.StructField {
	field, exists := utils.GetTableID(typeOf)
	if exists {
		pks := make([]reflect.StructField, 1)
		pks[0] = field
		return pks
	}
	return fieldsByTags("pk", typeOf)
}
