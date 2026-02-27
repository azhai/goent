package goent

import (
	"reflect"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// attributeStrings holds common attribute information for database columns and relations
// It contains basic metadata about database attributes
type attributeStrings struct {
	db            *DB     // Database connection
	schemaName    *string // Schema name
	tableId       int     // Table ID
	tableName     string  // Table name
	attributeName string  // Attribute name
	fieldId       int     // Field ID
}

// pk represents a primary key attribute
// It extends attributeStrings with auto-increment information
type pk struct {
	autoIncrement    bool // Whether the primary key is auto-incrementing
	attributeStrings      // Embedded attribute strings
}

// att represents a regular column attribute
// It extends attributeStrings with default value information
type att struct {
	isDefault        bool // Whether the column has a default value
	attributeStrings      // Embedded attribute strings
}

// ManyToSomeRelation represents a many-to-one or many-to-many relationship between tables
// It extends attributeStrings with relationship-specific information
type ManyToSomeRelation struct {
	IsDefault        bool // Whether the relationship is the default one
	attributeStrings      // Embedded attribute strings
}

// OneToSomeRelation represents a one-to-one or one-to-many relationship between tables
// It extends attributeStrings with relationship-specific information
type OneToSomeRelation struct {
	IsOneToMany      bool // Whether the relationship is one-to-many
	attributeStrings      // Embedded attribute strings
}

// createAttributeStrings creates an attributeStrings struct with the given parameters
// It handles keyword escaping and name formatting
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

// createAttFromColumn creates an att struct from a Column
// It converts a Column object to an att attribute
func createAttFromColumn(db *DB, col *Column, tableId int) att {
	attStr := createAttributeStrings(db, col.schemaName, col.tableName, col.FieldName, tableId, col.FieldId, db.driver)
	return att{attributeStrings: attStr, isDefault: col.HasDefault}
}

// createManyToSome creates a ManyToSomeRelation from the given body and type
// It returns nil if no matching primary key is found
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

// createOneToSome creates a OneToSomeRelation from the given body and type
// It returns nil if no matching primary key is found
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

// newAttr creates a new attribute from the body
// It creates a Column and converts it to an att attribute
func newAttr(b body) error {
	createAttFromColumn(b.mapp.db, &Column{
		ColumnName: utils.ToSnakeCase(b.fieldName),
		FieldName:  b.fieldName,
		HasDefault: b.nullable,
	}, b.mapp.tableId)
	return nil
}

// getPksFromType returns the primary key fields from the given type
// It looks for fields with pk tags or the TableName method
func getPksFromType(typeOf reflect.Type) []reflect.StructField {
	field, exists := utils.GetTableID(typeOf)
	if exists {
		pks := make([]reflect.StructField, 1)
		pks[0] = field
		return pks
	}
	return fieldsByTags("pk", typeOf)
}
