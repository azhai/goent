package goent

import (
	"reflect"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// RelationFunc is a function type that defines how to handle a relation field.
// It takes a body struct and the type of the relation field as parameters.
// The function returns the value of the relation field.
type RelationFunc func(b body, typeOf reflect.Type) any

// data used for map
type infosMap struct {
	db      *DB
	pks     []pk
	tableId int
	addr    uintptr
}

// data used for Migration
type infosMigrate struct {
	field      reflect.StructField
	table      *model.TableMigrate
	fieldNames []string
}

type stringInfos struct {
	prefixName string
	tableName  string
	fieldName  string
}

type body struct {
	tables      reflect.Value // database value of
	valueOf     reflect.Value // struct value of
	typeOf      reflect.Type  // struct type of
	fieldTypeOf reflect.Type
	mapp        *infosMap     // used on map
	migrate     *infosMigrate // used on migrate
	fieldId     int
	driver      model.Driver
	nullable    bool
	schema      *string
	stringInfos
}

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

type ManyToSomeRelation struct {
	IsDefault bool
	attributeStrings
}

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

func createAttFromColumn(db *DB, col *Column, tableId int) att {
	attStr := createAttributeStrings(db, col.schemaName, col.tableName, col.FieldName, tableId, col.FieldId, db.driver)
	return att{attributeStrings: attStr, isDefault: col.HasDefault}
}

func skipPrimaryKey[T comparable](slice []T, value T, tables reflect.Value, field reflect.StructField) bool {
	if slices.Contains(slice, value) {
		goeTag := field.Tag.Get("goe")
		if utils.HasTagValue(goeTag, "m2o") || utils.HasTagValue(goeTag, "o2o") {
			return false
		}
		table, prefix := foreignKeyNamePattern(tables, field.Name)
		if table == "" && prefix == "" {
			return true
		}
	}
	return false
}

func getTagValue(FieldTag string, subTag string) string {
	values := strings.SplitSeq(FieldTag, ";")
	for v := range values {
		if after, found := strings.CutPrefix(v, subTag); found {
			return after
		}
	}
	return ""
}

func foreignKeyNamePattern(dbTables reflect.Value, fieldName string) (table, suffix string) {
	var pks []reflect.StructField
	tableNames := utils.GetFieldNames(dbTables.Type())
	for r := 1; r < len(fieldName); r++ {
		table, suffix = fieldName[:r], fieldName[r:]
		if !slices.Contains(tableNames, table) {
			continue
		}
		foreign := utils.GetTableModel(dbTables.FieldByName(table))
		if pks = getPksFromType(foreign.Elem().Type()); len(pks) == 0 {
			continue
		}
		pkName := compareForeignKey(suffix, pks)
		if pkName != "" {
			return table, pkName
		}
	}
	return "", ""
}

func compareForeignKey(suffix string, pks []reflect.StructField) string {
	if len(pks) == 0 {
		return ""
	}
	if len(pks) == 1 {
		pkName := pks[0].Name
		if strings.HasPrefix(suffix, pkName) {
			return pkName
		}
		return ""
	}
	for c := 1; c <= len(suffix); c++ {
		pkName := suffix[:c]
		isSameName := func(f reflect.StructField) bool {
			return f.Name == pkName
		}
		if slices.ContainsFunc(pks, isSameName) {
			return pkName
		}
	}
	return ""
}

func checkAllFields(valueOf reflect.Value, table string) bool {
	for i := 0; i < valueOf.NumField(); i++ {
		fieldN := valueOf.Type().Field(i)
		// check if there is a slice to typeOf
		if fieldN.Type.Kind() == reflect.Slice {
			if fieldN.Type.Elem().Name() == table {
				return true
			}
		}
	}
	return false
}

func handlerStruct(b body, create func(b body) error) error {
	return create(b)
}

func handlerSlice(b body, helper func(b body) error) error {
	switch b.fieldTypeOf.Kind() {
	case reflect.Uint8:
		return helper(b)
	}
	return nil
}
