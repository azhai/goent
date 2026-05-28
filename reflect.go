package goent

import (
	"reflect"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// InitField initializes the fields of a model struct, setting up primary keys and attributes.
func InitField(db *DB, schema *string, tableId int, tables, modelOf reflect.Value) error {
	pks, fieldIds, err := getPk(db, schema, modelOf, tableId, db.driver)
	if err != nil {
		return err
	}

	for fieldId := range modelOf.NumField() {
		field := modelOf.Type().Field(fieldId)
		if skipPrimaryKey(fieldIds, fieldId, tables, field) {
			continue
		}
		addr := uintptr(modelOf.Field(fieldId).Addr().UnsafePointer())
		mapp := &infosMap{pks: pks, db: db, tableId: tableId, addr: addr}
		switch modelOf.Field(fieldId).Kind() {
		case reflect.Slice:
			err = handlerSlice(body{
				fieldId:     fieldId,
				fieldTypeOf: modelOf.Field(fieldId).Type().Elem(),
				driver:      db.driver,
				tables:      tables,
				valueOf:     modelOf,
				typeOf:      modelOf.Type(),
				schema:      schema,
				mapp:        mapp,
			}, helperAttribute)
			if err != nil {
				return err
			}
		case reflect.Struct:
			handlerStruct(body{
				fieldId:     fieldId,
				fieldTypeOf: modelOf.Field(fieldId).Type(),
				driver:      db.driver,
				valueOf:     modelOf,
				schema:      schema,
				mapp:        mapp,
			}, newAttr)
		case reflect.Pointer:
			helperAttribute(body{
				fieldId:  fieldId,
				driver:   db.driver,
				tables:   tables,
				valueOf:  modelOf,
				typeOf:   modelOf.Type(),
				schema:   schema,
				mapp:     mapp,
				nullable: true,
			})
		default:
			helperAttribute(body{
				fieldId: fieldId,
				driver:  db.driver,
				tables:  tables,
				valueOf: modelOf,
				typeOf:  modelOf.Type(),
				schema:  schema,
				mapp:    mapp,
			})
		}
	}
	return nil
}

func getPk(db *DB, schema *string, valueOf reflect.Value, tableId int, driver model.Driver) ([]pk, []int, error) {
	typeOf := valueOf.Type()
	fields := fieldsByTags("pk", typeOf)
	if len(fields) == 0 {
		id, valid := utils.GetTableID(typeOf)
		if valid {
			fields = append(fields, id)
		}
	}
	if len(fields) == 0 {
		return nil, nil, model.NewNoPrimaryKeyError(typeOf.Name())
	}

	table := utils.ParseTableNameByValue(valueOf)
	size := len(fields)
	pks, fieldIds := make([]pk, size), make([]int, size)
	for i := range fields {
		fieldId := getFieldId(typeOf, fields[i].Name)
		pks[i] = createPk(db, schema, table, fields[i].Name, isReturningId(fields[i]), tableId, fieldId, driver)
		fieldIds[i] = fieldId
	}

	return pks, fieldIds, nil
}

func createPk(db *DB, schema *string, table string, attributeName string, autoIncrement bool, tableId, fieldId int, Driver model.Driver) pk {
	table = Driver.KeywordHandler(table)
	attStr := createAttributeStrings(db, schema, table, attributeName, tableId, fieldId, Driver)
	return pk{attributeStrings: attStr, autoIncrement: autoIncrement}
}

func fieldsByTags(tag string, typeOf reflect.Type) []reflect.StructField {
	var fields []reflect.StructField
	for field := range typeOf.Fields() {
		if utils.HasTagValue(field.Tag.Get("goe"), tag) {
			fields = append(fields, field)
		}
	}
	return fields
}

func getFieldId(typeOf reflect.Type, fieldName string) int {
	for i := 0; i < typeOf.NumField(); i++ {
		if typeOf.Field(i).Name == fieldName {
			return i
		}
	}
	return 0
}

func isReturningId(id reflect.StructField) bool {
	geoTag := id.Tag.Get("goe")
	if utils.HasTagValue(geoTag, "not_incr") {
		return false
	}
	return getTagValue(geoTag, "default:") != "" || isAutoIncrementField(id)
}

func isAutoIncrementField(id reflect.StructField) bool {
	geoTag := id.Tag.Get("goe")
	if utils.HasTagValue(geoTag, "not_incr") {
		return false
	}
	return strings.Contains(id.Type.Kind().String(), "int")
}

func createRelation(b body, createMany RelationFunc, createOne RelationFunc) any {
	fieldOf := utils.GetTableModel(b.tables.FieldByName(b.tableName)).Elem()
	if !fieldOf.IsValid() {
		return nil
	}
	typeName := b.typeOf.Name()
	if checkAllFields(fieldOf, typeName) {
		return createMany(b, fieldOf.Type()) // M2O
	}
	if table := strings.ReplaceAll(typeName, b.tableName, ""); table != typeName {
		valueOf := utils.GetTableModel(b.tables.FieldByName(table))
		if valueOf.IsValid() && !valueOf.IsZero() {
			if checkAllFields(valueOf.Elem(), b.tableName) {
				return createMany(b, valueOf.Elem().Type()) // M2M
			}
		}
	}
	return createOne(b, fieldOf.Type()) // O2M/O2O
}

func helperAttribute(b body) error {
	fieldAtt := b.valueOf.Type().Field(b.fieldId)
	table, prefix := foreignKeyNamePattern(b.tables, fieldAtt.Name)
	if table == "" {
		return newAttr(b)
	}
	b.stringInfos = stringInfos{prefixName: prefix, tableName: table, fieldName: fieldAtt.Name}
	rel := createRelation(b, createManyToSome, createOneToSome)
	if rel == nil {
		return newAttr(b)
	}
	switch v := rel.(type) {
	case ManyToSomeRelation:
		for i := range b.mapp.pks {
			if !b.nullable && b.mapp.pks[i].fieldId == v.fieldId {
				b.mapp.pks[i].autoIncrement = false
			}
		}
	case OneToSomeRelation:
		goeTag := fieldAtt.Tag.Get("goe")
		v.IsOneToMany = utils.HasTagValue(goeTag, "o2m")
	}
	return nil
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
	columnName := utils.ToSnakeCase(b.fieldName)
	// Check for column tag override on the struct field
	if b.valueOf.IsValid() && b.fieldId < b.valueOf.NumField() {
		fieldOf := b.valueOf.Type().Field(b.fieldId)
		if col, ok := utils.GetTagValue(fieldOf.Tag.Get("goe"), "column"); ok && col != "" {
			columnName = col
		}
	}
	createAttFromColumn(b.mapp.db, &Column{
		ColumnName: columnName,
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
