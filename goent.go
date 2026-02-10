package goent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// Open opens a database connection
//
// # Example
//
//	goent.Open[Database](pgsql.Open("user=postgres password=postgres host=localhost port=5432 database=postgres", pgsql.Config{}))
func Open[T any](drv model.Driver, logFile string) (*T, error) {
	if logFile != "" {
		err := drv.AddLogger(utils.CreateLogger(logFile))
		if err != nil {
			return nil, err
		}
	}

	dc := drv.GetDatabaseConfig()
	dc.Init(drv.Name(), drv.ErrorTranslator())
	err := drv.Init()
	if err != nil {
		return nil, dc.ErrorHandler(context.TODO(), err)
	}

	ent := new(T)
	valueOf := reflect.ValueOf(ent).Elem()
	if valueOf.Kind() != reflect.Struct {
		return nil, errors.New("goent: invalid database, the target needs to be a struct")
	}
	dbId := valueOf.NumField() - 1
	if valueOf.Field(dbId).Type().Elem().Name() != "DB" {
		return nil, errors.New("goent: invalid database, last struct field needs to be goent.DB")
	}

	db, schemas := new(DB), make([]string, 0)
	db.SetDriver(drv)
	schemas, err = travelSchemas(db, dbId, valueOf)
	dc.SetSchemas(schemas)
	if err != nil {
		return nil, err
	}
	if ic := dc.InitCallback(); ic != nil {
		if err = ic(); err != nil {
			return nil, err
		}
	}
	return ent, nil
}

func travelSchemas(db *DB, dbId int, valueOf reflect.Value) ([]string, error) {
	var schemas []string
	valueOf.Field(dbId).Set(reflect.ValueOf(db))

	// set value for fields
	for i := range dbId { // Schema
		schemaOf := reflect.Indirect(valueOf.Field(i))

		schema := valueOf.Type().Field(i).Tag.Get("goe")
		if schema == "-" {
			continue
		}
		SchemaName := schemaOf.Type().Name()
		if schema == "" {
			schema = utils.ToSnakeCase(SchemaName)
		}
		schemas = append(schemas, schema)
		schemasMap[SchemaName] = &schema

		for j := range schemaOf.NumField() { // Table
			tableField := schemaOf.Field(j)
			tableType := utils.GetElemType(tableField)
			fieldName := schemaOf.Type().Field(j).Name
			tableOf, info := NewTableReflect(db, tableType, fieldName, schema, i, j)
			info.TableAddr = uintptr(tableField.Addr().UnsafePointer())
			setDBMethod := tableOf.MethodByName("SetDB")
			if setDBMethod.IsValid() {
				setDBMethod.Call([]reflect.Value{reflect.ValueOf(db)})
			}
			tableField.Set(tableOf)
			tableRegistry[info.TableAddr] = &info
		}
	}

	// init fields
	// for i := range tableId {
	// 	schema := schemaMap[i]
	// 	err := InitField(db, &schema, i, valueOf, models[i])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	return schemas, nil
}

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
	schemasMap  map[string]*string
	fieldId     int
	driver      model.Driver
	nullable    bool
	schema      *string
	stringInfos
}

func skipPrimaryKey[T comparable](slice []T, value T, tables reflect.Value, field reflect.StructField) bool {
	if slices.Contains(slice, value) {
		table, prefix := foreignKeyNamePattern(tables, field.Name)
		if table == "" && prefix == "" {
			return true
		}
	}
	return false
}

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
	for i := range pks {
		addr := uintptr(modelOf.Field(fieldIds[i]).Addr().UnsafePointer())
		addrMap.set(addr, pks[i])
	}
	return nil
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

func newAttr(b body) error {
	goeTag := b.valueOf.Type().Field(b.fieldId).Tag.Get("goe")
	at := createAtt(
		b.mapp.db,
		b.valueOf.Type().Field(b.fieldId).Name,
		b.schema,
		b.mapp.pks[0].tableName,
		b.mapp.tableId,
		b.fieldId,
		getTagValue(goeTag, "default:") != "",
		b.driver,
	)
	addrMap.set(b.mapp.addr, at)
	return nil
}

func getPks(typeOf reflect.Type) []reflect.StructField {
	var pks []reflect.StructField
	pks = append(pks, fieldsByTags("pk", typeOf)...)

	id, valid := utils.GetTableID(typeOf)
	isSameName := func(f reflect.StructField) bool {
		return f.Name == id.Name
	}
	if valid && !slices.ContainsFunc(pks, isSameName) {
		pks = append(pks, id)
	}
	return pks
}

func getPk(db *DB, schema *string, valueOf reflect.Value, tableId int, driver model.Driver) ([]pk, []int, error) {
	typeOf := valueOf.Type()
	fields := getPks(typeOf)
	if len(fields) == 0 {
		return nil, nil, fmt.Errorf("goent: getPk() struct %q don't have a primary key setted", typeOf.Name())
	}

	table := utils.ParseTableNameByValue(valueOf)
	pks := make([]pk, len(fields))
	fieldIds := make([]int, len(fields))
	for i := range fields {
		fieldId := getFieldId(typeOf, fields[i].Name)
		pks[i] = createPk(db, schema, table, fields[i].Name, isReturningId(fields[i]), tableId, fieldId, driver)
		fieldIds[i] = fieldId
	}

	return pks, fieldIds, nil
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
	return getTagValue(geoTag, "default:") != "" || isAutoIncrement(id)
}

func checkAllFields(valueOf reflect.Value, table string) bool {
	for i := 0; i < valueOf.NumField(); i++ {
		fieldN := valueOf.Field(i)
		// check if there is a slice to typeOf
		if fieldN.Kind() == reflect.Slice {
			if fieldN.Type().Elem().Name() == table {
				return true
			}
		}
	}
	return false
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

func primaryKeys(str reflect.Type) (pks []reflect.StructField) {
	field, exists := utils.GetTableID(str)
	if exists {
		pks := make([]reflect.StructField, 1)
		pks[0] = field
		return pks
	} else {
		// TODO: Return anonymous pk para len(pks) == 0
		return fieldsByTags("pk", str)
	}
}

func fieldsByTags(tag string, str reflect.Type) (f []reflect.StructField) {
	f = make([]reflect.StructField, 0)
	if str.Kind() != reflect.Struct {
		return f
	}
	tag = ";" + tag + ";"
	for i := 0; i < str.NumField(); i++ {
		goeTag := str.Field(i).Tag.Get("goe")
		if strings.Contains(";"+goeTag+";", tag) {
			f = append(f, str.Field(i))
		}
	}
	return f
}

func getTagValue(FieldTag string, subTag string) string {
	values := strings.Split(FieldTag, ";")
	for _, v := range values {
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
		if pks = getPks(foreign.Elem().Type()); len(pks) == 0 {
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
		if addrMap.get(b.mapp.addr) == nil {
			addrMap.set(b.mapp.addr, v)
		}
		for i := range b.mapp.pks {
			if !b.nullable && b.mapp.pks[i].fieldId == v.fieldId {
				b.mapp.pks[i].autoIncrement = false
			}
		}
	case OneToSomeRelation:
		goeTag := fieldAtt.Tag.Get("goe")
		v.IsOneToMany = utils.HasTagValue(goeTag, "o2m")
		if addrMap.get(b.mapp.addr) == nil {
			addrMap.set(b.mapp.addr, v)
		}
	}
	return nil
}
