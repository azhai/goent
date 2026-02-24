package goent

import (
	"context"
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
		return nil, ErrInvalidDatabase
	}
	dbId := valueOf.NumField() - 1
	if valueOf.Field(dbId).Type().Elem().Name() != "DB" {
		return nil, ErrInvalidDBField
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

	tableRegLock.Lock()
	defer tableRegLock.Unlock()

	// set value for fields
	for i := range dbId { // Schema
		field := valueOf.Field(i)
		fieldType := valueOf.Type().Field(i)

		// Handle embedded pointer structs - initialize if nil
		if field.Kind() == reflect.Pointer && field.IsNil() {
			field.Set(reflect.New(fieldType.Type.Elem()))
		}

		schemaOf := reflect.Indirect(field)
		if !schemaOf.IsValid() {
			continue
		}

		schema := fieldType.Tag.Get("goe")
		if schema == "-" {
			continue
		}

		// Skip non-struct fields (like embedded DB)
		if schemaOf.Kind() != reflect.Struct {
			continue
		}

		SchemaName := schemaOf.Type().Name()
		schemaName, _ := utils.ParseSchemaTag(schema)
		if schemaName == "" {
			schemaName = utils.ToSnakeCase(SchemaName)
		}
		schemas = append(schemas, schemaName)
		schemaCopy := schemaName
		schemaRegistry[SchemaName] = &schemaCopy

		for j := range schemaOf.NumField() { // Table
			tableField := schemaOf.Field(j)
			if !tableField.CanSet() {
				continue
			}
			tableType := utils.GetElemType(tableField)
			// Skip fields that are not Table types
			if tableType.Kind() != reflect.Struct || !strings.HasPrefix(tableType.Name(), "Table[") {
				continue
			}
			tableAddr := uintptr(tableField.Addr().UnsafePointer())
			fieldName := schemaOf.Type().Field(j).Name
			tableOf, info := NewTableReflect(db, tableType, tableAddr, fieldName, schema, i, j)
			setDBMethod := tableOf.MethodByName("SetDB")
			if setDBMethod.IsValid() {
				setDBMethod.Call([]reflect.Value{reflect.ValueOf(db)})
			}
			tableField.Set(tableOf)
			tableRegistry[info.TableAddr] = &info
		}
	}

	for _, info := range tableRegistry {
		for fkName, foreign := range info.Foreigns {
			if foreign.Reference != nil {
				continue
			}
			var refTableName string
			switch foreign.Type {
			case M2O, O2O:
				if _, ok := info.Columns[fkName]; !ok {
					continue
				}
				refTableName = strings.TrimSuffix(fkName, "_id")
			case O2M:
				refTableName = strings.TrimSuffix(foreign.ForeignKey, "_id")
			case M2M:
				if foreign.Middle == nil {
					continue
				}
				refTableName = strings.TrimSuffix(foreign.Middle.Right, "_id")
			}
			for otherAddr, otherInfo := range tableRegistry {
				if otherAddr == info.TableAddr {
					continue
				}
				if strings.EqualFold(otherInfo.TableName, refTableName) ||
					strings.EqualFold(otherInfo.FieldName, refTableName) ||
					strings.HasSuffix(strings.ToLower(otherInfo.TableName), "_"+strings.ToLower(refTableName)) {
					foreign.Reference = &Field{
						TableAddr:  otherAddr,
						ColumnName: "id",
					}
					for _, pk := range otherInfo.PrimaryKeys {
						if pk.IsAutoIncr {
							foreign.Reference.FieldId = pk.FieldId
							break
						}
					}
					break
				}
			}
		}
	}

	return schemas, nil
}

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
	// for i := range pks {
	// 	addr := uintptr(modelOf.Field(fieldIds[i]).Addr().UnsafePointer())
	// 	addrMap.set(addr, pks[i])
	// }
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

// func newAttr(b body) error {
// 	goeTag := b.valueOf.Type().Field(b.fieldId).Tag.Get("goe")
// 	at := createAtt(
// 		b.mapp.db,
// 		b.valueOf.Type().Field(b.fieldId).Name,
// 		b.schema,
// 		b.mapp.pks[0].tableName,
// 		b.mapp.tableId,
// 		b.fieldId,
// 		getTagValue(goeTag, "default:") != "",
// 		b.driver,
// 	)
// 	addrMap.set(b.mapp.addr, at)
// 	return nil
// }

// func getPks(typeOf reflect.Type) []reflect.StructField {
// 	var pks []reflect.StructField
// 	pks = append(pks, fieldsByTags("pk", typeOf)...)

// 	id, valid := utils.GetTableID(typeOf)
// 	isSameName := func(f reflect.StructField) bool {
// 		return f.Name == id.Name
// 	}
// 	if valid && !slices.ContainsFunc(pks, isSameName) {
// 		pks = append(pks, id)
// 	}
// 	return pks
// }

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
		return nil, nil, NewNoPrimaryKeyError(typeOf.Name())
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
	return getTagValue(geoTag, "default:") != "" || isAutoIncrement(id)
}

func checkAllFields(valueOf reflect.Value, table string) bool {
	for _, fieldN := range valueOf.Fields() {
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
