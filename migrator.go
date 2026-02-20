package goent

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// type fieldDesc struct {
// 	Field      reflect.Value
// 	FieldName  string
// 	HasSchema  bool
// 	SchemaName string
// }

type dbMigrator struct {
	// fieldDescs []*fieldDesc
	// schemasMap map[string]*string
	db *DB
	*model.Migrator
}

func migrateFrom(ent any, db *DB) *dbMigrator {
	valueOf := reflect.ValueOf(ent).Elem()
	dc := db.driver.GetDatabaseConfig()
	dm := &dbMigrator{
		Migrator: &model.Migrator{
			Tables:  make(map[string]*model.TableMigrate),
			Schemas: dc.Schemas(),
		},
		// fieldDescs: make([]*fieldDesc, 0),
		// schemasMap: schemaRegistry,
		db: db,
	}

	for _, info := range tableRegistry {
		elem := valueOf.Field(info.SchemaId).FieldByName(info.FieldName)
		// desc := &fieldDesc{Field: elem, HasSchema: true}
		// desc.FieldName, desc.SchemaName = info.FieldName, info.SchemaName
		// dm.fieldDescs = append(dm.fieldDescs, desc)

		elem = reflect.Indirect(elem)
		tm := &model.TableMigrate{Schema: &info.SchemaName, Name: info.TableName}
		tm, dm.Error = dm.typeField(tm, valueOf, elem, db.driver)
		if dm.Error != nil {
			return dm
		}
		tm.EscapingName = db.driver.KeywordHandler(tm.Name)
		dm.Tables[tm.Name] = tm
	}

	return dm
}

func (dm *dbMigrator) typeField(tm *model.TableMigrate, tables, elem reflect.Value, driver model.Driver) (*model.TableMigrate, error) {
	elem = utils.GetTableModel(elem)
	elemType := elem.Type()
	if elemType.Kind() == reflect.Pointer {
		elemType = elemType.Elem()
		elem = elem.Elem()
	}

	pks, fieldNames, err := migratePk(elemType, driver)
	if err != nil {
		return nil, err
	}

	for fieldId := range elem.NumField() {
		fieldOf := elemType.Field(fieldId)
		goeTag := fieldOf.Tag.Get("goe")
		if goeTag == "-" || skipPrimaryKey(fieldNames, fieldOf.Name, tables, fieldOf) {
			continue
		}

		elemField := elem.Field(fieldId)
		switch elemField.Kind() {

		case reflect.Interface, reflect.Func:
			continue

		case reflect.Slice:
			err = handlerSlice(body{
				fieldId:     fieldId,
				driver:      driver,
				tables:      tables,
				fieldTypeOf: elemField.Type().Elem(),
				typeOf:      elemType,
				valueOf:     elem,
				migrate: &infosMigrate{
					table:      tm,
					field:      fieldOf,
					fieldNames: fieldNames,
				},
			}, helperAttributeMigrate)
			if err != nil {
				return nil, err
			}

		case reflect.Struct:
			if fieldOf.Anonymous {
				continue
			}
			err = handlerStruct(body{
				fieldId:     fieldId,
				driver:      driver,
				nullable:    isNullable(fieldOf),
				fieldTypeOf: elemField.Type(),
				valueOf:     elem,
				migrate: &infosMigrate{
					table: tm,
					field: fieldOf,
				},
			}, migrateAtt)
			if err != nil {
				return nil, err
			}

		case reflect.Pointer:
			err = helperAttributeMigrate(body{
				fieldId:  fieldId,
				driver:   driver,
				nullable: true,
				tables:   tables,
				valueOf:  elem,
				typeOf:   elemType,
				migrate: &infosMigrate{
					table:      tm,
					field:      fieldOf,
					fieldNames: fieldNames,
				},
			})
			if err != nil {
				return nil, err
			}

		default:
			err = helperAttributeMigrate(body{
				fieldId: fieldId,
				driver:  driver,
				tables:  tables,
				valueOf: elem,
				typeOf:  elemType,
				migrate: &infosMigrate{
					table:      tm,
					field:      fieldOf,
					fieldNames: fieldNames,
				},
			})
			if err != nil {
				return nil, err
			}

		}
	}

	for _, pk := range pks {
		tm.PrimaryKeys = append(tm.PrimaryKeys, *pk)
	}
	// fmt.Printf("Migrate: %s\n%#v\n%#v\n\n", tm.Name, tm.OneToSomes, tm.ManyToSomes)
	return tm, nil
}

// func setTableFields(elem reflect.Value, tm *model.TableMigrate, db *DB, schema *string) error {
// 	if elem.Kind() == reflect.Ptr {
// 		elem = elem.Elem()
// 	}
// 	if tableNameField := elem.FieldByName("TableName"); tableNameField.IsValid() {
// 		tableNameField.SetString(tm.Name)
// 	}
// 	modelField := elem.FieldByName("Model")
// 	if !modelField.IsValid() {
// 		return nil
// 	}
// 	modelOf := modelField
// 	if modelOf.Kind() == reflect.Ptr {
// 		if modelOf.IsNil() {
// 			modelOf = reflect.New(modelOf.Type().Elem())
// 			modelField.Set(modelOf)
// 			modelOf = modelOf.Elem()
// 		} else {
// 			modelOf = modelOf.Elem()
// 		}
// 	}
//
// 	var pks []string
// 	fields := utils.NewCoMap[reflect.Value]()
// 	for _, pk := range tm.PrimaryKeys {
// 		pks = append(pks, pk.Name)
// 		if f := modelOf.FieldByName(pk.FieldName); f.IsValid() {
// 			fields.Set(pk.Name, f.Addr())
// 		}
// 	}
// 	for _, attr := range tm.Attributes {
// 		if f := modelOf.FieldByName(attr.FieldName); f.IsValid() {
// 			fields.Set(attr.Name, f.Addr())
// 		}
// 	}
// 	if pkeysField := elem.FieldByName("PrimaryNames"); pkeysField.IsValid() {
// 		pkeysField.Set(reflect.ValueOf(pks))
// 	}
// 	if columnsField := elem.FieldByName("Columns"); columnsField.IsValid() {
// 		columns := make(map[string]*Column)
// 		for _, pk := range tm.PrimaryKeys {
// 			columns[pk.Name] = &Column{
// 				FieldName:  pk.FieldName,
// 				ColumnName: pk.Name,
// 				ColumnType: pk.DataType,
// 			}
// 		}
// 		for _, attr := range tm.Attributes {
// 			columns[attr.Name] = &Column{
// 				FieldName:  attr.FieldName,
// 				ColumnName: attr.Name,
// 				ColumnType: attr.DataType,
// 				AllowNull:  attr.Nullable,
// 			}
// 		}
// 		columnsField.Set(reflect.ValueOf(columns))
// 	}
// 	if schemaNameField := elem.FieldByName("SchemaName"); schemaNameField.IsValid() {
// 		if schema != nil {
// 			schemaNameField.SetString(*schema)
// 		}
// 	}
// 	if schemaIdField := elem.FieldByName("SchemaId"); schemaIdField.IsValid() {
// 		schemaIdField.SetInt(0)
// 	}
// 	if tableIdField := elem.FieldByName("TableId"); tableIdField.IsValid() {
// 		tableIdField.SetInt(0)
// 	}
// 	return nil
// }

func createManyToSomeMigrate(b body, typeOf reflect.Type) any {
	fieldPks := getPksFromType(typeOf)
	count := 0
	for i := range fieldPks {
		if fieldPks[i].Name == b.prefixName {
			count++
		}
	}
	if count == 0 {
		return nil
	}

	rel := new(model.ManyToSomeMigrate)
	rel.TargetTable = utils.ParseTableNameByType(typeOf)
	rel.TargetColumn = utils.ToSnakeCase(b.prefixName)
	rel.TargetSchema = schemaRegistry[typeOf.Name()]
	rel.EscapingTargetTable = b.driver.KeywordHandler(rel.TargetTable)
	rel.EscapingTargetColumn = b.driver.KeywordHandler(rel.TargetColumn)

	rel.Name = utils.ToSnakeCase(b.fieldName)
	rel.EscapingName = b.driver.KeywordHandler(rel.Name)
	rel.Nullable = b.nullable
	rel.Default = getTagValue(b.migrate.field.Tag.Get("goe"), "default:")
	if err := checkIndex(b, rel.AttributeMigrate, true); err != nil {
		panic(err)
	}
	return rel
}

func createOneToSomeMigrate(b body, typeOf reflect.Type) any {
	fieldPks := getPksFromType(typeOf)
	count := 0
	for i := range fieldPks {
		if fieldPks[i].Name == b.prefixName {
			count++
		}
	}
	if count == 0 {
		return nil
	}

	rel := new(model.OneToSomeMigrate)
	rel.TargetTable = utils.ParseTableNameByType(typeOf)
	rel.TargetColumn = utils.ToSnakeCase(b.prefixName)
	rel.TargetSchema = schemaRegistry[typeOf.Name()]
	rel.EscapingTargetTable = b.driver.KeywordHandler(rel.TargetTable)
	rel.EscapingTargetColumn = b.driver.KeywordHandler(rel.TargetColumn)

	rel.Name = utils.ToSnakeCase(b.fieldName)
	rel.EscapingName = b.driver.KeywordHandler(rel.Name)
	rel.Nullable = b.nullable
	if err := checkIndex(b, rel.AttributeMigrate, true); err != nil {
		panic(err)
	}
	return rel
}

func migratePk(typeOf reflect.Type, driver model.Driver) ([]*model.PrimaryKeyMigrate, []string, error) {
	if typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}

	if typeOf.Kind() == reflect.Slice {
		return nil, nil, fmt.Errorf("goent: migratePk() cannot migrate slice type %v", typeOf)
	}

	if strings.HasPrefix(typeOf.Name(), "Table[") && strings.HasSuffix(typeOf.Name(), "]") {
		if typeOf.Kind() == reflect.Struct {
			modelField, ok := typeOf.FieldByName("Model")
			if ok {
				modelType := modelField.Type
				if modelType.Kind() == reflect.Pointer {
					modelType = modelType.Elem()
				}
				typeOf = modelType
			}
		}
	}

	fields := getPksFromType(typeOf)
	if len(fields) == 0 {
		return nil, nil, fmt.Errorf("goent: migratePk() struct %q don't have a primary key setted", typeOf.Name())
	}

	if typeOf.Name() == "" {
		return nil, nil, fmt.Errorf("goent: migratePk() struct %q don't have a primary key setted", typeOf.String())
	}

	pks := make([]*model.PrimaryKeyMigrate, len(fields))
	fieldsNames := make([]string, len(fields))
	for i := range fields {
		pks[i] = createMigratePk(fields[i].Name, isAutoIncrement(fields[i]), getTagType(fields[i]), getTagValue(fields[i].Tag.Get("goe"), "default:"), driver)
		fieldsNames[i] = fields[i].Name
	}
	return pks, fieldsNames, nil
}

func isAutoIncrement(id reflect.StructField) bool {
	return strings.Contains(id.Type.Kind().String(), "int")
}

func migrateAtt(b body) error {
	migField := b.migrate.field
	if slices.Contains(b.migrate.fieldNames, migField.Name) {
		return nil
	}
	at := createMigrateAtt(
		migField.Name,
		getTagType(migField),
		b.nullable,
		getTagValue(migField.Tag.Get("goe"), "default:"),
		b.driver,
	)
	b.migrate.table.Attributes = append(b.migrate.table.Attributes, at)

	return checkIndex(b, at, false)
}

func getTagType(field reflect.StructField) string {
	value := getTagValue(field.Tag.Get("goe"), "type:")
	if value != "" {
		return strings.ReplaceAll(value, " ", "")
	}
	dataType := field.Type.String()
	if dataType[0] == '*' {
		return dataType[1:]
	}
	return dataType
}

func isNullable(field reflect.StructField) bool {
	dataType := field.Type.String()
	return strings.HasPrefix(dataType, "sql.Null")
}

func getIndex(field reflect.StructField) string {
	value := getTagValue(field.Tag.Get("goe"), "index(")
	if value != "" {
		return value[0 : len(value)-1]
	}
	return ""
}

func getIndexValue(valueTag string, tag string) string {
	values := strings.SplitSeq(valueTag, " ")
	for v := range values {
		if _, value, ok := strings.Cut(v, tag); ok {
			return value
		}
	}
	return ""
}

func createMigratePk(attributeName string, autoIncrement bool, dataType, defaultTag string, driver model.Driver) *model.PrimaryKeyMigrate {
	return &model.PrimaryKeyMigrate{
		AttributeMigrate: model.AttributeMigrate{
			FieldName:    attributeName,
			Name:         utils.ToSnakeCase(attributeName),
			EscapingName: driver.KeywordHandler(utils.ToSnakeCase(attributeName)),
			DataType:     dataType,
			Default:      defaultTag,
		},
		AutoIncrement: autoIncrement,
	}
}

func createMigrateAtt(attributeName string, dataType string, nullable bool, defaultValue string, driver model.Driver) model.AttributeMigrate {
	return model.AttributeMigrate{
		FieldName:    attributeName,
		Name:         utils.ToSnakeCase(attributeName),
		EscapingName: driver.KeywordHandler(utils.ToSnakeCase(attributeName)),
		DataType:     dataType,
		Nullable:     nullable,
		Default:      defaultValue,
	}
}

func helperAttributeMigrate(b body) error {
	migField := b.migrate.field
	table, prefix := foreignKeyNamePattern(b.tables, migField.Name)
	if table == "" {
		return migrateAtt(b)
	}
	// fmt.Printf("\n# Foreign: %s, %s\n\n", table, prefix)

	b.stringInfos = stringInfos{prefixName: prefix, tableName: table, fieldName: migField.Name}
	rel := createRelation(b, createManyToSomeMigrate, createOneToSomeMigrate)
	if rel == nil {
		return migrateAtt(b)
	}

	migTable := b.migrate.table
	switch v := rel.(type) {
	case *model.ManyToSomeMigrate:
		// if v == nil {
		// 	return migrateAtt(b)
		// }
		v.DataType = getTagType(migField)
		migTable.ManyToSomes = append(migTable.ManyToSomes, *v)
	case *model.OneToSomeMigrate:
		// if v == nil {
		// 	if slices.Contains(b.migrate.fieldNames, b.migrate.field.Name) {
		// 		return nil
		// 	}
		// 	return migrateAtt(b)
		// }
		goeTag := migField.Tag.Get("goe")
		v.IsOneToMany = utils.HasTagValue(goeTag, "o2m")
		v.DataType = getTagType(migField)
		migTable.OneToSomes = append(migTable.OneToSomes, *v)
	}
	return nil
}

func checkIndex(b body, at model.AttributeMigrate, skipUnique bool) error {
	migTable, migField := b.migrate.table, b.migrate.field
	indexFunc := getIndex(migField)
	if indexFunc != "" {
		for index := range strings.SplitSeq(indexFunc, ",") {
			indexName := getIndexValue(index, "n:")

			if indexName == "" {
				indexName = migTable.Name + "_idx_" + strings.ToLower(migField.Name)
			}
			in := model.IndexMigrate{
				Name:         migTable.Name + "_" + indexName,
				EscapingName: b.driver.KeywordHandler(migTable.Name + "_" + indexName),
				Unique:       strings.Contains(index, "unique"),
				Func:         strings.ToLower(getIndexValue(index, "f:")),
				Attributes:   []model.AttributeMigrate{at},
			}

			var i int
			if i = slices.IndexFunc(migTable.Indexes, func(i model.IndexMigrate) bool {
				return i.Name == in.Name && i.Unique == in.Unique && i.Func == in.Func
			}); i == -1 {
				if c := slices.IndexFunc(migTable.Indexes, func(i model.IndexMigrate) bool {
					return i.Name == in.Name && (i.Unique != in.Unique || i.Func != in.Func)
				}); c != -1 {
					return fmt.Errorf(`goent: struct "%v" have two or more indexes with same name but different uniqueness/function "%v"`, migTable.Name, in.Name)
				}

				migTable.Indexes = append(migTable.Indexes, in)
				continue
			}
			migTable.Indexes[i].Attributes = append(migTable.Indexes[i].Attributes, at)
		}
	}

	tagValue := migField.Tag.Get("goe")
	if !skipUnique && utils.HasTagValue(tagValue, "unique") {
		in := model.IndexMigrate{
			Name:         migTable.Name + "_idx_" + strings.ToLower(migField.Name),
			EscapingName: b.driver.KeywordHandler(migTable.Name + "_idx_" + strings.ToLower(migField.Name)),
			Unique:       true,
			Attributes:   []model.AttributeMigrate{at},
		}
		migTable.Indexes = append(migTable.Indexes, in)
	}

	if utils.HasTagValue(tagValue, "index") {
		in := model.IndexMigrate{
			Name:         migTable.Name + "_idx_" + strings.ToLower(migField.Name),
			EscapingName: b.driver.KeywordHandler(migTable.Name + "_idx_" + strings.ToLower(migField.Name)),
			Unique:       false,
			Attributes:   []model.AttributeMigrate{at},
		}
		migTable.Indexes = append(migTable.Indexes, in)
	}
	return nil
}
