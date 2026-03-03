package goent

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

type fieldDesc struct {
	info         *TableInfo
	colName      string
	targetInfo   *TableInfo
	targetSchema *string
}

type dbMigrator struct {
	db *DB
	*model.Migrator
}

func migrateFrom(ent any, db *DB) *dbMigrator {
	valueOf := reflect.ValueOf(ent).Elem()
	dc := db.driver.GetDatabaseConfig()
	size := len(tableRegistry)
	dm := &dbMigrator{
		Migrator: &model.Migrator{
			Tables:  make(map[string]*model.TableMigrate, size),
			Schemas: dc.Schemas(),
		},
		db: db,
	}

	for _, info := range tableRegistry {
		// Get the schema field
		schemaField := valueOf.Field(info.SchemaId)
		if schemaField.Kind() == reflect.Pointer {
			schemaField = schemaField.Elem()
		}
		// Get the table field from the schema
		elem := schemaField.FieldByName(info.FieldName)
		elem = reflect.Indirect(elem)
		if !elem.IsValid() {
			continue
		}
		tm := &model.TableMigrate{Name: info.TableName}
		// Only set Schema for drivers that support it (not SQLite)
		if info.SchemaName != "" && db.driver.Name() != "SQLite" {
			tm.Schema = &info.SchemaName
		}
		tm, dm.Error = dm.typeField(tm, valueOf, elem, db.driver)
		if dm.Error != nil {
			return dm
		}
		tm.EscapingName = db.driver.KeywordHandler(tm.Name)
		dm.Tables[tm.Name] = tm
	}

	for _, info := range tableRegistry {
		tm := dm.Tables[info.TableName]
		if tm == nil {
			continue
		}
		for _, foreign := range info.Foreigns {
			if foreign.Type == M2M || foreign.Reference == nil {
				continue
			}
			colName := foreign.ForeignKey
			if colName == "" {
				continue
			}
			if dm.isRelationExists(tm, colName) {
				continue
			}
			fd := dm.createFieldDesc(info, colName, foreign)
			if fd == nil {
				continue
			}
			dm.addForeignRelation(tm, fd, foreign)
		}
	}

	return dm
}

func (dm *dbMigrator) isRelationExists(tm *model.TableMigrate, colName string) bool {
	for _, rel := range tm.ManyToSomes {
		if rel.Name == colName {
			return true
		}
	}
	for _, rel := range tm.OneToSomes {
		if rel.Name == colName {
			return true
		}
	}
	return false
}

func (dm *dbMigrator) createFieldDesc(info *TableInfo, colName string, foreign *Foreign) *fieldDesc {
	targetInfo := tableRegistry[foreign.Reference.TableAddr]
	if targetInfo == nil {
		return nil
	}
	if dm.Tables[targetInfo.TableName] == nil {
		return nil
	}
	if _, ok := info.Columns[colName]; !ok {
		return nil
	}
	var targetSchema *string
	if targetInfo.SchemaName != "" {
		targetSchema = &targetInfo.SchemaName
	}
	return &fieldDesc{
		info:         info,
		colName:      colName,
		targetInfo:   targetInfo,
		targetSchema: targetSchema,
	}
}

func (dm *dbMigrator) addForeignRelation(tm *model.TableMigrate, fd *fieldDesc, foreign *Foreign) {
	col := fd.info.Columns[fd.colName]
	attrMig := model.AttributeMigrate{
		FieldName:    col.FieldName,
		Name:         fd.colName,
		EscapingName: dm.db.driver.KeywordHandler(fd.colName),
		DataType:     col.ColumnType,
		Nullable:     col.AllowNull,
		FieldPos:     col.FieldId,
	}

	if foreign.Type == M2O {
		rel := model.ManyToSomeMigrate{
			TargetTable:          fd.targetInfo.TableName,
			TargetColumn:         foreign.Reference.ColumnName,
			EscapingTargetTable:  dm.db.driver.KeywordHandler(fd.targetInfo.TableName),
			EscapingTargetColumn: dm.db.driver.KeywordHandler(foreign.Reference.ColumnName),
			TargetSchema:         fd.targetSchema,
			AttributeMigrate:     attrMig,
		}
		tm.ManyToSomes = append(tm.ManyToSomes, rel)
	} else if foreign.Type == O2O || foreign.Type == O2M {
		rel := model.OneToSomeMigrate{
			IsOneToMany:          foreign.Type == O2M,
			TargetTable:          fd.targetInfo.TableName,
			TargetColumn:         foreign.Reference.ColumnName,
			EscapingTargetTable:  dm.db.driver.KeywordHandler(fd.targetInfo.TableName),
			EscapingTargetColumn: dm.db.driver.KeywordHandler(foreign.Reference.ColumnName),
			TargetSchema:         fd.targetSchema,
			AttributeMigrate:     attrMig,
		}
		tm.OneToSomes = append(tm.OneToSomes, rel)
	}
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
	schemaType := typeOf
	if schemaType.Kind() == reflect.Slice {
		schemaType = schemaType.Elem()
	}
	if schema := schemaRegistry[schemaType.Name()]; schema != nil && *schema != "" {
		rel.TargetSchema = schema
	}
	rel.EscapingTargetTable = b.driver.KeywordHandler(rel.TargetTable)
	rel.EscapingTargetColumn = b.driver.KeywordHandler(rel.TargetColumn)

	rel.Name = utils.ToSnakeCase(b.fieldName)
	rel.EscapingName = b.driver.KeywordHandler(rel.Name)
	rel.Nullable = b.nullable
	rel.Default = getTagValue(b.migrate.field.Tag.Get("goe"), "default:")
	rel.FieldPos = b.fieldId
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
	schemaType := typeOf
	if schemaType.Kind() == reflect.Slice {
		schemaType = schemaType.Elem()
	}
	if schema := schemaRegistry[schemaType.Name()]; schema != nil && *schema != "" {
		rel.TargetSchema = schema
	}
	rel.EscapingTargetTable = b.driver.KeywordHandler(rel.TargetTable)
	rel.EscapingTargetColumn = b.driver.KeywordHandler(rel.TargetColumn)

	rel.Name = utils.ToSnakeCase(b.fieldName)
	rel.EscapingName = b.driver.KeywordHandler(rel.Name)
	rel.Nullable = b.nullable
	rel.FieldPos = b.fieldId
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
		return nil, nil, model.ErrSliceTypeMigration
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
		return nil, nil, model.NewNoPrimaryKeyError(typeOf.Name())
	}

	if typeOf.Name() == "" {
		return nil, nil, model.NewNoPrimaryKeyError(typeOf.String())
	}

	size := len(fields)
	pks := make([]*model.PrimaryKeyMigrate, size)
	fieldsNames := make([]string, size)
	for i := range fields {
		geoTag := fields[i].Tag.Get("goe")
		fieldPos := getFieldPosition(typeOf, fields[i].Name)
		pks[i] = createMigratePk(fields[i].Name, isAutoIncrement(fields[i]),
			getTagType(fields[i]), getTagValue(geoTag, "default:"), driver, fieldPos)
		fieldsNames[i] = fields[i].Name
	}
	return pks, fieldsNames, nil
}

func getFieldPosition(typeOf reflect.Type, fieldName string) int {
	if typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	for i := 0; i < typeOf.NumField(); i++ {
		if typeOf.Field(i).Name == fieldName {
			return i
		}
	}
	return -1
}

func isAutoIncrement(id reflect.StructField) bool {
	geoTag := id.Tag.Get("goe")
	if utils.HasTagValue(geoTag, "not_incr") {
		return false
	}
	return strings.Contains(id.Type.Kind().String(), "int")
}

func migrateAtt(b body) error {
	migField := b.migrate.field
	if slices.Contains(b.migrate.fieldNames, migField.Name) {
		return nil
	}

	dataType := getTagType(migField)
	if strings.Contains(dataType, ".") {
		t := migField.Type
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct {
			if isTableTypeField(t) {
				return nil
			}
		}
	}

	at := createMigrateAtt(
		migField.Name,
		dataType,
		b.nullable,
		getTagValue(migField.Tag.Get("goe"), "default:"),
		b.driver,
		b.fieldId,
	)
	b.migrate.table.Attributes = append(b.migrate.table.Attributes, at)

	return checkIndex(b, at, false)
}

func getTagType(field reflect.StructField) string {
	value := getTagValue(field.Tag.Get("goe"), "type:")
	if value != "" {
		return strings.ReplaceAll(value, " ", "")
	}
	t := field.Type
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		return "[]" + t.Elem().Kind().String()
	}
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	if t.Kind() == reflect.Array {
		return fmt.Sprintf("[%d]%s", t.Len(), t.Elem().Kind().String())
	}
	return t.Kind().String()
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

func createMigratePk(attributeName string, autoIncrement bool, dataType, defaultTag string, driver model.Driver, fieldPos int) *model.PrimaryKeyMigrate {
	return &model.PrimaryKeyMigrate{
		AttributeMigrate: model.AttributeMigrate{
			FieldName:    attributeName,
			Name:         utils.ToSnakeCase(attributeName),
			EscapingName: driver.KeywordHandler(utils.ToSnakeCase(attributeName)),
			DataType:     dataType,
			Default:      defaultTag,
			FieldPos:     fieldPos,
		},
		AutoIncrement: autoIncrement,
	}
}

func createMigrateAtt(attributeName string, dataType string, nullable bool, defaultValue string, driver model.Driver, fieldPos int) model.AttributeMigrate {
	return model.AttributeMigrate{
		FieldName:    attributeName,
		Name:         utils.ToSnakeCase(attributeName),
		EscapingName: driver.KeywordHandler(utils.ToSnakeCase(attributeName)),
		DataType:     dataType,
		Nullable:     nullable,
		Default:      defaultValue,
		FieldPos:     fieldPos,
	}
}

func helperAttributeMigrate(b body) error {
	migField := b.migrate.field
	goeTag := migField.Tag.Get("goe")

	if utils.HasTagValue(goeTag, "m2o") || utils.HasTagValue(goeTag, "o2o") {
		table, _ := foreignKeyNamePattern(b.tables, migField.Name)
		if table == "" {
			table = findTableByFieldName(migField.Name)
		}
		if table != "" {
			fd := createFieldDescForTag(b, table, migField.Name)
			if fd != nil {
				return addRelationForTag(b, fd, migField, goeTag)
			}
		}
		return migrateAtt(b)
	}

	table, prefix := foreignKeyNamePattern(b.tables, migField.Name)
	if table == "" {
		return migrateAtt(b)
	}

	migTable := b.migrate.table
	fkColName := utils.ToSnakeCase(migField.Name) + "_id"
	if isRelationOrAttributeExists(migTable, fkColName, utils.ToSnakeCase(migField.Name)) {
		return nil
	}

	b.stringInfos = stringInfos{prefixName: prefix, tableName: table, fieldName: migField.Name}
	rel := createRelation(b, createManyToSomeMigrate, createOneToSomeMigrate)
	if rel == nil {
		return migrateAtt(b)
	}

	switch v := rel.(type) {
	case *model.ManyToSomeMigrate:
		v.DataType = getTagType(migField)
		migTable.ManyToSomes = append(migTable.ManyToSomes, *v)
	case *model.OneToSomeMigrate:
		v.IsOneToMany = utils.HasTagValue(goeTag, "o2m")
		v.DataType = getTagType(migField)
		migTable.OneToSomes = append(migTable.OneToSomes, *v)
	}
	return nil
}

func findTableByFieldName(fieldName string) string {
	tableName := strings.TrimSuffix(fieldName, "Id")
	tableName = strings.TrimSuffix(tableName, "ID")
	tableNameSnake := utils.ToSnakeCase(tableName)
	for _, info := range tableRegistry {
		if info.TableName == tableName || info.TableName == strings.ToLower(tableName) ||
			info.TableName == tableNameSnake || info.TableName == strings.ToLower(tableNameSnake) ||
			info.TableName == "t_"+tableNameSnake || info.TableName == "t_"+strings.ToLower(tableName) {
			return info.TableName
		}
	}
	return ""
}

func createFieldDescForTag(b body, tableName string, fieldName string) *fieldDesc {
	var targetInfo *TableInfo
	for _, info := range tableRegistry {
		if info.TableName == tableName {
			targetInfo = info
			break
		}
	}
	if targetInfo == nil || len(targetInfo.PrimaryKeys) == 0 {
		return nil
	}
	var targetSchema *string
	if targetInfo.SchemaName != "" {
		targetSchema = &targetInfo.SchemaName
	}
	return &fieldDesc{
		colName:      utils.ToSnakeCase(fieldName),
		targetInfo:   targetInfo,
		targetSchema: targetSchema,
	}
}

func addRelationForTag(b body, fd *fieldDesc, migField reflect.StructField, goeTag string) error {
	migTable := b.migrate.table
	pkName := fd.targetInfo.PrimaryKeys[0].ColumnName
	isO2O := utils.HasTagValue(goeTag, "o2o")

	attrMig := model.AttributeMigrate{
		FieldName:    migField.Name,
		Name:         utils.ToSnakeCase(migField.Name),
		EscapingName: b.driver.KeywordHandler(utils.ToSnakeCase(migField.Name)),
		DataType:     getTagType(migField),
		Nullable:     b.nullable,
		FieldPos:     b.fieldId,
	}

	if isO2O {
		rel := model.OneToSomeMigrate{
			IsOneToMany:          false,
			TargetTable:          fd.targetInfo.TableName,
			TargetColumn:         pkName,
			EscapingTargetTable:  b.driver.KeywordHandler(fd.targetInfo.TableName),
			EscapingTargetColumn: b.driver.KeywordHandler(pkName),
			TargetSchema:         fd.targetSchema,
			AttributeMigrate:     attrMig,
		}
		migTable.OneToSomes = append(migTable.OneToSomes, rel)
	} else {
		rel := model.ManyToSomeMigrate{
			TargetTable:          fd.targetInfo.TableName,
			TargetColumn:         pkName,
			EscapingTargetTable:  b.driver.KeywordHandler(fd.targetInfo.TableName),
			EscapingTargetColumn: b.driver.KeywordHandler(pkName),
			TargetSchema:         fd.targetSchema,
			AttributeMigrate:     attrMig,
		}
		migTable.ManyToSomes = append(migTable.ManyToSomes, rel)
	}
	return nil
}

func isRelationOrAttributeExists(tm *model.TableMigrate, colNames ...string) bool {
	for _, colName := range colNames {
		for _, rel := range tm.ManyToSomes {
			if rel.Name == colName {
				return true
			}
		}
		for _, rel := range tm.OneToSomes {
			if rel.Name == colName {
				return true
			}
		}
		for _, attr := range tm.Attributes {
			if attr.Name == colName {
				return true
			}
		}
	}
	return false
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
					return model.NewDuplicateIndexError(migTable.Name, in.Name)
				}

				migTable.Indexes = append(migTable.Indexes, in)
				continue
			}
			migTable.Indexes[i].Attributes = append(migTable.Indexes[i].Attributes, at)
		}
	}

	tagValue := migField.Tag.Get("goe")
	if !skipUnique && utils.HasTagValue(tagValue, "unique") {
		migTable.Indexes = append(migTable.Indexes, createIndexMigrate(b, at, true))
	}

	if utils.HasTagValue(tagValue, "index") {
		migTable.Indexes = append(migTable.Indexes, createIndexMigrate(b, at, false))
	}
	return nil
}

func createIndexMigrate(b body, at model.AttributeMigrate, unique bool) model.IndexMigrate {
	name := b.migrate.table.Name + "_idx_" + strings.ToLower(b.migrate.field.Name)
	return model.IndexMigrate{
		Name:         name,
		EscapingName: b.driver.KeywordHandler(name),
		Unique:       unique,
		Attributes:   []model.AttributeMigrate{at},
	}
}
