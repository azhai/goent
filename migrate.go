package goent

import (
	"context"
	"reflect"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// AutoMigrate auto-migrates all registered tables
func AutoMigrate(ent any) error {
	return AutoMigrateContext(context.Background(), ent)
}

// AutoMigrateContext auto-migrates all registered tables with context
func AutoMigrateContext(ctx context.Context, ent any) error {
	return NewSchemaOps(getDatabase(ent)).AutoMigrate(ctx, ent)
}

// fieldDesc describes a foreign key field for migration
type fieldDesc struct {
	info         *TableInfo
	colName      string
	targetInfo   *TableInfo
	targetSchema *string
}

// dbMigrator wraps model.Migrator with database access
type dbMigrator struct {
	*model.Migrator
	db *DB
}

// migrateFrom creates a dbMigrator from an entity by reflecting over registered tables
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

	// Phase 1: Build TableMigrate for each registered table
	for _, info := range tableRegistry {
		schemaField := valueOf.Field(info.SchemaId)
		if schemaField.Kind() == reflect.Pointer {
			schemaField = schemaField.Elem()
		}
		elem := schemaField.FieldByName(info.FieldName)
		elem = reflect.Indirect(elem)
		if !elem.IsValid() {
			continue
		}
		tm := &model.TableMigrate{Name: info.TableName}
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

	// Phase 2: Add foreign key relations from TableInfo
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
			if isColumnExistsInMigrate(tm, colName) {
				continue
			}
			fd := createFieldDesc(info, colName, foreign)
			if fd == nil {
				continue
			}
			addRelationToMigrate(tm, fd, foreign, db.driver)
		}
	}

	return dm
}

// typeField processes all fields of a model struct and populates the TableMigrate
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
		migBody := body{
			fieldId:     fieldId,
			driver:      driver,
			tables:      tables,
			fieldTypeOf: elemField.Type(),
			typeOf:      elemType,
			valueOf:     elem,
			migrate: &infosMigrate{
				table:      tm,
				field:      fieldOf,
				fieldNames: fieldNames,
			},
		}

		switch elemField.Kind() {
		case reflect.Interface, reflect.Func:
			continue
		case reflect.Slice:
			migBody.fieldTypeOf = elemField.Type().Elem()
			if err = handlerSlice(migBody, helperAttributeMigrate); err != nil {
				return nil, err
			}
		case reflect.Struct:
			if fieldOf.Anonymous {
				continue
			}
			migBody.nullable = isNullable(fieldOf)
			migBody.migrate.fieldNames = nil
			if err = handlerStruct(migBody, migrateAtt); err != nil {
				return nil, err
			}
		case reflect.Pointer:
			migBody.nullable = true
			migBody.fieldTypeOf = elemField.Type().Elem()
			if err = helperAttributeMigrate(migBody); err != nil {
				return nil, err
			}
		default:
			if err = helperAttributeMigrate(migBody); err != nil {
				return nil, err
			}
		}
	}

	for _, pk := range pks {
		tm.PrimaryKeys = append(tm.PrimaryKeys, *pk)
	}
	return tm, nil
}

// createFieldDesc creates a fieldDesc for a foreign key relationship
func createFieldDesc(info *TableInfo, colName string, foreign *Foreign) *fieldDesc {
	targetInfo := tableRegistry[foreign.Reference.TableAddr]
	if targetInfo == nil {
		return nil
	}
	// Check targetInfo is in the global registry (dm.Tables check was in the caller)
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

// createFieldDescByName creates a fieldDesc by looking up the target table by name
func createFieldDescByName(tableName string, fieldName string) *fieldDesc {
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

// addRelationToMigrate adds a foreign key relation to a TableMigrate based on Foreign info
func addRelationToMigrate(tm *model.TableMigrate, fd *fieldDesc, foreign *Foreign, driver model.Driver) {
	col := fd.info.Columns[fd.colName]
	attrMig := model.AttributeMigrate{
		FieldName:    col.FieldName,
		Name:         fd.colName,
		EscapingName: driver.KeywordHandler(fd.colName),
		DataType:     col.ColumnType,
		Nullable:     col.AllowNull,
		FieldPos:     col.FieldId,
	}

	relInfo := relationInfo{
		targetTable:  fd.targetInfo.TableName,
		targetColumn: foreign.Reference.ColumnName,
		targetSchema: fd.targetSchema,
		driver:       driver,
		attrMig:      attrMig,
		isOneToMany:  foreign.Type == O2M,
	}

	if foreign.Type == M2O {
		tm.ManyToSomes = append(tm.ManyToSomes, relInfo.buildManyToSome())
	} else if foreign.Type == O2O || foreign.Type == O2M {
		tm.OneToSomes = append(tm.OneToSomes, relInfo.buildOneToSome())
	}
}

// addRelationByTag adds a foreign key relation to a TableMigrate based on struct tags
func addRelationByTag(tm *model.TableMigrate, fd *fieldDesc, migField reflect.StructField, goeTag string, driver model.Driver, fieldId int, nullable bool) {
	pkName := fd.targetInfo.PrimaryKeys[0].ColumnName
	isO2O := utils.HasTagValue(goeTag, "o2o")

	attrMig := model.AttributeMigrate{
		FieldName:    migField.Name,
		Name:         utils.ToSnakeCase(migField.Name),
		EscapingName: driver.KeywordHandler(utils.ToSnakeCase(migField.Name)),
		DataType:     resolveTypeName(migField.Type),
		Nullable:     nullable,
		FieldPos:     fieldId,
	}

	relInfo := relationInfo{
		targetTable:  fd.targetInfo.TableName,
		targetColumn: pkName,
		targetSchema: fd.targetSchema,
		driver:       driver,
		attrMig:      attrMig,
		isOneToMany:  !isO2O && utils.HasTagValue(goeTag, "o2m"),
	}

	if isO2O {
		tm.OneToSomes = append(tm.OneToSomes, relInfo.buildOneToSome())
	} else {
		tm.ManyToSomes = append(tm.ManyToSomes, relInfo.buildManyToSome())
	}
}

// relationInfo is a helper to build migration relation structs with less duplication
type relationInfo struct {
	targetTable  string
	targetColumn string
	targetSchema *string
	driver       model.Driver
	attrMig      model.AttributeMigrate
	isOneToMany  bool
}

func (r relationInfo) buildManyToSome() model.ManyToSomeMigrate {
	return model.ManyToSomeMigrate{
		TargetTable:          r.targetTable,
		TargetColumn:         r.targetColumn,
		EscapingTargetTable:  r.driver.KeywordHandler(r.targetTable),
		EscapingTargetColumn: r.driver.KeywordHandler(r.targetColumn),
		TargetSchema:         r.targetSchema,
		AttributeMigrate:     r.attrMig,
	}
}

func (r relationInfo) buildOneToSome() model.OneToSomeMigrate {
	return model.OneToSomeMigrate{
		IsOneToMany:          r.isOneToMany,
		TargetTable:          r.targetTable,
		TargetColumn:         r.targetColumn,
		EscapingTargetTable:  r.driver.KeywordHandler(r.targetTable),
		EscapingTargetColumn: r.driver.KeywordHandler(r.targetColumn),
		TargetSchema:         r.targetSchema,
		AttributeMigrate:     r.attrMig,
	}
}
