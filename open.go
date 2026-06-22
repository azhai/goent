package goent

import (
	"context"
	"reflect"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// Open opens a database connection
//
// # Example
//
//	dsn := "postgres://user:pass@127.0.0.1:5432/test?sslmode=disable&search_path=public"
//	db, err := goent.Open[Database](pgsql.Open(dsn))
func Open[T any](drv model.Driver) (*T, error) {
	dc := drv.GetDatabaseConfig()
	dc.Init(drv.Name(), drv.ErrorTranslator())
	err := drv.Init()
	if err != nil {
		return nil, dc.ErrorHandler(context.TODO(), err)
	}

	ent := new(T)
	valueOf := reflect.ValueOf(ent).Elem()
	if valueOf.Kind() != reflect.Struct {
		return nil, model.ErrInvalidDatabase
	}
	dbId := valueOf.NumField() - 1
	if valueOf.Field(dbId).Type().Elem().Name() != "DB" {
		return nil, model.ErrInvalidDBField
	}

	db, schemas := new(DB), make([]string, 0, dbId)
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
			tableRegistry[info.TableAddr] = info
		}
	}

	for _, info := range tableRegistry {
		for fkName, foreign := range info.Foreigns {
			if foreign.Reference != nil {
				continue
			}
			refTableName, ok := info.getRefTableName(foreign, fkName)
			if !ok {
				continue
			}
			for otherAddr, otherInfo := range tableRegistry {
				if otherAddr == info.TableAddr {
					continue
				}
				if foreign, ok = otherInfo.setForeignReference(foreign, refTableName); ok {
					break
				}
			}
			// Fallback: try matching by RefType (e.g. AssigneeID -> Assignee field -> Contributor type)
			if foreign.Reference == nil && foreign.RefType != "" {
				for otherAddr, otherInfo := range tableRegistry {
					if otherAddr == info.TableAddr {
						continue
					}
					if foreign, ok = otherInfo.setForeignReference(foreign, foreign.RefType); ok {
						break
					}
				}
			}
		}
	}

	return schemas, nil
}
