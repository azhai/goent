package facade

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// QuickOpen opens a database connection with default configuration
func QuickOpen[T any](dbType, dbDSN, logFile string) (*T, error) {
	drv, err := OpenDSN(dbType, dbDSN)
	if err != nil {
		return nil, err
	}
	if logFile != "" {
		err = drv.AddLogger(utils.CreateLogger(logFile))
		if err != nil {
			return nil, err
		}
	}
	return Open[T](drv)
}

// OpenDSN opens a database connection with the given type and DSN
func OpenDSN(dbType, dbDSN string) (drv model.Driver, err error) {
	dbType = strings.ToLower(dbType)
	if dbType == "pgsql" || dbType == "postgres" {
		drv = pgsql.OpenDSN(dbDSN)
	} else if dbType == "" && strings.HasPrefix(dbDSN, "postgres://") {
		drv = pgsql.OpenDSN(dbDSN)
	} else {
		err = utils.MakeDirForFile(dbDSN)
		drv = sqlite.OpenDSN(dbDSN)
	}
	return
}

// Open opens a database connection
//
// # Example
//
//	goent.Open[Database](pgsql.Open("user=postgres password=postgres host=localhost port=5432 database=postgres", pgsql.Config{}))
func Open[T any](driver model.Driver) (*T, error) {
	dc := driver.GetDatabaseConfig()
	dc.Init(driver.Name(), driver.ErrorTranslator())
	err := driver.Init()
	if err != nil {
		return nil, dc.ErrorHandler(context.TODO(), err)
	}

	db := new(T)
	valueOf := reflect.ValueOf(db).Elem()
	if valueOf.Kind() != reflect.Struct {
		return nil, errors.New("goent: invalid database, the target needs to be a struct")
	}
	dbId := valueOf.NumField() - 1
	if valueOf.Field(dbId).Type().Elem().Name() != "DB" {
		return nil, errors.New("goent: invalid database, last struct field needs to be goent.DB")
	}

	var dbTarget *goent.DB
	dbTarget, err = newDbTarget(driver, valueOf, dbId)
	if err != nil {
		return nil, err
	}
	if ic := dc.InitCallback(); ic != nil {
		if err = ic(); err != nil {
			return nil, err
		}
	}

	dbTarget.SetDriver(driver)
	return db, nil
}

func newDbTarget(driver model.Driver, valueOf reflect.Value, dbId int) (*goent.DB, error) {
	var schemas []string
	dbTarget, tableId := new(goent.DB), 0
	valueOf.Field(dbId).Set(reflect.ValueOf(dbTarget))

	// set value for fields
	for i := range dbId {
		fieldOf := valueOf.Field(i)
		if !fieldOf.IsNil() {
			continue
		}
		typeOf := fieldOf.Type().Elem()
		tableOf := goent.NewTableReflect(typeOf)
		fieldOf.Set(tableOf)

		if !utils.IsFieldHasSchema(valueOf, i) {
			continue
		}
		for j := range fieldOf.Elem().NumField() {
			elem := fieldOf.Elem().Field(j)
			elemTypeOf := elem.Type().Elem()
			elem.Set(goent.NewTableReflect(elemTypeOf))
		}
	}

	// init fields
	var mValue reflect.Value
	for i := range dbId {
		elem := valueOf.Field(i).Elem()
		if !utils.IsFieldHasSchema(valueOf, i) {
			tableId++
			mValue = elem.FieldByName("Model").Elem()
			err := goent.InitField(nil, valueOf, mValue, dbTarget, tableId, driver)
			if err != nil {
				return nil, err
			}
			continue
		}
		schema := driver.KeywordHandler(utils.ColumnNamePattern(elem.Type().Name()))
		schemas = append(schemas, schema)
		for j := range elem.NumField() {
			tableId++
			mValue = elem.Field(j).Elem().FieldByName("Model").Elem()
			err := goent.InitField(&schema, valueOf, mValue, dbTarget, tableId, driver)
			if err != nil {
				return nil, err
			}
		}
	}

	driver.GetDatabaseConfig().SetSchemas(schemas)
	return dbTarget, nil
}
