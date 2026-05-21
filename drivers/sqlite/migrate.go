package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azhai/goent/model"
)

type body struct {
	driver  *Driver
	table   *model.TableMigrate
	dataMap map[string]*dataType
	sql     *strings.Builder
	conn    *sql.DB
	tables  map[string]*model.TableMigrate
	dbTable
}

func (dr *Driver) MigrateContext(ctx context.Context, migrator *model.Migrator) error {
	dataMap := map[string]*dataType{
		"string":    {"text", "''"},
		"int16":     {"integer", "0"},
		"int32":     {"integer", "0"},
		"int64":     {"integer", "0"},
		"float32":   {"real", "0"},
		"float64":   {"real", "0"},
		"[]uint8":   {"blob", "X''"},
		"time.Time": {"datetime", "'0000-01-01'"},
		"bool":      {"boolean", "false"},
		"uuid.UUID": {"uuid", "'00000000-0000-0000-0000-000000000000'"},
	}

	sql := new(strings.Builder)
	var err error

	sqlColumns := new(strings.Builder)
	for _, t := range migrator.Tables {
		err = checkTableChanges(body{
			table:   t,
			dataMap: dataMap,
			driver:  dr,
			sql:     sql,
			conn:    dr.sql,
			tables:  migrator.Tables,
		})
		if err != nil {
			return err
		}

		err = checkIndex(t.Indexes, t, sqlColumns, dr.sql)
		if err != nil {
			return err
		}
	}

	for _, t := range migrator.Tables {
		if !t.Migrated {
			createTable(t, dataMap, sql, migrator.Tables, false)
		}
	}

	sql.WriteString(sqlColumns.String())

	if sql.Len() != 0 {
		return dr.rawExecContext(ctx, sql.String())
	}
	return nil
}

func (dr *Driver) rawExecContext(ctx context.Context, rawSql string, args ...any) error {
	if dr.config.MigratePath == "" {
		query := model.CreateQuery(rawSql, args)
		return query.WrapExec(ctx, dr.NewConnection(), dr.GetDatabaseConfig())
	}
	root, err := os.OpenRoot(dr.config.MigratePath)
	if err != nil {
		return err
	}
	defer root.Close()

	filename := dr.Name() + "_" + strconv.FormatInt(time.Now().Unix(), 10) + ".sql"
	file, err := root.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(rawSql)
	return err
}

func (dr *Driver) DropTable(schema, table string) error {
	if len(schema) > 2 {
		table = schema + "." + table
	}
	return dr.rawExecContext(context.TODO(), fmt.Sprintf("DROP TABLE IF EXISTS %v;", table))
}

func (dr *Driver) RenameTable(schema, table, newTable string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		newTable = schema + "." + newTable
	}
	return dr.rawExecContext(context.TODO(), fmt.Sprintf("ALTER TABLE %v RENAME TO %v;", table, newTable))
}

func (dr *Driver) RenameColumn(schema, table, oldColumn, newColumn string) error {
	if len(schema) > 2 {
		table = schema + "." + table
	}
	return dr.rawExecContext(context.TODO(), renameColumn(table, oldColumn, newColumn))
}

func (dr *Driver) DropColumn(schema, table, column string) error {
	if len(schema) > 2 {
		table = schema + "." + table
	}
	return dr.rawExecContext(context.TODO(), dropColumn(table, column))
}

func checkTableChanges(b body) error {
	dbTbl, err := getTableColumns(b.conn, b.table.Name)
	if err != nil {
		return err
	}
	if len(dbTbl.columns) == 0 {
		return nil
	}
	b.dbTable = dbTbl
	b.table.Migrated = true
	checkFields(b)
	return nil
}

func createTable(tbl *model.TableMigrate, dataMap map[string]*dataType, sql *strings.Builder, tables map[string]*model.TableMigrate, skipDependency bool) {
	t := table{}
	t.name = fmt.Sprintf("CREATE TABLE %v (", tbl.EscapingTableName())
	processedAttrs := make(map[string]bool)

	columns := tbl.GetOrderedColumns()
	for _, col := range columns {
		if processedAttrs[col.Name()] {
			continue
		}
		processedAttrs[col.Name()] = true

		if col.IsPK {
			att := col.PK
			att.DataType = checkDataType(att.DataType, dataMap).typeName
			if att.AutoIncrement {
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, att.DataType))
			} else {
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL %v,", att.EscapingName, att.DataType, setDefault(att.Default)))
			}
		} else if col.Attr != nil {
			att := col.Attr
			att.DataType = checkDataType(att.DataType, dataMap).typeName
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v %v,", att.EscapingName, att.DataType, func() string {
				if att.Nullable {
					return "NULL"
				} else {
					return "NOT NULL"
				}
			}(), setDefault(att.Default)))
		} else if col.OneTo != nil {
			att := col.OneTo
			tb := tables[att.TargetTable]
			if tb.Migrated {
				t.createAttrs = append(t.createAttrs, foreignOneToSome(*att, dataMap))
			} else {
				if tb != tbl && !skipDependency {
					createTable(tb, dataMap, sql, tables, false)
				}
				t.createAttrs = append(t.createAttrs, foreignOneToSome(*att, dataMap))
			}
		} else if col.ManyTo != nil {
			att := col.ManyTo
			tb := tables[att.TargetTable]
			if tb.Migrated {
				t.createAttrs = append(t.createAttrs, foreignManyToSome(*att, dataMap))
			} else {
				if tb != tbl && !skipDependency {
					createTable(tb, dataMap, sql, tables, false)
				}
				t.createAttrs = append(t.createAttrs, foreignManyToSome(*att, dataMap))
			}
		}
	}

	tbl.Migrated = true
	t.createPk = fmt.Sprintf("primary key (%v", tbl.PrimaryKeys[0].EscapingName)
	for _, pk := range tbl.PrimaryKeys[1:] {
		t.createPk += fmt.Sprintf(",%v", pk.EscapingName)
	}
	t.createPk += ")"
	createTableSql(t.name, t.createPk, t.createAttrs, sql)
}

type table struct {
	name        string
	createPk    string
	createAttrs []string
}

func checkIndex(indexes []model.IndexMigrate, table *model.TableMigrate, sql *strings.Builder, conn *sql.DB) error {
	dis, err := getTableIndexes(conn, table.Name)
	if err != nil {
		return err
	}

	for i := range indexes {
		if dbIndex, exist := dis[indexes[i].Name]; exist {
			if indexes[i].Unique != dbIndex.unique {
				sql.WriteString(dropIndex(table, indexes[i].EscapingName))
				sql.WriteString(createIndex(indexes[i], table))
			} else if indexes[i].Func != "" && !strings.Contains(regexp.MustCompile(`(?:\()[a-z]+`).FindString(dbIndex.sql), indexes[i].Func) {
				sql.WriteString(dropIndex(table, indexes[i].EscapingName))
				sql.WriteString(createIndex(indexes[i], table))
			}
			dbIndex.migrated = true
			continue
		}
		sql.WriteString(createIndex(indexes[i], table))
	}

	for _, dbIndex := range dis {
		isSameName := func(m model.OneToSomeMigrate) bool {
			return m.Name == dbIndex.attname
		}
		if !dbIndex.migrated {
			if !slices.ContainsFunc(table.OneToSomes, isSameName) {
				sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", keywordHandler(dbIndex.indexName)) + "\n")
			}
		}
	}
	return nil
}

func checkFields(b body) {
	var alter bool
	for _, att := range b.table.PrimaryKeys {
		if column := b.dbTable.columns[att.Name]; column != nil {
			column.migrated = true
			if primaryKeyIsForeignKey(b.table, att.Name) {
				continue
			}

			dataType := checkDataType(att.DataType, b.dataMap).typeName
			if column.dataType != dataType {
				alter = true
				break
			}
			if !att.AutoIncrement && column.defaultValue != nil {
				if att.Default == "" {
					alter = true
					break
				}
				if *column.defaultValue != att.Default {
					alter = true
					break
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				alter = true
				break
			}
		}
	}

	for _, att := range b.table.OneToSomes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			if unique := checkFkUnique(b.conn, b.table.Name, att.Name); !unique {
				if foreignKeyIsPrimarykey(b.table, att.Name) {
					continue
				}
				alter = true
				break
			}
			if column.nullable != att.Nullable {
				alter = true
				break
			}
			continue
		}
		alter = true
		break
	}

	for _, att := range b.table.ManyToSomes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			if unique := checkFkUnique(b.conn, b.table.Name, att.Name); unique {
				alter = true
				break
			}
			if column.nullable != att.Nullable {
				alter = true
				break
			}
			continue
		}
		alter = true
		break
	}

	var newColumns []string
	for _, att := range b.table.Attributes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			dataType := checkDataType(att.DataType, b.dataMap).typeName
			if column.dataType != dataType {
				alter = true
			}
			if column.nullable != att.Nullable {
				alter = true
			}
			if column.defaultValue != nil {
				if att.Default == "" {
					alter = true
				}
				if *column.defaultValue != setDefault(att.Default)[8:] {
					alter = true
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				alter = true
			}
			continue
		}
		newColumns = append(newColumns, addColumn(b.table, att.EscapingName, checkDataType(att.DataType, b.dataMap), att.Nullable))
		alter = true
	}

	for _, c := range b.dbTable.columns {
		if !c.migrated {
			alter = true
			break
		}
	}

	for _, c := range newColumns {
		b.sql.WriteString(c)
	}

	if alter {
		alterSqlite(b)
	}
}

func alterSqlite(b body) {
	newTable := *b.table
	newTable.Name = "new_" + newTable.Name
	newTable.EscapingName = keywordHandler(newTable.Name)
	sqlBuilder := &strings.Builder{}

	insertColumns, selectColumns := tableAttributes(b.table)
	sqlBuilder.WriteString("BEGIN TRANSACTION; PRAGMA foreign_keys=OFF; \n")
	createTable(&newTable, b.dataMap, sqlBuilder, b.tables, true)
	sqlBuilder.WriteString(
		fmt.Sprintf("INSERT INTO %v (%v) SELECT %v FROM %v;\n",
			newTable.EscapingTableName(),
			insertColumns,
			selectColumns,
			b.table.EscapingTableName()))
	sqlBuilder.WriteString("DROP TABLE " + b.table.EscapingTableName() + ";\n")
	sqlBuilder.WriteString(fmt.Sprintf("ALTER TABLE %v RENAME TO %v;\n", newTable.EscapingTableName(), b.table.EscapingName))
	sqlBuilder.WriteString("PRAGMA foreign_keys=ON; COMMIT;")

	b.sql.WriteString(sqlBuilder.String())
}

func tableAttributes(t *model.TableMigrate) (string, string) {
	sql := strings.Builder{}
	sql.WriteString(t.PrimaryKeys[0].EscapingName)
	for _, p := range t.PrimaryKeys[1:] {
		sql.WriteString("," + p.EscapingName)
	}
	for _, a := range t.Attributes {
		sql.WriteString("," + a.EscapingName)
	}
	for _, a := range t.OneToSomes {
		sql.WriteString("," + a.EscapingName)
	}
	for _, a := range t.ManyToSomes {
		sql.WriteString("," + a.EscapingName)
	}
	newColumns := sql.String()

	return newColumns, newColumns
}
