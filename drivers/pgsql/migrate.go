package pgsql

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azhai/goent/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (dr *Driver) MigrateContext(ctx context.Context, migrator *model.Migrator) error {
	dataMap := map[string]dataType{
		"string":    {"text", "''"},
		"int16":     {"smallint", "0"},
		"int32":     {"integer", "0"},
		"int64":     {"bigint", "0"},
		"float32":   {"real", "0"},
		"float64":   {"double precision", "0"},
		"[]uint8":   {"bytea", "''"},
		"time.Time": {"timestamp", "to_timestamp(0)"},
		"bool":      {"boolean", "false"},
		"uuid.UUID": {"uuid", "'00000000-0000-0000-0000-000000000000'"},
	}

	sql := new(strings.Builder)
	sqlColumns := new(strings.Builder)
	sqlForeignKeys := new(strings.Builder)
	schemas := strings.Builder{}
	dbSchemas, err := getSchemas(dr.sql)
	if err != nil {
		return err
	}
	for _, s := range migrator.Schemas {
		if !slices.Contains(dbSchemas, s) {
			schemas.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v;\n", s))
		}
	}

	for _, t := range migrator.Tables {
		err = checkTableChanges(t, dataMap, sql, dr.sql)
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
			createTable(t, dataMap, sql, migrator.Tables, sqlForeignKeys)
		}
	}

	sql.WriteString(sqlColumns.String())
	sql.WriteString(sqlForeignKeys.String())

	if sql.Len() != 0 {
		schemas.WriteString(sql.String())
		return dr.rawExecContext(ctx, schemas.String())
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

func checkTableChanges(table *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder, conn *pgxpool.Pool) error {
	schema := "public"
	if table.Schema != nil && *table.Schema != "" {
		schema = *table.Schema
	}
	dbTbl, err := getTableColumns(conn, schema, table.Name)
	if err != nil {
		return err
	}
	if len(dbTbl.columns) == 0 {
		return nil
	}
	table.Migrated = true
	checkFields(conn, dbTbl, table, dataMap, sql)

	return nil
}

func primaryKeyIsForeignKey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.ManyToSomes, func(m model.ManyToSomeMigrate) bool {
		return m.Name == attName
	}) || slices.ContainsFunc(table.OneToSomes, func(m model.OneToSomeMigrate) bool {
		return m.Name == attName
	})
}

func foreignKeyIsPrimarykey(table *model.TableMigrate, attName string) bool {
	isSameName := func(m model.PrimaryKeyMigrate) bool {
		return m.Name == attName
	}
	return slices.ContainsFunc(table.PrimaryKeys, isSameName)
}

func createTable(tbl *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder, tables map[string]*model.TableMigrate, sqlForeignKeys *strings.Builder) {
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
			isFK := primaryKeyIsForeignKey(tbl, att.Name)
			if att.AutoIncrement {
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, checkTypeAutoIncrement(att.DataType)))
			} else {
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL %v,", att.EscapingName, att.DataType, setDefault(att.Default)))
			}
			if isFK {
				if targetTable, targetCol := findForeignKey(tbl, att.Name); targetTable != "" {
					sqlForeignKeys.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT fk_%v_%v FOREIGN KEY (%v) REFERENCES %v(%v);\n",
						tbl.EscapingTableName(), tbl.Name, att.Name, att.EscapingName, targetTable, targetCol))
				}
			}
		} else if col.Attr != nil {
			att := col.Attr
			att.DataType = checkDataType(att.DataType, dataMap).typeName
			if targetTable, targetCol := findForeignKey(tbl, att.Name); targetTable != "" {
				feature := "NULL"
				if !att.Nullable {
					feature = "NOT NULL"
				}
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v,", att.EscapingName, att.DataType, feature))
				sqlForeignKeys.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT fk_%v_%v FOREIGN KEY (%v) REFERENCES %v(%v);\n",
					tbl.EscapingTableName(), tbl.Name, att.Name, att.EscapingName, targetTable, targetCol))
			} else {
				t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v %v,", att.EscapingName, att.DataType, func() string {
					if att.Nullable {
						return "NULL"
					} else {
						return "NOT NULL"
					}
				}(), setDefault(att.Default)))
			}
		} else if col.OneTo != nil {
			att := col.OneTo
			tb := tables[att.TargetTable]
			if !tb.Migrated && tb != tbl {
				createTable(tb, dataMap, sql, tables, sqlForeignKeys)
			}
			att.DataType = checkDataType(att.DataType, dataMap).typeName
			feature := "NULL"
			if !att.Nullable {
				feature = "NOT NULL"
			}
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v,", att.EscapingName, att.DataType, feature))
			if !att.IsOneToMany {
				sqlForeignKeys.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT uq_%v_%v UNIQUE (%v);\n",
					tbl.EscapingTableName(), tbl.Name, att.Name, att.EscapingName))
			}
			sqlForeignKeys.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT fk_%v_%v FOREIGN KEY (%v) REFERENCES %v(%v);\n",
				tbl.EscapingTableName(), tbl.Name, att.Name, att.EscapingName, att.EscapingTargetTableName(), att.EscapingTargetColumn))
		} else if col.ManyTo != nil {
			att := col.ManyTo
			tb := tables[att.TargetTable]
			if !tb.Migrated && tb != tbl {
				createTable(tb, dataMap, sql, tables, sqlForeignKeys)
			}
			att.DataType = checkDataType(att.DataType, dataMap).typeName
			feature := "NULL"
			if !att.Nullable {
				feature = "NOT NULL"
			}
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v,", att.EscapingName, att.DataType, feature))
			sqlForeignKeys.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT fk_%v_%v FOREIGN KEY (%v) REFERENCES %v(%v);\n",
				tbl.EscapingTableName(), tbl.Name, att.Name, att.EscapingName, att.EscapingTargetTableName(), att.EscapingTargetColumn))
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

func findForeignKey(tbl *model.TableMigrate, attrName string) (targetTable, targetColumn string) {
	for _, fk := range tbl.ManyToSomes {
		if fk.Name == attrName {
			return fk.EscapingTargetTableName(), fk.EscapingTargetColumn
		}
	}
	for _, fk := range tbl.OneToSomes {
		if fk.Name == attrName {
			return fk.EscapingTargetTableName(), fk.EscapingTargetColumn
		}
	}
	return "", ""
}

type table struct {
	name        string
	createPk    string
	createAttrs []string
}

func checkIndex(indexes []model.IndexMigrate, table *model.TableMigrate, sql *strings.Builder, conn *pgxpool.Pool) error {
	schema := "public"
	if table.Schema != nil && *table.Schema != "" {
		schema = *table.Schema
	}
	dis, err := getTableIndexes(conn, schema, table.Name)
	if err != nil {
		return err
	}

	for i := range indexes {
		if dbIndex, exist := dis[indexes[i].Name]; exist {
			if indexes[i].Unique != dbIndex.unique {
				sql.WriteString(dropIndex(table, indexes[i].EscapingName))
				sql.WriteString(createIndex(indexes[i], table))
			} else if indexes[i].Func != "" && indexes[i].Func != dbIndex.attname {
				sql.WriteString(dropIndex(table, indexes[i].EscapingName))
				sql.WriteString(createIndex(indexes[i], table))
			}
			dbIndex.migrated = true
			continue
		}
		sql.WriteString(createIndex(indexes[i], table))
	}

	for _, dbIndex := range dis {
		if !dbIndex.migrated {
			isSameName := func(m model.OneToSomeMigrate) bool {
				return m.Name == dbIndex.attname
			}
			if !slices.ContainsFunc(table.OneToSomes, isSameName) {
				sql.WriteString(dropIndex(table, keywordHandler(dbIndex.indexName)))
			}
		}
	}
	return nil
}

func checkFields(conn *pgxpool.Pool, dbTable dbTable, table *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder) {
	schema := "public"
	if table.Schema != nil && *table.Schema != "" {
		schema = *table.Schema
	}
	for _, att := range table.PrimaryKeys {
		if column := dbTable.columns[att.Name]; column != nil {
			if primaryKeyIsForeignKey(table, att.Name) {
				continue
			}

			dataType := checkDataType(att.DataType, dataMap).typeName
			if att.AutoIncrement {
				dataType = checkTypeAutoIncrement(dataType)
			}
			if column.dataType != dataType {
				if att.AutoIncrement {
					sql.WriteString(alterColumn(table, att.EscapingName, fmt.Sprintf("%v USING %v::%v", checkTypeAutoIncrement(dataType), att.EscapingName, checkTypeAutoIncrement(dataType)), dataMap))
					sql.WriteString(fmt.Sprintf("CREATE SEQUENCE %v_%v_seq OWNED BY %v.%v;\n", table.Name, att.Name, table.EscapingTableName(), att.EscapingName))
					sql.WriteString(alterColumnDefault(table, att.EscapingName, fmt.Sprintf("nextval('%v_%v_seq'::regclass)", table.Name, att.Name)))
				} else {
					sql.WriteString(alterColumn(table, att.EscapingName, dataType, dataMap))
				}
			}
			if !att.AutoIncrement && column.defaultValue != nil {
				if att.Default == "" {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;",
						table.EscapingTableName(),
						att.EscapingName,
					))
					continue
				}
				if !strings.HasPrefix(*column.defaultValue, att.Default) {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
						table.EscapingTableName(),
						att.EscapingName,
						att.Default,
					))
					continue
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
					table.EscapingTableName(),
					att.EscapingName,
					att.Default,
				))
			}
		}
	}

	for _, att := range table.Attributes {
		if column, exist := dbTable.columns[att.Name]; exist {
			dataType := checkDataType(att.DataType, dataMap).typeName
			if column.dataType != dataType {
				sql.WriteString(alterColumn(table, att.EscapingName, dataType, dataMap))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
			}
			if column.defaultValue != nil {
				if att.Default == "" {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;",
						table.EscapingTableName(),
						att.EscapingName,
					))
					continue
				}
				if *column.defaultValue != att.Default {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
						table.EscapingTableName(),
						att.EscapingName,
						att.Default,
					))
					continue
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
					table.EscapingTableName(),
					att.EscapingName,
					att.Default,
				))
			}
			continue
		}
		dt := checkDataType(att.DataType, dataMap)
		if att.Default != "" {
			dt.zeroValue = att.Default
			sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, false))
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, true))
	}

	for _, att := range table.OneToSomes {
		if column, exist := dbTable.columns[att.Name]; exist {
			if _, unique := checkFkUnique(conn, schema, table.Name, att.Name); !unique {
				if foreignKeyIsPrimarykey(table, att.Name) {
					continue
				}
				if !att.IsOneToMany {
					c := fmt.Sprintf("%v_%v_key", table.Name, column.columnName)
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v UNIQUE (%v);\n",
						table.EscapingTableName(),
						keywordHandler(c),
						att.EscapingName))
				}
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
			}
			continue
		}
		sql.WriteString(addColumnUnique(table, att.EscapingName, checkDataType(att.DataType, dataMap), att.Nullable))
		sql.WriteString(addFkOneToSome(table, att))
	}

	for _, att := range table.ManyToSomes {
		if column, exist := dbTable.columns[att.Name]; exist {
			if c, unique := checkFkUnique(conn, schema, table.Name, att.Name); unique {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v;\n", table.EscapingTableName(), keywordHandler(c)))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
			}
			if column.defaultValue != nil {
				if att.Default == "" {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;",
						table.EscapingTableName(),
						att.EscapingName,
					))
					continue
				}
				if *column.defaultValue != att.Default {
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
						table.EscapingTableName(),
						att.EscapingName,
						att.Default,
					))
					continue
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
					table.EscapingTableName(),
					att.EscapingName,
					att.Default,
				))
			}
			continue
		}
		dt := checkDataType(att.DataType, dataMap)
		if att.Default != "" {
			dt.zeroValue = att.Default
			sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, false))
			sql.WriteString(addFkManyToSome(table, att))
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, true))
		sql.WriteString(addFkManyToSome(table, att))
	}
}
