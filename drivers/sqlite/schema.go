package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
)

type SQLiteSchemaDriver struct {
	query func(ctx context.Context, sql string, args ...any) (model.Rows, error)
	exec  func(ctx context.Context, sql string, args ...any) error
}

func NewSQLiteSchemaDriver(queryFn func(ctx context.Context, sql string, args ...any) (model.Rows, error), execFn func(ctx context.Context, sql string, args ...any) error) *SQLiteSchemaDriver {
	return &SQLiteSchemaDriver{query: queryFn, exec: execFn}
}

func (p *SQLiteSchemaDriver) ListTables(ctx context.Context) ([]string, error) {
	rows, err := p.query(ctx, `
		SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		names = append(names, name)
	}
	return names, nil
}

func (p *SQLiteSchemaDriver) ListTablesInSchema(ctx context.Context, _ string) ([]string, error) {
	return p.ListTables(ctx)
}

func (p *SQLiteSchemaDriver) GetColumns(ctx context.Context, table string) ([]model.ColumnDef, error) {
	rows, err := p.query(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []model.ColumnDef
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		col := model.ColumnDef{
			Name:     name,
			DataType: typ,
			Nullable: notNull == 0,
		}
		if dfltValue.Valid {
			col.Default = dfltValue.String
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func (p *SQLiteSchemaDriver) GetIndexes(ctx context.Context, table string) ([]model.IndexDef, error) {
	idxRows, err := p.query(ctx, fmt.Sprintf("PRAGMA index_list(%s)", table))
	if err != nil {
		return nil, err
	}
	type idxMeta struct {
		seq     int
		name    string
		unique  bool
		origin  string
		partial int
	}
	var metas []idxMeta
	for idxRows.Next() {
		var m idxMeta
		if err := idxRows.Scan(&m.seq, &m.name, &m.unique, &m.origin, &m.partial); err != nil {
			idxRows.Close()
			return nil, err
		}
		if m.origin == "c" {
			continue
		}
		metas = append(metas, m)
	}
	idxRows.Close()

	var result []model.IndexDef
	for _, m := range metas {
		colRows, err := p.query(ctx, fmt.Sprintf("PRAGMA index_info(%s)", m.name))
		if err != nil {
			continue
		}
		var columns []string
		for colRows.Next() {
			var seqno, cid int
			var colName sql.NullString
			if err := colRows.Scan(&seqno, &cid, &colName); err != nil {
				colRows.Close()
				continue
			}
			if colName.Valid {
				columns = append(columns, colName.String)
			}
		}
		colRows.Close()
		if len(columns) > 0 {
			result = append(result, model.IndexDef{
				Name:    m.name,
				Columns: columns,
				Unique:  m.unique,
			})
		}
	}
	return result, nil
}

func (p *SQLiteSchemaDriver) GetPrimaryKey(ctx context.Context, table string) ([]string, error) {
	cols, err := p.GetColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	rows, err := p.query(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dfltValue interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			continue
		}
		if pk > 0 {
			pks = append(pks, name)
		}
	}
	_ = cols
	return pks, nil
}

func (p *SQLiteSchemaDriver) GetForeignKeys(ctx context.Context, table string) ([]model.ForeignKeyDef, error) {
	rows, err := p.query(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fkMap := make(map[int]*model.ForeignKeyDef)
	for rows.Next() {
		var id, seq int
		var refTable, from, to string
		var onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		if _, ok := fkMap[id]; !ok {
			fkMap[id] = &model.ForeignKeyDef{Name: fmt.Sprintf("fk_%d", id), RefTable: refTable}
		}
		fkMap[id].Columns = append(fkMap[id].Columns, from)
		fkMap[id].RefColumns = append(fkMap[id].RefColumns, to)
	}
	var result []model.ForeignKeyDef
	for _, fk := range fkMap {
		result = append(result, *fk)
	}
	return result, nil
}

func (p *SQLiteSchemaDriver) DiscoverFKs(ctx context.Context, table string) ([]model.FKRef, error) {
	rows, err := p.query(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", table))
	if err != nil {
		return nil, fmt.Errorf("querying SQLite foreign keys: %w", err)
	}
	defer rows.Close()
	type rawFK struct {
		id, seq                   int
		table                     string
		from, to                  string
		onUpdate, onDelete, match string
	}
	var rawFKs []rawFK
	for rows.Next() {
		var fk rawFK
		if err := rows.Scan(&fk.id, &fk.seq, &fk.table, &fk.from, &fk.to, &fk.onUpdate, &fk.onDelete, &fk.match); err != nil {
			return nil, fmt.Errorf("scanning FK: %w", err)
		}
		rawFKs = append(rawFKs, fk)
	}
	var fks []model.FKRef
	for _, fk := range rawFKs {
		if fk.seq != 0 || fk.table != table || fk.to != "id" {
			continue
		}
		fks = append(fks, model.FKRef{
			FromTable:  fk.table,
			FromColumn: fk.from,
			Nullable:   p.IsColumnNullable(ctx, fk.table, fk.from),
		})
	}
	return fks, nil
}

func (p *SQLiteSchemaDriver) IsColumnNullable(ctx context.Context, table, column string) bool {
	rows, err := p.query(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return true
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dfltValue interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			continue
		}
		if name == column {
			return notNull == 0
		}
	}
	return true
}

func (p *SQLiteSchemaDriver) SequenceExists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (p *SQLiteSchemaDriver) GetVersion(_ context.Context) (string, error) {
	return "SQLite", nil
}

func (p *SQLiteSchemaDriver) GetTableStats(_ context.Context) ([]model.TableStat, error) {
	return nil, nil
}

func (p *SQLiteSchemaDriver) GetIndexStats(_ context.Context) ([]model.IndexStat, error) {
	return nil, nil
}

func (p *SQLiteSchemaDriver) GetTableRowCount(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

func (p *SQLiteSchemaDriver) ResetSequence(_ context.Context, _ string, _ int64) error {
	return nil
}

func (p *SQLiteSchemaDriver) DropTables(ctx context.Context, tables []string) error {
	for _, t := range tables {
		if err := p.exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", t)); err != nil {
			return err
		}
	}
	return nil
}

func (p *SQLiteSchemaDriver) TruncateTable(ctx context.Context, table string) error {
	return p.exec(ctx, fmt.Sprintf("DELETE FROM \"%s\"", table))
}

// --- DDL query functions used by migrate.go ---

// dbColumn represents a column in the database for migration comparison.
type dbColumn struct {
	columnName   string
	dataType     string
	defaultValue *string
	nullable     bool
	migrated     bool
}

// dbTable represents a table's columns for migration comparison.
type dbTable struct {
	columns map[string]*dbColumn
}

// getTableColumns queries column metadata for a table during migration.
func getTableColumns(conn *sql.DB, tableName string) (dbTable, error) {
	sqlTableInfos := `SELECT
	name AS column_name,
	lower(type) AS data_type,
	dflt_value AS column_default,
	NOT "notnull" AS is_nullable
	FROM pragma_table_info($1);
	`

	rows, err := conn.QueryContext(context.Background(), sqlTableInfos, tableName)
	if err != nil {
		return dbTable{}, err
	}
	defer rows.Close()

	dts := make(map[string]*dbColumn)
	dt := dbColumn{}
	for rows.Next() {
		err = rows.Scan(&dt.columnName, &dt.dataType, &dt.defaultValue, &dt.nullable)
		if err != nil {
			return dbTable{}, err
		}

		dts[dt.columnName] = &dbColumn{
			columnName:   dt.columnName,
			dataType:     dt.dataType,
			defaultValue: dt.defaultValue,
			nullable:     dt.nullable,
		}
	}
	return dbTable{columns: dts}, nil
}

// databaseIndex represents an index in the database for migration comparison.
type databaseIndex struct {
	indexName string
	unique    bool
	attname   string
	table     string
	sql       string
	migrated  bool
}

// getTableIndexes queries index metadata for a table during migration.
func getTableIndexes(conn *sql.DB, tableName string) (map[string]*databaseIndex, error) {
	sqlQuery := `
		WITH index_list AS (
			SELECT
				name AS index_name,
				[unique] AS is_unique,
				origin,
				partial
			FROM pragma_index_list($1)
			WHERE origin = 'c'  -- only regular indexes, exclude pk and unique constraints
		),
		index_columns AS (
			SELECT
				il.index_name,
				COALESCE(ii.name, '') AS column_name,
				ii.seqno
			FROM index_list il
			JOIN pragma_index_info(il.index_name) ii
		),
		index_sql AS (
			SELECT
				name AS index_name,
				sql  AS index_sql
			FROM sqlite_master
			WHERE type = 'index'
		)
		SELECT DISTINCT
			il.index_name,
			il.is_unique,
			$1 AS table_name,
			ic.column_name,
			COALESCE(isql.index_sql, '')
		FROM index_list il
		JOIN index_columns ic
			ON il.index_name = ic.index_name
		LEFT JOIN index_sql isql
			ON il.index_name = isql.index_name;
	`

	rows, err := conn.QueryContext(context.Background(), sqlQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dis := make(map[string]*databaseIndex)
	di := databaseIndex{}
	for rows.Next() {
		err = rows.Scan(&di.indexName, &di.unique, &di.table, &di.attname, &di.sql)
		if err != nil {
			return nil, err
		}
		dis[di.indexName] = &databaseIndex{
			indexName: di.indexName,
			unique:    di.unique,
			attname:   di.attname,
			table:     di.table,
			sql:       di.sql,
		}
	}
	return dis, nil
}

// checkFkUnique checks if a foreign key column has a unique constraint.
func checkFkUnique(conn *sql.DB, table, attribute string) bool {
	sql := `
	WITH index_list AS (
		SELECT
			name AS index_name,
			[unique] AS is_unique,
			origin,
			partial
		FROM pragma_index_list($1)
		WHERE origin != 'pk'  -- exclude primary key
	),
	index_columns AS (
		SELECT
			il.index_name,
			ii.name AS column_name,
			ii.seqno
		FROM index_list il
		JOIN pragma_index_info(il.index_name) ii
		WHERE ii.name = $2
	)
	SELECT DISTINCT
		il.is_unique
	FROM index_list il
	JOIN index_columns ic ON il.index_name = ic.index_name;`

	var b bool
	row := conn.QueryRowContext(context.Background(), sql, table, attribute)
	row.Scan(&b)
	return b
}

// dataType maps Go types to SQLite types for migration.
type dataType struct {
	typeName  string
	zeroValue string
}

// checkDataType resolves a Go type to a SQLite dataType.
func checkDataType(structDataType string, dataMap map[string]*dataType) dataType {
	dt := dataType{typeName: structDataType}
	switch structDataType {
	case "int8", "uint8", "uint16":
		dt = dataType{"int16", "0"}
	case "int", "uint", "uint32":
		dt = dataType{"int32", "0"}
	case "uint64":
		dt = dataType{"int64", "0"}
	case "[16]uint8":
		dt = dataType{"uuid", "'00000000-0000-0000-0000-000000000000'"}
	}

	if dataMap[dt.typeName] != nil {
		return *dataMap[dt.typeName]
	}

	if strings.Contains(structDataType, "uuid.UUID") {
		return dataType{"uuid", "'00000000-0000-0000-0000-000000000000'"}
	}

	for _, s := range []string{"number", "numeric", "decimal"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0"}
		}
	}

	if strings.Contains(structDataType, "Decimal") {
		return dataType{"decimal", "0"}
	}

	for _, s := range []string{"date", "time"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0000-01-01"}
		}
	}

	for _, s := range []string{"char", "varchar", "text"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "''"}
		}
	}

	return dt
}

// setDefault generates DEFAULT clause SQL.
func setDefault(d string) string {
	if d == "" {
		return ""
	}
	return fmt.Sprintf("DEFAULT %v", d)
}

// createTableSql generates CREATE TABLE SQL from parts.
func createTableSql(create, pks string, attributes []string, sql *strings.Builder) {
	sql.WriteString(create)
	for _, a := range attributes {
		sql.WriteString(a)
	}
	sql.WriteString(pks)
	sql.WriteString(");\n")
}

// renameColumn generates ALTER TABLE RENAME COLUMN SQL.
func renameColumn(table, oldColumnName, newColumnName string) string {
	return fmt.Sprintf("ALTER TABLE %v RENAME COLUMN %v TO %v;\n", table, oldColumnName, newColumnName)
}

// dropColumn generates ALTER TABLE DROP COLUMN SQL.
func dropColumn(table, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %v DROP COLUMN %v;\n", table, columnName)
}

// addColumn generates ADD COLUMN SQL.
func addColumn(table *model.TableMigrate, column string, dataType dataType, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NULL;\n", table.EscapingTableName(), column, dataType.typeName)
	}
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NOT NULL DEFAULT %v;\n", table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

// createIndex generates CREATE INDEX SQL.
func createIndex(index model.IndexMigrate, table *model.TableMigrate) string {
	return fmt.Sprintf("CREATE %v %v ON %v (%v);\n",
		func() string {
			if index.Unique {
				return "UNIQUE INDEX"
			}
			return "INDEX"
		}(),
		index.EscapingName,
		table.EscapingName,
		func() string {
			var s strings.Builder
			if index.Func != "" {
				s.WriteString(index.Func + "(")
			}
			s.WriteString(fmt.Sprintf("%v", index.Attributes[0].EscapingName))
			for _, a := range index.Attributes[1:] {
				s.WriteString(fmt.Sprintf(",%v", a.EscapingName))
			}
			if index.Func != "" {
				s.WriteString(")")
			}
			return s.String()
		}(),
	)
}

// dropIndex generates DROP INDEX SQL.
func dropIndex(_ *model.TableMigrate, idxName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %v;", idxName) + "\n"
}

// foreignManyToSome generates column definition with FK for many-to-one.
func foreignManyToSome(att model.ManyToSomeMigrate, dataMap map[string]*dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
	feature := "NULL"
	if !att.Nullable {
		feature = "NOT NULL"
	}
	return fmt.Sprintf("%v %v %v REFERENCES %v(%v),",
		att.EscapingName, att.DataType, feature,
		att.EscapingTargetTable, att.EscapingTargetColumn)
}

// foreignOneToSome generates column definition with FK for one-to-one/one-to-many.
func foreignOneToSome(att model.OneToSomeMigrate, dataMap map[string]*dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
	feature := "NULL"
	if !att.Nullable {
		feature = "NOT NULL"
	}
	if !att.IsOneToMany {
		feature = "UNIQUE " + feature
	}
	return fmt.Sprintf("%v %v %s REFERENCES %v(%v),",
		att.EscapingName, att.DataType, feature,
		att.EscapingTargetTable, att.EscapingTargetColumn)
}

// primaryKeyIsForeignKey checks if a primary key is also a foreign key.
func primaryKeyIsForeignKey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.ManyToSomes, func(m model.ManyToSomeMigrate) bool {
		return m.Name == attName
	}) || slices.ContainsFunc(table.OneToSomes, func(m model.OneToSomeMigrate) bool {
		return m.Name == attName
	})
}

// foreignKeyIsPrimarykey checks if a foreign key is also a primary key.
func foreignKeyIsPrimarykey(table *model.TableMigrate, attName string) bool {
	isSameName := func(m model.PrimaryKeyMigrate) bool {
		return m.Name == attName
	}
	return slices.ContainsFunc(table.PrimaryKeys, isSameName)
}
