package pgsql

import (
	"context"
	"fmt"
	"strings"

	"github.com/azhai/goent/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgSchemaDriver struct {
	schema string
	query  func(ctx context.Context, sql string, args ...any) (model.Rows, error)
	exec   func(ctx context.Context, sql string, args ...any) error
}

func NewPgSchemaDriver(schema string, queryFn func(ctx context.Context, sql string, args ...any) (model.Rows, error), execFn func(ctx context.Context, sql string, args ...any) error) *PgSchemaDriver {
	if schema == "" {
		schema = "public"
	}
	return &PgSchemaDriver{schema: schema, query: queryFn, exec: execFn}
}

func (p *PgSchemaDriver) ListTables(ctx context.Context) ([]string, error) {
	return p.ListTablesInSchema(ctx, p.schema)
}

func (p *PgSchemaDriver) ListTablesInSchema(ctx context.Context, schema string) ([]string, error) {
	rows, err := p.query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		ORDER BY table_name`, schema)
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

func (p *PgSchemaDriver) GetColumns(ctx context.Context, table string) ([]model.ColumnDef, error) {
	rows, err := p.query(ctx, `
		SELECT column_name, data_type, is_nullable = 'YES', COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, p.schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []model.ColumnDef
	for rows.Next() {
		var col model.ColumnDef
		if err := rows.Scan(&col.Name, &col.DataType, &col.Nullable, &col.Default); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func (p *PgSchemaDriver) GetIndexes(ctx context.Context, table string) ([]model.IndexDef, error) {
	rows, err := p.query(ctx, `
		SELECT i.relname, a.attname, ix.indisunique, ix.indisprimary
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = $1 AND t.relname = $2
		ORDER BY i.relname, a.attnum`, p.schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idxMap := make(map[string]*model.IndexDef)
	for rows.Next() {
		var idxName, colName string
		var isUnique, isPrimary bool
		if err := rows.Scan(&idxName, &colName, &isUnique, &isPrimary); err != nil {
			return nil, err
		}
		if isPrimary {
			continue
		}
		if _, ok := idxMap[idxName]; !ok {
			idxMap[idxName] = &model.IndexDef{Name: idxName, Unique: isUnique}
		}
		idxMap[idxName].Columns = append(idxMap[idxName].Columns, colName)
	}
	var result []model.IndexDef
	for _, idx := range idxMap {
		result = append(result, *idx)
	}
	return result, nil
}

func (p *PgSchemaDriver) GetPrimaryKey(ctx context.Context, table string) ([]string, error) {
	rows, err := p.query(ctx, `
		SELECT a.attname
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = $1 AND t.relname = $2 AND ix.indisprimary
		ORDER BY a.attnum`, p.schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		pks = append(pks, name)
	}
	return pks, nil
}

func (p *PgSchemaDriver) GetForeignKeys(ctx context.Context, table string) ([]model.ForeignKeyDef, error) {
	rows, err := p.query(ctx, `
		SELECT tc.constraint_name, kcu.column_name, ccu.table_name, ccu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1 AND tc.table_name = $2
		ORDER BY tc.constraint_name, kcu.ordinal_position`, p.schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fkMap := make(map[string]*model.ForeignKeyDef)
	for rows.Next() {
		var constraintName, columnName, refTable, refColumn string
		if err := rows.Scan(&constraintName, &columnName, &refTable, &refColumn); err != nil {
			return nil, err
		}
		if _, ok := fkMap[constraintName]; !ok {
			fkMap[constraintName] = &model.ForeignKeyDef{Name: constraintName, RefTable: refTable}
		}
		fkMap[constraintName].Columns = append(fkMap[constraintName].Columns, columnName)
		fkMap[constraintName].RefColumns = append(fkMap[constraintName].RefColumns, refColumn)
	}
	var result []model.ForeignKeyDef
	for _, fk := range fkMap {
		result = append(result, *fk)
	}
	return result, nil
}

func (p *PgSchemaDriver) DiscoverFKs(ctx context.Context, table string) ([]model.FKRef, error) {
	rows, err := p.query(ctx, `
		SELECT kcu.table_name, kcu.column_name,
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		JOIN information_schema.columns c
			ON kcu.table_name = c.table_name AND kcu.column_name = c.column_name AND kcu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = $1 AND ccu.table_name = $2 AND ccu.column_name = 'id'
		ORDER BY kcu.table_name, kcu.column_name`, p.schema, table)
	if err != nil {
		return nil, fmt.Errorf("querying PG foreign keys: %w", err)
	}
	defer rows.Close()
	var fks []model.FKRef
	for rows.Next() {
		var fk model.FKRef
		if err := rows.Scan(&fk.FromTable, &fk.FromColumn, &fk.Nullable); err != nil {
			return nil, fmt.Errorf("scanning FK: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

func (p *PgSchemaDriver) IsColumnNullable(_ context.Context, _, _ string) bool {
	return true
}

func (p *PgSchemaDriver) SequenceExists(ctx context.Context, seqName string) (bool, error) {
	rows, err := p.query(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_sequences WHERE schemaname = $1 AND sequencename = $2)",
		p.schema, seqName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var exists bool
	if rows.Next() {
		rows.Scan(&exists)
	}
	return exists, nil
}

func (p *PgSchemaDriver) GetVersion(ctx context.Context) (string, error) {
	rows, err := p.query(ctx, "SELECT version()")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var v string
	if rows.Next() {
		rows.Scan(&v)
	}
	return v, nil
}

func (p *PgSchemaDriver) GetTableStats(ctx context.Context) ([]model.TableStat, error) {
	rows, err := p.query(ctx, `
		SELECT relname,
			COALESCE(seq_scan, 0), COALESCE(idx_scan, 0),
			COALESCE(n_dead_tup, 0), COALESCE(n_live_tup, 0),
			last_vacuum, last_analyze
		FROM pg_stat_user_tables
		WHERE schemaname = $1`, p.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []model.TableStat
	for rows.Next() {
		var s model.TableStat
		if err := rows.Scan(&s.TableName, &s.SeqScan, &s.IdxScan,
			&s.NDeadTup, &s.NLiveTup, &s.LastVacuum, &s.LastAnalyze); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

func (p *PgSchemaDriver) GetIndexStats(ctx context.Context) ([]model.IndexStat, error) {
	rows, err := p.query(ctx, `
		SELECT schemaname, relname, indexrelname,
			COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0),
			COALESCE(idx_tup_read, 0), COALESCE(idx_tup_fetch, 0),
			COALESCE(pg_relation_size(indexrelid), 0)
		FROM pg_stat_user_indexes
		WHERE schemaname = $1`, p.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stats []model.IndexStat
	for rows.Next() {
		var s model.IndexStat
		var schema string
		if err := rows.Scan(&schema, &s.TableName, &s.IndexName,
			&s.IdxScan, &s.IdxFetch, &s.IdxTupRead, &s.IdxTupFetch, &s.Size); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, nil
}

func (p *PgSchemaDriver) GetTableRowCount(ctx context.Context, table string) (int64, error) {
	rows, err := p.query(ctx,
		fmt.Sprintf("SELECT COALESCE((SELECT reltuples::bigint FROM pg_class WHERE oid = '%s'::regclass), 0)", table))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		rows.Scan(&count)
	}
	return count, nil
}

func (p *PgSchemaDriver) ResetSequence(ctx context.Context, seqName string, nextVal int64) error {
	return p.exec(ctx, fmt.Sprintf("SELECT setval('%s', %d, false)", seqName, nextVal))
}

func (p *PgSchemaDriver) DropTables(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", joinTableNames(tables))
	return p.exec(ctx, dropSQL)
}

func (p *PgSchemaDriver) TruncateTable(ctx context.Context, table string) error {
	return p.exec(ctx, fmt.Sprintf("TRUNCATE TABLE \"%s\" CASCADE", table))
}

func joinTableNames(tables []string) string {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = fmt.Sprintf("\"%s\"", t)
	}
	s := ""
	for i, q := range quoted {
		if i > 0 {
			s += ", "
		}
		s += q
	}
	return s
}

// --- DDL query functions used by migrate.go ---

// getSchemas queries all non-system schemas in the database.
func getSchemas(conn *pgxpool.Pool) ([]string, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema';
	`)
	if err != nil {
		return nil, err
	}

	var s string
	schemas := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&s)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, nil
}

// dbColumn represents a column in the database for migration comparison.
type dbColumn struct {
	columnName   string
	dataType     string
	defaultValue *string
	nullable     bool
}

// dbTable represents a table's columns for migration comparison.
type dbTable struct {
	columns map[string]*dbColumn
}

// getTableColumns queries column metadata for a table during migration.
func getTableColumns(conn *pgxpool.Pool, tableName string) (dbTable, error) {
	sqlTableInfos := `SELECT
	column_name, CASE
	WHEN data_type = 'character varying'
	THEN CONCAT('varchar','(',character_maximum_length,')')
	when data_type = 'integer' then case WHEN column_default like 'nextval%' THEN 'serial' ELSE data_type end
	when data_type = 'bigint' then case WHEN column_default like 'nextval%' THEN 'bigserial' ELSE data_type end
	when data_type like 'timestamp%' then 'timestamp'
	when data_type like 'numeric' then CONCAT('decimal', '(',numeric_precision, ',', numeric_scale, ')')
	ELSE data_type END,
	column_default,
	CASE
	WHEN is_nullable = 'YES'
	THEN True
	ELSE False END AS is_nullable
	FROM information_schema.columns WHERE table_name = $1;
	`

	rows, err := conn.Query(context.Background(), sqlTableInfos, tableName)
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
	migrated  bool
}

// getTableIndexes queries index metadata for a table during migration.
func getTableIndexes(conn *pgxpool.Pool, tableName string) (map[string]*databaseIndex, error) {
	sqlQuery := `SELECT DISTINCT ci.relname, i.indisunique as is_unique, c.relname, a.attname FROM pg_index i
	JOIN pg_attribute a ON i.indexrelid = a.attrelid
	JOIN pg_class ci ON ci.oid = i.indexrelid
	JOIN pg_class c ON c.oid = i.indrelid
	where i.indisprimary = false AND c.relname = $1;
	`

	rows, err := conn.Query(context.Background(), sqlQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dis := make(map[string]*databaseIndex)
	di := databaseIndex{}
	for rows.Next() {
		err = rows.Scan(&di.indexName, &di.unique, &di.table, &di.attname)
		if err != nil {
			return nil, err
		}
		dis[di.indexName] = &databaseIndex{
			indexName: di.indexName,
			unique:    di.unique,
			attname:   di.attname,
			table:     di.table,
		}
	}
	return dis, nil
}

// checkFkUnique checks if a foreign key column has a unique constraint.
func checkFkUnique(conn *pgxpool.Pool, table, attribute string) (string, bool) {
	sql := `SELECT ci.relname, i.indisunique as is_unique FROM pg_index i
	JOIN pg_attribute a ON i.indexrelid = a.attrelid
	JOIN pg_class ci ON ci.oid = i.indexrelid
	JOIN pg_class c ON c.oid = i.indrelid
	where i.indisprimary = false AND c.relname = $1 AND a.attname = $2;`

	var b bool
	var s string
	row := conn.QueryRow(context.Background(), sql, table, attribute)
	row.Scan(&s, &b)
	return s, b
}

// alterColumn generates ALTER COLUMN TYPE SQL.
func alterColumn(table *model.TableMigrate, column, dataType string, dataMap map[string]dataType) string {
	if dt, ok := dataMap[dataType]; ok {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table.EscapingTableName(), column, dt.typeName)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table.EscapingTableName(), column, dataType)
}

// alterColumnDefault generates ALTER COLUMN SET DEFAULT SQL.
func alterColumnDefault(table *model.TableMigrate, column, defa string) string {
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;\n", table.EscapingTableName(), column, defa)
}

// nullableColumn generates ALTER COLUMN NULL/NOT NULL SQL.
func nullableColumn(table *model.TableMigrate, columnName string, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP NOT NULL;\n", table.EscapingTableName(), columnName)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET NOT NULL;\n", table.EscapingTableName(), columnName)
}

// addColumn generates ADD COLUMN SQL.
func addColumn(table *model.TableMigrate, column string, dataType dataType, nullable bool, dropDefault bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NULL;\n", table.EscapingTableName(), column, dataType.typeName)
	}
	if dropDefault {
		return fmt.Sprintf("ALTER TABLE %[1]v ADD COLUMN %[2]v %[3]v NOT NULL DEFAULT %[4]v;\n ALTER TABLE %[1]v ALTER COLUMN %[2]v DROP DEFAULT;\n",
			table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
	}
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NOT NULL DEFAULT %v;\n",
		table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

// addColumnUnique generates ADD COLUMN UNIQUE SQL.
func addColumnUnique(table *model.TableMigrate, column string, dataType dataType, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v UNIQUE NULL;\n", table.EscapingTableName(), column, dataType)
	}
	return fmt.Sprintf("ALTER TABLE %[1]v ADD COLUMN %[2]v %[3]v UNIQUE NOT NULL DEFAULT %[4]v;\n ALTER TABLE %[1]v ALTER COLUMN %[2]v DROP DEFAULT;\n",
		table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

// addFkManyToSome generates ADD CONSTRAINT FOREIGN KEY SQL for many-to-one.
func addFkManyToSome(table *model.TableMigrate, att model.ManyToSomeMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingTableName(),
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

// addFkOneToSome generates ADD CONSTRAINT FOREIGN KEY SQL for one-to-one/one-to-many.
func addFkOneToSome(table *model.TableMigrate, att model.OneToSomeMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingTableName(),
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

// dropIndex generates DROP INDEX SQL.
func dropIndex(table *model.TableMigrate, idxName string) string {
	if table.Schema != nil {
		return fmt.Sprintf("DROP INDEX IF EXISTS %v;", *table.Schema+"."+idxName) + "\n"
	}
	return fmt.Sprintf("DROP INDEX IF EXISTS %v;", idxName) + "\n"
}

// renameColumn generates ALTER TABLE RENAME COLUMN SQL.
func renameColumn(table, oldColumnName, newColumnName string) string {
	return fmt.Sprintf("ALTER TABLE %v RENAME COLUMN %v TO %v;\n", table, oldColumnName, newColumnName)
}

// dropColumn generates ALTER TABLE DROP COLUMN SQL.
func dropColumn(table, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %v DROP COLUMN %v;\n", table, columnName)
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

// setDefault generates DEFAULT clause SQL.
func setDefault(d string) string {
	if d == "" {
		return ""
	}
	return fmt.Sprintf("DEFAULT %v", d)
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
		table.EscapingTableName(),
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

// dataType maps Go types to PostgreSQL types for migration.
type dataType struct {
	typeName  string
	zeroValue string
}

// checkDataType resolves a Go type to a PostgreSQL dataType.
func checkDataType(structDataType string, dataMap map[string]dataType) dataType {
	dt := dataType{typeName: structDataType}
	switch structDataType {
	case "int8", "uint8", "uint16":
		dt = dataType{"int16", "0"}
	case "int", "uint", "uint32":
		dt = dataType{"int32", "0"}
	case "uint64":
		dt = dataType{"int64", "0"}
	}

	if dt, ok := dataMap[dt.typeName]; ok {
		return dt
	}

	// Handle full package path types like "github.com/google/uuid.UUID"
	if idx := strings.LastIndex(structDataType, "/"); idx != -1 {
		shortName := structDataType[idx+1:]
		if dt, ok := dataMap[shortName]; ok {
			return dt
		}
	}

	for _, s := range []string{"number", "numeric", "decimal"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0"}
		}
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

// checkTypeAutoIncrement converts between serial and integer types.
func checkTypeAutoIncrement(structDataType string) string {
	dataMap := map[string]string{
		"smallint":    "smallserial",
		"integer":     "serial",
		"bigint":      "bigserial",
		"smallserial": "smallint",
		"serial":      "integer",
		"bigserial":   "bigint",
	}
	return dataMap[structDataType]
}
