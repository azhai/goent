package reverse

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
)

// UnquoteWord removes quotes from a word if it is quoted.
func UnquoteWord(word string) string {
	if word == "" {
		return word
	}
	return strings.Trim(word, " '`\"")
}

// SQLTemplates holds database-specific SQL queries
type SQLTemplates struct {
	GetTables      string
	GetColumns     string
	GetPrimaryKey  string
	GetForeignKeys string
	GetIndexes     string
}

type ReverseConfig struct {
	DriverType  string
	DatabaseDSN string
	SchemaName  string
	TablePrefix string
}

func (rc *ReverseConfig) FixConfigData() {
	rc.DatabaseDSN = UnquoteWord(rc.DatabaseDSN)
	rc.DriverType = "sqlite"
	if strings.HasPrefix(rc.DatabaseDSN, "postgres://") {
		rc.DriverType = "pgsql"
	}
	rc.SchemaName = UnquoteWord(rc.SchemaName)
	if rc.DriverType == "pgsql" && rc.SchemaName == "" {
		rc.SchemaName = "public"
	}
	rc.TablePrefix = UnquoteWord(rc.TablePrefix)
}

// ReverseEngineer implements database reverse engineering
type ReverseEngineer struct {
	driver    model.Driver
	conn      model.Connection
	templates SQLTemplates
	Config    *ReverseConfig
}

// NewPgsqlReverseEngineer creates a new PostgreSQL reverse engineer
func NewPgsqlReverseEngineer(dsn, schema string) (*ReverseEngineer, error) {
	cfg := &ReverseConfig{
		DriverType:  "pgsql",
		DatabaseDSN: dsn,
		SchemaName:  schema,
	}
	cfg.FixConfigData()

	driver := pgsql.OpenDSN(cfg.DatabaseDSN)
	if driver == nil {
		return nil, fmt.Errorf("failed to create PostgreSQL driver")
	}
	if err := driver.Init(); err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	templates := SQLTemplates{
		GetTables: `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = $1 AND table_type = 'BASE TABLE'
			ORDER BY table_name`,
		GetColumns: `
			SELECT 
				column_name,
				CASE 
					WHEN data_type = 'character varying' THEN CONCAT('varchar(', COALESCE(character_maximum_length::text, ''), ')')
					WHEN data_type = 'integer' AND column_default LIKE 'nextval%' THEN 'serial'
					WHEN data_type = 'bigint' AND column_default LIKE 'nextval%' THEN 'bigserial'
					WHEN data_type LIKE 'timestamp%' THEN 'timestamp'
					WHEN data_type = 'numeric' THEN CONCAT('numeric(', COALESCE(numeric_precision::text, ''), ',', COALESCE(numeric_scale::text, ''), ')')
					ELSE data_type
				END as data_type,
				is_nullable = 'YES',
				column_default
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position`,
		GetPrimaryKey: `
			SELECT tc.constraint_name, kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
				AND tc.table_schema = $1 AND tc.table_name = $2
			ORDER BY kcu.ordinal_position`,
		GetForeignKeys: `
			SELECT 
				tc.constraint_name,
				kcu.column_name,
				ccu.table_name AS referenced_table,
				ccu.column_name AS referenced_column
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
			WHERE tc.constraint_type = 'FOREIGN KEY'
				AND tc.table_schema = $1 AND tc.table_name = $2
			ORDER BY tc.constraint_name, kcu.ordinal_position`,
		GetIndexes: `
			SELECT 
				i.relname as index_name,
				a.attname as column_name,
				ix.indisunique as is_unique
			FROM pg_class t
			JOIN pg_index ix ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
			WHERE n.nspname = $1 AND t.relname = $2
				AND NOT ix.indisprimary
			ORDER BY i.relname, a.attnum`,
	}

	eng := &ReverseEngineer{
		driver:    driver,
		conn:      driver.NewConnection(),
		templates: templates,
		Config:    cfg,
	}
	return eng, nil
}

// NewSqliteReverseEngineer creates a new SQLite reverse engineer
func NewSqliteReverseEngineer(dsn string) (*ReverseEngineer, error) {
	cfg := &ReverseConfig{
		DriverType:  "sqlite",
		DatabaseDSN: dsn,
	}
	cfg.FixConfigData()

	driver := sqlite.OpenDSN(cfg.DatabaseDSN)
	if driver == nil {
		return nil, fmt.Errorf("failed to create SQLite driver")
	}
	if err := driver.Init(); err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	templates := SQLTemplates{
		GetTables:      `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`,
		GetColumns:     `SELECT name, lower(type), dflt_value, "notnull" = 0 FROM pragma_table_info(?)`,
		GetPrimaryKey:  `SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk`,
		GetForeignKeys: `SELECT id, "table", "from", "to" FROM pragma_foreign_key_list(?)`,
		GetIndexes: `
			SELECT 
				il.name as index_name,
				ii.name as column_name,
				il."unique" = 1 as is_unique
			FROM pragma_index_list(?) il
			JOIN pragma_index_info(il.name) ii
			WHERE il."origin" = 'c'
			ORDER BY il.name, ii.seqno`,
	}

	eng := &ReverseEngineer{
		driver:    driver,
		conn:      driver.NewConnection(),
		templates: templates,
		Config:    cfg,
	}
	return eng, nil
}

// Close closes the database connection
func (r *ReverseEngineer) Close() {
	r.driver.Close()
}

func (r *ReverseEngineer) queryTableInfo(sql, schema, table string) (model.Rows, error) {
	var args []any
	if r.Config.DriverType == "pgsql" {
		args = []any{schema, table}
	} else {
		args = []any{table}
	}
	qr := model.CreateQuery(sql, args)
	return r.conn.QueryContext(context.Background(), &qr)
}

func (r *ReverseEngineer) GetDatabaseConfig() *model.DatabaseConfig {
	return r.driver.GetDatabaseConfig()
}

// GetTables returns all tables in the schema
func (r *ReverseEngineer) GetTables(prefix string) ([]string, error) {
	var rows model.Rows
	var err error

	rows, err = r.queryTableInfo(r.templates.GetTables, r.Config.SchemaName, "")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if prefix == "" || strings.HasPrefix(name, prefix) {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// GetTableInfo returns detailed information about a table
func (r *ReverseEngineer) GetTableInfo(tableName string) (*TableInfo, error) {
	info := &TableInfo{Schema: r.Config.SchemaName, Name: tableName}

	columns, err := r.getColumns(tableName)
	if err != nil {
		return nil, err
	}
	info.Columns = columns

	pk, err := r.getPrimaryKey(tableName)
	if err != nil {
		return nil, err
	}
	info.PrimaryKey = pk

	if pk != nil {
		for _, col := range info.Columns {
			if slices.Contains(pk.Columns, col.Name) {
				col.IsPrimaryKey = true
			}
		}
	}

	fks, err := r.getForeignKeys(tableName)
	if err != nil {
		return nil, err
	}
	info.ForeignKeys = fks

	indexes, err := r.getIndexes(tableName)
	if err != nil {
		return nil, err
	}
	info.Indexes = indexes

	fkColumns := make(map[string]bool)
	for _, fk := range fks {
		for _, col := range fk.Columns {
			fkColumns[col] = true
		}
	}

	for _, idx := range indexes {
		if len(idx.Columns) != 1 {
			continue
		}
		colName := idx.Columns[0]
		for _, col := range info.Columns {
			if col.Name == colName {
				if idx.IsUnique {
					col.IsUnique = true
				} else if !fkColumns[colName] {
					col.IsIndex = true
				}
				break
			}
		}
	}

	return info, nil
}

func (r *ReverseEngineer) getColumns(tableName string) ([]*ColumnInfo, error) {
	var rows model.Rows
	var err error

	rows, err = r.queryTableInfo(r.templates.GetColumns, r.Config.SchemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ColumnInfo
	for rows.Next() {
		col := &ColumnInfo{DefaultValue: new(string)}
		if r.Config.DriverType == "pgsql" {
			if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &col.DefaultValue); err != nil {
				return nil, err
			}
		} else {
			var notNull bool
			if err := rows.Scan(&col.Name, &col.DataType, &col.DefaultValue, &notNull); err != nil {
				return nil, err
			}
			col.IsNullable = !notNull
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func (r *ReverseEngineer) getPrimaryKey(tableName string) (*PrimaryKeyInfo, error) {
	var rows model.Rows
	var err error

	rows, err = r.queryTableInfo(r.templates.GetPrimaryKey, r.Config.SchemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pk *PrimaryKeyInfo
	for rows.Next() {
		var constraintName, columnName string
		if r.Config.DriverType == "pgsql" {
			if err := rows.Scan(&constraintName, &columnName); err != nil {
				return nil, err
			}
		} else {
			constraintName = "PRIMARY"
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
		}
		if pk == nil {
			pk = &PrimaryKeyInfo{Name: constraintName}
		}
		pk.Columns = append(pk.Columns, columnName)
	}
	return pk, nil
}

func (r *ReverseEngineer) getForeignKeys(tableName string) ([]*ForeignKeyInfo, error) {
	var rows model.Rows
	var err error

	rows, err = r.queryTableInfo(r.templates.GetForeignKeys, r.Config.SchemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if r.Config.DriverType == "pgsql" {
		fkMap := make(map[string]*ForeignKeyInfo)
		for rows.Next() {
			var constraintName, columnName, referencedTable, referencedColumn string
			if err := rows.Scan(&constraintName, &columnName, &referencedTable, &referencedColumn); err != nil {
				return nil, err
			}
			fk, ok := fkMap[constraintName]
			if !ok {
				fk = &ForeignKeyInfo{Name: constraintName, ReferencedTable: referencedTable}
				fkMap[constraintName] = fk
			}
			fk.Columns = append(fk.Columns, columnName)
			fk.ReferencedColumns = append(fk.ReferencedColumns, referencedColumn)
		}
		var fks []*ForeignKeyInfo
		for _, fk := range fkMap {
			fks = append(fks, fk)
		}
		return fks, nil
	}

	fkMap := make(map[int]*ForeignKeyInfo)
	for rows.Next() {
		var id int
		var referencedTable, fromCol, toCol string
		if err := rows.Scan(&id, &referencedTable, &fromCol, &toCol); err != nil {
			return nil, err
		}
		fk, ok := fkMap[id]
		if !ok {
			fk = &ForeignKeyInfo{Name: fmt.Sprintf("fk_%d", id), ReferencedTable: referencedTable}
			fkMap[id] = fk
		}
		fk.Columns = append(fk.Columns, fromCol)
		fk.ReferencedColumns = append(fk.ReferencedColumns, toCol)
	}
	var fks []*ForeignKeyInfo
	for _, fk := range fkMap {
		fks = append(fks, fk)
	}
	return fks, nil
}

func (r *ReverseEngineer) getIndexes(tableName string) ([]*IndexInfo, error) {
	var rows model.Rows
	var err error

	rows, err = r.queryTableInfo(r.templates.GetIndexes, r.Config.SchemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	idxMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var indexName, columnName string
		var isUnique bool
		if err := rows.Scan(&indexName, &columnName, &isUnique); err != nil {
			return nil, err
		}
		idx, ok := idxMap[indexName]
		if !ok {
			idx = &IndexInfo{Name: indexName, IsUnique: isUnique}
			idxMap[indexName] = idx
		}
		idx.Columns = append(idx.Columns, columnName)
	}
	var indexes []*IndexInfo
	for _, idx := range idxMap {
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// MapSQLTypeToGo maps SQL data types to Go types
func MapSQLTypeToGo(sqlType string, driver string) string {
	sqlType = strings.ToLower(sqlType)

	if driver == "sqlite" {
		switch sqlType {
		case "integer", "int":
			return "int"
		case "real", "double", "float":
			return "float64"
		case "text", "varchar", "char":
			return "string"
		case "blob":
			return "[]byte"
		case "boolean", "bool":
			return "bool"
		case "datetime", "date", "timestamp":
			return "time.Time"
		default:
			if strings.Contains(sqlType, "int") {
				return "int"
			}
			return "string"
		}
	}

	switch {
	case strings.Contains(sqlType, "serial") || strings.Contains(sqlType, "bigserial"):
		if strings.Contains(sqlType, "big") {
			return "int64"
		}
		return "int"
	case strings.Contains(sqlType, "int"):
		if strings.Contains(sqlType, "bigint") {
			return "int64"
		}
		if strings.Contains(sqlType, "smallint") {
			return "int16"
		}
		return "int"
	case strings.Contains(sqlType, "float") || strings.Contains(sqlType, "double") ||
		strings.Contains(sqlType, "numeric") || strings.Contains(sqlType, "decimal"):
		return "float64"
	case strings.Contains(sqlType, "real"):
		return "float32"
	case strings.Contains(sqlType, "bool"):
		return "bool"
	case strings.Contains(sqlType, "timestamp") || strings.Contains(sqlType, "date") || strings.Contains(sqlType, "time"):
		return "time.Time"
	case strings.Contains(sqlType, "uuid"):
		return "uuid.UUID"
	case strings.Contains(sqlType, "bytea"):
		return "[]byte"
	case strings.Contains(sqlType, "json"):
		return "json.RawMessage"
	default:
		return "string"
	}
}
