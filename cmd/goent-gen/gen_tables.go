package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
)

// TableInfo represents database table metadata
type TableInfo struct {
	Schema      string
	Name        string
	Columns     []*ColumnInfo
	PrimaryKey  *PrimaryKeyInfo
	ForeignKeys []*ForeignKeyInfo
	Indexes     []*IndexInfo
}

// ColumnInfo represents database column metadata
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	IsPrimaryKey bool
	IsUnique     bool
	IsIndex      bool
	DefaultValue *string
}

// PrimaryKeyInfo represents primary key metadata
type PrimaryKeyInfo struct {
	Name    string
	Columns []string
}

// ForeignKeyInfo represents foreign key metadata
type ForeignKeyInfo struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	IsUnique          bool
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name     string
	Columns  []string
	IsUnique bool
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
	DatabaseDSN string
	SchemaName  string
	TablePrefix string
}

// ReverseEngineer implements database reverse engineering
type ReverseEngineer struct {
	dbType    string
	driver    model.Driver
	conn      model.Connection
	schema    string
	templates SQLTemplates
}

// NewPgsqlReverseEngineer creates a new PostgreSQL reverse engineer
func NewPgsqlReverseEngineer(dsn, schema string) (*ReverseEngineer, error) {
	if schema == "" {
		schema = "public"
	}
	driver := pgsql.OpenDSN(dsn)
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

	return &ReverseEngineer{
		dbType:    "pgsql",
		driver:    driver,
		conn:      driver.NewConnection(),
		schema:    schema,
		templates: templates,
	}, nil
}

// NewSqliteReverseEngineer creates a new SQLite reverse engineer
func NewSqliteReverseEngineer(dsn string) (*ReverseEngineer, error) {
	driver := sqlite.OpenDSN(dsn)
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

	return &ReverseEngineer{
		dbType:    "sqlite",
		driver:    driver,
		conn:      driver.NewConnection(),
		schema:    "",
		templates: templates,
	}, nil
}

// Close closes the database connection
func (r *ReverseEngineer) Close() {
	r.driver.Close()
}

func (r *ReverseEngineer) query(query string, args ...any) (model.Rows, error) {
	qr := model.CreateQuery(query, args)
	return r.conn.QueryContext(context.Background(), &qr)
}

// GetTables returns all tables in the schema
func (r *ReverseEngineer) GetTables(prefix string) ([]string, error) {
	var rows model.Rows
	var err error

	if r.dbType == "pgsql" {
		rows, err = r.query(r.templates.GetTables, r.schema)
	} else {
		rows, err = r.query(r.templates.GetTables)
	}
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
	info := &TableInfo{Schema: r.schema, Name: tableName}

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
		if len(idx.Columns) == 1 {
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
	}

	return info, nil
}

func (r *ReverseEngineer) getColumns(tableName string) ([]*ColumnInfo, error) {
	var rows model.Rows
	var err error

	if r.dbType == "pgsql" {
		rows, err = r.query(r.templates.GetColumns, r.schema, tableName)
	} else {
		rows, err = r.query(r.templates.GetColumns, tableName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*ColumnInfo
	for rows.Next() {
		col := &ColumnInfo{}
		if r.dbType == "pgsql" {
			var defaultValue *string
			if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &defaultValue); err != nil {
				return nil, err
			}
			col.DefaultValue = defaultValue
		} else {
			var defaultValue *string
			var notNull bool
			if err := rows.Scan(&col.Name, &col.DataType, &defaultValue, &notNull); err != nil {
				return nil, err
			}
			col.IsNullable = !notNull
			col.DefaultValue = defaultValue
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func (r *ReverseEngineer) getPrimaryKey(tableName string) (*PrimaryKeyInfo, error) {
	var rows model.Rows
	var err error

	if r.dbType == "pgsql" {
		rows, err = r.query(r.templates.GetPrimaryKey, r.schema, tableName)
	} else {
		rows, err = r.query(r.templates.GetPrimaryKey, tableName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pk *PrimaryKeyInfo
	for rows.Next() {
		if r.dbType == "pgsql" {
			var constraintName, columnName string
			if err := rows.Scan(&constraintName, &columnName); err != nil {
				return nil, err
			}
			if pk == nil {
				pk = &PrimaryKeyInfo{Name: constraintName}
			}
			pk.Columns = append(pk.Columns, columnName)
		} else {
			var columnName string
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
			if pk == nil {
				pk = &PrimaryKeyInfo{Name: "PRIMARY"}
			}
			pk.Columns = append(pk.Columns, columnName)
		}
	}
	return pk, nil
}

func (r *ReverseEngineer) getForeignKeys(tableName string) ([]*ForeignKeyInfo, error) {
	var rows model.Rows
	var err error

	if r.dbType == "pgsql" {
		rows, err = r.query(r.templates.GetForeignKeys, r.schema, tableName)
	} else {
		rows, err = r.query(r.templates.GetForeignKeys, tableName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if r.dbType == "pgsql" {
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

	if r.dbType == "pgsql" {
		rows, err = r.query(r.templates.GetIndexes, r.schema, tableName)
	} else {
		rows, err = r.query(r.templates.GetIndexes, tableName)
	}
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
	case strings.Contains(sqlType, "float") || strings.Contains(sqlType, "double") || strings.Contains(sqlType, "numeric") || strings.Contains(sqlType, "decimal"):
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

// GenerateModel generates Go struct code for a table
func GenerateModel(buf *bytes.Buffer, info *TableInfo, allTables map[string]*TableInfo,
	junctionTables map[string][]string, driver, prefix string) error {
	structName := ToCamelCase(info.Name)
	structName = TrimShortPrefix(structName, prefix)
	structName = ToSingular(structName)
	tableName := info.Name

	fmt.Fprintf(buf, "// %s represents the %s table\n", structName, tableName)
	fmt.Fprintf(buf, "type %s struct {\n", structName)

	var o2oFields []string
	var o2mFields []string
	for otherTableName, otherTableInfo := range allTables {
		if otherTableName == info.Name {
			continue
		}
		for _, fk := range otherTableInfo.ForeignKeys {
			if fk.ReferencedTable == info.Name && len(fk.ReferencedColumns) == 1 {
				fieldName := ToCamelCase(otherTableName)
				fieldName = TrimShortPrefix(fieldName, prefix)
				fieldName = ToSingular(fieldName)
				if fk.IsUnique {
					o2oFields = append(o2oFields, fieldName)
				} else {
					o2mFields = append(o2mFields, fieldName)
				}
			}
		}
	}

	var m2mFields []struct {
		fieldName    string
		junctionName string
	}
	for junctionName, referencedTables := range junctionTables {
		for i, refTable := range referencedTables {
			if refTable == info.Name {
				otherTable := referencedTables[1-i]
				junctionInfo := allTables[junctionName]
				if junctionInfo != nil {
					for _, fk := range junctionInfo.ForeignKeys {
						if fk.ReferencedTable == info.Name && len(fk.ReferencedColumns) == 1 {
							otherStructName := ToCamelCase(otherTable)
							otherStructName = TrimShortPrefix(otherStructName, prefix)
							otherStructName = ToSingular(otherStructName)
							m2mFields = append(m2mFields, struct {
								fieldName    string
								junctionName string
							}{otherStructName, junctionName})
						}
					}
				}
			}
		}
	}

	for _, col := range info.Columns {
		goType := MapSQLTypeToGo(col.DataType, driver)
		fieldName := ToCamelCase(col.Name)

		var tagParts []string
		if col.IsPrimaryKey {
			if len(info.PrimaryKey.Columns) > 1 {
				tagParts = append(tagParts, "pk", "not_incr")
			} else {
				tagParts = append(tagParts, "pk")
			}
		} else if col.IsUnique {
			tagParts = append(tagParts, "unique")
		} else if col.IsIndex {
			tagParts = append(tagParts, "index")
		}

		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 && fk.Columns[0] == col.Name {
				tagParts = append(tagParts, "m2o")
				break
			}
		}

		tagStr := ""
		if len(tagParts) > 0 {
			tagStr = fmt.Sprintf(" `goe:\"%s\"`", strings.Join(tagParts, ";"))
		}
		fmt.Fprintf(buf, "\t%s %s%s\n", fieldName, goType, tagStr)
	}

	for _, fk := range info.ForeignKeys {
		if len(fk.Columns) == 1 {
			refStructName := ToCamelCase(fk.ReferencedTable)
			refStructName = TrimShortPrefix(refStructName, prefix)
			refStructName = ToSingular(refStructName)
			fmt.Fprintf(buf, "\t%s *%s\n", refStructName, refStructName)
		}
	}

	for _, f := range o2oFields {
		fmt.Fprintf(buf, "\t%s *%s `goe:\"o2o\"`\n", f, f)
	}

	for _, f := range o2mFields {
		fmt.Fprintf(buf, "\t%s []*%s `goe:\"o2m\"`\n", ToPlural(f), f)
	}

	for _, f := range m2mFields {
		fmt.Fprintf(buf, "\t%s []*%s `goe:\"m2m\"` // via %s\n", ToPlural(f.fieldName), f.fieldName, f.junctionName)
	}

	fmt.Fprintf(buf, "}\n\n")

	fmt.Fprintf(buf, "// TableName returns the database table name\n")
	fmt.Fprintf(buf, "func (*%s) TableName() string {\n", structName)
	fmt.Fprintf(buf, "\treturn \"%s\"\n", tableName)
	fmt.Fprintf(buf, "}\n")

	return nil
}

// findJunctionTables identifies junction tables (m2m relationship tables)
func findJunctionTables(tables map[string]*TableInfo) map[string][]string {
	result := make(map[string][]string)

	for tableName, info := range tables {
		if len(info.ForeignKeys) == 2 && len(info.Columns) <= 4 {
			var referencedTables []string
			for _, fk := range info.ForeignKeys {
				if len(fk.Columns) == 1 {
					referencedTables = append(referencedTables, fk.ReferencedTable)
				}
			}
			if len(referencedTables) == 2 {
				result[tableName] = referencedTables
			}
		}
	}
	return result
}

// GenerateModelsFile generates a complete Go file with all models
func GenerateModelsFile(buf *bytes.Buffer, tables map[string]*TableInfo,
	pkgName, schemaName, dbType, filterPrefix string) error {

	fmt.Fprintf(buf, "// Code generated by goent-gen. DO NOT EDIT.\n")
	fmt.Fprintf(buf, "//\n")
	fmt.Fprintf(buf, "//go:build !ignore_autogenerated\n")
	fmt.Fprintf(buf, "//go:generate goent-gen tables .\n\n")
	fmt.Fprintf(buf, "package %s\n\n", pkgName)

	importPkgs := NewImports()
	importPkgs.AddThirdPackage("\"github.com/azhai/goent\"")
	if dbType == "pgsql" {
		importPkgs.AddThirdPackage("\"github.com/azhai/goent/drivers/pgsql\"")
	} else {
		importPkgs.AddThirdPackage("\"github.com/azhai/goent/drivers/sqlite\"")
	}

	junctionTables := findJunctionTables(tables)
	for _, info := range tables {
		for _, col := range info.Columns {
			goType := MapSQLTypeToGo(col.DataType, dbType)
			if goType == "time.Time" {
				importPkgs.AddStdPackage("\"time\"")
			}
			if goType == "uuid.UUID" {
				importPkgs.AddThirdPackage("\"github.com/google/uuid\"")
			}
			if goType == "json.RawMessage" {
				importPkgs.AddStdPackage("\"encoding/json\"")
			}
		}
	}
	importPkgs.WriteTo(buf)

	prefix := extractCommonPrefix(tables)
	if filterPrefix != "" {
		prefix = filterPrefix
	}
	schemaStructName := ToCamelCase(schemaName) + "Schema"
	schemaTag := ""
	if schemaName != "" || prefix != "" {
		parts := make([]string, 0, 2)
		if schemaName != "" {
			parts = append(parts, schemaName)
		}
		if prefix != "" {
			parts = append(parts, fmt.Sprintf("prefix:%s", prefix))
		}
		schemaTag = fmt.Sprintf(" `goe:\"%s\"`", strings.Join(parts, ";"))
	}

	fmt.Fprintf(buf, "// Connect opens a database connection.\n")
	fmt.Fprintf(buf, "func Connect(dbDSN, logFile string) (*Database, error) {\n")
	driverPkg := "pgsql"
	if dbType == "sqlite" {
		driverPkg = "sqlite"
	}
	fmt.Fprintf(buf, "\treturn goent.Open[Database](%s.OpenDSN(dbDSN), logFile)\n", driverPkg)
	fmt.Fprintf(buf, "}\n\n")

	fmt.Fprintf(buf, "// Database represents the database connection.\n")
	fmt.Fprintf(buf, "type Database struct {\n")
	fmt.Fprintf(buf, "\t%s%s\n", schemaStructName, schemaTag)
	fmt.Fprintf(buf, "\t*goent.DB\n")
	fmt.Fprintf(buf, "}\n\n")

	fmt.Fprintf(buf, "// %s is the %s schema of the database.\n", schemaStructName, schemaName)
	fmt.Fprintf(buf, "type %s struct {\n", schemaStructName)
	for _, tableName := range sortedKeys(tables) {
		fieldName := ToCamelCase(tableName)
		fieldName = TrimShortPrefix(fieldName, prefix)
		fieldName = ToSingular(fieldName)
		fmt.Fprintf(buf, "\t%s *goent.Table[%s]\n", fieldName, fieldName)
	}
	fmt.Fprintf(buf, "}\n\n")

	for _, tableName := range sortedKeys(tables) {
		info := tables[tableName]
		err := GenerateModel(buf, info, tables, junctionTables, dbType, prefix)
		if err != nil {
			return err
		}
	}
	return nil
}

func extractCommonPrefix(tables map[string]*TableInfo) string {
	if len(tables) == 0 {
		return ""
	}

	var names []string
	for name := range tables {
		names = append(names, name)
	}

	if len(names) == 1 {
		return ""
	}

	prefixCounts := make(map[string]int)
	for _, name := range names {
		for i := 2; i <= len(name); i++ {
			if name[i-1] == '_' {
				prefix := name[:i]
				prefixCounts[prefix]++
			}
		}
	}

	var bestPrefix string
	var bestCount int
	for prefix, count := range prefixCounts {
		if count > bestCount && count >= len(names)/2 {
			bestPrefix = prefix
			bestCount = count
		}
	}

	if bestPrefix == "" || len(bestPrefix) < 2 {
		return ""
	}

	return bestPrefix
}

func sortedKeys(m map[string]*TableInfo) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

// RunTablesGeneration generates models from database tables
func RunTablesGeneration(cfg *ReverseConfig, outputPath string) error {
	var (
		err    error
		buf    bytes.Buffer
		engine *ReverseEngineer
		dsn    = cfg.DatabaseDSN
		dbType = "sqlite"
	)

	if strings.HasPrefix(dsn, "postgres://") || strings.Contains(dsn, "user=") ||
		strings.Contains(dsn, "password=") || strings.Contains(dsn, "database=") {
		dbType = "pgsql"
		engine, err = NewPgsqlReverseEngineer(dsn, cfg.SchemaName)
	} else {
		dbType = "sqlite"
		engine, err = NewSqliteReverseEngineer(dsn)
	}
	if err != nil {
		return err
	}
	defer engine.Close()

	tableNames, err := engine.GetTables(cfg.TablePrefix)
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	tables := make(map[string]*TableInfo)
	for _, name := range tableNames {
		if cfg.TablePrefix != "" && !strings.HasPrefix(name, cfg.TablePrefix) {
			continue
		}
		info, err := engine.GetTableInfo(name)
		if err != nil {
			return fmt.Errorf("failed to get table info for %s: %w", name, err)
		}
		tables[name] = info
	}

	pkgName := filepath.Base(filepath.Dir(outputPath))
	err = GenerateModelsFile(&buf, tables, pkgName, cfg.SchemaName, dbType, cfg.TablePrefix)
	if err != nil {
		return fmt.Errorf("failed to generate models: %w", err)
	} else if buf.Len() == 0 {
		return fmt.Errorf("failed to generate models: empty buffer")
	}

	if err := WriteToFile(&buf, outputPath); err != nil {
		return err
	}
	fmt.Printf("Generated: %s (driver: %s)\n", outputPath, dbType)
	return nil
}
