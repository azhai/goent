package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/azhai/goent"
)

func runDBTransit(args []string) {
	if len(args) < 1 {
		printDBTransitUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "export":
		runExport(args[1:])
	case "import":
		runImport(args[1:])
	case "--help", "-h":
		printDBTransitUsage()
	default:
		fmt.Printf("Unknown sub-command: %s\n\n", args[0])
		printDBTransitUsage()
		os.Exit(1)
	}
}

func printDBTransitUsage() {
	fmt.Println("Usage: goent-tools db-transit <export|import> [options] [dsn] [tables...]")
	fmt.Println()
	fmt.Println("Import/export table structure and data in JSON Lines format.")
	printDSNHelp()
	fmt.Println()
	fmt.Println("Sub-commands:")
	fmt.Println("  export    Export table schema and data to files")
	fmt.Println("  import    Import table schema and data from files")
	fmt.Println()
	fmt.Println("Export options:")
	fmt.Println("  --dir <path>       Output directory (default: ./export)")
	fmt.Println("  --schema-only      Export only table structure, no data")
	fmt.Println("  --data-only        Export only data, no table structure")
	fmt.Println()
	fmt.Println("Import options:")
	fmt.Println("  --dir <path>       Input directory (default: ./export)")
	fmt.Println("  --schema-only      Import only table structure, no data")
	fmt.Println("  --data-only        Import only data, no table structure")
	fmt.Println("  --truncate         Truncate tables before importing data")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  goent-tools db-transit export 'postgres://user:pass@localhost/db?sslmode=disable'")
	fmt.Println("  goent-tools db-transit export --dir ./backup 'postgres://...' comment issue")
	fmt.Println("  goent-tools db-transit export --schema-only 'sqlite.db'")
	fmt.Println("  goent-tools db-transit import --dir ./backup 'postgres://...'")
	fmt.Println("  goent-tools db-transit import --truncate 'postgres://...' comment")
	fmt.Println("  DB_DSN='postgres://...' goent-tools db-transit export comment issue")
	fmt.Println()
	fmt.Println("Output format:")
	fmt.Println("  <dir>/<table>.schema.json   - Table structure (columns, types, indexes, FKs)")
	fmt.Println("  <dir>/<table>.data.jsonl    - Table data, one JSON object per line")
}

type schemaInfo struct {
	Table    string        `json:"table"`
	Columns  []colDef      `json:"columns"`
	Indexes  []idxDef      `json:"indexes,omitempty"`
	FKs      []fkDef       `json:"foreign_keys,omitempty"`
	PK       []string      `json:"primary_key,omitempty"`
	Uniques  [][]string    `json:"uniques,omitempty"`
}

type colDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default,omitempty"`
}

type idxDef struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

type fkDef struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefTable   string   `json:"referenced_table"`
	RefColumns []string `json:"referenced_columns"`
}

func runExport(args []string) {
	var (
		dir        string
		schemaOnly bool
		dataOnly   bool
	)

	filtered := parseExportArgs(args, &dir, &schemaOnly, &dataOnly)

	var cliDSN string
	var tables []string
	if len(filtered) >= 1 && isLikelyDSN(filtered[0]) {
		cliDSN = filtered[0]
		tables = filtered[1:]
	} else {
		tables = filtered
	}

	cfg, err := ParseDSNArgs(cliDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err.Error())
		printDBTransitUsage()
		os.Exit(1)
	}

	tdb, err := OpenToolsDB(cfg)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer CloseDB(tdb)

	ctx := context.Background()

	if len(tables) == 0 {
		var err error
		tables, err = goent.ListTables(ctx, tdb.DB, cfg.IsPg)
		if err != nil {
			fmt.Printf("Error listing tables: %v\n", err)
			os.Exit(1)
		}
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("Error creating directory %s: %v\n", dir, err)
		os.Exit(1)
	}

	fmt.Printf("Exporting to %s/\n", dir)

	for _, table := range tables {
		fmt.Printf("  Exporting table: %s\n", table)

		if !dataOnly {
			schema, err := exportSchema(ctx, tdb, table, cfg.IsPg)
			if err != nil {
				fmt.Printf("    Error exporting schema: %v\n", err)
				continue
			}
			schemaPath := filepath.Join(dir, table+".schema.json")
			data, _ := json.MarshalIndent(schema, "", "  ")
			if err := os.WriteFile(schemaPath, data, 0644); err != nil {
				fmt.Printf("    Error writing schema: %v\n", err)
				continue
			}
			fmt.Printf("    Schema -> %s (%d columns)\n", schemaPath, len(schema.Columns))
		}

		if !schemaOnly {
			dataPath := filepath.Join(dir, table+".data.jsonl")
			count, err := exportData(ctx, tdb, table, dataPath)
			if err != nil {
				fmt.Printf("    Error exporting data: %v\n", err)
				continue
			}
			fmt.Printf("    Data   -> %s (%d rows)\n", dataPath, count)
		}
	}

	fmt.Println("Export complete!")
}

func runImport(args []string) {
	var (
		dir        string
		schemaOnly bool
		dataOnly   bool
		truncate   bool
	)

	filtered := parseImportArgs(args, &dir, &schemaOnly, &dataOnly, &truncate)

	var cliDSN string
	var tables []string
	if len(filtered) >= 1 && isLikelyDSN(filtered[0]) {
		cliDSN = filtered[0]
		tables = filtered[1:]
	} else {
		tables = filtered
	}

	cfg, err := ParseDSNArgs(cliDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err.Error())
		printDBTransitUsage()
		os.Exit(1)
	}

	tdb, err := OpenToolsDB(cfg)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer CloseDB(tdb)

	ctx := context.Background()

	if len(tables) == 0 {
		entries, err := os.ReadDir(dir)
		if err != nil {
			fmt.Printf("Error reading directory %s: %v\n", dir, err)
			os.Exit(1)
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".schema.json") {
				tableName := strings.TrimSuffix(entry.Name(), ".schema.json")
				tables = append(tables, tableName)
			}
		}
	}

	fmt.Printf("Importing from %s/\n", dir)

	for _, table := range tables {
		fmt.Printf("  Importing table: %s\n", table)

		if !dataOnly {
			schemaPath := filepath.Join(dir, table+".schema.json")
			if err := importSchema(ctx, tdb, table, schemaPath, cfg.IsPg); err != nil {
				fmt.Printf("    Error importing schema: %v\n", err)
				continue
			}
			fmt.Printf("    Schema imported from %s\n", schemaPath)
		}

		if !schemaOnly {
			if truncate {
				if err := tdb.RawExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", q(table))); err != nil {
					if err2 := tdb.RawExecContext(ctx, fmt.Sprintf("DELETE FROM %s", q(table))); err2 != nil {
						fmt.Printf("    Warning: could not truncate: %v\n", err)
					}
				} else {
					fmt.Printf("    Truncated table %s\n", table)
				}
			}

			dataPath := filepath.Join(dir, table+".data.jsonl")
			count, err := importData(ctx, tdb, table, dataPath, cfg.IsPg)
			if err != nil {
				fmt.Printf("    Error importing data: %v\n", err)
				continue
			}
			fmt.Printf("    Data imported from %s (%d rows)\n", dataPath, count)
		}
	}

	if cfg.IsPg {
		if len(tables) > 0 {
			_ = tdb.RawExecContext(ctx, "SELECT setval(pg_get_serial_sequence(t.relname, 'id'), COALESCE((SELECT MAX(id) FROM "+q(tables[0])+"), 0) + 1, false) FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = 'public' AND t.relkind = 'r'")
		}
		for _, table := range tables {
			resetImportSequence(ctx, tdb, table)
		}
	}

	fmt.Println("Import complete!")
}

func resetImportSequence(ctx context.Context, tdb *ToolsDB, table string) {
	seqName := table + "_id_seq"
	seqExists, err := goent.SequenceExists(ctx, tdb.DB, seqName)
	if err != nil || !seqExists {
		return
	}
	var maxID sql.NullInt64
	row2 := tdb.RawQueryRowContext(ctx, fmt.Sprintf("SELECT MAX(id) FROM %s", q(table)))
	row2.Scan(&maxID)
	nextVal := int64(1)
	if maxID.Valid {
		nextVal = maxID.Int64 + 1
	}
	_ = tdb.RawExecContext(ctx, fmt.Sprintf("SELECT setval('%s', %d, false)", seqName, nextVal))
	fmt.Printf("    Reset sequence %s to %d\n", seqName, nextVal)
}

func parseExportArgs(args []string, dir *string, schemaOnly, dataOnly *bool) []string {
	*dir = "./export"
	var filtered []string
	skip := false
	for i, arg := range args {
		if skip {
			skip = false
			continue
		}
		switch arg {
		case "--dir":
			if i+1 < len(args) {
				*dir = args[i+1]
				skip = true
			}
		case "--schema-only":
			*schemaOnly = true
		case "--data-only":
			*dataOnly = true
		default:
			filtered = append(filtered, arg)
		}
	}
	return filtered
}

func parseImportArgs(args []string, dir *string, schemaOnly, dataOnly, truncate *bool) []string {
	*dir = "./export"
	var filtered []string
	skip := false
	for i, arg := range args {
		if skip {
			skip = false
			continue
		}
		switch arg {
		case "--dir":
			if i+1 < len(args) {
				*dir = args[i+1]
				skip = true
			}
		case "--schema-only":
			*schemaOnly = true
		case "--data-only":
			*dataOnly = true
		case "--truncate":
			*truncate = true
		default:
			filtered = append(filtered, arg)
		}
	}
	return filtered
}

func exportSchema(ctx context.Context, tdb *ToolsDB, table string, isPg bool) (*schemaInfo, error) {
	info := &schemaInfo{Table: table}

	if isPg {
		return exportPgSchema(ctx, tdb, table, info)
	}
	return exportSQLiteSchema(ctx, tdb, table, info)
}

func exportPgSchema(ctx context.Context, tdb *ToolsDB, table string, info *schemaInfo) (*schemaInfo, error) {
	colRows, err := tdb.RawQueryContext(ctx, `
		SELECT column_name, data_type, is_nullable = 'YES', COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	for colRows.Next() {
		var col colDef
		if err := colRows.Scan(&col.Name, &col.Type, &col.Nullable, &col.Default); err != nil {
			colRows.Close()
			return nil, err
		}
		if strings.HasPrefix(col.Default, "nextval(") {
			col.Default = "auto_increment"
		}
		info.Columns = append(info.Columns, col)
	}
	colRows.Close()

	idxRows, err := tdb.RawQueryContext(ctx, `
		SELECT i.relname, a.attname, ix.indisunique, ix.indisprimary
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = 'public' AND t.relname = $1
		ORDER BY i.relname, a.attnum`, table)
	if err != nil {
		return nil, err
	}
	idxMap := make(map[string]*idxDef)
	pkMap := make(map[string][]string)
	for idxRows.Next() {
		var idxName, colName string
		var isUnique, isPrimary bool
		if err := idxRows.Scan(&idxName, &colName, &isUnique, &isPrimary); err != nil {
			idxRows.Close()
			return nil, err
		}
		if isPrimary {
			pkMap[idxName] = append(pkMap[idxName], colName)
			continue
		}
		if _, ok := idxMap[idxName]; !ok {
			idxMap[idxName] = &idxDef{Name: idxName, Unique: isUnique}
		}
		idxMap[idxName].Columns = append(idxMap[idxName].Columns, colName)
	}
	idxRows.Close()

	for _, cols := range pkMap {
		info.PK = cols
		break
	}
	for _, idx := range idxMap {
		info.Indexes = append(info.Indexes, *idx)
	}

	fkRows, err := tdb.RawQueryContext(ctx, `
		SELECT tc.constraint_name, kcu.column_name, ccu.table_name, ccu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage ccu
			ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = 'public' AND tc.table_name = $1
		ORDER BY tc.constraint_name, kcu.ordinal_position`, table)
	if err != nil {
		return info, nil
	}
	fkMap := make(map[string]*fkDef)
	for fkRows.Next() {
		var constraintName, columnName, refTable, refColumn string
		if err := fkRows.Scan(&constraintName, &columnName, &refTable, &refColumn); err != nil {
			fkRows.Close()
			return info, nil
		}
		if _, ok := fkMap[constraintName]; !ok {
			fkMap[constraintName] = &fkDef{Name: constraintName, RefTable: refTable}
		}
		fkMap[constraintName].Columns = append(fkMap[constraintName].Columns, columnName)
		fkMap[constraintName].RefColumns = append(fkMap[constraintName].RefColumns, refColumn)
	}
	fkRows.Close()
	for _, fk := range fkMap {
		info.FKs = append(info.FKs, *fk)
	}

	return info, nil
}

func exportSQLiteSchema(ctx context.Context, tdb *ToolsDB, table string, info *schemaInfo) (*schemaInfo, error) {
	colRows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	for colRows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := colRows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk); err != nil {
			colRows.Close()
			return nil, err
		}
		col := colDef{
			Name:     name,
			Type:     typ,
			Nullable: notNull == 0,
		}
		if dfltValue.Valid {
			col.Default = dfltValue.String
		}
		if pk > 0 {
			info.PK = append(info.PK, name)
		}
		info.Columns = append(info.Columns, col)
	}
	colRows.Close()

	idxRows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", table))
	if err != nil {
		return info, nil
	}
	type idxMeta struct {
		seq     int
		name    string
		unique  bool
		origin  string
		partial int
	}
	var idxMetas []idxMeta
	for idxRows.Next() {
		var m idxMeta
		if err := idxRows.Scan(&m.seq, &m.name, &m.unique, &m.origin, &m.partial); err != nil {
			idxRows.Close()
			return info, nil
		}
		if m.origin == "c" {
			continue
		}
		idxMetas = append(idxMetas, m)
	}
	idxRows.Close()

	for _, m := range idxMetas {
		colInfoRows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("PRAGMA index_info(%s)", m.name))
		if err != nil {
			continue
		}
		var columns []string
		for colInfoRows.Next() {
			var seqno, cid int
			var colName sql.NullString
			if err := colInfoRows.Scan(&seqno, &cid, &colName); err != nil {
				colInfoRows.Close()
				continue
			}
			if colName.Valid {
				columns = append(columns, colName.String)
			}
		}
		colInfoRows.Close()
		if len(columns) > 0 {
			info.Indexes = append(info.Indexes, idxDef{
				Name:    m.name,
				Columns: columns,
				Unique:  m.unique,
			})
		}
	}

	fkRows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", table))
	if err != nil {
		return info, nil
	}
	fkMap := make(map[int]*fkDef)
	for fkRows.Next() {
		var id, seq int
		var refTable, from, to string
		var onUpdate, onDelete, match string
		if err := fkRows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			fkRows.Close()
			return info, nil
		}
		if _, ok := fkMap[id]; !ok {
			fkMap[id] = &fkDef{Name: fmt.Sprintf("fk_%d", id), RefTable: refTable}
		}
		fkMap[id].Columns = append(fkMap[id].Columns, from)
		fkMap[id].RefColumns = append(fkMap[id].RefColumns, to)
	}
	fkRows.Close()
	for _, fk := range fkMap {
		info.FKs = append(info.FKs, *fk)
	}

	return info, nil
}

func exportData(ctx context.Context, tdb *ToolsDB, table, dataPath string) (int64, error) {
	db := tdb.DriverSQL()
	defer db.Close()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", q(table)))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}

	f, err := os.Create(dataPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	defer writer.Flush()

	var count int64
	for rows.Next() {
		values := make([]interface{}, len(colTypes))
		valuePtrs := make([]interface{}, len(colTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return count, err
		}

		record := make(map[string]interface{})
		for i, ct := range colTypes {
			val := values[i]
			switch v := val.(type) {
			case []byte:
				var s string
				if err := json.Unmarshal(v, &s); err == nil {
					record[ct.Name()] = s
				} else {
					record[ct.Name()] = string(v)
				}
			default:
				record[ct.Name()] = v
			}
		}

		data, err := json.Marshal(record)
		if err != nil {
			return count, err
		}
		writer.Write(data)
		writer.WriteByte('\n')
		count++
	}

	return count, nil
}

func importSchema(ctx context.Context, tdb *ToolsDB, table, schemaPath string, isPg bool) error {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}

	var schema schemaInfo
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("parsing schema: %w", err)
	}

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", q(table))
	if !isPg {
		dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s", q(table))
	}
	_ = tdb.RawExecContext(ctx, dropSQL)

	var colDefs []string
	for _, col := range schema.Columns {
		def := fmt.Sprintf("%s %s", col.Name, col.Type)
		if !col.Nullable {
			def += " NOT NULL"
		}
		if col.Default != "" && col.Default != "auto_increment" {
			def += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		colDefs = append(colDefs, def)
	}

	if len(schema.PK) > 0 {
		colDefs = append(colDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(schema.PK, ", ")))
	}

	for _, fk := range schema.FKs {
		colDefs = append(colDefs, fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
			strings.Join(fk.Columns, ", "), fk.RefTable, strings.Join(fk.RefColumns, ", ")))
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", q(table), strings.Join(colDefs, ",\n  "))
	if err := tdb.RawExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	for _, idx := range schema.Indexes {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		idxSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
			unique, idx.Name, q(table), strings.Join(idx.Columns, ", "))
		if err := tdb.RawExecContext(ctx, idxSQL); err != nil {
			fmt.Printf("    Warning: could not create index %s: %v\n", idx.Name, err)
		}
	}

	return nil
}

func importData(ctx context.Context, tdb *ToolsDB, table, dataPath string, isPg bool) (int64, error) {
	f, err := os.Open(dataPath)
	if err != nil {
		return 0, fmt.Errorf("opening data file: %w", err)
	}
	defer f.Close()

	colTypes, err := getTableColumnTypes(ctx, tdb, table)
	if err != nil {
		return 0, err
	}

	placeholders := make([]string, len(colTypes))
	for i := range placeholders {
		if len(colTypes) > 0 && isPg {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		} else {
			placeholders[i] = "?"
		}
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		q(table),
		strings.Join(colTypes, ", "),
		strings.Join(placeholders, ", "))

	reader := bufio.NewReader(f)
	var count int64
	var batch []map[string]interface{}
	batchSize := 500

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line != "" {
			var record map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(line), &record); jsonErr != nil {
				fmt.Printf("    Warning: skipping invalid JSON line: %v\n", jsonErr)
				continue
			}
			batch = append(batch, record)
		}

		if len(batch) >= batchSize || (err != nil && len(batch) > 0) {
			if insertErr := insertBatch(ctx, tdb, insertSQL, colTypes, batch, isPg); insertErr != nil {
				return count, insertErr
			}
			count += int64(len(batch))
			batch = batch[:0]
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}
	}

	return count, nil
}

func getTableColumnTypes(ctx context.Context, tdb *ToolsDB, table string) ([]string, error) {
	db := tdb.DriverSQL()
	defer db.Close()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", q(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var names []string
	for _, ct := range colTypes {
		names = append(names, ct.Name())
	}
	return names, nil
}

func insertBatch(ctx context.Context, tdb *ToolsDB, insertSQL string, colTypes []string, batch []map[string]interface{}, isPg bool) error {
	tx, err := tdb.BeginTx(ctx)
	if err != nil {
		return err
	}

	for _, record := range batch {
		values := make([]interface{}, len(colTypes))
		for i, col := range colTypes {
			val, ok := record[col]
			if !ok {
				values[i] = nil
			} else {
				values[i] = convertValue(val)
			}
		}
		if isPg {
			if err := TxExec(ctx, tx, insertSQL, values...); err != nil {
				tx.Rollback()
				return fmt.Errorf("inserting row: %w", err)
			}
		} else {
			if err := TxExec(ctx, tx, insertSQL, values...); err != nil {
				tx.Rollback()
				return fmt.Errorf("inserting row: %w", err)
			}
		}
	}

	return tx.Commit()
}

func convertValue(val interface{}) interface{} {
	switch v := val.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case float64:
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	case map[string]interface{}, []interface{}:
		data, _ := json.Marshal(v)
		return string(data)
	default:
		return v
	}
}
