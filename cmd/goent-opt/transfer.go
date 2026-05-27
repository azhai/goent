package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

func (w *TableWork) exportSchemaSQL() (string, error) {
	ts, err := w.SchemaOps().GetTableSchema(w.ctx, w.Table)
	if err != nil {
		return "", err
	}

	var lines []string
	var colDefs []string
	for _, col := range ts.Columns {
		def := fmt.Sprintf("  %s %s", quote(col.Name), col.DataType)
		if !col.Nullable {
			def += " NOT NULL"
		}
		if col.Default != "" && !strings.HasPrefix(col.Default, "nextval(") {
			def += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		colDefs = append(colDefs, def)
	}
	if len(ts.PK) > 0 {
		colDefs = append(colDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(ts.PK, ", ")))
	}
	for _, fk := range ts.FKs {
		colDefs = append(colDefs, fmt.Sprintf("  FOREIGN KEY (%s) REFERENCES %s(%s)",
			strings.Join(fk.Columns, ", "), quote(fk.RefTable), strings.Join(fk.RefColumns, ", ")))
	}

	lines = append(lines, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		quote(w.Table), strings.Join(colDefs, ",\n")))

	for _, idx := range ts.Indexes {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		lines = append(lines, fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);",
			unique, quote(idx.Name), quote(w.Table), strings.Join(idx.Columns, ", ")))
	}

	return strings.Join(lines, "\n\n"), nil
}

func exportData(ctx context.Context, cfg DBConfig, table, dataPath string) (int64, error) {
	db := OpenDriverSQL(cfg)
	defer db.Close()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", quote(table)))
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
		values := make([]any, len(colTypes))
		valuePtrs := make([]any, len(colTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return count, err
		}

		record := make(map[string]any)
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

func (w *TableWork) importSchema(schemaPath string) error {
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}
	return w.importSchemaSQL(string(data))
}

func (w *TableWork) importSchemaSQL(sqlContent string) error {
	_ = w.SchemaOps().DB.Driver().DropTable("", w.Table)
	for _, stmt := range strings.Split(sqlContent, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := w.db.RawExecContext(w.ctx, stmt); err != nil {
			return fmt.Errorf("executing SQL: %w", err)
		}
	}
	return nil
}

func (w *TableWork) importData(cfg DBConfig, dataPath string) (int64, error) {
	isPg := cfg.IsPg
	f, err := os.Open(dataPath)
	if err != nil {
		return 0, fmt.Errorf("opening data file: %w", err)
	}
	defer f.Close()

	colTypes, err := getTableColumnTypes(w.ctx, cfg, w.Table)
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
		quote(w.Table),
		strings.Join(colTypes, ", "),
		strings.Join(placeholders, ", "))

	reader := bufio.NewReader(f)
	var count int64
	var batch []map[string]any
	batchSize := 500

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line != "" {
			var record map[string]any
			if jsonErr := json.Unmarshal([]byte(line), &record); jsonErr != nil {
				fmt.Printf("    Warning: skipping invalid JSON line: %v\n", jsonErr)
				continue
			}
			batch = append(batch, record)
		}

		if len(batch) >= batchSize || (err != nil && len(batch) > 0) {
			tx, txErr := w.BeginTx()
			if txErr != nil {
				return count, txErr
			}
			w.WithTx(tx)
			if insertErr := w.insertBatch(insertSQL, colTypes, batch); insertErr != nil {
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

func (w *TableWork) insertBatch(insertSQL string, colTypes []string, batch []map[string]any) error {
	for _, record := range batch {
		values := make([]any, len(colTypes))
		for i, col := range colTypes {
			val, ok := record[col]
			if !ok {
				values[i] = nil
			} else {
				values[i] = convertValue(val)
			}
		}
		if err := w.TxExec(insertSQL, values...); err != nil {
			w.tx.Rollback()
			return fmt.Errorf("inserting row: %w", err)
		}
	}

	return w.tx.Commit()
}

func (w *TableWork) resetImportSequence() {
	ops := w.SchemaOps()
	seqName := w.Table + "_id_seq"
	seqExists, err := ops.SequenceExists(w.ctx, seqName)
	if err != nil || !seqExists {
		return
	}
	var maxID sql.NullInt64
	row2 := w.db.RawQueryRowContext(w.ctx, fmt.Sprintf("SELECT MAX(id) FROM %s", quote(w.Table)))
	row2.Scan(&maxID)
	nextVal := int64(1)
	if maxID.Valid {
		nextVal = maxID.Int64 + 1
	}
	if err := ops.ResetSequence(w.ctx, seqName, nextVal); err != nil {
		fmt.Printf("    Warning: failed to reset sequence: %v\n", err)
		return
	}
	fmt.Printf("    Reset sequence %s to %d\n", seqName, nextVal)
}

func getTableColumnTypes(ctx context.Context, cfg DBConfig, table string) ([]string, error) {
	db := OpenDriverSQL(cfg)
	defer db.Close()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", quote(table)))
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

func convertValue(val any) any {
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
	case map[string]any, []any:
		data, _ := json.Marshal(v)
		return string(data)
	default:
		return v
	}
}
