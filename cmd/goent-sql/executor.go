package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// Executor handles SQL execution and result formatting
type Executor struct {
	db     *sql.DB
	driver string
	isPg   bool
}

// NewExecutor creates a new SQL executor
func NewExecutor(cfg DBConfig) (*Executor, error) {
	driverName := "sqlite"
	if cfg.IsPg {
		driverName = "pgx"
	}
	db, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return &Executor{db: db, driver: cfg.Driver, isPg: cfg.IsPg}, nil
}

// Close closes the database connection
func (e *Executor) Close() error {
	return e.db.Close()
}

// Reconnect switches to a new database connection
func (e *Executor) Reconnect(cfg DBConfig) error {
	if err := e.Close(); err != nil {
		return err
	}
	newEx, err := NewExecutor(cfg)
	if err != nil {
		return err
	}
	*e = *newEx
	return nil
}

// QueryResult holds the result of a SQL query
type QueryResult struct {
	Columns []string
	Rows    []map[string]any
	Count   int
	Elapsed time.Duration
}

// ExecSQL executes a SQL statement and returns the result
func (e *Executor) ExecSQL(query string, args ...any) (*QueryResult, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}

	upper := strings.ToUpper(query)
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "TABLE")

	if isSelect {
		return e.executeQuery(query, args...)
	}
	return e.executeModify(query, args...)
}

func (e *Executor) executeQuery(query string, args ...any) (*QueryResult, error) {
	start := time.Now()
	rows, err := e.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &QueryResult{Columns: cols}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = convertValue(values[i])
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result.Count = len(result.Rows)
	result.Elapsed = time.Since(start)
	return result, nil
}

func (e *Executor) executeModify(query string, args ...any) (*QueryResult, error) {
	start := time.Now()
	res, err := e.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	affected, _ := res.RowsAffected()
	result := &QueryResult{
		Count:   int(affected),
		Elapsed: time.Since(start),
	}
	return result, nil
}

// PrintResult formats and prints a QueryResult to stdout
func PrintResult(result *QueryResult) {
	if result == nil {
		return
	}
	if len(result.Columns) == 0 {
		// Modify statement
		fmt.Printf("(%d row(s) affected) [%s]\n", result.Count, formatDuration(result.Elapsed))
		return
	}
	if len(result.Rows) == 0 {
		fmt.Println("(0 rows)")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	// Header
	fmt.Fprintln(w, strings.Join(result.Columns, "\t"))
	// Separator
	seps := make([]string, len(result.Columns))
	for i := range seps {
		seps[i] = "---"
	}
	fmt.Fprintln(w, strings.Join(seps, "\t"))
	// Rows
	for _, row := range result.Rows {
		vals := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			vals[i] = fmtVal(row[col])
		}
		fmt.Fprintln(w, strings.Join(vals, "\t"))
	}
	w.Flush()
	fmt.Printf("(%d row(s)) [%s]\n", result.Count, formatDuration(result.Elapsed))
}

// PrintResultTo formats and prints a QueryResult to the given writer
func PrintResultTo(w io.Writer, result *QueryResult) {
	if result == nil || len(result.Columns) == 0 || len(result.Rows) == 0 {
		return
	}
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(result.Columns, "\t"))
	for _, row := range result.Rows {
		vals := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			vals[i] = fmtVal(row[col])
		}
		fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}
	tw.Flush()
}

// ListTables returns a list of table names in the current schema
func (e *Executor) ListTables(schema string) ([]string, error) {
	var query string
	var args []any
	if e.isPg {
		query = "SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename"
		args = []any{schema}
	} else {
		query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	}
	rows, err := e.db.Query(query, args...)
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
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// DescribeTable returns column info for a table
func (e *Executor) DescribeTable(table string, schema string) (*QueryResult, error) {
	if e.isPg {
		query := `SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position`
		return e.executeQuery(query, schema, table)
	}
	query := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table, e.isPg))
	return e.executeQuery(query)
}

// GetDBName extracts a short database name from DSN for the prompt
func (e *Executor) GetDBName(dsn string) string {
	if e.isPg {
		// Extract database name from postgres DSN
		parts := strings.Split(dsn, "/")
		if len(parts) > 0 {
			last := parts[len(parts)-1]
			last = strings.TrimSuffix(last, "?sslmode=disable")
			last = strings.Split(last, "?")[0]
			return last
		}
	}
	return dsn
}

func convertValue(v any) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return val
	}
}

func fmtVal(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dμs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func quoteIdent(name string, isPg bool) string {
	if isPg {
		return `"` + name + `"`
	}
	return "`" + name + "`"
}
