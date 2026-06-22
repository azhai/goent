package backup

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func pgTool(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("%s not found in PATH", name)
	}
	return path, nil
}

func (e *Engine) createPostgresArchive(ctx context.Context, tw *tar.Writer, backupType string, since *time.Time) (int64, error) {
	tmpDir, err := os.MkdirTemp("", "goent-pg-backup-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tmpDir)

	if backupType == "full" {
		if err := e.createPostgresFullBackup(tmpDir); err != nil {
			return 0, err
		}
	} else {
		var baseTime time.Time
		if since != nil {
			baseTime = *since
		}
		if err := e.createPostgresIncrementalBackup(tmpDir, baseTime); err != nil {
			return 0, err
		}
	}

	return addDirToTar(tw, tmpDir, "")
}

func (e *Engine) createPostgresFullBackup(dir string) error {
	pgDump, err := pgTool("pg_dump")
	if err != nil {
		return err
	}
	psql, err := pgTool("psql")
	if err != nil {
		return err
	}

	schemaFile := filepath.Join(dir, "schema.sql")
	cmd := exec.Command(pgDump, "--dbname", e.cfg.DSN, "--schema-only", "--clean", "--if-exists", "--schema", e.cfg.schema(), "--file", schemaFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_dump schema failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	tables, err := e.pgTables(psql)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if e.cfg.isLogTable(table) {
			continue
		}
		csvFile := filepath.Join(dir, table+".csv")
		out, err := os.Create(csvFile)
		if err != nil {
			return err
		}
		copyCmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet",
			"--command", fmt.Sprintf("COPY %s.%s TO STDOUT WITH (FORMAT csv, HEADER true)", e.cfg.schema(), table))
		copyCmd.Stdout = out
		if err := copyCmd.Run(); err != nil {
			out.Close()
			return fmt.Errorf("COPY %s failed: %w", table, err)
		}
		out.Close()
	}
	return nil
}

func (e *Engine) createPostgresIncrementalBackup(dir string, since time.Time) error {
	psql, err := pgTool("psql")
	if err != nil {
		return err
	}

	sinceStr := since.UTC().Format(time.RFC3339Nano)

	jsonlFile := filepath.Join(dir, "incremental.jsonl")
	out, err := os.Create(jsonlFile)
	if err != nil {
		return err
	}
	defer out.Close()

	tables, err := e.pgTables(psql)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if e.cfg.isExcludedFromIncremental(table) {
			continue
		}
		hasUpdatedAt, err := e.pgHasColumn(psql, table, "updated_at")
		if err != nil {
			return err
		}
		if !hasUpdatedAt {
			continue
		}
		cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--tuples-only", "--no-align",
			"--command", fmt.Sprintf(
				"SELECT jsonb_build_object('table', '%s', 'action', 'upsert', 'data', to_jsonb(t)) FROM (SELECT * FROM %s.%s WHERE updated_at > '%s') t",
				table, e.cfg.schema(), table, sinceStr))
		data, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("incremental query %s failed: %w", table, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if _, err := fmt.Fprintln(out, line); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) extractPostgresArchive(ctx context.Context, gr *gzip.Reader, incremental bool) error {
	tmpDir, err := os.MkdirTemp("", "goent-pg-restore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		target := filepath.Join(tmpDir, header.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return err
		}
		out.Close()
	}

	if incremental {
		return e.restorePostgresIncremental(tmpDir)
	}
	return e.restorePostgresFull(tmpDir)
}

func (e *Engine) restorePostgresFull(dir string) error {
	psql, err := pgTool("psql")
	if err != nil {
		return err
	}

	schemaFile := filepath.Join(dir, "schema.sql")
	if _, err := os.Stat(schemaFile); err != nil {
		return fmt.Errorf("schema.sql not found in backup")
	}

	cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--file", schemaFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("restore schema failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	tables, err := e.pgTables(psql)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if e.cfg.isLogTable(table) {
			continue
		}
		csvFile := filepath.Join(dir, table+".csv")
		if _, err := os.Stat(csvFile); err != nil {
			continue
		}
		copyCmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet",
			"--command", fmt.Sprintf("SET session_replication_role = 'replica'; COPY %s.%s FROM STDIN WITH (FORMAT csv, HEADER true)", e.cfg.schema(), table))
		f, err := os.Open(csvFile)
		if err != nil {
			return err
		}
		copyCmd.Stdin = f
		if out, err := copyCmd.CombinedOutput(); err != nil {
			f.Close()
			return fmt.Errorf("COPY %s restore failed: %w: %s", table, err, strings.TrimSpace(string(out)))
		}
		f.Close()
	}
	return nil
}

func (e *Engine) restorePostgresIncremental(dir string) error {
	jsonlFile := filepath.Join(dir, "incremental.jsonl")
	if _, err := os.Stat(jsonlFile); err != nil {
		return nil
	}

	psql, err := pgTool("psql")
	if err != nil {
		return err
	}

	columnsCache := make(map[string][]string)

	f, err := os.Open(jsonlFile)
	if err != nil {
		return err
	}
	defer f.Close()

	rowsByTable := make(map[string][]json.RawMessage)
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row struct {
			Table  string          `json:"table"`
			Action string          `json:"action"`
			Data   json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return fmt.Errorf("invalid incremental line: %w", err)
		}
		if row.Action != "upsert" {
			continue
		}
		rowsByTable[row.Table] = append(rowsByTable[row.Table], row.Data)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	for table, rows := range rowsByTable {
		cols, ok := columnsCache[table]
		if !ok {
			cols, err = e.pgColumns(psql, table)
			if err != nil {
				return err
			}
			columnsCache[table] = cols
		}
		if err := e.applyIncrementalUpsert(psql, table, cols, rows); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) applyIncrementalUpsert(psql, table string, cols []string, rows []json.RawMessage) error {
	if len(cols) == 0 || len(rows) == 0 {
		return nil
	}
	quoted := make([]string, len(cols))
	excluded := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("\"%s\"", c)
		excluded[i] = fmt.Sprintf("EXCLUDED.\"%s\"", c)
	}
	colsList := strings.Join(quoted, ", ")
	updateList := strings.Join(excluded, ", ")

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("goent-incr-%s-*.jsonl", table))
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	for _, r := range rows {
		if _, err := fmt.Fprintln(tmpFile, string(r)); err != nil {
			tmpFile.Close()
			return err
		}
	}
	tmpFile.Close()

	sql := fmt.Sprintf(
		"CREATE TEMP TABLE _incr (data jsonb); COPY _incr(data) FROM '%s'; INSERT INTO %s.%s (%s) SELECT (jsonb_populate_record(null::%s.%s, data)).* FROM _incr ON CONFLICT (id) DO UPDATE SET (%s) = (%s);",
		tmpFile.Name(), e.cfg.schema(), table, colsList, e.cfg.schema(), table, colsList, updateList)

	cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--command", sql)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("upsert %s failed: %w: %s", table, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (e *Engine) pgTables(psql string) ([]string, error) {
	cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--tuples-only", "--no-align",
		"--command", fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE' ORDER BY table_name", e.cfg.schema()))
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list tables failed: %w", err)
	}
	var tables []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			tables = append(tables, line)
		}
	}
	return tables, nil
}

func (e *Engine) pgHasColumn(psql, table, column string) (bool, error) {
	cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--tuples-only", "--no-align",
		"--command", fmt.Sprintf("SELECT 1 FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s' AND column_name='%s' LIMIT 1", e.cfg.schema(), table, column))
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) == "1", nil
}

func (e *Engine) pgColumns(psql, table string) ([]string, error) {
	cmd := exec.Command(psql, "--dbname", e.cfg.DSN, "--quiet", "--tuples-only", "--no-align",
		"--command", fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s' ORDER BY ordinal_position", e.cfg.schema(), table))
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var cols []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			cols = append(cols, line)
		}
	}
	return cols, nil
}
