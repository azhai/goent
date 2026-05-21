package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/azhai/goent"
)

type rule struct {
	Name       string
	Enabled    bool
	MinVersion float64
}

type recommendation struct {
	Rule    string
	Table   string
	Message string
	SQL     string
}

func runPgOptimize(args *PgOptimizeArgs) {
	if args.Init != "" {
		if err := writeDefaultRules(args.Init); err != nil {
			fmt.Printf("Error writing rules file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Default rules file written to: %s\n", args.Init)
		return
	}

	env := NewEnvSafe()
	dbType := resolveDBType(env)
	cfg, err := ToDBConfig(args.DSN, dbType)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}

	db, err := OpenDB(cfg)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer CloseDB(db)

	if !goent.NewSchemaOps(db.DB).IsPg() {
		fmt.Fprintln(os.Stderr, "Error: pg-optimize requires a PostgreSQL database")
		os.Exit(1)
	}

	ctx := context.Background()

	pgVersion, err := getPgVersion(ctx, db.DB)
	if err != nil {
		fmt.Printf("Error getting PostgreSQL version: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PostgreSQL version: %.1f\n\n", pgVersion)

	if args.Rules == "" {
		args.Rules = "optimized-rules.txt"
		if _, err := os.Stat(args.Rules); err != nil {
			fmt.Printf("Rules file not found: %s\n", args.Rules)
			fmt.Println("Run with --init to generate a default rules file:")
			fmt.Println("  goent-opt pg-optimize --init optimized-rules.txt")
			os.Exit(1)
		}
	}

	rules, err := loadRules(args.Rules)
	if err != nil {
		fmt.Printf("Error loading rules: %v\n", err)
		os.Exit(1)
	}

	activeRules := filterRules(rules, pgVersion)
	fmt.Printf("Active rules (PG %.1f):\n", pgVersion)
	for _, r := range activeRules {
		fmt.Printf("  - %s\n", r.Name)
	}
	fmt.Println()

	concurrent := false
	for _, r := range activeRules {
		if r.Name == "concurrent" && r.Enabled {
			concurrent = true
		}
	}

	schema, err := loadSchema(ctx, db.DB)
	if err != nil {
		fmt.Printf("Error loading schema: %v\n", err)
		os.Exit(1)
	}

	stats, err := loadStats(ctx, db.DB)
	if err != nil {
		fmt.Printf("Warning: could not load index stats: %v\n", err)
		stats = &dbStats{}
	}

	var recs []recommendation

	for _, r := range activeRules {
		if !r.Enabled || r.Name == "concurrent" {
			continue
		}
		var newRecs []recommendation
		switch r.Name {
		case "fk_index":
			newRecs = checkFKIndex(schema, concurrent)
		case "unused_index":
			newRecs = checkUnusedIndex(schema, stats)
		case "redundant_index":
			newRecs = checkRedundantIndex(schema)
		case "duplicate_index":
			newRecs = checkDuplicateIndex(schema)
		case "covering_index":
			newRecs = checkCoveringIndex(schema, stats, concurrent)
		case "partial_index":
			newRecs = checkPartialIndex(schema, stats, concurrent)
		case "brin_index":
			newRecs = checkBRINIndex(schema, stats, concurrent)
		case "composite_index":
			newRecs = checkCompositeIndex(schema, concurrent)
		case "table_bloat":
			newRecs = checkTableBloat(stats)
		case "index_bloat":
			newRecs = checkIndexBloat(stats)
		}
		recs = append(recs, newRecs...)
	}

	if len(recs) == 0 {
		fmt.Println("No optimization recommendations found. Your database looks good!")
		return
	}

	fmt.Printf("=== Found %d recommendations ===\n\n", len(recs))
	for i, rec := range recs {
		fmt.Printf("[%d] Rule: %s | Table: %s\n", i+1, rec.Rule, rec.Table)
		fmt.Printf("    %s\n", rec.Message)
		if rec.SQL != "" {
			fmt.Printf("    SQL:\n")
			for _, line := range strings.Split(rec.SQL, "\n") {
				fmt.Printf("      %s\n", line)
			}
		}
		fmt.Println()
	}

	if args.DryRun {
		fmt.Println("[DRY RUN] No changes were made.")
		return
	}

	var sqls []string
	for _, rec := range recs {
		if rec.SQL != "" && !strings.HasPrefix(strings.TrimSpace(rec.SQL), "--") {
			sqls = append(sqls, rec.SQL)
		}
	}

	if len(sqls) == 0 {
		fmt.Println("No executable SQL statements. Only informational recommendations above.")
		return
	}

	fmt.Printf("=== Execute %d SQL statements? (y/N) ===\n", len(sqls))
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(strings.ToLower(input))
	if input != "y" && input != "yes" {
		fmt.Println("Aborted.")
		return
	}

	for i, stmt := range sqls {
		fmt.Printf("Executing %d/%d...\n", i+1, len(sqls))
		if err := db.DB.RawExecContext(ctx, stmt); err != nil {
			fmt.Printf("  Error: %v\n", err)
			fmt.Printf("  SQL: %s\n", stmt)
		} else {
			fmt.Println("  OK")
		}
	}
	fmt.Println("Done!")
}



func writeDefaultRules(path string) error {
	content := `# PostgreSQL Index Optimization Rules
# ====================================
# Format: rule_name = on|off [min_pg_version]
#
# min_pg_version: optional, only apply when PostgreSQL version >= this value
# Example: covering_index = on 11  (only for PG 11+)
#
# Run with --init to generate this default file:
#   goent-opt pg-optimize --init optimized-rules.txt

# --- Foreign Key Indexes ---
fk_index = on

# --- Unused Indexes ---
unused_index = on

# --- Redundant Indexes ---
redundant_index = on

# --- Duplicate Indexes ---
duplicate_index = on

# --- Covering Indexes (INCLUDE) ---
covering_index = on 11

# --- Partial Indexes ---
partial_index = on

# --- BRIN Indexes for Large Tables ---
brin_index = on 9.5

# --- Composite Index Suggestions ---
composite_index = on

# --- Concurrent Index Creation ---
concurrent = on

# --- Table Bloat Detection ---
table_bloat = on

# --- Index Bloat Detection ---
index_bloat = on 12
`
	return os.WriteFile(path, []byte(content), 0644)
}

func loadRules(path string) ([]rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading rules file: %w", err)
	}

	var rules []rule
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 || parts[1] != "=" {
			continue
		}
		r := rule{Name: parts[0]}
		r.Enabled = parts[2] == "on"
		if len(parts) >= 4 {
			v, err := strconv.ParseFloat(parts[3], 64)
			if err == nil {
				r.MinVersion = v
			}
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func filterRules(rules []rule, pgVersion float64) []rule {
	var active []rule
	for _, r := range rules {
		if r.MinVersion > 0 && pgVersion < r.MinVersion {
			continue
		}
		active = append(active, r)
	}
	return active
}

func getPgVersion(ctx context.Context, db *goent.DB) (float64, error) {
	versionStr, err := goent.NewSchemaOps(db).GetVersion(ctx)
	if err != nil {
		return 0, err
	}

	parts := strings.Fields(versionStr)
	if len(parts) < 2 {
		return 0, fmt.Errorf("unexpected version string: %s", versionStr)
	}

	v, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, fmt.Errorf("parsing version: %w", err)
	}
	return v, nil
}

type pgColumnInfo struct {
	Name       string
	DataType   string
	IsNullable bool
}

type pgIndexInfo struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
}

type pgFKInfo struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

type pgTableInfo struct {
	Name     string
	Columns  []pgColumnInfo
	Indexes  []pgIndexInfo
	FKs      []pgFKInfo
	RowCount int64
}

type pgIndexStat struct {
	TableName   string
	IndexName   string
	IdxScan     int64
	IdxFetch    int64
	IdxTupRead  int64
	IdxTupFetch int64
	Size        int64
}

type pgTableStat struct {
	TableName   string
	SeqScan     int64
	IdxScan     int64
	NDeadTup    int64
	NLiveTup    int64
	LastVacuum  sql.NullTime
	LastAnalyze sql.NullTime
}

type dbStats struct {
	IndexStats []pgIndexStat
	TableStats []pgTableStat
}

func loadSchema(ctx context.Context, db *goent.DB) ([]pgTableInfo, error) {
	probe := goent.NewSchemaOps(db)
	tableNames, err := probe.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	var tables []pgTableInfo
	for _, name := range tableNames {
		ti := pgTableInfo{Name: name}

		cols, err := probe.GetColumns(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, col := range cols {
			ti.Columns = append(ti.Columns, pgColumnInfo{Name: col.Name, DataType: col.DataType, IsNullable: col.Nullable})
		}

		indexes, err := probe.GetIndexes(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, idx := range indexes {
			ti.Indexes = append(ti.Indexes, pgIndexInfo{Name: idx.Name, Columns: idx.Columns, IsUnique: idx.Unique})
		}

		fks, err := probe.GetForeignKeys(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, fk := range fks {
			ti.FKs = append(ti.FKs, pgFKInfo{Name: fk.Name, Columns: fk.Columns, ReferencedTable: fk.RefTable, ReferencedColumns: fk.RefColumns})
		}

		ti.RowCount, _ = probe.GetTableRowCount(ctx, name)
		tables = append(tables, ti)
	}

	return tables, nil
}

func loadStats(ctx context.Context, db *goent.DB) (*dbStats, error) {
	probe := goent.NewSchemaOps(db)
	stats := &dbStats{}

	idxStats, err := probe.GetIndexStats(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range idxStats {
		stats.IndexStats = append(stats.IndexStats, pgIndexStat{
			TableName: s.TableName, IndexName: s.IndexName,
			IdxScan: s.IdxScan, IdxFetch: s.IdxFetch,
			IdxTupRead: s.IdxTupRead, IdxTupFetch: s.IdxTupFetch, Size: s.Size,
		})
	}

	tblStats, err := probe.GetTableStats(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range tblStats {
		stats.TableStats = append(stats.TableStats, pgTableStat{
			TableName: s.TableName, SeqScan: s.SeqScan, IdxScan: s.IdxScan,
			NDeadTup: s.NDeadTup, NLiveTup: s.NLiveTup,
			LastVacuum: s.LastVacuum, LastAnalyze: s.LastAnalyze,
		})
	}

	return stats, nil
}

func pgHasIndexOnColumn(table pgTableInfo, colName string) bool {
	for _, idx := range table.Indexes {
		if len(idx.Columns) > 0 && idx.Columns[0] == colName {
			return true
		}
	}
	return false
}

func pgHasExactIndex(table pgTableInfo, cols []string) bool {
	for _, idx := range table.Indexes {
		if len(idx.Columns) != len(cols) {
			continue
		}
		match := true
		for i, c := range cols {
			if idx.Columns[i] != c {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func createIndexStmt(concurrent bool, idxName, table, columns string, extra string) string {
	conc := ""
	if concurrent {
		conc = " CONCURRENTLY"
	}
	return fmt.Sprintf("CREATE INDEX%s %s ON %s (%s)%s;", conc, idxName, table, columns, extra)
}

func checkFKIndex(tables []pgTableInfo, concurrent bool) []recommendation {
	var recs []recommendation
	for _, t := range tables {
		for _, fk := range t.FKs {
			for _, col := range fk.Columns {
				if pgHasIndexOnColumn(t, col) {
					continue
				}
				idxName := fmt.Sprintf("idx_%s_%s", t.Name, col)
				recs = append(recs, recommendation{
					Rule:    "fk_index",
					Table:   t.Name,
					Message: fmt.Sprintf("Foreign key column %s.%s has no index. This slows down DELETE on %s and JOIN queries.", t.Name, col, fk.ReferencedTable),
					SQL:     createIndexStmt(concurrent, idxName, t.Name, col, ""),
				})
			}
		}
	}
	return recs
}

func checkUnusedIndex(tables []pgTableInfo, stats *dbStats) []recommendation {
	var recs []recommendation
	if stats == nil {
		return recs
	}

	statMap := make(map[string]pgIndexStat)
	for _, s := range stats.IndexStats {
		statMap[s.IndexName] = s
	}

	for _, t := range tables {
		for _, idx := range t.Indexes {
			if idx.IsPrimary {
				continue
			}
			s, ok := statMap[idx.Name]
			if !ok {
				continue
			}
			if s.IdxScan == 0 && s.Size > 0 {
				recs = append(recs, recommendation{
					Rule:    "unused_index",
					Table:   t.Name,
					Message: fmt.Sprintf("Index %s on %s has never been used (0 scans, size: %s). Consider dropping it.", idx.Name, t.Name, humanSize(s.Size)),
					SQL:     fmt.Sprintf("DROP INDEX %s;", idx.Name),
				})
			}
		}
	}
	return recs
}

func checkRedundantIndex(tables []pgTableInfo) []recommendation {
	var recs []recommendation

	for _, t := range tables {
		for i, idx1 := range t.Indexes {
			if idx1.IsPrimary || idx1.IsUnique {
				continue
			}
			for j, idx2 := range t.Indexes {
				if i == j || len(idx2.Columns) <= len(idx1.Columns) {
					continue
				}
				isPrefix := true
				for k := 0; k < len(idx1.Columns); k++ {
					if idx1.Columns[k] != idx2.Columns[k] {
						isPrefix = false
						break
					}
				}
				if isPrefix {
					recs = append(recs, recommendation{
						Rule:    "redundant_index",
						Table:   t.Name,
						Message: fmt.Sprintf("Index %s(%v) is a prefix of %s(%v). The shorter index may be redundant.", idx1.Name, idx1.Columns, idx2.Name, idx2.Columns),
						SQL:     fmt.Sprintf("-- DROP INDEX %s; -- Verify before dropping", idx1.Name),
					})
				}
			}
		}
	}
	return recs
}

func checkDuplicateIndex(tables []pgTableInfo) []recommendation {
	var recs []recommendation

	for _, t := range tables {
		for i, idx1 := range t.Indexes {
			if idx1.IsPrimary {
				continue
			}
			for j := i + 1; j < len(t.Indexes); j++ {
				idx2 := t.Indexes[j]
				if idx2.IsPrimary {
					continue
				}
				if len(idx1.Columns) != len(idx2.Columns) {
					continue
				}
				match := true
				for k := 0; k < len(idx1.Columns); k++ {
					if idx1.Columns[k] != idx2.Columns[k] {
						match = false
						break
					}
				}
				if match {
					dropIdx := idx2.Name
					if idx1.IsUnique && !idx2.IsUnique {
						dropIdx = idx2.Name
					} else if !idx1.IsUnique && idx2.IsUnique {
						dropIdx = idx1.Name
					}
					recs = append(recs, recommendation{
						Rule:    "duplicate_index",
						Table:   t.Name,
						Message: fmt.Sprintf("Duplicate indexes %s and %s on columns (%v). Only one is needed.", idx1.Name, idx2.Name, idx1.Columns),
						SQL:     fmt.Sprintf("-- DROP INDEX %s; -- Verify before dropping", dropIdx),
					})
				}
			}
		}
	}
	return recs
}

func checkCoveringIndex(tables []pgTableInfo, stats *dbStats, concurrent bool) []recommendation {
	var recs []recommendation

	statMap := make(map[string]pgIndexStat)
	if stats != nil {
		for _, s := range stats.IndexStats {
			statMap[s.IndexName] = s
		}
	}

	for _, t := range tables {
		if len(t.Indexes) == 0 || len(t.Columns) <= 2 {
			continue
		}
		for _, idx := range t.Indexes {
			if idx.IsPrimary || len(idx.Columns) != 1 {
				continue
			}
			s, hasStat := statMap[idx.Name]
			if !hasStat || s.IdxScan < 100 {
				continue
			}
			idxCol := idx.Columns[0]
			var otherCols []string
			for _, col := range t.Columns {
				if col.Name == idxCol {
					continue
				}
				isInAnyIndex := false
				for _, i2 := range t.Indexes {
					for _, ic := range i2.Columns {
						if ic == col.Name {
							isInAnyIndex = true
							break
						}
					}
					if isInAnyIndex {
						break
					}
				}
				if isInAnyIndex {
					continue
				}
				if len(otherCols) < 3 {
					otherCols = append(otherCols, col.Name)
				}
			}
			if len(otherCols) == 0 {
				continue
			}
			includeCols := strings.Join(otherCols, ", ")
			newIdxName := fmt.Sprintf("idx_%s_%s_covering", t.Name, idxCol)
			recs = append(recs, recommendation{
				Rule:    "covering_index",
				Table:   t.Name,
				Message: fmt.Sprintf("Frequently used index %s on %s.%s (%d scans). Consider adding INCLUDE (%s) for index-only scans.", idx.Name, t.Name, idxCol, s.IdxScan, includeCols),
				SQL:     fmt.Sprintf("-- DROP INDEX %s;\n%s", idx.Name, createIndexStmt(concurrent, newIdxName, t.Name, idxCol, fmt.Sprintf(" INCLUDE (%s)", includeCols))),
			})
		}
	}
	return recs
}

func checkPartialIndex(tables []pgTableInfo, stats *dbStats, concurrent bool) []recommendation {
	var recs []recommendation

	for _, t := range tables {
		for _, col := range t.Columns {
			lowerName := strings.ToLower(col.Name)
			isBool := col.DataType == "boolean" || col.DataType == "bool"
			isStatus := strings.Contains(lowerName, "status") || strings.Contains(lowerName, "type") || strings.Contains(lowerName, "state")
			if !isBool && !isStatus {
				continue
			}
			if pgHasIndexOnColumn(t, col.Name) {
				continue
			}
			if isBool {
				idxName := fmt.Sprintf("idx_%s_%s_true", t.Name, col.Name)
				recs = append(recs, recommendation{
					Rule:    "partial_index",
					Table:   t.Name,
					Message: fmt.Sprintf("Boolean column %s.%s may benefit from a partial index on true values only.", t.Name, col.Name),
					SQL:     createIndexStmt(concurrent, idxName, t.Name, col.Name, fmt.Sprintf(" WHERE %s = true", col.Name)),
				})
			}
			if isStatus {
				idxName := fmt.Sprintf("idx_%s_%s_active", t.Name, col.Name)
				recs = append(recs, recommendation{
					Rule:    "partial_index",
					Table:   t.Name,
					Message: fmt.Sprintf("Status column %s.%s may benefit from a partial index for common values.", t.Name, col.Name),
					SQL:     fmt.Sprintf("-- Analyze distinct values first:\n-- SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY COUNT(*) DESC LIMIT 10;\n-- Then create: CREATE INDEX %s ON %s (%s) WHERE %s = 'most_common_value';", col.Name, t.Name, col.Name, idxName, t.Name, col.Name, col.Name),
				})
			}
		}
	}
	return recs
}

func checkBRINIndex(tables []pgTableInfo, stats *dbStats, concurrent bool) []recommendation {
	var recs []recommendation

	for _, t := range tables {
		if t.RowCount < 100000 {
			continue
		}
		for _, col := range t.Columns {
			lowerType := strings.ToLower(col.DataType)
			isOrdered := strings.Contains(lowerType, "timestamp") ||
				strings.Contains(lowerType, "date") ||
				strings.Contains(lowerType, "serial") ||
				strings.Contains(lowerType, "bigserial") ||
				(strings.Contains(lowerType, "integer") && strings.Contains(strings.ToLower(col.Name), "id"))
			if !isOrdered {
				continue
			}
			if pgHasIndexOnColumn(t, col.Name) {
				continue
			}
			idxName := fmt.Sprintf("idx_%s_%s_brin", t.Name, col.Name)
			recs = append(recs, recommendation{
				Rule:    "brin_index",
				Table:   t.Name,
				Message: fmt.Sprintf("Large table %s (%d rows) with naturally ordered column %s may benefit from a tiny BRIN index instead of B-tree.", t.Name, t.RowCount, col.Name),
				SQL:     fmt.Sprintf("CREATE INDEX %s ON %s USING BRIN (%s);", idxName, t.Name, col.Name),
			})
		}
	}
	return recs
}

func checkCompositeIndex(tables []pgTableInfo, concurrent bool) []recommendation {
	var recs []recommendation

	for _, t := range tables {
		var fkCols []string
		for _, fk := range t.FKs {
			for _, col := range fk.Columns {
				if !pgHasIndexOnColumn(t, col) {
					fkCols = append(fkCols, col)
				}
			}
		}
		if len(fkCols) < 2 {
			continue
		}

		if pgHasExactIndex(t, fkCols) {
			continue
		}

		idxName := fmt.Sprintf("idx_%s_%s", t.Name, strings.Join(fkCols, "_"))
		cols := strings.Join(fkCols, ", ")
		recs = append(recs, recommendation{
			Rule:    "composite_index",
			Table:   t.Name,
			Message: fmt.Sprintf("Table %s has %d FK columns without indexes. A composite index on (%s) may be more efficient than separate indexes.", t.Name, len(fkCols), cols),
			SQL:     createIndexStmt(concurrent, idxName, t.Name, cols, ""),
		})
	}
	return recs
}

func checkTableBloat(stats *dbStats) []recommendation {
	var recs []recommendation
	if stats == nil {
		return recs
	}

	for _, ts := range stats.TableStats {
		if ts.NLiveTup == 0 {
			continue
		}
		deadRatio := float64(ts.NDeadTup) / float64(ts.NLiveTup+ts.NDeadTup)
		if deadRatio > 0.1 {
			recs = append(recs, recommendation{
				Rule:    "table_bloat",
				Table:   ts.TableName,
				Message: fmt.Sprintf("Table %s has %.1f%% dead tuples (%d dead / %d live). Consider VACUUM.", ts.TableName, deadRatio*100, ts.NDeadTup, ts.NLiveTup),
				SQL:     fmt.Sprintf("VACUUM %s;", ts.TableName),
			})
		} else if deadRatio > 0.05 {
			recs = append(recs, recommendation{
				Rule:    "table_bloat",
				Table:   ts.TableName,
				Message: fmt.Sprintf("Table %s has %.1f%% dead tuples (%d dead / %d live). Consider scheduling VACUUM.", ts.TableName, deadRatio*100, ts.NDeadTup, ts.NLiveTup),
				SQL:     fmt.Sprintf("-- VACUUM %s;", ts.TableName),
			})
		}
	}
	return recs
}

func checkIndexBloat(stats *dbStats) []recommendation {
	var recs []recommendation
	if stats == nil {
		return recs
	}

	for _, ts := range stats.TableStats {
		if ts.NLiveTup == 0 {
			continue
		}
		if ts.SeqScan > 0 && ts.IdxScan == 0 && ts.NLiveTup > 10000 {
			recs = append(recs, recommendation{
				Rule:    "index_bloat",
				Table:   ts.TableName,
				Message: fmt.Sprintf("Table %s (%d rows) has %d seq scans but 0 index scans. May need better indexes or REINDEX.", ts.TableName, ts.NLiveTup, ts.SeqScan),
				SQL:     fmt.Sprintf("-- REINDEX TABLE %s;", ts.TableName),
			})
		}
	}
	return recs
}

func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
