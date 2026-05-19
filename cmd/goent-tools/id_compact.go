package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

type fkRef struct {
	FromTable  string
	FromColumn string
	Nullable   bool
}

func runIDCompact(args []string) {
	var (
		gapThreshold int64
		dryRun       bool
	)

	fs := flag.NewFlagSet("compact-ids", flag.ExitOnError)
	fs.Int64Var(&gapThreshold, "gap", 1024, "minimum ID gap to trigger compaction (must be >= 1)")
	fs.BoolVar(&dryRun, "dry-run", false, "preview changes without modifying data")
	fs.Usage = func() {
		fmt.Println("Usage: goent-tools id-compact [options] [dsn] <table1> [table2] ...")
		fmt.Println()
		fmt.Println("Compacts auto-increment ID gaps and resets sequence to max(id)+1.")
		printDSNHelp()
		fmt.Println()
		fmt.Println("Options:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  goent-tools id-compact 'postgres://user:pass@localhost/db?sslmode=disable' comment issue")
		fmt.Println("  goent-tools id-compact 'gitfolio.db' comment")
		fmt.Println("  goent-tools id-compact --gap 2048 --dry-run 'postgres://...' comment")
		fmt.Println("  DB_DSN='postgres://...' goent-tools id-compact comment issue")
	}
	fs.Parse(args)

	if gapThreshold < 1 {
		fmt.Fprintln(os.Stderr, "Error: --gap must be at least 1")
		os.Exit(1)
	}
	if gapThreshold > 1000000 {
		fmt.Printf("Warning: --gap value %d is very large, compaction may not be needed\n", gapThreshold)
	}

	remain := fs.Args()

	var cliDSN string
	var tables []string
	if len(remain) >= 1 && isLikelyDSN(remain[0]) {
		cliDSN = remain[0]
		tables = remain[1:]
	} else {
		tables = remain
	}

	cfg, err := ParseDSNArgs(cliDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err.Error())
		os.Exit(1)
	}

	if len(tables) == 0 {
		fs.Usage()
		os.Exit(1)
	}

	tdb, err := OpenToolsDB(cfg)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer CloseDB(tdb)

	ctx := context.Background()

	for _, table := range tables {
		fmt.Printf("\n=== Processing table: %s ===\n", table)
		if dryRun {
			fmt.Println("[DRY RUN] No changes will be made")
		}
		if err := compactTable(ctx, tdb, table, cfg.IsPg, gapThreshold, dryRun); err != nil {
			fmt.Printf("Error compacting table %s: %v\n", table, err)
		}
	}
}

func compactTable(ctx context.Context, tdb *ToolsDB, table string, isPg bool, gapThreshold int64, dryRun bool) error {
	var count int64
	var maxID int64
	rows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM %s", q(table)))
	if err != nil {
		return fmt.Errorf("querying count and max id: %w", err)
	}
	if rows.Next() {
		if err := rows.Scan(&count, &maxID); err != nil {
			rows.Close()
			return fmt.Errorf("querying count and max id: %w", err)
		}
	}
	rows.Close()

	if count == 0 {
		fmt.Println("  Table is empty, nothing to do")
		return nil
	}

	totalGap := maxID - count
	fmt.Printf("  Rows: %d, Max ID: %d, Total gap: %d (threshold: %d)\n", count, maxID, totalGap, gapThreshold)

	if totalGap <= gapThreshold {
		fmt.Printf("  No compaction needed, resetting sequence to %d\n", maxID+1)
		if !dryRun {
			if err := resetSequence(ctx, tdb, table, isPg, maxID+1); err != nil {
				fmt.Printf("  Warning: failed to reset sequence: %v\n", err)
			}
		}
		return nil
	}

	idRows, err := tdb.RawQueryContext(ctx, fmt.Sprintf("SELECT id FROM %s ORDER BY id", q(table)))
	if err != nil {
		return fmt.Errorf("querying IDs: %w", err)
	}

	var ids []int64
	for idRows.Next() {
		var id int64
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return fmt.Errorf("scanning ID: %w", err)
		}
		ids = append(ids, id)
	}
	idRows.Close()

	type segment struct {
		startIdx int
		endIdx   int
		oldStart int64
		oldEnd   int64
		newStart int64
		newEnd   int64
	}

	var segments []segment
	currentNew := int64(1)
	segStartIdx := 0

	for i, id := range ids {
		expectedNew := currentNew + int64(i-segStartIdx)
		gap := id - expectedNew
		if gap > gapThreshold {
			if i > segStartIdx {
				segments = append(segments, segment{
					startIdx: segStartIdx,
					endIdx:   i - 1,
					oldStart: ids[segStartIdx],
					oldEnd:   ids[i-1],
					newStart: currentNew,
					newEnd:   currentNew + int64(i-1-segStartIdx),
				})
			}
			currentNew = id - gap + gapThreshold
			segStartIdx = i
		}
	}

	if segStartIdx <= len(ids)-1 {
		segments = append(segments, segment{
			startIdx: segStartIdx,
			endIdx:   len(ids) - 1,
			oldStart: ids[segStartIdx],
			oldEnd:   ids[len(ids)-1],
			newStart: currentNew,
			newEnd:   currentNew + int64(len(ids)-1-segStartIdx),
		})
	}

	needCompact := false
	for _, seg := range segments {
		if seg.oldStart != seg.newStart {
			needCompact = true
			break
		}
	}

	if !needCompact {
		fmt.Printf("  No segments need compaction, resetting sequence to %d\n", maxID+1)
		if !dryRun {
			if err := resetSequence(ctx, tdb, table, isPg, maxID+1); err != nil {
				fmt.Printf("  Warning: failed to reset sequence: %v\n", err)
			}
		}
		return nil
	}

	fmt.Println("  Segments to compact:")
	for i, seg := range segments {
		if seg.oldStart == seg.newStart {
			fmt.Printf("    Segment %d: IDs %d-%d (unchanged, %d rows)\n",
				i+1, seg.oldStart, seg.oldEnd, seg.endIdx-seg.startIdx+1)
		} else {
			fmt.Printf("    Segment %d: IDs %d-%d -> %d-%d (%d rows)\n",
				i+1, seg.oldStart, seg.oldEnd, seg.newStart, seg.newEnd, seg.endIdx-seg.startIdx+1)
		}
	}

	finalMaxID := segments[len(segments)-1].newEnd
	fmt.Printf("  Final max ID will be: %d (was %d)\n", finalMaxID, maxID)

	if dryRun {
		fmt.Println("  [DRY RUN] Stopping here, no changes made")
		return nil
	}

	fks, err := discoverFKs(ctx, tdb, table, isPg)
	if err != nil {
		fmt.Printf("  Warning: could not discover foreign keys: %v\n", err)
		fks = nil
	}
	if len(fks) > 0 {
		fmt.Printf("  Foreign key references to update:\n")
		for _, fk := range fks {
			fmt.Printf("    %s.%s\n", fk.FromTable, fk.FromColumn)
		}
	}

	tx, err := tdb.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	segMap := make(map[int64]int64)
	for _, seg := range segments {
		if seg.oldStart == seg.newStart {
			continue
		}
		for i := seg.startIdx; i <= seg.endIdx; i++ {
			segMap[ids[i]] = seg.newStart + int64(i-seg.startIdx)
		}
	}

	var oldIDs []int64
	for id := range segMap {
		oldIDs = append(oldIDs, id)
	}
	sort.Slice(oldIDs, func(i, j int) bool { return oldIDs[i] < oldIDs[j] })

	fmt.Printf("  Updating %d IDs in table %s...\n", len(segMap), table)

	if isPg {
		if err := compactPostgres(ctx, tx, table, oldIDs, segMap, fks); err != nil {
			tx.Rollback()
			return err
		}
	} else {
		if err := compactSQLite(ctx, tx, table, oldIDs, segMap, fks); err != nil {
			tx.Rollback()
			return err
		}
	}

	nextVal := finalMaxID + 1
	if isPg {
		seqName := table + "_id_seq"
		seqExists, _ := goent.SequenceExists(ctx, tdb.DB, seqName)
		if seqExists {
			fmt.Printf("  Setting sequence %s to %d...\n", seqName, nextVal)
			if err := TxExec(ctx, tx, fmt.Sprintf("SELECT setval('%s', %d, false)", seqName, nextVal)); err != nil {
				fmt.Printf("  Warning: failed to set sequence: %v\n", err)
			}
		}
	} else {
		fmt.Printf("  SQLite auto-increment will resume from %d\n", nextVal)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	fmt.Printf("  Done! Max ID is now %d, next ID will be %d\n", finalMaxID, nextVal)
	return nil
}

func resetSequence(ctx context.Context, tdb *ToolsDB, table string, isPg bool, nextVal int64) error {
	if isPg {
		seqName := table + "_id_seq"
		seqExists, err := goent.SequenceExists(ctx, tdb.DB, seqName)
		if err != nil {
			return fmt.Errorf("checking sequence: %w", err)
		}
		if !seqExists {
			return nil
		}
		if err := tdb.RawExecContext(ctx, fmt.Sprintf("SELECT setval('%s', %d, false)", seqName, nextVal)); err != nil {
			return fmt.Errorf("setting sequence: %w", err)
		}
		fmt.Printf("  Set sequence %s to %d\n", seqName, nextVal)
	} else {
		fmt.Printf("  SQLite auto-increment will resume from %d\n", nextVal)
	}
	return nil
}

func discoverFKs(ctx context.Context, tdb *ToolsDB, table string, isPg bool) ([]fkRef, error) {
	fks, err := goent.DiscoverFKs(ctx, tdb.DB, table, isPg)
	if err != nil {
		return nil, err
	}
	result := make([]fkRef, len(fks))
	for i, fk := range fks {
		result[i] = fkRef{FromTable: fk.FromTable, FromColumn: fk.FromColumn, Nullable: fk.Nullable}
	}
	return result, nil
}

func compactPostgres(ctx context.Context, tx model.Transaction, table string, oldIDs []int64, segMap map[int64]int64, fks []fkRef) error {
	tempTable := "_compact_tmp_" + table
	if err := TxExec(ctx, tx, fmt.Sprintf("CREATE TEMP TABLE %s (old_id BIGINT, new_id BIGINT)", q(tempTable))); err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	batchSize := 500
	for i := 0; i < len(oldIDs); i += batchSize {
		end := i + batchSize
		if end > len(oldIDs) {
			end = len(oldIDs)
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("INSERT INTO %s (old_id, new_id) VALUES ", q(tempTable)))
		for j := i; j < end; j++ {
			if j > i {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("(%d, %d)", oldIDs[j], segMap[oldIDs[j]]))
		}
		if err := TxExec(ctx, tx, sb.String()); err != nil {
			return fmt.Errorf("inserting into temp table: %w", err)
		}
	}

	for _, fk := range fks {
		fmt.Printf("  Updating FK %s.%s...\n", fk.FromTable, fk.FromColumn)
		nullCond := ""
		if fk.Nullable {
			nullCond = fmt.Sprintf(" AND %s IS NOT NULL", fk.FromColumn)
		}
		query := fmt.Sprintf("UPDATE %s SET %s = t.new_id FROM %s t WHERE %s = t.old_id%s",
			q(fk.FromTable), fk.FromColumn, q(tempTable), fk.FromColumn, nullCond)
		if err := TxExec(ctx, tx, query); err != nil {
			return fmt.Errorf("updating FK %s.%s: %w", fk.FromTable, fk.FromColumn, err)
		}
	}

	fmt.Printf("  Updating primary key in %s...\n", table)
	query := fmt.Sprintf("UPDATE %s SET id = t.new_id FROM %s t WHERE %s.id = t.old_id", q(table), q(tempTable), q(table))
	if err := TxExec(ctx, tx, query); err != nil {
		return fmt.Errorf("updating primary key: %w", err)
	}

	TxExec(ctx, tx, "DROP TABLE "+q(tempTable))
	return nil
}

func compactSQLite(ctx context.Context, tx model.Transaction, table string, oldIDs []int64, segMap map[int64]int64, fks []fkRef) error {
	conflictIDs := make(map[int64]bool)
	for oldID, newID := range segMap {
		if newID < oldID {
			for _, id := range oldIDs {
				if id == newID {
					conflictIDs[id] = true
				}
			}
		}
	}

	if len(conflictIDs) > 0 {
		fmt.Printf("  Phase 1: Moving %d conflicting IDs to temporary negative IDs...\n", len(conflictIDs))
		for oldID := range conflictIDs {
			tempID := -oldID
			if err := TxExec(ctx, tx, fmt.Sprintf("UPDATE %s SET id = ? WHERE id = ?", q(table)), tempID, oldID); err != nil {
				return fmt.Errorf("moving to temp ID %d->%d: %w", oldID, tempID, err)
			}
			for _, fk := range fks {
				nullCond := ""
				if fk.Nullable {
					nullCond = " IS NOT NULL"
				}
				if err := TxExec(ctx, tx, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?%s", q(fk.FromTable), fk.FromColumn, fk.FromColumn, nullCond), tempID, oldID); err != nil {
					return fmt.Errorf("updating FK %s.%s temp: %w", fk.FromTable, fk.FromColumn, err)
				}
			}
		}
	}

	fmt.Printf("  Phase 2: Updating %d IDs to new values...\n", len(segMap))
	sortedIDs := make([]int64, 0, len(segMap))
	for id := range segMap {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] > sortedIDs[j] })

	for _, oldID := range sortedIDs {
		newID := segMap[oldID]
		if newID == oldID {
			continue
		}
		var srcID int64
		if conflictIDs[oldID] {
			srcID = -oldID
		} else {
			srcID = oldID
		}
		if err := TxExec(ctx, tx, fmt.Sprintf("UPDATE %s SET id = ? WHERE id = ?", q(table)), newID, srcID); err != nil {
			return fmt.Errorf("updating ID %d->%d: %w", oldID, newID, err)
		}
		for _, fk := range fks {
			nullCond := ""
			if fk.Nullable {
				nullCond = " IS NOT NULL"
			}
			if err := TxExec(ctx, tx, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?%s", q(fk.FromTable), fk.FromColumn, fk.FromColumn, nullCond), newID, srcID); err != nil {
				return fmt.Errorf("updating FK %s.%s: %w", fk.FromTable, fk.FromColumn, err)
			}
		}
	}

	return nil
}
