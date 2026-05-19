package goent

import (
	"context"
	"fmt"
)

type FKRef struct {
	FromTable  string
	FromColumn string
	Nullable   bool
}

func ListTables(ctx context.Context, db *DB, isPg bool) ([]string, error) {
	if isPg {
		rows, err := db.RawQueryContext(ctx, `
			SELECT table_name FROM information_schema.tables
			WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
			ORDER BY table_name`)
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

	rows, err := db.RawQueryContext(ctx, `
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

func DiscoverFKs(ctx context.Context, db *DB, table string, isPg bool) ([]FKRef, error) {
	if isPg {
		return discoverPgFKs(ctx, db, table)
	}
	return discoverSQLiteFKs(ctx, db, table)
}

func discoverPgFKs(ctx context.Context, db *DB, table string) ([]FKRef, error) {
	query := `
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
			AND tc.table_schema = 'public' AND ccu.table_name = $1 AND ccu.column_name = 'id'
		ORDER BY kcu.table_name, kcu.column_name`
	rows, err := db.RawQueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("querying PG foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []FKRef
	for rows.Next() {
		var fk FKRef
		if err := rows.Scan(&fk.FromTable, &fk.FromColumn, &fk.Nullable); err != nil {
			return nil, fmt.Errorf("scanning FK: %w", err)
		}
		fks = append(fks, fk)
	}
	return fks, nil
}

func discoverSQLiteFKs(ctx context.Context, db *DB, table string) ([]FKRef, error) {
	rows, err := db.RawQueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", table))
	if err != nil {
		return nil, fmt.Errorf("querying SQLite foreign keys: %w", err)
	}
	defer rows.Close()

	type rawFK struct {
		id, seq  int
		table    string
		from, to string
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

	var fks []FKRef
	for _, fk := range rawFKs {
		if fk.seq != 0 || fk.table != table || fk.to != "id" {
			continue
		}
		fks = append(fks, FKRef{
			FromTable:  fk.table,
			FromColumn: fk.from,
			Nullable:   IsColumnNullable(ctx, db, fk.table, fk.from),
		})
	}
	return fks, nil
}

func IsColumnNullable(ctx context.Context, db *DB, table, column string) bool {
	rows, err := db.RawQueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
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

func SequenceExists(ctx context.Context, db *DB, seqName string) (bool, error) {
	rows, err := db.RawQueryContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = $1)",
		seqName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var exists bool
	if rows.Next() {
		if err := rows.Scan(&exists); err != nil {
			return false, err
		}
	}
	return exists, nil
}

func GetPgVersion(ctx context.Context, db *DB) (string, error) {
	rows, err := db.RawQueryContext(ctx, "SELECT version()")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var versionStr string
	if rows.Next() {
		if err := rows.Scan(&versionStr); err != nil {
			return "", err
		}
	}
	return versionStr, nil
}
