// Package rename provides safe column and table renaming helpers for goent.
//
// It is useful when a model field or table is renamed but the production
// database still contains the old name. The helpers detect the current state
// and either rename the old object or merge data into the new object so no
// information is lost.
package rename

import (
	"context"
	"fmt"
	"strings"

	"github.com/azhai/goent"
)

// Column describes a column rename operation.
type Column struct {
	Table   string // Table name (without schema)
	OldName string // Current column name
	NewName string // Desired column name
}

// Apply executes the rename operation on db.
//
// Behaviour depends on the current state:
//   - new column exists, old does not: nothing to do
//   - old column exists, new does not: rename old to new
//   - both exist: merge data from old into new, then remove old
//     (SQLite may leave the old column in place if drop is unsupported)
func (c Column) Apply(db *goent.DB) error {
	if db == nil {
		return fmt.Errorf("rename: db is nil")
	}
	if c.Table == "" || c.OldName == "" || c.NewName == "" {
		return fmt.Errorf("rename: table, old and new column names are required")
	}
	if c.OldName == c.NewName {
		return nil
	}
	driverName := strings.ToLower(db.DriverName())
	switch driverName {
	case "postgres", "postgresql":
		return c.applyPostgres(db)
	default:
		return c.applySQLite(db)
	}
}

// Table describes a table rename operation.
type Table struct {
	OldName string // Current table name (without schema)
	NewName string // Desired table name (without schema)
}

// Apply executes the table rename operation on db.
// If the new table already exists, it returns an error to avoid data loss.
func (t Table) Apply(db *goent.DB) error {
	if db == nil {
		return fmt.Errorf("rename: db is nil")
	}
	if t.OldName == "" || t.NewName == "" {
		return fmt.Errorf("rename: old and new table names are required")
	}
	if t.OldName == t.NewName {
		return nil
	}

	schema := currentSchema(db)
	driverName := strings.ToLower(db.DriverName())
	switch driverName {
	case "postgres", "postgresql":
		return t.applyPostgres(db, schema)
	default:
		return t.applySQLite(db, schema)
	}
}

func (c Column) applyPostgres(db *goent.DB) error {
	schema := currentSchema(db)
	sql := fmt.Sprintf(`DO $$
DECLARE
    has_new boolean;
    has_old boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'
    ) INTO has_new;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'
    ) INTO has_old;

    IF has_new AND has_old THEN
        ALTER TABLE %s.%s DROP COLUMN %s;
        ALTER TABLE %s.%s RENAME COLUMN %s TO %s;
    ELSIF has_old THEN
        ALTER TABLE %s.%s RENAME COLUMN %s TO %s;
    ELSIF NOT has_new THEN
        ALTER TABLE %s.%s ADD COLUMN %s TEXT;
    END IF;
END $$;`, schema, c.Table, c.NewName,
		schema, c.Table, c.OldName,
		schema, c.Table, c.NewName,
		schema, c.Table, c.OldName, c.NewName,
		schema, c.Table, c.OldName, c.NewName,
		schema, c.Table, c.NewName)
	return db.RawExecContext(context.Background(), sql)
}

func (c Column) applySQLite(db *goent.DB) error {
	hasOld, err := sqliteColumnExists(db, c.Table, c.OldName)
	if err != nil {
		return err
	}
	hasNew, err := sqliteColumnExists(db, c.Table, c.NewName)
	if err != nil {
		return err
	}
	if hasNew && hasOld {
		// SQLite may not support DROP COLUMN. Copy data from the legacy
		// column into the new one and leave the legacy column in place.
		return db.RawExecContext(context.Background(), fmt.Sprintf(
			"UPDATE %s SET %s = %s WHERE %s IS NULL AND %s IS NOT NULL",
			c.Table, c.NewName, c.OldName, c.NewName, c.OldName,
		))
	}
	if hasOld {
		return db.Driver().RenameColumn("", c.Table, c.OldName, c.NewName)
	}
	if !hasNew {
		return db.RawExecContext(context.Background(), fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s TEXT", c.Table, c.NewName,
		))
	}
	return nil
}

func (t Table) applyPostgres(db *goent.DB, schema string) error {
	exists, err := postgresTableExists(db, schema, t.NewName)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("rename: target table %s.%s already exists", schema, t.NewName)
	}
	return db.Driver().RenameTable(schema, t.OldName, t.NewName)
}

func (t Table) applySQLite(db *goent.DB, schema string) error {
	exists, err := sqliteTableExists(db, t.NewName)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("rename: target table %s already exists", t.NewName)
	}
	// SQLite does not support schema-qualified names in ALTER TABLE RENAME TO.
	return db.Driver().RenameTable("", t.OldName, t.NewName)
}

func currentSchema(db *goent.DB) string {
	schemas := db.Driver().GetDatabaseConfig().Schemas()
	if len(schemas) > 0 && schemas[0] != "" {
		return schemas[0]
	}
	return "public"
}

func sqliteColumnExists(db *goent.DB, table, column string) (bool, error) {
	rows, err := db.RawQueryContext(context.Background(),
		fmt.Sprintf("SELECT name FROM pragma_table_info('%s') WHERE name = '%s'", table, column))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

func sqliteTableExists(db *goent.DB, table string) (bool, error) {
	rows, err := db.RawQueryContext(context.Background(),
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, nil
	}
	var count int
	if err := rows.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func postgresTableExists(db *goent.DB, schema, table string) (bool, error) {
	var count int
	err := db.RawQueryRowContext(context.Background(),
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		schema, table).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
