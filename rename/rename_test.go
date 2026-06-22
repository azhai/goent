package rename_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/rename"
)

type testDB struct {
	*goent.DB
}

func openTestDB(t *testing.T) *testDB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	drv := sqlite.Open(dsn, sqlite.NewConfig(sqlite.Config{}))
	db, err := goent.Open[testDB](drv)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func TestColumn_RenameOldToNew(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE col_test (id INTEGER PRIMARY KEY, old_col INTEGER)")
	_ = db.RawExecContext(ctx, "INSERT INTO col_test (old_col) VALUES (42)")

	err := rename.Column{Table: "col_test", OldName: "old_col", NewName: "new_col"}.Apply(db.DB)
	if err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	var val int
	err = db.RawQueryRowContext(ctx, "SELECT new_col FROM col_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query new_col: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestColumn_MergeWhenBothExist(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE col_merge (id INTEGER PRIMARY KEY, new_col INTEGER, old_col INTEGER)")
	_ = db.RawExecContext(ctx, "INSERT INTO col_merge (id, old_col) VALUES (1, 99)")

	err := rename.Column{Table: "col_merge", OldName: "old_col", NewName: "new_col"}.Apply(db.DB)
	if err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	var val int
	err = db.RawQueryRowContext(ctx, "SELECT new_col FROM col_merge WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query new_col: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestColumn_NoOpWhenNewExists(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE col_noop (id INTEGER PRIMARY KEY, new_col INTEGER)")
	_ = db.RawExecContext(ctx, "INSERT INTO col_noop (new_col) VALUES (7)")

	err := rename.Column{Table: "col_noop", OldName: "old_col", NewName: "new_col"}.Apply(db.DB)
	if err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	var val int
	err = db.RawQueryRowContext(ctx, "SELECT new_col FROM col_noop WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("query new_col: %v", err)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

func TestTable_Rename(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE old_table (id INTEGER PRIMARY KEY)")

	err := rename.Table{OldName: "old_table", NewName: "new_table"}.Apply(db.DB)
	if err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	var count int
	err = db.RawQueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='new_table'").Scan(&count)
	if err != nil {
		t.Fatalf("query new_table: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected new_table to exist, got %d", count)
	}
}

func TestTable_RejectExistingTarget(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE existing_old (id INTEGER PRIMARY KEY)")
	_ = db.RawExecContext(ctx, "CREATE TABLE existing_new (id INTEGER PRIMARY KEY)")

	err := rename.Table{OldName: "existing_old", NewName: "existing_new"}.Apply(db.DB)
	if err == nil {
		t.Fatal("expected error when target table exists")
	}
}
