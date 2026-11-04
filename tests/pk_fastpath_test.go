package goent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

// PKRecord 挂在 schema "bakery" 下，用于复现 PK 快路径里带 schema 前缀的表名。
type PKRecord struct {
	ID   int64 `goe:"pk"`
	Name string
}

// TestPKFastPathUsesFormattedTableName 覆盖 PK 快路径的读（FindByPK）与写（DeleteByPK）。
// SQLite 忽略 schema，若缓存 SQL 沿用 getFullName() 拼出的 bakery.t_pk_record，
// 会直接报 no such table。
func TestPKFastPathUsesFormattedTableName(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteTestDB(t)

	// SQLite 忽略 schema，实际存在的表名只有 t_pk_record
	err := db.RawExecContext(ctx,
		"CREATE TABLE t_pk_record (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		t.Fatalf("Expected create table, got error: %v", err)
	}

	table := goent.SimpleTable[PKRecord](db.DB, "t_pk_record", "bakery")
	err = db.RawExecContext(ctx,
		"INSERT INTO t_pk_record (id, name) VALUES (1, 'cement'), (2, 'steel'), (3, 'sand')")
	if err != nil {
		t.Fatalf("Expected insert rows, got error: %v", err)
	}

	// 读快路径：回退成 getFullName() 时会报 no such table: bakery.t_pk_record
	got, err := table.FindByPK(1)
	if err != nil {
		t.Fatalf("Expected FindByPK, got error: %v", err)
	}
	if got.ID != 1 || got.Name != "cement" {
		t.Errorf("Expected record {1 cement}, got {%v %q}", got.ID, got.Name)
	}

	// 写快路径：同样走缓存的 deleteByPKSql
	if err = table.Delete().ByPK(2); err != nil {
		t.Fatalf("Expected DeleteByPK, got error: %v", err)
	}
	if _, err = table.FindByPK(2); !errors.Is(err, model.ErrNoRows) {
		t.Errorf("Expected deleted record to be gone, got error: %v", err)
	}
	if _, err = table.FindByPK(3); err != nil {
		t.Errorf("Expected remaining record, got error: %v", err)
	}
}
