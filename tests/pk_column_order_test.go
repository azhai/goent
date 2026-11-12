package goent_test

import (
	"context"
	"testing"

	"github.com/azhai/goent"
)

// PKReorder 复现"物理列顺序与结构体字段顺序不一致"的表：
// 旧版手写 DDL 用 ALTER TABLE 追加的列会落到物理表末尾，
// 而 sortedFields（扫描目标）是按结构体字段顺序排的。
type PKReorder struct {
	ID        int64 `goe:"pk"`
	Code      string
	Label     string // 物理表里排在最后
	CreatedAt int64
}

// TestFindByPKColumnOrderMismatch 覆盖 PK 快路径的列顺序。
//
// 这条 SQL 曾经是 SELECT *，扫描目标却按结构体顺序排列：一旦物理表把
// label 放在 created_at 之后，第 3 列（INTEGER）就会扫进 string 字段、
// 第 4 列（TEXT ''）扫进 int64 字段，报
// "converting driver.Value type string ("") to a int64"。
func TestFindByPKColumnOrderMismatch(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteTestDB(t)

	err := db.RawExecContext(ctx, `CREATE TABLE t_pk_reorder (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		code TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		label TEXT NOT NULL DEFAULT ''
	)`)
	if err != nil {
		t.Fatalf("Expected create table, got error: %v", err)
	}
	err = db.RawExecContext(ctx,
		"INSERT INTO t_pk_reorder (id, code, created_at, label) VALUES (1, 'a1', 1700000000, 'first'), (2, 'b2', 1700000100, '')")
	if err != nil {
		t.Fatalf("Expected insert rows, got error: %v", err)
	}

	table := goent.SimpleTable[PKReorder](db.DB, "t_pk_reorder", "")

	got, err := table.FindByPK(1)
	if err != nil {
		t.Fatalf("Expected FindByPK, got error: %v", err)
	}
	if got.ID != 1 || got.Code != "a1" || got.Label != "first" || got.CreatedAt != 1700000000 {
		t.Errorf("Expected {1 a1 first 1700000000}, got %+v", *got)
	}

	// 空字符串默认值也必须原样扫回来，而不是被当成 int64 解析
	empty, err := table.FindByPK(2)
	if err != nil {
		t.Fatalf("Expected FindByPK on the empty-label row, got error: %v", err)
	}
	if empty.Label != "" || empty.CreatedAt != 1700000100 {
		t.Errorf(`Expected {2 b2 "" 1700000100}, got %+v`, *empty)
	}
}
