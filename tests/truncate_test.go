package goent_test

import (
	"context"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type truncateTestDB struct {
	*goent.DB
}

// TruncateRecord 挂在 schema "bakery" 下，用于复现带 schema 前缀的表名。
type TruncateRecord struct {
	ID   int64 `goe:"pk"`
	Name string
}

// TestTruncateUsesFormattedTableName 验证 Truncate 拼装 SQL 时使用按驱动格式化后的表名，
// 而不是未经格式化的 schema.table。SQLite 不支持 schema，若沿用 bakery.t_raw_material
// 这样的表名会清表失败。
func TestTruncateUsesFormattedTableName(t *testing.T) {
	ctx := context.Background()
	drv := sqlite.OpenInMemory(sqlite.NewConfig(sqlite.Config{}))
	db, err := goent.Open[truncateTestDB](drv)
	if err != nil {
		t.Fatalf("Expected sqlite db, got error: %v", err)
	}
	defer drv.Close()

	// SQLite 忽略 schema，实际存在的表名只有 t_raw_material
	err = db.RawExecContext(ctx,
		"CREATE TABLE t_raw_material (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		t.Fatalf("Expected create table, got error: %v", err)
	}

	table := goent.SimpleTable[TruncateRecord](db.DB, "t_raw_material", "bakery")
	if name := table.GetFormattedName(); name != `"t_raw_material"` {
		t.Fatalf("Expected formatted table name %q, got %q", `"t_raw_material"`, name)
	}

	for _, name := range []string{"cement", "steel", "sand"} {
		if err = db.RawExecContext(ctx,
			"INSERT INTO t_raw_material (name) VALUES (?)", name); err != nil {
			t.Fatalf("Expected insert row, got error: %v", err)
		}
	}

	if err = table.TruncateContext(ctx); err != nil {
		t.Fatalf("Expected truncate, got error: %v", err)
	}

	var count int64
	if err = db.RawQueryRowContext(ctx,
		"SELECT COUNT(*) FROM t_raw_material").Scan(&count); err != nil {
		t.Fatalf("Expected count, got error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected t_raw_material to be empty after truncate, got %v rows", count)
	}
}
