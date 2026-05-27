package plus_test

import (
	"context"
	"testing"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/plus"
)

type ShardRecord struct {
	ID     int64 `goe:"pk"`
	UserID int64
	Name   string
}

type testDB struct {
	*goent.DB
}

func openTestDB(t *testing.T) *testDB {
	t.Helper()
	drv := sqlite.OpenInMemory(sqlite.NewConfig(sqlite.Config{}))
	db, err := goent.Open[testDB](drv)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func closeTestDB(t *testing.T, db *testDB) {
	t.Helper()
	if db != nil && db.Driver() != nil {
		db.Driver().Close()
	}
}

func setupShardTables(t *testing.T, db *testDB) *plus.TableCache {
	t.Helper()
	cache := plus.NewTableCache(time.Hour)
	ctx := context.Background()
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()
	for i := 0; i < 16; i++ {
		tableName := (&plus.ModuloHex{Bits: 4}).ResolveTableName("t_shard", int64(i))
		sqlStr := "CREATE TABLE IF NOT EXISTS " + tableName +
			" (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT)"
		qr := model.CreateQuery(sqlStr, nil)
		if err := qr.WrapExec(ctx, conn, cfg); err != nil {
			t.Fatalf("create table %s: %v", tableName, err)
		}
		cache.Add(tableName)
	}
	return cache
}

func teardownShardTables(t *testing.T, db *testDB, cache *plus.TableCache) {
	t.Helper()
	ctx := context.Background()
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()
	pattern := "t_shard_*"
	names := cache.Match(pattern)
	for _, name := range names {
		qr := model.CreateQuery("DROP TABLE IF EXISTS "+name, nil)
		_ = qr.WrapExec(ctx, conn, cfg)
	}
}

func insertRawShardRecord(t *testing.T, db *testDB, tableName string, userID int64, name string) int64 {
	t.Helper()
	ctx := context.Background()
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()
	qr := model.CreateQuery("INSERT INTO "+tableName+" (user_id, name) VALUES (?, ?)", []any{userID, name})
	if err := qr.WrapExec(ctx, conn, cfg); err != nil {
		t.Fatalf("raw insert into %s: %v", tableName, err)
	}
	rowQr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	row, err := rowQr.WrapQueryRow(ctx, conn, cfg)
	if err != nil {
		t.Fatalf("last_insert_rowid: %v", err)
	}
	var id int64
	row.Scan(&id)
	return id
}

func queryRawShardRecord(t *testing.T, db *testDB, tableName string, pkID int64) *ShardRecord {
	t.Helper()
	ctx := context.Background()
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()
	qr := model.CreateQuery("SELECT id, user_id, name FROM "+tableName+" WHERE id = ?", []any{pkID})
	rows, err := qr.WrapQuery(ctx, conn, cfg)
	if err != nil || !rows.Next() {
		return nil
	}
	defer rows.Close()
	var rec ShardRecord
	if err := rows.Scan(&rec.ID, &rec.UserID, &rec.Name); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return &rec
}

func queryRawShardRecords(t *testing.T, db *testDB, tableName string, name string) []*ShardRecord {
	t.Helper()
	ctx := context.Background()
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()

	var sqlStr string
	var args []any
	if name != "" {
		sqlStr = "SELECT id, user_id, name FROM " + tableName + " WHERE name = ?"
		args = []any{name}
	} else {
		sqlStr = "SELECT id, user_id, name FROM " + tableName
	}

	qr := model.CreateQuery(sqlStr, args)
	rows, err := qr.WrapQuery(ctx, conn, cfg)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var result []*ShardRecord
	for rows.Next() {
		var rec ShardRecord
		if err := rows.Scan(&rec.ID, &rec.UserID, &rec.Name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result = append(result, &rec)
	}
	return result
}

func TestNewShardedTable(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := plus.NewTableCache(time.Hour)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)
	if st == nil {
		t.Fatal("NewShardedTable returned nil")
	}
	if st.BaseName() != "t_shard" {
		t.Errorf("baseName = %q, want %q", st.BaseName(), "t_shard")
	}
}

func TestShardedTable_ResolveTable(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := plus.NewTableCache(time.Hour)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	tests := []struct {
		val    int64
		expect string
	}{
		{0, "t_shard_00"},
		{1, "t_shard_01"},
		{15, "t_shard_0f"},
		{31, "t_shard_0f"},
	}
	for _, tt := range tests {
		tbl := st.ResolveTable(tt.val)
		if tbl == nil {
			t.Fatalf("ResolveTable(%d) returned nil", tt.val)
		}
		if tbl.TableName != tt.expect {
			t.Errorf("ResolveTable(%d).TableName = %q, want %q", tt.val, tbl.TableName, tt.expect)
		}
	}

	tbl1 := st.ResolveTable(int64(255))
	tbl2 := st.ResolveTable(int64(255))
	if tbl1 != tbl2 {
		t.Error("ResolveTable should return cached instance")
	}
}

func TestShardedTable_FindByPK_Routing(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	_ = plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	id0 := insertRawShardRecord(t, db, "t_shard_00", 0, "user_00")
	_ = insertRawShardRecord(t, db, "t_shard_0f", 15, "user_0f")
	idFF := insertRawShardRecord(t, db, "t_shard_0f", 255, "user_ff")

	found := queryRawShardRecord(t, db, "t_shard_00", id0)
	if found == nil || found.Name != "user_00" {
		t.Errorf("found Name = %v, want %q", found, "user_00")
	}

	found = queryRawShardRecord(t, db, "t_shard_0f", idFF)
	if found == nil || found.Name != "user_ff" {
		t.Errorf("found Name = %v, want %q", found, "user_ff")
	}
}

func TestShardedTable_FindOne_Raw(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	insertRawShardRecord(t, db, "t_shard_05", 5, "find_me")

	tableName := st.ResolveTable(int64(5)).TableName
	rows := queryRawShardRecords(t, db, tableName, "find_me")
	if len(rows) == 0 || rows[0].Name != "find_me" {
		t.Errorf("found Name = %v, want %q", rows, "find_me")
	}
}
