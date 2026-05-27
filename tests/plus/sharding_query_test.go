package plus_test

import (
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/plus"
)

func TestShardedTable_SelectAll(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	insertRawShardRecord(t, db, "t_shard_00", 0, "a")
	insertRawShardRecord(t, db, "t_shard_01", 1, "b")
	insertRawShardRecord(t, db, "t_shard_0f", 15, "c")

	var totalCount int64
	err := st.EachTable(func(tbl *goent.Table[ShardRecord]) error {
		rows := queryRawShardRecords(t, db, tbl.TableName, "")
		totalCount += int64(len(rows))
		return nil
	})
	if err != nil {
		t.Fatalf("EachTable: %v", err)
	}
	if totalCount != 3 {
		t.Errorf("total rows across tables = %d, want 3", totalCount)
	}
}

func TestShardedTable_CountAll(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	insertRawShardRecord(t, db, "t_shard_00", 0, "x")
	insertRawShardRecord(t, db, "t_shard_00", 16, "y")
	insertRawShardRecord(t, db, "t_shard_0f", 15, "z")

	count, err := st.CountAll()
	if err != nil {
		t.Fatalf("CountAll: %v", err)
	}
	if count != 3 {
		t.Errorf("CountAll = %d, want 3", count)
	}
}

func TestShardedTable_EachTable(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)

	var tableNames []string
	err := st.EachTable(func(tbl *goent.Table[ShardRecord]) error {
		tableNames = append(tableNames, tbl.TableName)
		return nil
	})
	if err != nil {
		t.Fatalf("EachTable: %v", err)
	}
	if len(tableNames) != 16 {
		t.Errorf("EachTable visited %d tables, want 16", len(tableNames))
	}
}
