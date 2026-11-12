package plus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/plus"
)

func newShardTable(t *testing.T, db *testDB, cache *plus.TableCache) *plus.ShardedTable[ShardRecord] {
	t.Helper()
	return plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "user_id",
		&plus.ModuloHex{Bits: 4}, cache)
}

// TestShardedTable_Insert verifies the row lands in the sub-table selected by
// the shard key, not in the base table.
func TestShardedTable_Insert(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := newShardTable(t, db, cache)

	rec := &ShardRecord{UserID: 5, Name: "inserted"}
	if err := st.Insert(rec); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	rows := queryRawShardRecords(t, db, "t_shard_05", "inserted")
	if len(rows) != 1 {
		t.Fatalf("t_shard_05 has %d rows, want 1", len(rows))
	}
	if rows[0].UserID != 5 {
		t.Errorf("UserID = %d, want 5", rows[0].UserID)
	}
	// Neighbouring shards must stay empty.
	if got := queryRawShardRecords(t, db, "t_shard_04", "inserted"); len(got) != 0 {
		t.Errorf("t_shard_04 should be empty, got %d rows", len(got))
	}
}

// TestShardedTable_Insert_NoShardKey guards against a nil dereference when the
// shard key cannot be read; the row must be rejected instead of written to a
// random sub-table.
func TestShardedTable_Insert_NoShardKey(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := newShardTable(t, db, cache)

	if err := st.Insert(nil); !errors.Is(err, plus.ErrNoShardKey) {
		t.Errorf("Insert(nil) = %v, want ErrNoShardKey", err)
	}

	wrong := newShardTableWrongKey(t, db, cache)
	if err := wrong.Insert(&ShardRecord{UserID: 5}); !errors.Is(err, plus.ErrNoShardKey) {
		t.Errorf("Insert with unknown shard key = %v, want ErrNoShardKey", err)
	}
}

func newShardTableWrongKey(t *testing.T, db *testDB, cache *plus.TableCache) *plus.ShardedTable[ShardRecord] {
	t.Helper()
	return plus.NewShardedTable[ShardRecord](db.DB, "t_shard", "tenant_id",
		&plus.ModuloHex{Bits: 4}, cache)
}

// TestShardedTable_UpdateByPK makes sure Update targets the row by primary key
// while the shard value only selects the sub-table.
func TestShardedTable_UpdateByPK(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := newShardTable(t, db, cache)

	// Two rows in the same sub-table: shard value 5 and 21 both map to _05.
	keep := insertRawShardRecord(t, db, "t_shard_05", 5, "keep")
	target := insertRawShardRecord(t, db, "t_shard_05", 21, "target")

	if err := st.Update(int64(5), target, goent.Pair{Key: "name", Value: "updated"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if got := queryRawShardRecord(t, db, "t_shard_05", target); got == nil || got.Name != "updated" {
		t.Errorf("target row = %+v, want name %q", got, "updated")
	}
	if got := queryRawShardRecord(t, db, "t_shard_05", keep); got == nil || got.Name != "keep" {
		t.Errorf("neighbour row = %+v, want name %q (Update must not touch other rows)", got, "keep")
	}
}

// TestShardedTable_DeleteByPK checks the same separation for Delete.
func TestShardedTable_DeleteByPK(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := newShardTable(t, db, cache)

	keep := insertRawShardRecord(t, db, "t_shard_05", 5, "keep")
	target := insertRawShardRecord(t, db, "t_shard_05", 21, "target")

	if err := st.DeleteByPK(int64(5), target); err != nil {
		t.Fatalf("DeleteByPK: %v", err)
	}
	if got := queryRawShardRecord(t, db, "t_shard_05", target); got != nil {
		t.Errorf("row %d should be deleted, got %+v", target, got)
	}
	if got := queryRawShardRecord(t, db, "t_shard_05", keep); got == nil {
		t.Errorf("row %d must survive deleting a different primary key", keep)
	}
}

func TestShardedTable_FindByPK(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := setupShardTables(t, db)
	defer teardownShardTables(t, db, cache)
	st := newShardTable(t, db, cache)

	id := insertRawShardRecord(t, db, "t_shard_07", 7, "find_by_pk")

	got, err := st.FindByPK(int64(7), id)
	if err != nil {
		t.Fatalf("FindByPK: %v", err)
	}
	if got == nil || got.Name != "find_by_pk" {
		t.Fatalf("FindByPK = %+v, want name %q", got, "find_by_pk")
	}
}

// TestModuloHex_EdgeBits covers the shift-amount limits: a negative Bits used to
// panic and Bits >= 63 used to produce negative table names.
func TestModuloHex_EdgeBits(t *testing.T) {
	cases := []struct {
		bits   int
		val    int64
		expect string
	}{
		{0, 12345, "t_x_00"}, // no sharding: every value lands on _00
		{-4, 255, "t_x_00"},  // must not panic
		{4, -1, "t_x_0f"},    // -1 & 0x0f == 0x0f
		{8, 255, "t_x_ff"},
		{8, 256, "t_x_00"},
		{64, 255, "t_x_ff"},                // clamped mask instead of an overflowing shift
		{64, -255, "t_x_7fffffffffffff01"}, // never negative, unlike the old 1<<63 mask
	}
	for _, c := range cases {
		m := plus.ModuloHex{Bits: c.bits}
		if got := m.ResolveTableName("t_x", c.val); got != c.expect {
			t.Errorf("ModuloHex{Bits:%d}.ResolveTableName(%d) = %q, want %q",
				c.bits, c.val, got, c.expect)
		}
	}
}

func TestModuloHex_ShardCount(t *testing.T) {
	cases := []struct {
		bits int
		want int
	}{
		{0, 1},
		{-1, 1},
		{4, 16},
		{8, 256},
	}
	for _, c := range cases {
		if got := (plus.ModuloHex{Bits: c.bits}).ShardCount(); got != c.want {
			t.Errorf("ModuloHex{Bits:%d}.ShardCount() = %d, want %d", c.bits, got, c.want)
		}
	}
}

func TestTimeGranularity_ZeroValue(t *testing.T) {
	// A non-time shard value must not panic.
	if got := plus.ByDay.ResolveTableName("log", "not a time"); got != "log_00010101" {
		t.Errorf("ResolveTableName with a bad value = %q, want %q", got, "log_00010101")
	}
	if got := plus.ByDay.ResolveTableName("log", nil); got != "log_00010101" {
		t.Errorf("ResolveTableName(nil) = %q, want %q", got, "log_00010101")
	}
	tm := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if got, want := plus.ByMonth.ResolveTableName("log", tm), "log_202609"; got != want {
		t.Errorf("ResolveTableName() = %q, want %q", got, want)
	}
}

func TestTableCache_RefreshSchema_Sqlite(t *testing.T) {
	db := openTestDB(t)
	defer closeTestDB(t, db)
	cache := newShardTableCache(t, db)
	defer teardownShardTables(t, db, cache)

	conn := db.Driver().NewConnection()
	_ = conn

	if err := cache.Refresh(context.Background(), db.DB); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if cache.Size() == 0 {
		t.Error("Refresh should load existing tables")
	}
	if !cache.Exists("t_shard_00") {
		t.Error("Refresh should include t_shard_00")
	}
	if cache.IsExpired() {
		t.Error("cache should not be expired right after Refresh")
	}
	if got := cache.Match("t_shard_0*"); len(got) == 0 {
		t.Error("Match after Refresh returned nothing")
	}
}

func newShardTableCache(t *testing.T, db *testDB) *plus.TableCache {
	t.Helper()
	return setupShardTables(t, db)
}
