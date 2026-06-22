package backup

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type testDB struct {
	*goent.DB
}

func openTestDB(t *testing.T) (*testDB, string) {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), t.Name()+".db")
	drv := sqlite.Open(dsn, sqlite.NewConfig(sqlite.Config{}))
	db, err := goent.Open[testDB](drv)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db, dsn
}

func TestEngine_FullSQLite(t *testing.T) {
	db, dsn := openTestDB(t)
	defer goent.Close(db)
	ctx := context.Background()

	_ = db.RawExecContext(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	_ = db.RawExecContext(ctx, "INSERT INTO items (name) VALUES ('hello')")

	dir := t.TempDir()
	engine := New(db.DB, Config{Dir: dir, DSN: dsn})
	res := engine.Full(ctx, "test")
	if res.Err != nil {
		t.Fatalf("full backup failed: %v", res.Err)
	}
	if res.Path == "" {
		t.Fatal("expected backup path")
	}
	if res.Size == 0 {
		t.Fatal("expected non-zero backup size")
	}

	// Restore into a fresh file database.
	restoreDir := t.TempDir()
	restorePath := restoreDir + "/restored.db"
	restoreEngine := New(db.DB, Config{Dir: restoreDir, DSN: "file:" + restorePath, DBPath: restorePath})
	if err := restoreEngine.Restore(ctx, res.Path, false); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Open the restored database and verify the data.
	restoredDrv := sqlite.Open(restorePath, sqlite.NewConfig(sqlite.Config{}))
	restoredDB, err := goent.Open[testDB](restoredDrv)
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer goent.Close(restoredDB)

	var name string
	err = restoredDB.RawQueryRowContext(ctx, "SELECT name FROM items WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("query restored db: %v", err)
	}
	if name != "hello" {
		t.Fatalf("expected hello, got %s", name)
	}
}

func TestEngine_FullRejectsMissingDSN(t *testing.T) {
	db, _ := openTestDB(t)
	defer goent.Close(db)
	ctx := context.Background()

	engine := New(db.DB, Config{Dir: t.TempDir()})
	res := engine.Full(ctx, "test")
	if res.Err == nil {
		t.Fatal("expected error when DSN/DBPath is missing")
	}
}

func Test_sqliteDBPath(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"data.db", "data.db"},
		{"file:data.db", "data.db"},
		{"file:data.db?mode=rwc", "data.db"},
		{"/absolute/data.db", "/absolute/data.db"},
		{":memory:", ""},
		{"file::memory:?mode=memory&cache=shared", ""},
	}
	for _, tc := range tests {
		if got := sqliteDBPath(tc.dsn); got != tc.want {
			t.Errorf("sqliteDBPath(%q) = %q, want %q", tc.dsn, got, tc.want)
		}
	}
}
