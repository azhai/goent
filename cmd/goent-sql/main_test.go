package main

import (
	"testing"
)

func TestParseVarValue(t *testing.T) {
	// Scalar
	v, err := ParseVarValue("hello")
	if err != nil || !v.IsScalar() || v.Scalar() != "hello" {
		t.Errorf("expected scalar 'hello', got %v", v)
	}

	// JSON array
	v, err = ParseVarValue(`[1, 2, 3]`)
	if err != nil || !v.IsList() || v.Len() != 3 {
		t.Errorf("expected list [1,2,3], got %v", v)
	}

	// JSON object
	v, err = ParseVarValue(`{"host":"db","port":5432}`)
	if err != nil || !v.IsMap() || v.Len() != 1 {
		t.Errorf("expected map with 1 row, got %v", v)
	}
}

func TestVarStoreExpandVars(t *testing.T) {
	store := NewVarStore()
	store.SetScalar("name", "users")
	store.SetScalar("min_age", "18")

	// Simple scalar
	result := store.ExpandVars("SELECT * FROM $name WHERE age > $min_age")
	expected := "SELECT * FROM users WHERE age > 18"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestVarStoreExpandList(t *testing.T) {
	store := NewVarStore()
	v, _ := ParseVarValue(`[10, 20, 30]`)
	store.Set("ids", v)

	result := store.ExpandVars("SELECT * FROM t WHERE id IN $ids")
	expected := "SELECT * FROM t WHERE id IN (10, 20, 30)"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestVarStoreExpandMap(t *testing.T) {
	store := NewVarStore()
	v, _ := ParseVarValue(`{"host":"db","port":5432}`)
	store.Set("config", v)

	// Access key
	result := store.ExpandVars("$config.host")
	if result != "db" {
		t.Errorf("expected 'db', got %q", result)
	}
}

func TestVarStoreExpandQueryResult(t *testing.T) {
	store := NewVarStore()
	result := &QueryResult{
		Columns: []string{"id", "name"},
		Rows: []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		},
		Count: 2,
	}
	store.SetQueryResult("rows", result)

	// Access by index and key
	expanded := store.ExpandVars("$rows[0].name")
	if expanded != "Alice" {
		t.Errorf("expected 'Alice', got %q", expanded)
	}

	expanded = store.ExpandVars("$rows[1].id")
	if expanded != "2" {
		t.Errorf("expected '2', got %q", expanded)
	}
}

func TestParseForLoop(t *testing.T) {
	lines := []string{
		`\for row in $rows { SELECT * FROM t WHERE id = $row.id }`,
	}
	loop, endIdx, err := ParseForLoop(lines)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loop == nil {
		t.Fatal("expected loop, got nil")
	}
	if loop.VarName != "row" || loop.IterVar != "rows" {
		t.Errorf("expected var=row iter=rows, got var=%s iter=%s", loop.VarName, loop.IterVar)
	}
	if len(loop.Body) != 1 || loop.Body[0] != "SELECT * FROM t WHERE id = $row.id" {
		t.Errorf("unexpected body: %v", loop.Body)
	}
	if endIdx != 1 {
		t.Errorf("expected endIdx=1, got %d", endIdx)
	}
}

func TestParseForLoopMultiLine(t *testing.T) {
	lines := []string{
		`\for row in $rows {`,
		`  SELECT * FROM t WHERE id = $row.id`,
		`}`,
	}
	loop, endIdx, err := ParseForLoop(lines)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loop == nil {
		t.Fatal("expected loop, got nil")
	}
	if loop.VarName != "row" || loop.IterVar != "rows" {
		t.Errorf("expected var=row iter=rows, got var=%s iter=%s", loop.VarName, loop.IterVar)
	}
	if len(loop.Body) != 1 {
		t.Errorf("expected 1 body line, got %d: %v", len(loop.Body), loop.Body)
	}
	if endIdx != 3 {
		t.Errorf("expected endIdx=3, got %d", endIdx)
	}
}

func TestSortMapKeys(t *testing.T) {
	m := map[string]any{"zebra": 1, "alpha": 2, "middle": 3}
	sorted := sortMapKeys(m)
	keys := make([]string, 0, len(sorted))
	for k := range sorted {
		keys = append(keys, k)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	// Verify all keys present
	expected := map[string]bool{"zebra": true, "alpha": true, "middle": true}
	for _, k := range keys {
		if !expected[k] {
			t.Errorf("unexpected key: %s", k)
		}
	}
}

func TestToDBConfig(t *testing.T) {
	// PostgreSQL
	cfg, err := ToDBConfig("postgres://user:pass@localhost/db", "")
	if err != nil || !cfg.IsPg {
		t.Errorf("expected PostgreSQL, got %+v", cfg)
	}

	// SQLite
	cfg, err = ToDBConfig("test.db", "")
	if err != nil || cfg.IsPg {
		t.Errorf("expected SQLite, got %+v", cfg)
	}

	// Empty DSN
	_, err = ToDBConfig("", "")
	if err == nil {
		t.Error("expected error for empty DSN")
	}
}
