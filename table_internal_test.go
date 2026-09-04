package goent

import (
	"testing"
)

// TestTableInfo_GetBulkInsertFields verifies the cached bulk-insert field list
// excludes the auto-increment primary key and is computed exactly once.
func TestTableInfo_GetBulkInsertFields(t *testing.T) {
	info := &TableInfo{
		TableAddr:   1,
		TableName:   "t",
		ColumnNames: []string{"id", "name", "age"},
		Columns: map[string]*Column{
			"id":   {FieldId: 0, ColumnName: "id", IsPK: true, isAutoIncr: true},
			"name": {FieldId: 1, ColumnName: "name"},
			"age":  {FieldId: 2, ColumnName: "age"},
		},
		PrimaryKeys: []*Index{
			{Column: &Column{FieldId: 0, ColumnName: "id"}, IsAutoIncr: true},
		},
		sortedFields: []*Field{
			{TableAddr: 1, ColumnName: "id", FieldId: 0},
			{TableAddr: 1, ColumnName: "name", FieldId: 1},
			{TableAddr: 1, ColumnName: "age", FieldId: 2},
		},
	}

	fields := info.GetBulkInsertFields()
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2", len(fields))
	}
	if fields[0].ColumnName != "name" || fields[1].ColumnName != "age" {
		t.Errorf("fields = [%s %s], want [name age]", fields[0].ColumnName, fields[1].ColumnName)
	}

	// Must be cached: identical slice returned on subsequent calls.
	if again := info.GetBulkInsertFields(); len(again) != len(fields) || again[0] != fields[0] {
		t.Error("GetBulkInsertFields is not cached (returned a different slice)")
	}
}

// TestTableInfo_GetBulkInsertFields_NoAutoIncrPK ensures a non-auto-increment
// primary key is included in the bulk-insert field list.
func TestTableInfo_GetBulkInsertFields_NoAutoIncrPK(t *testing.T) {
	info := &TableInfo{
		TableAddr:   2,
		TableName:   "u",
		ColumnNames: []string{"code", "label"},
		Columns: map[string]*Column{
			"code":  {FieldId: 0, ColumnName: "code", IsPK: true, isAutoIncr: false},
			"label": {FieldId: 1, ColumnName: "label"},
		},
		PrimaryKeys: []*Index{
			{Column: &Column{FieldId: 0, ColumnName: "code"}, IsAutoIncr: false},
		},
		sortedFields: []*Field{
			{TableAddr: 2, ColumnName: "code", FieldId: 0},
			{TableAddr: 2, ColumnName: "label", FieldId: 1},
		},
	}

	fields := info.GetBulkInsertFields()
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2 (PK must be included)", len(fields))
	}
	if fields[0].ColumnName != "code" {
		t.Errorf("fields[0] = %q, want code", fields[0].ColumnName)
	}
}
