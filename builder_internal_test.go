package goent

import (
	"reflect"
	"testing"

	"github.com/azhai/goent/model"
)

// TestBuildBulkInsertFlat verifies that the flattened InsertValues []any (row-major)
// is rendered into correct multi-row VALUES SQL with positional args, after the
// [][]any -> []any flattening refactor.
func TestBuildBulkInsertFlat(t *testing.T) {
	b := NewBuilder().(*Builder)
	b.Type = model.InsertAllQuery
	b.SetTableName("book")
	b.VisitFields = []*Field{
		{ColumnName: "isbn"},
		{ColumnName: "title"},
		{ColumnName: "author"},
	}
	// Flat, row-major: (a,b,c), (d,e,f)
	b.InsertValues = []any{"a", "b", "c", "d", "e", "f"}

	sql, args := b.Build(false)
	wantSQL := "INSERT INTO book(isbn, title, author) VALUES ($1,$2,$3), ($4,$5,$6)"
	if sql != wantSQL {
		t.Fatalf("bulk insert SQL = %q, want %q", sql, wantSQL)
	}
	wantArgs := []any{"a", "b", "c", "d", "e", "f"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("bulk insert args = %v, want %v", args, wantArgs)
	}
}

// TestBuildBulkInsertFlatSingleRow covers the edge case of exactly one row so the
// trailing "), (" separator is never emitted.
func TestBuildBulkInsertFlatSingleRow(t *testing.T) {
	b := NewBuilder().(*Builder)
	b.Type = model.InsertAllQuery
	b.SetTableName("book")
	b.VisitFields = []*Field{{ColumnName: "isbn"}, {ColumnName: "title"}}
	b.InsertValues = []any{"x", "y"}

	sql, args := b.Build(false)
	wantSQL := "INSERT INTO book(isbn, title) VALUES ($1,$2)"
	if sql != wantSQL {
		t.Fatalf("single-row bulk SQL = %q, want %q", sql, wantSQL)
	}
	if !reflect.DeepEqual(args, []any{"x", "y"}) {
		t.Fatalf("single-row bulk args = %v", args)
	}
}

// TestBuildBulkInsertEmpty ensures a missing InsertValues slice is handled without
// panicking and yields no args (the early-return guard).
func TestBuildBulkInsertEmpty(t *testing.T) {
	b := NewBuilder().(*Builder)
	b.Type = model.InsertAllQuery
	b.SetTableName("book")
	b.VisitFields = []*Field{{ColumnName: "isbn"}}
	b.InsertValues = nil

	_, args := b.Build(false)
	if len(args) != 0 {
		t.Fatalf("empty bulk insert args = %v, want empty", args)
	}
}
