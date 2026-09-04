package goent

import (
	"testing"
)

// TestField_String_Cache verifies the precomputed qualifiedName cache path:
// String() returns it directly (no registry lookup) and Function composition
// still applies on top of the cached name.
func TestField_String_Cache(t *testing.T) {
	// Cached qualified name is returned directly.
	f := &Field{ColumnName: "id", qualifiedName: "t.id"}
	if got := f.String(); got != "t.id" {
		t.Errorf("String() with cache = %q, want %q", got, "t.id")
	}

	// Function composition still applies to the cached name.
	ff := &Field{ColumnName: "id", qualifiedName: "t.id", Function: "COUNT(%s)"}
	if got := ff.String(); got != "COUNT(t.id)" {
		t.Errorf("String() with Function = %q, want %q", got, "COUNT(t.id)")
	}
}

// TestField_String_Fallback covers the non-cached paths so a regression in the
// fallback (registry lookup) is caught: with no table it returns just the column
// name, and with an unknown table it returns "".
func TestField_String_Fallback(t *testing.T) {
	// No cache, no table: GetTableFieldName(0, name) returns just the column.
	f := &Field{ColumnName: "id"}
	if got := f.String(); got != "id" {
		t.Errorf("String() fallback (no table) = %q, want %q", got, "id")
	}

	// Unknown table with no cache → registry lookup fails → empty string.
	f2 := &Field{TableAddr: 99, ColumnName: "id"}
	if got := f2.String(); got != "" {
		t.Errorf("String() with unregistered table = %q, want %q", got, "")
	}
}
