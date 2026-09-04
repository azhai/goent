package goent

import (
	"reflect"
	"testing"
)

// TestBuildDestForeign verifies that buildDest resolves scan destinations for a
// main struct plus a materialized foreign struct using the precomputed plan
// (no per-row map). It exercises the reflection-based scanning path that
// GenScanDest models bypass.
func TestBuildDestForeign(t *testing.T) {
	type foreignRow struct {
		City string
	}
	type resultRow struct {
		ID   int64
		Name string
		Addr *foreignRow
	}

	valueOf := reflect.ValueOf(&resultRow{}).Elem()
	addrField, ok := valueOf.Type().FieldByName("Addr")
	if !ok {
		t.Fatal("Addr field not found")
	}

	ctx := &fetchContext{
		tblInfo: &TableInfo{TableName: "main", TableAddr: 1},
		fields: []*Field{
			{TableAddr: 1, ColumnName: "id", FieldId: 0},
			{TableAddr: 2, ColumnName: "city", FieldId: 0},
		},
		foreigns: []*Foreign{
			{MountField: "Addr", Reference: &Field{TableAddr: 2}},
		},
		// Pre-seed foreignOffsets so initForeignOffsets is a no-op (avoids the
		// table registry, which is irrelevant to the scan-destination logic).
		foreignOffsets: []foreignOffset{
			{mountIdx: addrField.Index[0], elemType: reflect.TypeOf(foreignRow{}), foreignTable: 2},
		},
		mainTableAddr: 1,
	}

	dest := ctx.buildDest(valueOf)
	if len(dest) != 2 {
		t.Fatalf("expected 2 scan destinations, got %d", len(dest))
	}

	// Foreign struct must have been materialized and mounted.
	if valueOf.FieldByIndex(addrField.Index).IsNil() {
		t.Fatal("foreign struct was not materialized into the mount field")
	}

	// Write through the returned pointers and confirm they land in the right
	// struct fields.
	*(dest[0].(*int64)) = 42
	*(dest[1].(*string)) = "London"
	row := valueOf.Interface().(resultRow)
	if row.ID != 42 {
		t.Errorf("main field not written: got ID=%d, want 42", row.ID)
	}
	if row.Addr == nil || row.Addr.City != "London" {
		t.Errorf("foreign field not written: got Addr=%+v", row.Addr)
	}
}

// TestBuildDestWildcard exercises the wildcard ("*") branch without panicking
// when the referenced table is not registered (it should simply be skipped).
func TestBuildDestWildcard(t *testing.T) {
	type resultRow struct {
		ID int64
	}

	valueOf := reflect.ValueOf(&resultRow{}).Elem()
	ctx := &fetchContext{
		tblInfo:       &TableInfo{TableName: "main", TableAddr: 1},
		fields:        []*Field{{TableAddr: 1, ColumnName: "*", FieldId: 0}},
		mainTableAddr: 1,
	}

	dest := ctx.buildDest(valueOf)
	if dest == nil {
		t.Fatal("expected a (possibly empty) destination slice, got nil")
	}
}
