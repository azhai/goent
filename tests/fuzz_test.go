package goent_test

import (
	"fmt"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

// FuzzBuilderSelectSQL fuzzes the Builder's SELECT SQL generation
// with random table names to ensure no panics or empty SQL.
func FuzzBuilderSelectSQL(f *testing.F) {
	f.Add("animals")
	f.Add("t_user")
	f.Add("public.orders")
	f.Add("")

	f.Fuzz(func(t *testing.T, tableName string) {
		// Empty table name panics by design; recover and skip.
		if tableName == "" {
			defer func() {
				_ = recover()
			}()
			b := goent.GetBuilder()
			defer goent.PutBuilder(b)
			b.Type = model.SelectQuery
			b.SetTableName(tableName)
			_, _ = b.Build(false)
			return
		}

		b := goent.GetBuilder()
		defer goent.PutBuilder(b)

		b.Type = model.SelectQuery
		b.SetTableName(tableName)

		sql, args := b.Build(false)
		if sql == "" {
			t.Errorf("Empty SQL for table %q", tableName)
		}
		if len(args) != 0 {
			t.Errorf("Expected 0 args, got %d", len(args))
		}
	})
}

// FuzzBuilderDeleteSQL fuzzes the DeleteBuilder's DELETE SQL generation.
func FuzzBuilderDeleteSQL(f *testing.F) {
	f.Add("animals")
	f.Add("t_user")
	f.Add("public.orders")

	f.Fuzz(func(t *testing.T, tableName string) {
		b := goent.GetDeleteBuilder()
		defer goent.PutDeleteBuilder(b)

		b.SetTableName(tableName)

		sql, args := b.Build()
		if sql == "" {
			t.Errorf("Empty SQL for table %q", tableName)
		}
		if len(args) != 0 {
			t.Errorf("Expected 0 args, got %d", len(args))
		}
	})
}

// FuzzConditionEquals fuzzes the Equals condition builder.
func FuzzConditionEquals(f *testing.F) {
	f.Add("id", int64(1))
	f.Add("name", int64(0))
	f.Add("", int64(-1))

	f.Fuzz(func(t *testing.T, colName string, val int64) {
		cond := goent.Equals(&goent.Field{ColumnName: colName}, val)
		if cond.IsEmpty() {
			t.Errorf("Equals condition should not be empty")
		}
		if cond.Template == "" {
			t.Errorf("Template should not be empty")
		}
	})
}

// FuzzConditionIn fuzzes the In condition with various slice sizes.
// Fuzz only allows []byte as a slice type, so we use a size hint and fill
// the slice deterministically inside the test body.
func FuzzConditionIn(f *testing.F) {
	f.Add(int(0))
	f.Add(int(1))
	f.Add(int(5))

	f.Fuzz(func(t *testing.T, size int) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("In panicked for size=%d: %v", size, r)
			}
		}()
		if size < 0 || size > 1000 {
			t.Skip("size out of range")
		}
		vals := make([]int64, size)
		for i := range vals {
			vals[i] = int64(i)
		}
		cond := goent.In(&goent.Field{ColumnName: "id"}, vals)
		_ = cond
	})
}

// FuzzInBatch fuzzes the InBatch function with various batch sizes and ID counts.
func FuzzInBatch(f *testing.F) {
	f.Add(int(5), int(2))
	f.Add(int(1), int(500))
	f.Add(int(0), int(500))

	f.Fuzz(func(t *testing.T, count, batchSize int) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("InBatch panicked for count=%d batchSize=%d: %v", count, batchSize, r)
			}
		}()
		if count < 0 || count > 1000 || batchSize < 0 || batchSize > 10000 {
			t.Skip("args out of range")
		}
		ids := make([]int64, count)
		for i := range ids {
			ids[i] = int64(i)
		}
		cond := goent.InBatch(&goent.Field{ColumnName: "id"}, ids, batchSize)
		_ = cond
	})
}

// FuzzBuilderInsertSQL fuzzes INSERT SQL generation with various field counts.
func FuzzBuilderInsertSQL(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(5)

	f.Fuzz(func(t *testing.T, fieldCount int) {
		if fieldCount < 0 || fieldCount > 50 {
			t.Skip("fieldCount out of range")
		}

		b := goent.GetBuilder()
		defer goent.PutBuilder(b)

		b.Type = model.InsertQuery
		b.SetTableName("animals")

		for i := 0; i < fieldCount; i++ {
			fld := &goent.Field{
				TableAddr:  0,
				FieldId:    i,
				ColumnName: fmt.Sprintf("col_%d", i),
			}
			b.Changes[fld] = i
		}

		sql, args := b.Build(false)
		if fieldCount > 0 {
			if sql == "" {
				t.Errorf("Empty SQL for fieldCount=%d", fieldCount)
			}
			if len(args) != fieldCount {
				t.Errorf("Expected %d args, got %d", fieldCount, len(args))
			}
		}
	})
}
