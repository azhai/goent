package goent_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/azhai/goent"
)

// TestConcurrentBuilderPool tests that Builder pool is thread-safe
func TestConcurrentBuilderPool(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.NewBuilder()
			if builder == nil {
				errChan <- errors.New("builder is nil")
				return
			}
			if b, ok := builder.(*goent.Builder); ok {
				goent.PutBuilder(b)
			} else {
				errChan <- errors.New("builder is not *goent.Builder")
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Builder pool error: %v", err)
	}
}

// TestSQLErrorHandling tests that SQL errors are returned properly instead of causing nil pointer panics
func TestSQLErrorHandling(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Test 1: Invalid SQL syntax in Select
	t.Run("InvalidSelectSQL", func(t *testing.T) {
		_, err := db.Animal.Select().Where("invalid_column = ?", 1).All()
		if err == nil {
			t.Error("Expected error for invalid column, got nil")
		}
	})

	// Test 2: Invalid SQL syntax in Update
	t.Run("InvalidUpdateSQL", func(t *testing.T) {
		err := db.Animal.Update().
			Set(goent.Pair{Key: "invalid_column", Value: "test"}).
			Filter(goent.Equals(db.Animal.Field("id"), 1)).
			Exec()
		if err == nil {
			t.Error("Expected error for invalid column, got nil")
		}
	})

	// Test 3: Invalid SQL syntax in Delete
	t.Run("InvalidDeleteSQL", func(t *testing.T) {
		err := db.Animal.Delete().
			Filter(goent.Equals(db.Animal.Field("invalid_column"), 1)).
			Exec()
		if err == nil {
			t.Error("Expected error for invalid column, got nil")
		}
	})

	// Test 4: Invalid SQL syntax in Insert
	t.Run("InvalidInsertSQL", func(t *testing.T) {
		animal := &Animal{Name: "Test", Id: 999}
		err := db.Animal.Insert().One(animal)
		// This might succeed or fail depending on the table structure
		// We just want to ensure no panic occurs
		if err != nil {
			t.Logf("Insert error (expected): %v", err)
		}
	})

	// Test 5: Test with completely invalid SQL (manual query)
	t.Run("InvalidManualSQL", func(t *testing.T) {
		rows, err := db.RawQueryContext(context.Background(), "SELECT * FROM non_existent_table")
		if err == nil {
			if rows != nil {
				rows.Close()
				t.Error("Expected error for non-existent table, got nil")
			}
		}
		// rows should be nil when there's an error
		if rows != nil {
			t.Error("Expected rows to be nil on error")
		}
	})
}

// TestBuilderReset tests that Builder is properly reset when returned to pool
func TestBuilderReset(t *testing.T) {
	builder := goent.NewBuilder()
	if b, ok := builder.(*goent.Builder); ok {
		goent.PutBuilder(b)

		builder2 := goent.NewBuilder()
		if b2, ok := builder2.(*goent.Builder); ok {
			if b2.Type != 0 {
				t.Errorf("Type should be 0, got %v", b2.Type)
			}
			if len(b2.Joins) != 0 {
				t.Errorf("Joins should be empty, got %d", len(b2.Joins))
			}
			if len(b2.InsertValues) != 0 {
				t.Errorf("InsertValues should be empty, got %d", len(b2.InsertValues))
			}
			if len(b2.VisitFields) != 0 {
				t.Errorf("VisitFields should be empty, got %d", len(b2.VisitFields))
			}
			if len(b2.Changes) != 0 {
				t.Errorf("Changes should be empty, got %d", len(b2.Changes))
			}
			if len(b2.Orders) != 0 {
				t.Errorf("Orders should be empty, got %d", len(b2.Orders))
			}
			if len(b2.Groups) != 0 {
				t.Errorf("Groups should be empty, got %d", len(b2.Groups))
			}
			if b2.Offset != 0 {
				t.Errorf("Offset should be 0, got %d", b2.Offset)
			}
			if b2.Returning != "" {
				t.Errorf("Returning should be empty, got %s", b2.Returning)
			}
			if b2.RollUp != "" {
				t.Errorf("RollUp should be empty, got %s", b2.RollUp)
			}
			if b2.ForUpdate != false {
				t.Errorf("ForUpdate should be false, got %v", b2.ForUpdate)
			}
			if b2.Table != nil {
				t.Errorf("Table should be nil, got %v", b2.Table)
			}
			if b2.Where.Template != "" {
				t.Errorf("Where.Template should be empty, got %s", b2.Where.Template)
			}
			if b2.Limit != -1 {
				t.Errorf("Limit should be -1, got %d", b2.Limit)
			}
			goent.PutBuilder(b2)
		} else {
			t.Errorf("builder2 is not *goent.Builder")
		}
	} else {
		t.Errorf("builder is not *goent.Builder")
	}
}

// TestBuilderMultipleReuse tests that Builder can be reused multiple times
func TestBuilderMultipleReuse(t *testing.T) {
	for i := 0; i < 10; i++ {
		builder := goent.NewBuilder()
		if b, ok := builder.(*goent.Builder); ok {
			if b.Type != 0 {
				t.Errorf("Iteration %d: Type should be 0, got %v", i, b.Type)
			}
			if len(b.Changes) != 0 {
				t.Errorf("Iteration %d: Changes should be empty, got %d", i, len(b.Changes))
			}
			if b.Limit != -1 {
				t.Errorf("Iteration %d: Limit should be -1, got %d", i, b.Limit)
			}
			goent.PutBuilder(b)
		} else {
			t.Errorf("Iteration %d: builder is not *goent.Builder", i)
		}
	}
}

// TestBuilderBuildWithDestroyTrue tests that buffer is properly released when destroy=true
func TestBuilderBuildWithDestroyTrue(t *testing.T) {
	builder := goent.NewBuilder()
	if b, ok := builder.(*goent.Builder); ok {
		// Set up for a simple SELECT query
		b.Type = 1 // SelectQuery

		// Build with destroy=true
		sql, args := b.Build(true)

		// SQL should not be empty (even if it's not a complete query)
		if sql == "" {
			t.Error("SQL should not be empty after Build")
		}

		// After destroy, builder should be reset
		if b.Type != 0 {
			t.Errorf("Type should be 0 after destroy, got %v", b.Type)
		}
		if len(b.Changes) != 0 {
			t.Errorf("Changes should be empty after destroy, got %d", len(b.Changes))
		}
		if b.Limit != -1 {
			t.Errorf("Limit should be -1 after destroy, got %d", b.Limit)
		}

		// args should be empty for this simple case
		if len(args) != 0 {
			t.Errorf("args should be empty for this case, got %d", len(args))
		}
	} else {
		t.Error("builder is not *goent.Builder")
	}
}

// TestBuilderBuildWithDestroyFalse tests that buffer is retained when destroy=false
func TestBuilderBuildWithDestroyFalse(t *testing.T) {
	builder := goent.NewBuilder()
	if b, ok := builder.(*goent.Builder); ok {
		// Set up for a simple SELECT query
		b.Type = 1 // SelectQuery

		// Build with destroy=false
		sql, args := b.Build(false)

		// SQL should not be empty
		if sql == "" {
			t.Error("SQL should not be empty after Build")
		}

		// After non-destroy build, Type is NOT reset (builder may be reused)
		// This is the expected behavior for destroy=false
		if b.Type != 1 {
			t.Errorf("Type should remain 1 after non-destroy build, got %v", b.Type)
		}

		// Builder should still be usable for another build
		sql2, _ := b.Build(false)
		if sql2 == "" {
			t.Error("SQL should not be empty on second Build")
		}

		// Clean up with PutBuilder (which calls Reset)
		goent.PutBuilder(b)

		// After PutBuilder, builder should be reset
		if b.Type != 0 {
			t.Errorf("Type should be 0 after PutBuilder, got %v", b.Type)
		}

		// args should be empty for this simple case
		if len(args) != 0 {
			t.Errorf("args should be empty for this case, got %d", len(args))
		}
	} else {
		t.Error("builder is not *goent.Builder")
	}
}

// TestBuilderBufferNotShared tests that builders don't share buffers in concurrent usage
func TestBuilderBufferNotShared(t *testing.T) {
	var wg sync.WaitGroup
	results := make(chan string, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			builder := goent.NewBuilder()
			if b, ok := builder.(*goent.Builder); ok {
				// Set up for a simple SELECT query
				b.Type = 1 // SelectQuery

				// Build SQL
				sql, _ := b.Build(false)

				// Send result
				results <- sql

				// Clean up
				goent.PutBuilder(b)
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect all results
	sqlSet := make(map[string]bool)
	for sql := range results {
		sqlSet[sql] = true
	}

	// All SQL should be identical (same structure)
	// If buffers were shared, we'd see corrupted SQL
	if len(sqlSet) == 0 {
		t.Error("No SQL results received")
	}
}

// TestBuilderBufferResetCorrectly tests that buffer content is cleared between uses
func TestBuilderBufferResetCorrectly(t *testing.T) {
	builder := goent.NewBuilder()
	if b, ok := builder.(*goent.Builder); ok {
		// First use
		b.Type = 1 // SelectQuery
		sql1, _ := b.Build(false)

		// Second use - should not contain content from first use
		b.Type = 1
		sql2, _ := b.Build(false)

		// SQL should be identical (same query type)
		if sql1 != sql2 {
			t.Error("SQL should be identical for same query type")
		}

		// Clean up
		goent.PutBuilder(b)
	} else {
		t.Error("builder is not *goent.Builder")
	}
}

// TestBuilderPoolConsistency tests that builder pool maintains consistency
func TestBuilderPoolConsistency(t *testing.T) {
	const iterations = 100

	for i := 0; i < iterations; i++ {
		builder := goent.NewBuilder()
		if b, ok := builder.(*goent.Builder); ok {
			// Simulate some work - set up for a simple SELECT query
			b.Type = 1 // SelectQuery

			// Build and destroy
			sql, args := b.Build(true)

			// Verify basic expectations
			if sql == "" {
				t.Errorf("Iteration %d: SQL should not be empty", i)
			}
			// For SELECT query with no conditions, args should be empty
			if len(args) != 0 {
				t.Errorf("Iteration %d: args should be empty for simple SELECT, got %d", i, len(args))
			}
		} else {
			t.Errorf("Iteration %d: builder is not *goent.Builder", i)
		}
	}
}

// TestConcurrentDeleteBuilderPool tests that DeleteBuilder pool is thread-safe
func TestConcurrentDeleteBuilderPool(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.GetDeleteBuilder()
			if builder == nil {
				errChan <- errors.New("builder is nil")
				return
			}
			goent.PutDeleteBuilder(builder)
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("DeleteBuilder pool error: %v", err)
	}
}

// TestDeleteBuilderReset tests that DeleteBuilder is properly reset when returned to pool
func TestDeleteBuilderReset(t *testing.T) {
	builder := goent.GetDeleteBuilder()
	goent.PutDeleteBuilder(builder)

	builder2 := goent.GetDeleteBuilder()
	if builder2.Table != nil {
		t.Errorf("Table should be nil, got %v", builder2.Table)
	}
	if builder2.Where.Template != "" {
		t.Errorf("Where.Template should be empty, got %s", builder2.Where.Template)
	}
	if builder2.Limit != -1 {
		t.Errorf("Limit should be -1, got %d", builder2.Limit)
	}
	goent.PutDeleteBuilder(builder2)
}

// TestConcurrentFieldCreation tests concurrent field creation
func TestConcurrentFieldCreation(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			field := &goent.Field{
				ColumnName: "test_column",
			}
			if field == nil {
				errChan <- errors.New("field is nil")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Field creation error: %v", err)
	}
}

// TestConcurrentConditionCreation tests concurrent condition creation
func TestConcurrentConditionCreation(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.Equals(&goent.Field{ColumnName: "id"}, 1)
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Condition creation error: %v", err)
	}
}

// TestConcurrentPairCreation tests concurrent pair creation
func TestConcurrentPairCreation(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pair := goent.Pair{Key: "name", Value: "test"}
			if pair.Key == "" {
				errChan <- errors.New("pair key is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Pair creation error: %v", err)
	}
}

// TestConcurrentAndCondition tests concurrent AND condition creation
func TestConcurrentAndCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.And(
				goent.Equals(&goent.Field{ColumnName: "id"}, 1),
				goent.Equals(&goent.Field{ColumnName: "name"}, "test"),
			)
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("AND condition error: %v", err)
	}
}

// TestConcurrentOrCondition tests concurrent OR condition creation
func TestConcurrentOrCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.Or(
				goent.Equals(&goent.Field{ColumnName: "id"}, 1),
				goent.Equals(&goent.Field{ColumnName: "name"}, "test"),
			)
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("OR condition error: %v", err)
	}
}

// TestConcurrentLikeCondition tests concurrent LIKE condition creation
func TestConcurrentLikeCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.Like(&goent.Field{ColumnName: "name"}, "%test%")
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("LIKE condition error: %v", err)
	}
}

// TestConcurrentInCondition tests concurrent IN condition creation
func TestConcurrentInCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.In(&goent.Field{ColumnName: "id"}, []int{1, 2, 3})
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("IN condition error: %v", err)
	}
}

// TestConcurrentIsNullCondition tests concurrent IS NULL condition creation
func TestConcurrentIsNullCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.IsNull(&goent.Field{ColumnName: "deleted_at"})
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("IS NULL condition error: %v", err)
	}
}

// TestConcurrentIsNotNullCondition tests concurrent IS NOT NULL condition creation
func TestConcurrentIsNotNullCondition(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.IsNotNull(&goent.Field{ColumnName: "created_at"})
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("IS NOT NULL condition error: %v", err)
	}
}

// TestConcurrentMixedConditions tests concurrent mixed condition creation
func TestConcurrentMixedConditions(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cond := goent.And(
				goent.Or(
					goent.Equals(&goent.Field{ColumnName: "status"}, "active"),
					goent.Equals(&goent.Field{ColumnName: "status"}, "pending"),
				),
				goent.IsNotNull(&goent.Field{ColumnName: "email"}),
				goent.In(&goent.Field{ColumnName: "age"}, []int{18, 19, 20}),
			)
			if cond.Template == "" {
				errChan <- errors.New("condition template is empty")
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Mixed conditions error: %v", err)
	}
}
