package goent_test

import (
	"strings"
	"testing"

	"github.com/azhai/goent"
)

// TestAppendValueParamSingleValue tests that conditions with single values
// (Value{single: val, Length: 1, Args: nil}) don't cause nil slice panics
// (was bug: else branch read val.Args[0] when Args was nil).
func TestAppendValueParamSingleValue(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{{Name: "Lion", Id: 100}, {Name: "Tiger", Id: 101}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Equals creates Value{single: 100, Length: 1} — Args is nil
	result, err := db.Animal.Select().Filter(goent.Equals(db.Animal.Field("id"), 100)).One()
	if err != nil {
		t.Fatalf("Equals filter error: %v", err)
	}
	if result.Name != "Lion" {
		t.Errorf("Expected Name=Lion, got Name=%s", result.Name)
	}
}

// TestAppendValueParamInClause tests IN conditions with Args slice
func TestAppendValueParamInClause(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{{Name: "Cat", Id: 200}, {Name: "Dog", Id: 201}, {Name: "Fox", Id: 202}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// In creates Value{Args: []any{200, 201}, Length: 2}
	results, err := db.Animal.Select().Filter(goent.In(db.Animal.Field("id"), []int{200, 201})).All()
	if err != nil {
		t.Fatalf("In filter error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestAppendValueParamMixedConditions tests mixed single-value and IN conditions
func TestAppendValueParamMixedConditions(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{{Name: "Cat", Id: 300}, {Name: "Dog", Id: 301}, {Name: "Fox", Id: 302}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Mix Equals (single value) and In (slice value)
	cond := goent.And(
		goent.Equals(db.Animal.Field("name"), "Cat"),
		goent.In(db.Animal.Field("id"), []int{300, 301}),
	)
	results, err := db.Animal.Select().Filter(cond).All()
	if err != nil {
		t.Fatalf("Mixed conditions error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].Name != "Cat" {
		t.Errorf("Expected Name=Cat, got Name=%s", results[0].Name)
	}
}

// TestDeleteBuilderWithSingleValue tests DeleteBuilder with single-value conditions
// (was bug: same nil panic in DeleteBuilder.appendValueParam)
func TestDeleteBuilderWithSingleValue(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{{Name: "ToDelete", Id: 400}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Delete with Equals (single value) should not panic
	err = db.Animal.Delete().Filter(goent.Equals(db.Animal.Field("id"), 400)).Exec()
	if err != nil {
		t.Fatalf("Delete with Equals error: %v", err)
	}

	// Verify deleted
	count, err := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), 400)).Count("id")
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count=0 after delete, got %d", count)
	}
}

// TestBuildTemplateSharedLogic tests that Builder and DeleteBuilder produce
// consistent WHERE clauses for the same condition (was DRY violation).
func TestBuildTemplateSharedLogic(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{{Name: "Test", Id: 500}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Both Select and Delete should handle the same condition correctly
	cond := goent.Like(db.Animal.Field("name"), "%Test%")

	results, err := db.Animal.Select().Filter(cond).All()
	if err != nil {
		t.Fatalf("Select with Like error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result from Select, got %d", len(results))
	}

	err = db.Animal.Delete().Filter(cond).Exec()
	if err != nil {
		t.Fatalf("Delete with Like error: %v", err)
	}

	count, _ := db.Animal.Count("id")
	if count != 0 {
		t.Errorf("Expected 0 animals after delete, got %d", count)
	}
}

// TestBuilderPoolResetConsistency tests that builders returned to the pool
// are properly reset and don't leak state from previous uses
func TestBuilderPoolResetConsistency(t *testing.T) {
	for i := 0; i < 50; i++ {
		builder := goent.NewBuilder()
		if b, ok := builder.(*goent.Builder); ok {
			b.Type = 1
			b.SetTableName("animal")
			sql, _ := b.Build(true)
			if sql == "" {
				t.Errorf("Iteration %d: SQL should not be empty", i)
			}
			if !strings.Contains(sql, "animal") {
				t.Errorf("Iteration %d: SQL should contain table name", i)
			}
			goent.PutBuilder(b)
		}
	}
}

// TestEmptySliceInRawWhere tests that raw Where clauses with empty slice
// arguments produce valid SQL instead of truncated "IN " syntax.
func TestEmptySliceInRawWhere(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{
		{Name: "Cat", Id: 600},
		{Name: "Dog", Id: 601},
		{Name: "Fox", Id: 602},
	}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Empty IN should not produce syntax error and should return nothing.
	results, err := db.Animal.Select().Where("id IN ?", []int{}).All()
	if err != nil {
		t.Fatalf("Empty IN raw where error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty IN, got %d", len(results))
	}

	// Empty NOT IN should not produce syntax error and should return all rows.
	results, err = db.Animal.Select().Where("id NOT IN ?", []int{}).All()
	if err != nil {
		t.Fatalf("Empty NOT IN raw where error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results for empty NOT IN, got %d", len(results))
	}

	// Empty IN on delete should not produce syntax error and should delete nothing.
	err = db.Animal.Delete().Where("id IN ?", []int{}).Exec()
	if err != nil {
		t.Fatalf("Empty IN delete error: %v", err)
	}
	count, err := db.Animal.Count("id")
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 animals after empty IN delete, got %d", count)
	}
}
