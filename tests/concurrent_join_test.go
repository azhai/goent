package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent"
	"github.com/google/uuid"
)

// TestConcurrentLeftJoin tests concurrent LEFT JOIN queries
func TestConcurrentLeftJoin(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Prepare test data - insert habitats and animals
	err = db.Habitat.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete habitats, got error: %v", err)
	}
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	// Insert test data
	habitat := &Habitat{Name: "Forest"}
	habitat.Id = uuid.New()  // Generate UUID for primary key
	err = db.Habitat.Insert().One(habitat)
	if err != nil {
		t.Fatalf("Expected insert habitat, got error: %v", err)
	}

	animal := &Animal{Name: "Tiger", HabitatId: &habitat.Id}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent LEFT JOIN queries
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Use LeftJoin to join Animal with Habitat
			results, err := db.Animal.Select().
				LeftJoin("habitat_id", db.Habitat.Field("id")).
				All()
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: got %d results", id, len(results))
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("LeftJoin error: %v", err)
	}
}

// TestConcurrentJoin tests concurrent JOIN queries
func TestConcurrentJoin(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	// Insert test data
	animal := &Animal{Name: "Lion", Id: 50}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent select queries (simulating join scenario)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Simple concurrent select
			results, err := db.Animal.Select().All()
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: got %d animals", id, len(results))
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Join error: %v", err)
	}
}

// TestConcurrentSelectWithCondition tests concurrent conditional queries
func TestConcurrentSelectWithCondition(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Clean up and prepare data
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	animals := []*Animal{
		{Name: "Cat", Id: 60},
		{Name: "Dog", Id: 61},
		{Name: "Bird", Id: 62},
	}
	for _, a := range animals {
		err := db.Animal.Insert().One(a)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 15)

	// Test concurrent conditional selects
	for i := 0; i < 5; i++ {
		for _, a := range animals {
			wg.Add(1)
			go func(name string, id int) {
				defer wg.Done()

				results, err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("id"), id),
				).Select().All()
				if err != nil {
					errors <- err
					return
				}
				t.Logf("Query for %s (id=%d): got %d results", name, id, len(results))
			}(a.Name, a.Id)
		}
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Conditional select error: %v", err)
	}
}
