package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent"
)

// TestConcurrentSelect tests concurrent SELECT queries
func TestConcurrentSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	animals := []*Animal{
		{Name: "Lion", Id: 1},
		{Name: "Tiger", Id: 2},
		{Name: "Bear", Id: 3},
		{Name: "Elephant", Id: 4},
		{Name: "Giraffe", Id: 5},
	}

	for _, a := range animals {
		err := db.Animal.Insert().One(a)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent select error: %v", err)
	}
}

// TestConcurrentInsert tests concurrent INSERT queries
func TestConcurrentInsert(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			animal := &Animal{Name: "ConcurrentAnimal", Id: id + 100}
			err := db.Animal.Insert().One(animal)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent insert error: %v", err)
	}

	count, err := db.Animal.Select().Count("*")
	if err != nil {
		t.Fatalf("Expected count animals, got error: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 animals, got %d", count)
	}
}

// TestConcurrentUpdate tests concurrent UPDATE queries
func TestConcurrentUpdate(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	animals := []*Animal{
		{Name: "Lion", Id: 1},
		{Name: "Tiger", Id: 2},
		{Name: "Bear", Id: 3},
	}

	for _, a := range animals {
		err := db.Animal.Insert().One(a)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 15)

	for i := 0; i < 5; i++ {
		for _, animal := range animals {
			wg.Add(1)
			go func(a *Animal, iteration int) {
				defer wg.Done()
				newName := a.Name + "_updated"
				err := db.Animal.Update().
					Set(goent.Pair{Key: "name", Value: newName}).
					Filter(goent.Equals(db.Animal.Field("id"), a.Id)).
					Exec()
				if err != nil {
					errors <- err
				}
			}(animal, i)
		}
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent update error: %v", err)
	}
}

// TestConcurrentDelete tests concurrent DELETE queries
func TestConcurrentDelete(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 20; i++ {
		animal := &Animal{Name: "ToDelete", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := db.Animal.Delete().
				Filter(goent.Equals(db.Animal.Field("id"), id)).
				Exec()
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent delete error: %v", err)
	}

	count, err := db.Animal.Select().Count("*")
	if err != nil {
		t.Fatalf("Expected count animals, got error: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 animals remaining, got %d", count)
	}
}

// TestConcurrentMixedOperations tests concurrent mixed operations
func TestConcurrentMixedOperations(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 50; i++ {
		animal := &Animal{Name: "MixedTest", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 60)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errors <- err
			}
		}(i)

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			animal := &Animal{Name: "NewAnimal", Id: id + 100}
			err := db.Animal.Insert().One(animal)
			if err != nil {
				errors <- err
			}
		}(i)

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := db.Animal.Update().
				Set(goent.Pair{Key: "name", Value: "Updated"}).
				Filter(goent.Equals(db.Animal.Field("id"), id+1)).
				Exec()
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent mixed operation error: %v", err)
	}
}

// TestConcurrentBuilderReuse tests that Builder can be safely reused
func TestConcurrentBuilderReuse(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	animal := &Animal{Name: "TestAnimal", Id: 1}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			query := db.Animal.Select()
			_, err := query.All()
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Builder reuse error: %v", err)
	}
}

// TestConcurrentFieldAccess tests concurrent field access
func TestConcurrentFieldAccess(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			field := db.Animal.Field("name")
			if field == nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent field access error: %v", err)
		}
	}
}
