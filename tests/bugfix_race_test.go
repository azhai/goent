package goent_test

import (
	"sync"
	"testing"
)

// TestConcurrentFindByPK tests that FindByPK does not have data races
// when called concurrently (was bug: shared argsByPK slice).
func TestConcurrentFindByPK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{
		{Name: "Lion", Id: 1},
		{Name: "Tiger", Id: 2},
		{Name: "Bear", Id: 3},
	}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// Concurrent FindByPK with different IDs should not race
	var wg sync.WaitGroup
	errChan := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			id := int64(iter%3 + 1)
			_, err := db.Animal.FindByPK(id)
			if err != nil {
				errChan <- err
			}
		}(i)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("FindByPK error: %v", err)
	}
}

// TestConcurrentByPK tests that StateSelect.ByPK does not have data races
// when called concurrently (was bug: shared argsByPK slice).
func TestConcurrentByPK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}
	animals := []*Animal{
		{Name: "Cat", Id: 10},
		{Name: "Dog", Id: 11},
		{Name: "Fox", Id: 12},
	}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			id := int64(iter%3 + 10)
			_, err := db.Animal.Select().ByPK(id)
			if err != nil {
				errChan <- err
			}
		}(i)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("ByPK error: %v", err)
	}
}

// TestConcurrentFindByPKAndSelect tests that FindByPK and Select.All()
// can be used concurrently without sync.Once collision
// (was bug: fetchAllOnce shared between fetchAll and fetchByPK).
func TestConcurrentFindByPKAndSelect(t *testing.T) {
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
		{Name: "Ant", Id: 20},
		{Name: "Bee", Id: 21},
	}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 200)

	// Mix FindByPK and Select().All() concurrently
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(iter int) {
			defer wg.Done()
			id := int64(iter%2 + 20)
			_, err := db.Animal.FindByPK(id)
			if err != nil {
				errChan <- err
			}
		}(i)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errChan <- err
			}
		}()
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent FindByPK+Select error: %v", err)
	}
}

// TestFindByPKReturnsCorrectRecord verifies FindByPK returns the correct record
// (was bug: sync.Once collision could cause nil connByPK).
func TestFindByPKReturnsCorrectRecord(t *testing.T) {
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
		{Name: "Lion", Id: 50},
		{Name: "Tiger", Id: 51},
	}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Failed to insert animal: %v", err)
		}
	}

	// FindByPK should return the correct record
	a, err := db.Animal.FindByPK(50)
	if err != nil {
		t.Fatalf("FindByPK(50) error: %v", err)
	}
	if a.Name != "Lion" {
		t.Errorf("Expected Name=Lion, got Name=%s", a.Name)
	}

	a, err = db.Animal.FindByPK(51)
	if err != nil {
		t.Fatalf("FindByPK(51) error: %v", err)
	}
	if a.Name != "Tiger" {
		t.Errorf("Expected Name=Tiger, got Name=%s", a.Name)
	}
}

// TestFindByPKNoPrimaryKey tests FindByPK on a table without a single primary key
func TestFindByPKNoPrimaryKey(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// AnimalFood has composite primary key, FindByPK should return ErrNoPrimaryKey
	_, err = db.AnimalFood.FindByPK(1)
	if err == nil {
		t.Error("Expected error for composite PK table, got nil")
	}
}
