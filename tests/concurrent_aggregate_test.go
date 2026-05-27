package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent"
)

// TestConcurrentAggregateCount tests concurrent Count operations
func TestConcurrentAggregateCount(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	for i := 1; i <= 20; i++ {
		animal := &Animal{Name: "CountTest", Id: i + 6000}
		db.Animal.Insert().One(animal)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("name"), "CountTest"),
			).Count("*")
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Aggregate Count error: %v", err)
	}

	// Cleanup
	db.Animal.Delete().Where("name = ?", "CountTest").Exec()
}

// TestConcurrentAggregateMax tests concurrent Max operations
func TestConcurrentAggregateMax(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	for i := 1; i <= 10; i++ {
		animal := &Animal{Name: "MaxTest", Id: i + 7000}
		db.Animal.Insert().One(animal)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("name"), "MaxTest"),
			).Max("id")
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Aggregate Max error: %v", err)
	}

	// Cleanup
	db.Animal.Delete().Where("name = ?", "MaxTest").Exec()
}

// TestConcurrentAggregateMin tests concurrent Min operations
func TestConcurrentAggregateMin(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	for i := 1; i <= 10; i++ {
		animal := &Animal{Name: "MinTest", Id: i + 8000}
		db.Animal.Insert().One(animal)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("name"), "MinTest"),
			).Min("id")
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Aggregate Min error: %v", err)
	}

	// Cleanup
	db.Animal.Delete().Where("name = ?", "MinTest").Exec()
}
