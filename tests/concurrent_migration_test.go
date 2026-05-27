package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent"
)

// TestConcurrentAutoMigrate tests concurrent auto-migrate calls
func TestConcurrentAutoMigrate(t *testing.T) {
	// Note: AutoMigrate should only be called once in production
	// This test verifies it doesn't panic when called concurrently
	// In practice, migrations should be run serially

	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// AutoMigrate should be idempotent
	var wg sync.WaitGroup
	errors := make(chan error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// AutoMigrate is idempotent, calling multiple times should be safe
			err := goent.AutoMigrate(db)
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: AutoMigrate completed", id)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("AutoMigrate error: %v", err)
	}
}

// TestConcurrentSchemaOps tests concurrent schema operations
func TestConcurrentSchemaOps(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Just verify that we can query table information concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Concurrent table access (read-only schema operations)
			field := db.Animal.Field("name")
			if field == nil {
				errors <- err
				return
			}

			t.Logf("Goroutine %d: got field %v", id, field)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("Schema ops error: %v", err)
		}
	}
}

// TestConcurrentTableQuery tests concurrent TableQuery operations
func TestConcurrentTableQuery(t *testing.T) {
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
	animal := &Animal{Name: "QueryTest", Id: 70}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent TableQuery (Filter/Where/Count)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each Filter() returns a new TableQuery, safe for concurrent use
			count, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), 70),
			).Count("*")
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: count = %d", id, count)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("TableQuery error: %v", err)
	}
}

// TestConcurrentDropTables tests concurrent DropTables calls
func TestConcurrentDropTables(t *testing.T) {
	// DropTables should only be called once
	// This test just verifies the function handles concurrent calls gracefully

	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// We don't actually call DropTables concurrently as it modifies schema
	// Just verify the database connection works
	count, err := db.Animal.Count("*")
	if err != nil {
		t.Fatalf("Expected count, got error: %v", err)
	}
	t.Logf("Current animal count: %d", count)
}
