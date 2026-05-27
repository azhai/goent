package goent_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/azhai/goent/model"
)

// TestConcurrentTransaction tests concurrent transaction operations
func TestConcurrentTransaction(t *testing.T) {
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

	var wg sync.WaitGroup
	errors := make(chan error, 5)

	// Test concurrent transactions inserting data
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tx, err := db.NewTransaction()
			if err != nil {
				errors <- err
				return
			}
			defer tx.Rollback()

			animal := &Animal{Name: "TxAnimal", Id: id + 200}
			err = db.Animal.Insert().OnTransaction(tx).One(animal)
			if err != nil {
				errors <- err
				return
			}

			err = tx.Commit()
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Transaction error: %v", err)
	}

	// Verify all animals were inserted
	count, err := db.Animal.Count("*")
	if err != nil {
		t.Fatalf("Expected count animals, got error: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected 5 animals, got %d", count)
	}
}

// TestConcurrentTransactionRollback tests concurrent transaction rollbacks
func TestConcurrentTransactionRollback(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert initial data
	animal := &Animal{Name: "RollbackTest", Id: 999}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 5)

	// Test concurrent transactions with rollback
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tx, err := db.NewTransaction()
			if err != nil {
				errors <- err
				return
			}
			// Rollback without commit
			defer tx.Rollback()

			// This insert should be rolled back
			a := &Animal{Name: "ShouldRollback", Id: id + 500}
			err = db.Animal.Insert().OnTransaction(tx).One(a)
			if err != nil {
				errors <- err
			}
			// Intentionally not committing
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Transaction rollback error: %v", err)
	}
}

// TestConcurrentTransactionIsolation tests transaction isolation
func TestConcurrentTransactionIsolation(t *testing.T) {
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

	var wg sync.WaitGroup
	errors := make(chan error, 3)

	// Test serializable transactions
	tx1, err := db.NewTransactionContext(context.Background(), sql.LevelSerializable)
	if err != nil {
		t.Fatalf("Expected transaction 1, got error: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db.NewTransactionContext(context.Background(), sql.LevelSerializable)
	if err != nil {
		t.Fatalf("Expected transaction 2, got error: %v", err)
	}
	defer tx2.Rollback()

	// Insert in tx1
	wg.Add(1)
	go func() {
		defer wg.Done()
		a := &Animal{Name: "Tx1Animal", Id: 1001}
		err := db.Animal.Insert().OnTransaction(tx1).One(a)
		if err != nil {
			errors <- err
		}
	}()

	// Insert in tx2
	wg.Add(1)
	go func() {
		defer wg.Done()
		a := &Animal{Name: "Tx2Animal", Id: 1002}
		err := db.Animal.Insert().OnTransaction(tx2).One(a)
		if err != nil {
			errors <- err
		}
	}()

	wg.Wait()

	// Commit both
	err = tx1.Commit()
	if err != nil {
		t.Errorf("Expected tx1 commit, got error: %v", err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Errorf("Expected tx2 commit, got error: %v", err)
	}

	close(errors)
	for err := range errors {
		t.Errorf("Transaction isolation error: %v", err)
	}
}

// TestConcurrentBeginTransaction tests concurrent BeginTransaction helper
func TestConcurrentBeginTransaction(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	errors := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			err := db.BeginTransaction(func(tx model.Transaction) error {
				animal := &Animal{Name: "BeginTxAnimal", Id: id + 300}
				return db.Animal.Insert().OnTransaction(tx).One(animal)
			})

			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("BeginTransaction error: %v", err)
	}
}
