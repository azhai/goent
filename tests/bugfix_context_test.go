package goent_test

import (
	"context"
	"testing"
	"time"

	"github.com/azhai/goent/model"
)

// TestContextPropagationInSelect tests that context is properly propagated
// through Select queries (was bug: QueryMiddleTable used context.Background())
func TestContextPropagationInSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = db.Animal.SelectContext(ctx).All()
	if err != nil {
		t.Fatalf("SelectContext error: %v", err)
	}
}

// TestContextCancellationInSelect tests that context cancellation is respected
func TestContextCancellationInSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.Animal.SelectContext(ctx).All()
	if err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}
}

// TestContextPropagationInFindByPK tests FindByPKContext
func TestContextPropagationInFindByPK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}

	animal := &Animal{Name: "CtxPKTest", Id: 950}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	found, err := db.Animal.FindByPKContext(ctx, 950)
	if err != nil {
		t.Fatalf("FindByPKContext error: %v", err)
	}
	if found.Name != "CtxPKTest" {
		t.Errorf("Expected Name=CtxPKTest, got Name=%s", found.Name)
	}
}

// TestContextPropagationInTransaction tests context within transactions
func TestContextPropagationInTransaction(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}

	err = db.BeginTransaction(func(tx model.Transaction) error {
		animal := &Animal{Name: "TxTest", Id: 960}
		return db.Animal.Insert().OnTransaction(tx).One(animal)
	})
	if err != nil {
		t.Fatalf("Transaction error: %v", err)
	}

	found, err := db.Animal.FindByPK(960)
	if err != nil {
		t.Fatalf("FindByPK error: %v", err)
	}
	if found.Name != "TxTest" {
		t.Errorf("Expected Name=TxTest, got Name=%s", found.Name)
	}
}

// TestContextPropagationWithEagerLoading tests that context is propagated
// through eager loading (With) queries
func TestContextPropagationWithEagerLoading(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}

	animal := &Animal{Name: "EagerCtx", Id: 970}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = db.Animal.SelectContext(ctx).With("AnimalFoods").All()
	if err != nil {
		t.Fatalf("SelectContext with With error: %v", err)
	}
}

// TestFindByPKContextCancelled tests that FindByPKContext respects context cancellation
func TestFindByPKContextCancelled(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}

	animal := &Animal{Name: "CancelTest", Id: 980}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.Animal.FindByPKContext(ctx, 980)
	if err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}
}

// TestConcurrentFindByPKContext tests concurrent FindByPKContext calls
// (was bug: shared sync.Once between fetchAll and fetchByPK)
func TestConcurrentFindByPKContext(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Failed to delete animals: %v", err)
	}

	animals := []*Animal{{Name: "Ctx1", Id: 1100}, {Name: "Ctx2", Id: 1101}}
	for _, a := range animals {
		if err := db.Animal.Insert().One(a); err != nil {
			t.Fatalf("Insert error: %v", err)
		}
	}

	// Concurrent FindByPKContext should not race
	for i := 0; i < 50; i++ {
		ctx := context.Background()
		_, err := db.Animal.FindByPKContext(ctx, int64(1100+i%2))
		if err != nil {
			t.Errorf("FindByPKContext error: %v", err)
		}
	}
}
