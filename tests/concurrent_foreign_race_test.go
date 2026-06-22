package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent"
	"github.com/google/uuid"
)

// TestConcurrentForeignGetMountFieldIdx tests that Foreign.getMountFieldIdx
// is race-free when called concurrently from multiple goroutines.
// This was a potential data race before the atomic.Int32 fix.
//
// Note: We use Animal (int64 PK) -> AnimalFood (int64 PK) because mapRowsByPK
// currently only supports int64 primary keys.
func TestConcurrentForeignGetMountFieldIdx(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	// Prepare data: Animal has AnimalFoods (o2m)
	_ = db.AnimalFood.Delete().Exec()
	_ = db.Animal.Delete().Exec()
	_ = db.Food.Delete().Exec()

	food := &Food{Id: uuid.New(), Name: "Meat"}
	if err := db.Food.Insert().One(food); err != nil {
		t.Fatalf("Insert food: %v", err)
	}

	animal := &Animal{Name: "Tiger", Id: 100}
	if err := db.Animal.Insert().One(animal); err != nil {
		t.Fatalf("Insert animal: %v", err)
	}

	af := &AnimalFood{AnimalId: 100, FoodId: food.Id}
	if err := db.AnimalFood.Insert().One(af); err != nil {
		t.Fatalf("Insert animal_food: %v", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Concurrent With("AnimalFoods") on Animal triggers getMountFieldIdx on the shared Foreign
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := db.Animal.Select().With("AnimalFoods").All()
			if err != nil {
				errChan <- err
				return
			}
			if len(results) == 0 {
				errChan <- errConcurrentForeignNotLoaded
				return
			}
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// errConcurrentForeignNotLoaded is a sentinel error for foreign field not loaded.
var errConcurrentForeignNotLoaded = &concurrentForeignErr{"foreign field not loaded"}

type concurrentForeignErr struct{ msg string }

func (e *concurrentForeignErr) Error() string { return e.msg }

// TestConcurrentForeignQueryByName tests concurrent QueryForeignByName calls.
func TestConcurrentForeignQueryByName(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	_ = db.AnimalFood.Delete().Exec()
	_ = db.Animal.Delete().Exec()
	_ = db.Food.Delete().Exec()

	food := &Food{Id: uuid.New(), Name: "Fish"}
	if err := db.Food.Insert().One(food); err != nil {
		t.Fatalf("Insert food: %v", err)
	}

	animal := &Animal{Name: "Cat", Id: 200}
	if err := db.Animal.Insert().One(animal); err != nil {
		t.Fatalf("Insert animal: %v", err)
	}

	af := &AnimalFood{AnimalId: 200, FoodId: food.Id}
	if err := db.AnimalFood.Insert().One(af); err != nil {
		t.Fatalf("Insert animal_food: %v", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := db.Animal.Select().All()
			if err != nil {
				errChan <- err
				return
			}
			if len(results) == 0 {
				return
			}
			// QueryForeignByName triggers getMountFieldIdx
			if err := goent.QueryForeignByName(db.Animal, results, "AnimalFoods"); err != nil {
				errChan <- err
			}
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestConcurrentForeignM2M tests concurrent many-to-many foreign queries.
func TestConcurrentForeignM2M(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	_ = db.Animal.Delete().Exec()
	_ = db.Food.Delete().Exec()
	_ = db.AnimalFood.Delete().Exec()

	food := &Food{Id: uuid.New(), Name: "Fish"}
	if err := db.Food.Insert().One(food); err != nil {
		t.Fatalf("Insert food: %v", err)
	}

	animal := &Animal{Name: "Cat", Id: 300}
	if err := db.Animal.Insert().One(animal); err != nil {
		t.Fatalf("Insert animal: %v", err)
	}

	af := &AnimalFood{AnimalId: 300, FoodId: food.Id}
	if err := db.AnimalFood.Insert().One(af); err != nil {
		t.Fatalf("Insert animal_food: %v", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := db.Animal.Select().With("AnimalFoods").All()
			if err != nil {
				errChan <- err
			}
			_ = results
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestConcurrentForeignO2M tests concurrent one-to-many foreign queries.
// Uses Animal (int64 PK) -> AnimalFoods to avoid the uuid PK limitation in mapRowsByPK.
func TestConcurrentForeignO2M(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	_ = db.AnimalFood.Delete().Exec()
	_ = db.Animal.Delete().Exec()
	_ = db.Food.Delete().Exec()

	animal := &Animal{Name: "Lizard", Id: 400}
	if err := db.Animal.Insert().One(animal); err != nil {
		t.Fatalf("Insert animal: %v", err)
	}

	for i := 0; i < 10; i++ {
		food := &Food{Id: uuid.New(), Name: "Bug"}
		if err := db.Food.Insert().One(food); err != nil {
			t.Fatalf("Insert food: %v", err)
		}
		af := &AnimalFood{AnimalId: 400, FoodId: food.Id}
		if err := db.AnimalFood.Insert().One(af); err != nil {
			t.Fatalf("Insert animal_food: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 50)

	// Concurrent With("AnimalFoods") on Animal triggers O2M getMountFieldIdx
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Select().With("AnimalFoods").All()
			if err != nil {
				errChan <- err
			}
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}
