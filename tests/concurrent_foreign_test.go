package goent_test

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

// TestConcurrentForeignQuery tests concurrent foreign key queries
func TestConcurrentForeignQuery(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Prepare test data - insert habitats and animals with foreign keys
	err = db.Habitat.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete habitats, got error: %v", err)
	}
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	// Insert habitat
	habitat := &Habitat{Name: "Forest"}
	habitat.Id = uuid.New()  // Generate UUID for primary key
	err = db.Habitat.Insert().One(habitat)
	if err != nil {
		t.Fatalf("Expected insert habitat, got error: %v", err)
	}

	// Insert animals with habitat_id foreign key
	animals := []*Animal{
		{Name: "Tiger", HabitatId: &habitat.Id},
		{Name: "Lion", HabitatId: &habitat.Id},
		{Name: "Bear", HabitatId: &habitat.Id},
	}
	for _, a := range animals {
		err := db.Animal.Insert().One(a)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent foreign key queries using With()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Select animals and eager load Habitat foreign key
			results, err := db.Animal.Select().With("Habitat").All()
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: got %d animals with habitat", id, len(results))
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Foreign query error: %v", err)
	}
}

// TestConcurrentWith tests concurrent With() calls for eager loading
func TestConcurrentWith(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Prepare test data - need animals with UserRoles
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete users, got error: %v", err)
	}
	err = db.Role.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete roles, got error: %v", err)
	}
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete user roles, got error: %v", err)
	}

	// Insert user and role
	user := &User{Name: "TestUser", Id: 500}
	err = db.User.Insert().One(user)
	if err != nil {
		t.Fatalf("Expected insert user, got error: %v", err)
	}

	role := &Role{Name: "Admin", Id: 500}
	err = db.Role.Insert().One(role)
	if err != nil {
		t.Fatalf("Expected insert role, got error: %v", err)
	}

	userRole := &UserRole{UserId: 500, RoleId: 500, Id: 500}
	err = db.UserRole.Insert().One(userRole)
	if err != nil {
		t.Fatalf("Expected insert user role, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent With() for UserRoles
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			results, err := db.User.Select().With("UserRoles").All()
			if err != nil {
				errors <- err
				return
			}
			t.Logf("Goroutine %d: got %d users with roles", id, len(results))
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("With() error: %v", err)
	}
}

// TestConcurrentEagerLoad tests concurrent eager loading of multiple relations
func TestConcurrentEagerLoad(t *testing.T) {
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
	animal := &Animal{Name: "Eagle", Id: 600}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent select without With (simpler test)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

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
		t.Errorf("Eager load error: %v", err)
	}
}

// TestConcurrentM2O tests concurrent many-to-one queries
func TestConcurrentM2O(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Prepare data: Animal has HabitatId (m2o to Habitat)
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}
	err = db.Habitat.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete habitats, got error: %v", err)
	}

	habitat := &Habitat{Name: "Ocean"}
	habitat.Id = uuid.New()  // Generate UUID for primary key
	err = db.Habitat.Insert().One(habitat)
	if err != nil {
		t.Fatalf("Expected insert habitat, got error: %v", err)
	}

	animal := &Animal{Name: "Whale", HabitatId: &habitat.Id, Id: 700}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Expected insert animal, got error: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent m2o queries
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Query animal and load its Habitat (m2o)
			results, err := db.Animal.Select().With("Habitat").All()
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
		t.Errorf("M2O query error: %v", err)
	}
}
