package goent_test

import (
	"testing"

	"github.com/azhai/goent"
)

// TestGenInsertValuesFastPath tests that models implementing GenInsertValues
// use the fast path in InsertOne (was bug: insertOneFastPath always used reflect).
func TestGenInsertValuesFastPath(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Status implements GenInsertValues
	status := &Status{Name: "Active"}
	err = db.Status.Insert().One(status)
	if err != nil {
		t.Fatalf("Insert Status error: %v", err)
	}
	if status.ID <= 0 {
		t.Errorf("Expected auto-increment ID > 0, got %d", status.ID)
	}

	// Verify the record
	found, err := db.Status.Select().Filter(goent.Equals(db.Status.Field("id"), status.ID)).One()
	if err != nil {
		t.Fatalf("Select Status error: %v", err)
	}
	if found.Name != "Active" {
		t.Errorf("Expected Name=Active, got Name=%s", found.Name)
	}
}

// TestGenInsertValuesBatch tests batch insert with GenInsertValues
func TestGenInsertValuesBatch(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	statuses := []*Status{
		{Name: "Pending"},
		{Name: "Completed"},
		{Name: "Cancelled"},
	}
	err = db.Status.Insert().All(false, statuses)
	if err != nil {
		t.Fatalf("Batch insert Status error: %v", err)
	}

	count, err := db.Status.Count("id")
	if err != nil {
		t.Fatalf("Count Status error: %v", err)
	}
	if count < 3 {
		t.Errorf("Expected at least 3 statuses, got %d", count)
	}
}

// TestGenScanDestSelect tests that models implementing GenScanDest
// use the fast path in Select
func TestGenScanDestSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	status := &Status{Name: "ScanTest"}
	err = db.Status.Insert().One(status)
	if err != nil {
		t.Fatalf("Insert Status error: %v", err)
	}

	// Select should use ScanDest fast path
	results, err := db.Status.Select().All()
	if err != nil {
		t.Fatalf("Select Status error: %v", err)
	}
	if len(results) == 0 {
		t.Error("Expected at least 1 status")
	}
}

// TestInsertOneBasic tests basic InsertOne without GenInsertValues
func TestInsertOneBasic(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal does NOT implement GenInsertValues
	animal := &Animal{Name: "InsertOneTest", Id: 600}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("InsertOne Animal error: %v", err)
	}

	// Verify
	found, err := db.Animal.FindByPK(600)
	if err != nil {
		t.Fatalf("FindByPK error: %v", err)
	}
	if found.Name != "InsertOneTest" {
		t.Errorf("Expected Name=InsertOneTest, got Name=%s", found.Name)
	}
}

// TestInsertOneWithDefault tests InsertOne with default values on Status table
func TestInsertOneWithDefault(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Status has auto-increment ID and Name — simple table for testing defaults
	status := &Status{Name: "DefaultTest"}
	err = db.Status.Insert().One(status)
	if err != nil {
		t.Fatalf("InsertOne Status error: %v", err)
	}
	if status.ID <= 0 {
		t.Errorf("Expected auto-increment ID > 0, got %d", status.ID)
	}
}
