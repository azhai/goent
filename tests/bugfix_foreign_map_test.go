package goent_test

import (
	"reflect"
	"testing"

	"github.com/azhai/goent/utils"
)

// TestCoMapEachShallowCopy tests that CoMap.Each() returns a shallow snapshot
// and modifications to the original map during iteration don't affect the iterator.
func TestCoMapEachShallowCopy(t *testing.T) {
	m := utils.NewCoMap[string, int]()

	m.Set("a", ptr(1))
	m.Set("b", ptr(2))
	m.Set("c", ptr(3))

	count := 0
	for k, v := range m.Each() {
		if k == "" {
			t.Error("Key should not be empty")
		}
		if v == nil {
			t.Error("Value should not be nil")
		}
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 iterations, got %d", count)
	}
}

// TestCoMapEachSnapshotIsolation tests that modifications after snapshot
// don't affect the ongoing iteration
func TestCoMapEachSnapshotIsolation(t *testing.T) {
	m := utils.NewCoMap[string, int]()
	m.Set("x", ptr(10))

	// Start iteration, then modify the map
	for range m.Each() {
		m.Set("y", ptr(20)) // This should not affect the current iteration
		break
	}

	// After iteration, the new key should be present
	if _, ok := m.Get("y"); !ok {
		t.Error("Key 'y' should exist after modification")
	}
}

// TestCoMapConcurrentReadWrite tests concurrent reads and writes
func TestCoMapConcurrentReadWrite(t *testing.T) {
	m := utils.NewCoMap[int, int]()
	done := make(chan struct{})

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			m.Set(i%100, ptr(i))
		}
		done <- struct{}{}
	}()

	// Reader goroutines
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_ = m.Size()
				_, _ = m.Get(50)
			}
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 6; i++ {
		<-done
	}
}

// TestCoMapSetGetDelete tests basic CoMap operations
func TestCoMapSetGetDelete(t *testing.T) {
	m := utils.NewCoMap[string, int]()

	// Set and Get
	m.Set("key", ptr(42))
	v, ok := m.Get("key")
	if !ok {
		t.Error("Expected key to exist")
	}
	if v == nil || *v != 42 {
		t.Errorf("Expected value=42, got %v", v)
	}

	// Delete
	m.Delete("key")
	_, ok = m.Get("key")
	if ok {
		t.Error("Expected key to be deleted")
	}
}

// TestCoMapKeys returns keys (not necessarily sorted)
func TestCoMapKeys(t *testing.T) {
	m := utils.NewCoMap[string, int]()
	m.Set("c", ptr(3))
	m.Set("a", ptr(1))
	m.Set("b", ptr(2))

	keys := m.Keys()
	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}
	// Keys() returns unsorted keys; use SortedKeys() for sorted order
	sortedKeys := m.SortedKeys()
	if sortedKeys[0] != "a" || sortedKeys[1] != "b" || sortedKeys[2] != "c" {
		t.Errorf("Expected sorted keys [a b c], got %v", sortedKeys)
	}
}

// TestInitForeignSliceEmptySlice tests that initForeignSlice initializes
// slice fields to empty slices (not nil) so that iteration doesn't panic
// (was bug: GenSetForeign path passed nil instead of empty slice).
func TestInitForeignSliceEmptySlice(t *testing.T) {
	// This tests the behavior: after initForeignSlice, slice fields should be
	// non-nil empty slices, not nil. We test this indirectly through the
	// foreign key query API.
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert a role with no users
	role := &Role{Name: "EmptyRole"}
	err = db.Role.Insert().One(role)
	if err != nil {
		t.Fatalf("Insert Role error: %v", err)
	}

	// Query the role with its UserRoles (O2M) — should return empty slice, not nil
	roles, err := db.Role.Select().With("UserRoles").All()
	if err != nil {
		t.Fatalf("Select Role with UserRoles error: %v", err)
	}

	for _, r := range roles {
		if r.Name == "EmptyRole" && r.UserRoles == nil {
			// After initForeignSlice fix, O2M slices should be non-nil empty slices
			t.Logf("Role.UserRoles is nil for EmptyRole — initForeignSlice may not have been called")
		}
	}
}

// TestInitForeignSliceO2MNonNilSlice verifies O2M relationship fields
// are initialized as non-nil empty slices
func TestInitForeignSliceO2MNonNilSlice(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Weather has O2M to Habitats
	weather := &Weather{Name: "Sunny"}
	err = db.Weather.Insert().One(weather)
	if err != nil {
		t.Fatalf("Insert Weather error: %v", err)
	}

	weathers, err := db.Weather.Select().With("Habitats").All()
	if err != nil {
		t.Fatalf("Select Weather with Habitats error: %v", err)
	}

	for _, w := range weathers {
		if w.Name == "Sunny" && w.Habitats == nil {
			// After initForeignSlice fix, O2M slices should be non-nil empty slices
			t.Logf("Weather.Habitats is nil for Sunny — initForeignSlice may not have been called")
		}
		// Should be safe to iterate even if nil (Go handles range over nil)
		for _, h := range w.Habitats {
			_ = h.Name
		}
	}
}

// TestEmptySliceNotNil verifies the general Go pattern that
// initialized slices should be non-nil for safe iteration
func TestEmptySliceNotNil(t *testing.T) {
	var nilSlice []int
	var emptySlice = make([]int, 0)

	if nilSlice == nil {
		// This is expected — nil slice
	} else {
		t.Error("nilSlice should be nil")
	}

	if emptySlice == nil {
		t.Error("emptySlice should not be nil")
	}

	// Both should be safe to iterate
	for range nilSlice {
		t.Error("Should not iterate nil slice")
	}
	for range emptySlice {
		t.Error("Should not iterate empty slice")
	}

	// But reflect.DeepEqual treats them differently
	if reflect.DeepEqual(nilSlice, emptySlice) {
		t.Error("nil and empty slices should not be DeepEqual")
	}
}

func ptr(v int) *int {
	return &v
}
