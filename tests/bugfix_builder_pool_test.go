package goent_test

import (
	"sync"
	"testing"
)

// TestBugfix_InsertAllSingleDelegatesBuilderOwner is a regression test for a
// Builder double-Put bug: StateInsert.All used to `defer PutBuilder` AND, in
// the len(data)==1 branch, delegate to One(), which also PutBuilder-ed the same
// *Builder. Returning the same Builder to the sync.Pool twice lets the pool
// hand one *Builder to two goroutines at once, so one goroutine's
// ResetForSave -> clear(Changes) races with the other's writes and the runtime
// aborts with "fatal error: concurrent map writes".
//
// Run with -race; the fix makes every path return the builder exactly once.
func TestBugfix_InsertAllSingleDelegatesBuilderOwner(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: database setup failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a := Animal{Name: "Cat"}
			// Single-element All() exercises the delegate-to-One path.
			_ = db.Animal.Insert().All(false, []*Animal{&a})
		}()
	}
	wg.Wait()
}

// TestBugfix_SelectAllConcurrentPoolSafety stresses the Builder pool under many
// concurrent readers. The double-Put class of bug (insert.go and select.go)
// crashed the whole process; this keeps the pool working under load.
func TestBugfix_SelectAllConcurrentPoolSafety(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: database setup failed: %v", err)
		return
	}
	_ = db.Animal.Insert().All(false, []*Animal{{Name: "Cat"}, {Name: "Dog"}})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.Animal.Select().All()
		}()
	}
	wg.Wait()
}
