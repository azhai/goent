package goent_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

// TestConcurrentBuilderBuildStress tests that Builder.Build is safe under
// high concurrency when each goroutine uses its own builder from the pool.
func TestConcurrentBuilderBuildStress(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 200)
	var successCount int64

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			b := goent.GetBuilder()
			defer goent.PutBuilder(b)

			b.Type = model.SelectQuery
			b.SetTableName("animals")
			b.CoreWhere() // ensure accessor is race-free

			sql, args := b.Build(false)
			if sql == "" {
				errChan <- fmt.Errorf("iter %d: empty SQL", iter)
				return
			}
			if len(args) != 0 {
				errChan <- fmt.Errorf("iter %d: expected 0 args, got %d", iter, len(args))
				return
			}
			atomic.AddInt64(&successCount, 1)
		}(i)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
	if successCount != 200 {
		t.Errorf("Expected 200 successes, got %d", successCount)
	}
}

// TestConcurrentBuilderWithConditions tests concurrent Build with WHERE conditions.
// Uses the DB-level API to ensure end-to-end race-free query building.
func TestConcurrentBuilderWithConditions(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}
	_ = db.Animal.Delete().Exec()

	var wg sync.WaitGroup
	errChan := make(chan error, 200)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			_, err := db.Animal.Select().
				Where("id = ?", iter).
				All()
			if err != nil {
				errChan <- err
			}
		}(i)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestConcurrentDeleteBuilderBuildStress tests DeleteBuilder.Build under concurrency.
func TestConcurrentDeleteBuilderBuildStress(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 200)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			b := goent.GetDeleteBuilder()
			defer goent.PutDeleteBuilder(b)

			b.SetTableName("animals")

			sql, args := b.Build()
			if sql == "" {
				errChan <- fmt.Errorf("iter %d: empty SQL", iter)
				return
			}
			if len(args) != 0 {
				errChan <- fmt.Errorf("iter %d: expected 0 args, got %d", iter, len(args))
				return
			}
		}(i)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestConcurrentBuilderPoolGetPut tests rapid Get/Put cycles from multiple goroutines.
func TestConcurrentBuilderPoolGetPut(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 500)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				b := goent.GetBuilder()
				if b == nil {
					errChan <- fmt.Errorf("nil builder from pool")
					return
				}
				goent.PutBuilder(b)
			}
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestConcurrentTableInfoCachedAccess tests that TableInfo cached accessors
// (GetPKField, GetPrimaryInfo, GetFormattedName, etc.) are race-free.
func TestConcurrentTableInfoCachedAccess(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	info := db.Animal.TableInfo
	var wg sync.WaitGroup
	errChan := make(chan error, 8)

	accessors := []struct {
		name string
		fn   func() error
	}{
		{"GetPKField", func() error {
			f := info.GetPKField()
			if f == nil {
				return fmt.Errorf("GetPKField returned nil")
			}
			return nil
		}},
		{"GetPrimaryInfo", func() error {
			_, _, pkeys := info.GetPrimaryInfo()
			if len(pkeys) == 0 {
				return fmt.Errorf("GetPrimaryInfo returned empty pkeys")
			}
			return nil
		}},
		{"GetFormattedName", func() error {
			n := info.GetFormattedName()
			if n == "" {
				return fmt.Errorf("GetFormattedName returned empty")
			}
			return nil
		}},
		{"Table", func() error {
			tbl := info.Table()
			if tbl == nil {
				return fmt.Errorf("Table returned nil")
			}
			return nil
		}},
		{"GetSelectByPKSql", func() error {
			s := info.GetSelectByPKSql()
			if s == "" {
				return fmt.Errorf("GetSelectByPKSql returned empty")
			}
			return nil
		}},
		{"GetDeleteByPKSql", func() error {
			s := info.GetDeleteByPKSql()
			if s == "" {
				return fmt.Errorf("GetDeleteByPKSql returned empty")
			}
			return nil
		}},
		{"GetConnection", func() error {
			c := info.GetConnection()
			if c == nil {
				return fmt.Errorf("GetConnection returned nil")
			}
			return nil
		}},
		{"GetConfig", func() error {
			c := info.GetConfig()
			if c == nil {
				return fmt.Errorf("GetConfig returned nil")
			}
			return nil
		}},
	}

	// Run each accessor concurrently 100 times
	for _, acc := range accessors {
		wg.Add(1)
		go func(name string, fn func() error) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := fn(); err != nil {
					errChan <- fmt.Errorf("%s: %v", name, err)
					return
				}
			}
		}(acc.name, acc.fn)
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}
