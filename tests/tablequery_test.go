package goent_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/azhai/goent"
)

func TestTableQueryFilterChaining(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	animals := []*Animal{
		{Name: "Cat", Id: 1},
		{Name: "Dog", Id: 2},
		{Name: "Bird", Id: 3},
	}
	for _, a := range animals {
		err := db.Animal.Insert().One(a)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	t.Run("FilterCount", func(t *testing.T) {
		count, err := db.Animal.Filter(goent.Equals(db.Animal.Field("name"), "Cat")).Count("id")
		if err != nil {
			t.Fatalf("Expected count, got error: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected count 1, got %d", count)
		}
	})

	t.Run("FilterMax", func(t *testing.T) {
		max, err := db.Animal.Filter(goent.Equals(db.Animal.Field("name"), "Cat")).Max("id")
		if err != nil {
			t.Fatalf("Expected max, got error: %v", err)
		}
		if max != 1 {
			t.Errorf("Expected max 1, got %d", max)
		}
	})

	t.Run("FilterMin", func(t *testing.T) {
		min, err := db.Animal.Filter(goent.In(db.Animal.Field("name"), []string{"Cat", "Dog"})).Min("id")
		if err != nil {
			t.Fatalf("Expected min, got error: %v", err)
		}
		if min != 1 {
			t.Errorf("Expected min 1, got %d", min)
		}
	})

	t.Run("FilterSum", func(t *testing.T) {
		sum, err := db.Animal.Filter(goent.In(db.Animal.Field("name"), []string{"Cat", "Dog"})).Sum("id")
		if err != nil {
			t.Fatalf("Expected sum, got error: %v", err)
		}
		if sum != 3 {
			t.Errorf("Expected sum 3, got %d", sum)
		}
	})

	t.Run("WhereCount", func(t *testing.T) {
		count, err := db.Animal.Where("id > ?", 0).Count("id")
		if err != nil {
			t.Fatalf("Expected count, got error: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
	})

	t.Run("FilterSelectAll", func(t *testing.T) {
		results, err := db.Animal.Filter(goent.Equals(db.Animal.Field("name"), "Cat")).Select().All()
		if err != nil {
			t.Fatalf("Expected select, got error: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
		if results[0].Name != "Cat" {
			t.Errorf("Expected name Cat, got %s", results[0].Name)
		}
	})

	t.Run("WhereSelectAll", func(t *testing.T) {
		results, err := db.Animal.Where("id > ?", 1).Select().All()
		if err != nil {
			t.Fatalf("Expected select, got error: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	t.Run("FilterDelete", func(t *testing.T) {
		err := db.Animal.Filter(goent.Equals(db.Animal.Field("name"), "Bird")).Delete().Exec()
		if err != nil {
			t.Fatalf("Expected delete, got error: %v", err)
		}
		count, _ := db.Animal.Count("id")
		if count != 2 {
			t.Errorf("Expected 2 animals after delete, got %d", count)
		}
	})

	t.Run("ChainedFilter", func(t *testing.T) {
		count, err := db.Animal.
			Filter(goent.Greater(db.Animal.Field("id"), 0)).
			Filter(goent.Less(db.Animal.Field("id"), 3)).
			Count("id")
		if err != nil {
			t.Fatalf("Expected count, got error: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}
	})

	t.Run("TableCountNoCondition", func(t *testing.T) {
		count, err := db.Animal.Count("id")
		if err != nil {
			t.Fatalf("Expected count, got error: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}
	})
}

func TestConcurrentTableQueryNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 10; i++ {
		animal := &Animal{Name: "RaceTest", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			switch id % 5 {
			case 0:
				_, err := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), id%10+1)).Count("id")
				if err != nil {
					errChan <- err
				}
			case 1:
				_, err := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), id%10+1)).Max("id")
				if err != nil {
					errChan <- err
				}
			case 2:
				_, err := db.Animal.Where("id > ?", 0).Count("id")
				if err != nil {
					errChan <- err
				}
			case 3:
				_, err := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), id%10+1)).Select().All()
				if err != nil {
					errChan <- err
				}
			case 4:
				_, err := db.Animal.Count("id")
				if err != nil {
					errChan <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent TableQuery error: %v", err)
	}
}

func TestConcurrentFilterWhereNoSharedState(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		animal := &Animal{Name: "StateTest", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			q1 := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), 1))
			q2 := db.Animal.Filter(goent.Equals(db.Animal.Field("id"), 2))
			q3 := db.Animal.Where("id = ?", 3)

			count1, err := q1.Count("id")
			if err != nil {
				errChan <- err
				return
			}
			if count1 != 1 {
				errChan <- err
				return
			}

			count2, err := q2.Count("id")
			if err != nil {
				errChan <- err
				return
			}
			if count2 != 1 {
				errChan <- err
				return
			}

			count3, err := q3.Count("id")
			if err != nil {
				errChan <- err
				return
			}
			if count3 != 1 {
				errChan <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Shared state race error: %v", err)
	}
}

func TestConcurrentTableMethodsNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		animal := &Animal{Name: "MethodTest", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 30)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Count("id")
			if err != nil {
				errChan <- err
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Max("id")
			if err != nil {
				errChan <- err
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent table method error: %v", err)
	}
}

func TestConcurrentInsertSelectNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			animal := &Animal{Name: "ConcurrentAnimal", Id: id + 200}
			err := db.Animal.Insert().One(animal)
			if err != nil {
				errChan <- err
			}
		}(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent insert/select error: %v", err)
	}
}

func TestConcurrentUpdateDeleteNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}

	for i := 1; i <= 20; i++ {
		animal := &Animal{Name: "UpdateDeleteTest", Id: i}
		err := db.Animal.Insert().One(animal)
		if err != nil {
			t.Fatalf("Expected insert animal, got error: %v", err)
		}
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := db.Animal.Update().
				Set(goent.Pair{Key: "name", Value: "Updated"}).
				Filter(goent.Equals(db.Animal.Field("id"), id)).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := db.Animal.Delete().
				Filter(goent.Equals(db.Animal.Field("id"), id+10)).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent update/delete error: %v", err)
	}
}

func TestConcurrentFieldAccessNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 200)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f1 := db.Animal.Field("name")
			f2 := db.Animal.Field("id")
			if f1 == nil || f2 == nil {
				errChan <- errors.New("field is nil")
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Animal.TableName
			_ = db.Animal.Columns
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Errorf("Concurrent field access error: %v", err)
		}
	}
}

func TestConcurrentBuilderPoolStress(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 500)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.NewBuilder()
			if builder == nil {
				errChan <- errors.New("builder is nil")
				return
			}
			if b, ok := builder.(*goent.Builder); ok {
				goent.PutBuilder(b)
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Errorf("Builder pool stress error: %v", err)
		}
	}
}

func TestConcurrentDeleteBuilderPoolStress(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 500)

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.GetDeleteBuilder()
			if builder == nil {
				errChan <- errors.New("builder is nil")
				return
			}
			goent.PutDeleteBuilder(builder)
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Errorf("DeleteBuilder pool stress error: %v", err)
		}
	}
}
