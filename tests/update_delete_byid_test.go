package goent_test

import (
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

// insertTestAnimals inserts n animals with names "Animal_1".."Animal_n"
// and returns them. Cleans up before and after the test.
func insertTestAnimals(t *testing.T, n int) []*Animal {
	t.Helper()
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return nil
	}
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	animals := make([]*Animal, n)
	for i := range animals {
		animals[i] = &Animal{Name: "Animal_" + itoa(i+1)}
	}
	if err := db.Animal.Insert().All(true, animals); err != nil {
		t.Fatalf("Failed to insert animals: %v", err)
	}
	return animals
}

// itoa is a simple int-to-string to avoid strconv import in test helpers.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func TestUpdateByID(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected database, got error: %v", err)
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "UpdateByID_Basic",
			testCase: func(t *testing.T) {
				animals := insertTestAnimals(t, 5)

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), animals[1].Id),
				).UpdateByID().
					Set(goent.Pair{Key: "name", Value: "Updated"}).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID failed: %v", err)
				}
				if len(ids) != 3 {
					t.Fatalf("Expected 3 affected IDs, got %d", len(ids))
				}

				// Verify IDs are correct (animals[2], [3], [4])
				expected := []int64{int64(animals[2].Id), int64(animals[3].Id), int64(animals[4].Id)}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				for i := range expected {
					if ids[i] != expected[i] {
						t.Errorf("Expected id %d, got %d", expected[i], ids[i])
					}
				}

				// Verify names were updated
				for _, a := range animals[2:] {
					got, err := db.Animal.FindByPK(int64(a.Id))
					if err != nil {
						t.Fatalf("FindByPK failed: %v", err)
					}
					if got.Name != "Updated" {
						t.Errorf("Expected name 'Updated', got %q", got.Name)
					}
				}

				// Verify animals[0] and [1] were NOT updated
				for _, a := range animals[:2] {
					got, err := db.Animal.FindByPK(int64(a.Id))
					if err != nil {
						t.Fatalf("FindByPK failed: %v", err)
					}
					if got.Name != a.Name {
						t.Errorf("Expected name %q (unchanged), got %q", a.Name, got.Name)
					}
				}
			},
		},
		{
			desc: "UpdateByID_EmptyResult",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 3)

				ids, err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "NonExistent"),
				).UpdateByID().
					Set(goent.Pair{Key: "name", Value: "X"}).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID on empty result should not error: %v", err)
				}
				if len(ids) != 0 {
					t.Errorf("Expected 0 affected IDs, got %d", len(ids))
				}
			},
		},
		{
			desc: "UpdateByID_Take",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 10)

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).UpdateByID().
					Set(goent.Pair{Key: "name", Value: "Limited"}).
					Take(3).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID with Take failed: %v", err)
				}
				if len(ids) != 3 {
					t.Fatalf("Expected 3 affected IDs (Take limit), got %d", len(ids))
				}

				// Verify exactly 3 animals were renamed
				count, err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "Limited"),
				).Count("id")
				if err != nil {
					t.Fatalf("Count failed: %v", err)
				}
				if count != 3 {
					t.Errorf("Expected 3 'Limited' animals, got %d", count)
				}
				// Total should still be 10
				total, _ := db.Animal.Count("id")
				if total != 10 {
					t.Errorf("Expected 10 total animals, got %d", total)
				}
			},
		},
		{
			desc: "UpdateByID_SetMap",
			testCase: func(t *testing.T) {
				animals := insertTestAnimals(t, 3)

				ids, err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("id"), animals[0].Id),
				).UpdateByID().
					SetMap(goent.Dict{"name": "FromMap"}).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID SetMap failed: %v", err)
				}
				if len(ids) != 1 {
					t.Fatalf("Expected 1 affected ID, got %d", len(ids))
				}

				got, _ := db.Animal.FindByPK(int64(animals[0].Id))
				if got.Name != "FromMap" {
					t.Errorf("Expected name 'FromMap', got %q", got.Name)
				}
			},
		},
		{
			desc: "UpdateByID_NoChanges_Error",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 3)

				_, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).UpdateByID().Exec()
				if err == nil {
					t.Error("Expected error when no changes set, got nil")
				}
			},
		},
		{
			desc: "UpdateByID_CompositePK_Error",
			testCase: func(t *testing.T) {
				// AnimalFood has composite PK, should return ErrNoPrimaryKey
				_, err := db.AnimalFood.Filter(
					goent.Equals(db.AnimalFood.Field("animal_id"), 1),
				).UpdateByID().
					Set(goent.Pair{Key: "food_id", Value: "00000000-0000-0000-0000-000000000000"}).
					Exec()
				if !errors.Is(err, model.ErrNoPrimaryKey) {
					t.Errorf("Expected ErrNoPrimaryKey, got: %v", err)
				}
			},
		},
		{
			desc: "UpdateByID_BatchSize",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 10)

				// Use batchSize=3 to force multiple batches
				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).UpdateByID().
					Set(goent.Pair{Key: "name", Value: "Batched"}).
					BatchSize(3).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID with BatchSize failed: %v", err)
				}
				if len(ids) != 10 {
					t.Fatalf("Expected 10 affected IDs, got %d", len(ids))
				}

				// All should be renamed
				count, _ := db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "Batched"),
				).Count("id")
				if count != 10 {
					t.Errorf("Expected 10 'Batched' animals, got %d", count)
				}
			},
		},
		{
			desc: "UpdateByID_Transaction",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 3)

				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Failed to create transaction: %v", err)
				}
				defer tx.Rollback()

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).UpdateByID().
					Set(goent.Pair{Key: "name", Value: "TxUpdate"}).
					OnTransaction(tx).
					Exec()
				if err != nil {
					t.Fatalf("UpdateByID in transaction failed: %v", err)
				}
				if len(ids) != 3 {
					t.Fatalf("Expected 3 affected IDs, got %d", len(ids))
				}

				// Inside transaction, should see updated names
				count, _ := db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "TxUpdate"),
				).OnTransaction(tx).Count("id")
				if count != 3 {
					t.Errorf("Expected 3 'TxUpdate' in transaction, got %d", count)
				}

				// Outside transaction, should NOT see updated names
				count, _ = db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "TxUpdate"),
				).Count("id")
				if count != 0 {
					t.Errorf("Expected 0 'TxUpdate' outside transaction, got %d", count)
				}

				// Commit and verify
				if err := tx.Commit(); err != nil {
					t.Fatalf("Commit failed: %v", err)
				}
				count, _ = db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "TxUpdate"),
				).Count("id")
				if count != 3 {
					t.Errorf("Expected 3 'TxUpdate' after commit, got %d", count)
				}
			},
		},
		{
			desc: "TableQuery_Update_Basic",
			testCase: func(t *testing.T) {
				animals := insertTestAnimals(t, 3)

				// Test the new Update() method on TableQuery
				err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("id"), animals[0].Id),
				).Update().
					Set(goent.Pair{Key: "name", Value: "DirectUpdate"}).
					Exec()
				if err != nil {
					t.Fatalf("TableQuery.Update failed: %v", err)
				}

				got, _ := db.Animal.FindByPK(int64(animals[0].Id))
				if got.Name != "DirectUpdate" {
					t.Errorf("Expected name 'DirectUpdate', got %q", got.Name)
				}
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.testCase)
	}
}

func TestDeleteByID(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected database, got error: %v", err)
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "DeleteByID_Basic",
			testCase: func(t *testing.T) {
				animals := insertTestAnimals(t, 5)

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), animals[2].Id),
				).DeleteByID().Exec()
				if err != nil {
					t.Fatalf("DeleteByID failed: %v", err)
				}
				if len(ids) != 2 {
					t.Fatalf("Expected 2 deleted IDs, got %d", len(ids))
				}

				// Verify deleted records are gone
				for _, a := range animals[3:] {
					_, err := db.Animal.FindByPK(int64(a.Id))
					if !errors.Is(err, model.ErrNoRows) {
						t.Errorf("Expected ErrNoRows for deleted id %d, got: %v", a.Id, err)
					}
				}

				// Verify remaining records
				count, _ := db.Animal.Count("id")
				if count != 3 {
					t.Errorf("Expected 3 remaining animals, got %d", count)
				}
			},
		},
		{
			desc: "DeleteByID_EmptyResult",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 3)

				ids, err := db.Animal.Filter(
					goent.Equals(db.Animal.Field("name"), "NonExistent"),
				).DeleteByID().Exec()
				if err != nil {
					t.Fatalf("DeleteByID on empty result should not error: %v", err)
				}
				if len(ids) != 0 {
					t.Errorf("Expected 0 deleted IDs, got %d", len(ids))
				}
				count, _ := db.Animal.Count("id")
				if count != 3 {
					t.Errorf("Expected 3 animals still present, got %d", count)
				}
			},
		},
		{
			desc: "DeleteByID_Take",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 10)

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).DeleteByID().Take(4).Exec()
				if err != nil {
					t.Fatalf("DeleteByID with Take failed: %v", err)
				}
				if len(ids) != 4 {
					t.Fatalf("Expected 4 deleted IDs, got %d", len(ids))
				}
				count, _ := db.Animal.Count("id")
				if count != 6 {
					t.Errorf("Expected 6 remaining animals, got %d", count)
				}
			},
		},
		{
			desc: "DeleteByID_BatchSize",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 10)

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).DeleteByID().BatchSize(3).Exec()
				if err != nil {
					t.Fatalf("DeleteByID with BatchSize failed: %v", err)
				}
				if len(ids) != 10 {
					t.Fatalf("Expected 10 deleted IDs, got %d", len(ids))
				}
				count, _ := db.Animal.Count("id")
				if count != 0 {
					t.Errorf("Expected 0 remaining animals, got %d", count)
				}
			},
		},
		{
			desc: "DeleteByID_CompositePK_Error",
			testCase: func(t *testing.T) {
				// AnimalFood has composite PK
				_, err := db.AnimalFood.Filter(
					goent.Equals(db.AnimalFood.Field("animal_id"), 1),
				).DeleteByID().Exec()
				if !errors.Is(err, model.ErrNoPrimaryKey) {
					t.Errorf("Expected ErrNoPrimaryKey, got: %v", err)
				}
			},
		},
		{
			desc: "DeleteByID_Transaction",
			testCase: func(t *testing.T) {
				insertTestAnimals(t, 3)

				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Failed to create transaction: %v", err)
				}

				ids, err := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).DeleteByID().OnTransaction(tx).Exec()
				if err != nil {
					t.Fatalf("DeleteByID in transaction failed: %v", err)
				}
				if len(ids) != 3 {
					t.Fatalf("Expected 3 deleted IDs, got %d", len(ids))
				}

				// Inside transaction, records should be gone
				count, _ := db.Animal.Filter(
					goent.Greater(db.Animal.Field("id"), 0),
				).OnTransaction(tx).Count("id")
				if count != 0 {
					t.Errorf("Expected 0 animals in transaction, got %d", count)
				}

				// Outside transaction, records should still be present
				count, _ = db.Animal.Count("id")
				if count != 3 {
					t.Errorf("Expected 3 animals outside transaction, got %d", count)
				}

				// Rollback
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback failed: %v", err)
				}

				// After rollback, records should be present
				count, _ = db.Animal.Count("id")
				if count != 3 {
					t.Errorf("Expected 3 animals after rollback, got %d", count)
				}
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.testCase)
	}
}

// ============ Race Condition Tests ============

// TestConcurrentUpdateByIDNoRace tests that concurrent UpdateByID calls
// with different conditions don't cause data races.
func TestConcurrentUpdateByIDNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 20)

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).UpdateByID().
				Set(goent.Pair{Key: "name", Value: "Concurrent_" + itoa(idx)}).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent UpdateByID error: %v", err)
	}
}

// TestConcurrentDeleteByIDNoRace tests that concurrent DeleteByID calls
// with different conditions don't cause data races.
func TestConcurrentDeleteByIDNoRace(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 20)

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).DeleteByID().Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent DeleteByID error: %v", err)
	}

	count, _ := db.Animal.Count("id")
	if count != 0 {
		t.Errorf("Expected 0 animals after concurrent delete, got %d", count)
	}
}

// TestConcurrentUpdateByIDAndSelect tests that UpdateByID and Select
// can run concurrently without data races.
func TestConcurrentUpdateByIDAndSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 10)

	var wg sync.WaitGroup
	errChan := make(chan error, 40)

	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx%len(animals)]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).UpdateByID().
				Set(goent.Pair{Key: "name", Value: "Mixed_" + itoa(idx)}).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
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
		t.Errorf("Concurrent UpdateByID+Select error: %v", err)
	}
}

// TestConcurrentUpdateByIDAndDeleteByID tests that UpdateByID and DeleteByID
// can run concurrently on different records without data races.
func TestConcurrentUpdateByIDAndDeleteByID(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 20)

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 10; i++ {
		wg.Add(2)
		// Update first half
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).UpdateByID().
				Set(goent.Pair{Key: "name", Value: "Keep_" + itoa(idx)}).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
		// Delete second half
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx+10]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).DeleteByID().Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent UpdateByID+DeleteByID error: %v", err)
	}

	// First 10 should remain, last 10 deleted
	count, _ := db.Animal.Count("id")
	if count != 10 {
		t.Errorf("Expected 10 remaining animals, got %d", count)
	}
}

// TestConcurrentUpdateByIDAndDirectUpdate tests that two-phase UpdateByID
// and direct Update can run concurrently without data races.
func TestConcurrentUpdateByIDAndDirectUpdate(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 20)

	var wg sync.WaitGroup
	errChan := make(chan error, 20)

	for i := 0; i < 10; i++ {
		wg.Add(2)
		// Two-phase update on first half
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).UpdateByID().
				Set(goent.Pair{Key: "name", Value: "TwoPhase_" + itoa(idx)}).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
		// Direct update on second half
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx+10]
			err := db.Animal.Update().
				Set(goent.Pair{Key: "name", Value: "Direct_" + itoa(idx)}).
				Filter(goent.Equals(db.Animal.Field("id"), animal.Id)).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Concurrent UpdateByID+Update error: %v", err)
	}
}

// TestConcurrentUpdateByIDBuilderPoolStress tests that the builder pool
// is not corrupted under heavy concurrent two-phase operations.
func TestConcurrentUpdateByIDBuilderPoolStress(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 50)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Mix UpdateByID, DeleteByID, Select, Count, and direct Update
	for i := 0; i < 20; i++ {
		wg.Add(5)
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx%len(animals)]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).UpdateByID().
				Set(goent.Pair{Key: "name", Value: "Stress_" + itoa(idx)}).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
		go func(idx int) {
			defer wg.Done()
			_, err := db.Animal.Count("id")
			if err != nil {
				errChan <- err
			}
		}(i)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Select().All()
			if err != nil {
				errChan <- err
			}
		}()
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx%len(animals)]
			err := db.Animal.Update().
				Set(goent.Pair{Key: "name", Value: "Direct_" + itoa(idx)}).
				Filter(goent.Equals(db.Animal.Field("id"), animal.Id)).
				Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
		go func() {
			defer wg.Done()
			_, err := db.Animal.Max("id")
			if err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Errorf("Builder pool stress error: %v", err)
	}
}

// TestConcurrentDeleteByIDAndSelect tests that DeleteByID and Select
// can run concurrently without data races.
func TestConcurrentDeleteByIDAndSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 20)

	var wg sync.WaitGroup
	errChan := make(chan error, 40)

	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			animal := animals[idx]
			_, err := db.Animal.Filter(
				goent.Equals(db.Animal.Field("id"), animal.Id),
			).DeleteByID().Exec()
			if err != nil {
				errChan <- err
			}
		}(i)
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
		t.Errorf("Concurrent DeleteByID+Select error: %v", err)
	}
}

// TestUpdateByIDTakeNoLimit tests that Take(TakeNoLimit) is treated as no limit.
func TestUpdateByIDTakeNoLimit(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
	}
	animals := insertTestAnimals(t, 5)

	// TakeNoLimit should mean no limit applied
	ids, err := db.Animal.Filter(
		goent.Greater(db.Animal.Field("id"), 0),
	).UpdateByID().
		Set(goent.Pair{Key: "name", Value: "NoLimit"}).
		Take(goent.TakeNoLimit).
		Exec()
	if err != nil {
		t.Fatalf("UpdateByID with TakeNoLimit failed: %v", err)
	}
	if len(ids) != len(animals) {
		t.Errorf("Expected %d IDs (no limit), got %d", len(animals), len(ids))
	}
}
