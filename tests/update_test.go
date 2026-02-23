package tests_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/azhai/goent"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestUpdate(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected database, got error: %v", err)
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "Update_Flag",
			testCase: func(t *testing.T) {
				f := Flag{
					Id:         uuid.New(),
					Name:       "Flag",
					Float32:    1.1,
					Float64:    2.2,
					Today:      time.Now(),
					Int:        -1,
					Int8:       -8,
					Int16:      -16,
					Int32:      -32,
					Int64:      -64,
					Uint:       1,
					Uint8:      8,
					Uint16:     16,
					Uint32:     32,
					Uint64:     64,
					Bool:       true,
					Byte:       []byte{1, 2, 3},
					NullId:     sql.Null[uuid.UUID]{V: uuid.New(), Valid: true},
					NullString: sql.NullString{String: "String Value", Valid: true},
					Price:      decimal.NewFromUint64(99),
				}
				err = db.Flag.Insert().One(&f)
				if err != nil {
					t.Errorf("Expected a insert, got error: %v", err)
				}

				price, err := decimal.NewFromString("9.99")
				if err != nil {
					t.Errorf("Expected a price, got error: %v", err)
				}

				ff := Flag{
					Name:    "Flag_Test",
					Float32: 3.3,
					Float64: 4.4,
					Bool:    false,
					Byte:    []byte{1},
					Price:   price,
				}
				err = db.Flag.Update().
					Set(
						goent.Pair{Key: "name", Value: ff.Name},
						goent.Pair{Key: "bool", Value: ff.Bool}).
					Set(
						goent.Pair{Key: "float64", Value: ff.Float64},
						goent.Pair{Key: "float32", Value: ff.Float32},
						goent.Pair{Key: "price", Value: ff.Price},
						goent.Pair{Key: "null_id", Value: ff.NullId},
						goent.Pair{Key: "null_string", Value: ff.NullString},
						goent.Pair{Key: "byte", Value: ff.Byte}).
					Filter(goent.Equals(db.Flag.Field("id"), f.Id)).
					Exec()
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				var fselect *Flag
				fselect, err = db.Flag.Select().Match(Flag{Id: f.Id}).One()
				if err != nil {
					t.Fatalf("Expected a select, got error: %v", err)
				}

				if fselect.Name != ff.Name {
					t.Errorf("Expected a update on name, got : %v", fselect.Name)
				}
				if fselect.Float32 != ff.Float32 {
					t.Errorf("Expected a update on float32, got : %v", fselect.Float32)
				}
				if fselect.Float64 != ff.Float64 {
					t.Errorf("Expected a update on float64, got : %v", fselect.Float64)
				}
				if fselect.Bool != ff.Bool {
					t.Errorf("Expected a update on bool, got : %v", fselect.Bool)
				}
				if len(fselect.Byte) != len(ff.Byte) {
					t.Errorf("Expected a update on byte, got : %v", len(fselect.Byte))
				}
				if !fselect.Price.Equal(ff.Price) {
					t.Errorf("Expected a update on price, got : %v", fselect.Price)
				}
				if fselect.NullId != ff.NullId {
					t.Errorf("Expected a update on null_id, got : %v", fselect.NullId)
				}
				if fselect.NullString != ff.NullString {
					t.Errorf("Expected a update on null_string, got : %v", fselect.NullString)
				}
			},
		},
		{
			desc: "Save_Flag",
			testCase: func(t *testing.T) {
				f := Flag{
					Id:      uuid.New(),
					Name:    "Flag",
					Float32: 1.1,
					Float64: 2.2,
					Today:   time.Now(),
					Int:     -1,
					Int8:    -8,
					Int16:   -16,
					Int32:   -32,
					Int64:   -64,
					Uint:    1,
					Uint8:   8,
					Uint16:  16,
					Uint32:  32,
					Uint64:  64,
					Bool:    true,
					Byte:    []byte{1, 2, 3},
					Price:   decimal.NewFromUint64(99),
				}
				err = db.Flag.Insert().One(&f)
				if err != nil {
					t.Errorf("Expected a insert, got error: %v", err)
				}

				price, err := decimal.NewFromString("9.99")
				if err != nil {
					t.Errorf("Expected a price, got error: %v", err)
				}

				ff := Flag{
					Id:         f.Id,
					Name:       "Flag_Test",
					Float32:    3.3,
					Float64:    4.4,
					Byte:       []byte{1},
					NullId:     sql.Null[uuid.UUID]{V: uuid.New(), Valid: true},
					NullString: sql.NullString{String: "String Value", Valid: true},
					Price:      price,
				}
				err = db.Flag.Save().One(&ff)
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				var fselect *Flag
				fselect, err = db.Flag.Select().Match(Flag{Id: f.Id}).One()
				if err != nil {
					t.Fatalf("Expected a select, got error: %v", err)
				}

				if fselect.Name != ff.Name {
					t.Errorf("Expected a update on name, got : %v", fselect.Name)
				}
				if fselect.Float32 != ff.Float32 {
					t.Errorf("Expected a update on float32, got : %v", fselect.Float32)
				}
				if fselect.Float64 != ff.Float64 {
					t.Errorf("Expected a update on float64, got : %v", fselect.Float64)
				}
				if len(fselect.Byte) != len(ff.Byte) {
					t.Errorf("Expected a update on byte, got : %v", len(fselect.Byte))
				}
				if !fselect.Price.Equal(ff.Price) {
					t.Errorf("Expected a update on price, got : %v", fselect.Price)
				}
				if fselect.NullId != ff.NullId {
					t.Errorf("Expected a update on null_id, got : %v", fselect.NullId)
				}
				if fselect.NullString != ff.NullString {
					t.Errorf("Expected a update on null_string, got : %v", fselect.NullString)
				}
			},
		},
		{
			desc: "Update_Race",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}
				var wg sync.WaitGroup
				for range 10 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						au := Animal{Id: a.Id}
						au.Name = "Update Cat"
						db.Animal.Save().One(&au)
					}()
				}
				wg.Wait()
			},
		},
		{
			desc: "Update_Animal",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}
				t.Logf("After insert: a.Id = %d", a.Id)

				w := Weather{
					Name: "Warm",
				}
				err = db.Weather.Insert().One(&w)
				if err != nil {
					t.Fatalf("Expected a insert weather, got error: %v", err)
				}

				h := Habitat{
					Id:        uuid.New(),
					Name:      "City",
					WeatherId: w.Id,
				}
				err = db.Habitat.Insert().One(&h)
				if err != nil {
					t.Fatalf("Expected a insert habitat, got error: %v", err)
				}

				a.HabitatId = &h.Id
				a.Name = "Update Cat"

				// Check record exists before save
				beforeSave, err := db.Animal.Select().Filter(goent.Equals(db.Animal.Field("id"), a.Id)).One()
				if err != nil {
					t.Fatalf("Expected to find record before save, got error: %v", err)
				}
				if beforeSave.Name != "Cat" {
					t.Errorf("Expected name 'Cat' before save, got: %v", beforeSave.Name)
				}

				err = db.Animal.Save().One(&a)
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				var aselect *Animal
				aselect, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
				if err != nil {
					t.Fatalf("Expected a select, got error: %v", err)
				}

				if aselect.HabitatId == nil || *aselect.HabitatId != h.Id {
					t.Errorf("Expected a update on id habitat, got : %v", aselect.HabitatId)
				}
				if aselect.Name != "Update Cat" {
					t.Errorf("Expected a update on name, got : %v", aselect.Name)
				}

				aselect.HabitatId = nil
				err = db.Animal.Update().Set(goent.Pair{Key: "habitat_id", Value: aselect.HabitatId}).
					Filter(goent.Equals(db.Animal.Field("id"), aselect.Id)).Exec()
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				aselect, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
				if err != nil {
					t.Fatalf("Expected a select, got error: %v", err)
				}

				if aselect.HabitatId != nil {
					t.Errorf("Expected HabitatId to be nil, got : %v", aselect.HabitatId)
				}
			},
		},
		{
			desc: "Update_Animal_Tx_Commit",
			testCase: func(t *testing.T) {
				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Expected tx, got error: %v", err)
				}
				defer func() {
					if r := recover(); r != nil {
						tx.Rollback()
					}
				}()

				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().OnTransaction(tx).One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}

				sv, err := tx.SavePoint()
				if err != nil {
					t.Fatalf("Expected save point, got error: %v", err)
				}
				defer func() {
					if r := recover(); r != nil {
						sv.Rollback()
					}
				}()
				as := Animal{
					Name: "Dog",
				}
				err = db.Animal.Insert().OnTransaction(tx).One(&as)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}

				_, err = db.Animal.Select().OnTransaction(tx).Match(as).One()
				if err != nil {
					t.Fatalf("Expected a find animal, got error: %v", err)
				}
				err = sv.Rollback()
				if err != nil {
					t.Fatalf("Expected Rollback SavePoint, got error: %v", err)
				}
				_, err = db.Animal.Select().OnTransaction(tx).Match(as).One()
				if !errors.Is(err, goent.ErrNotFound) {
					t.Fatalf("Expected a goent.ErrNotFound, got: %v", err)
				}

				w := Weather{
					Name: "Warm",
				}
				err = db.Weather.Insert().OnTransaction(tx).One(&w)
				if err != nil {
					t.Fatalf("Expected a insert weather, got error: %v", err)
				}

				h := Habitat{
					Id:        uuid.New(),
					Name:      "City",
					WeatherId: w.Id,
				}
				err = db.Habitat.Insert().OnTransaction(tx).One(&h)
				if err != nil {
					t.Fatalf("Expected a insert habitat, got error: %v", err)
				}

				a.HabitatId = &h.Id
				a.Name = "Update Cat"
				err = db.Animal.Save().OnTransaction(tx).One(&a)
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				_, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
				if !errors.Is(err, goent.ErrNotFound) {
					t.Fatalf("Expected a goent.ErrNotFound, got error: %v", err)
				}

				err = tx.Commit()
				if err != nil {
					t.Fatalf("Expected Commit, got error: %v", err)
				}

				var aselect *Animal
				aselect, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
				if err != nil {
					t.Fatalf("Expected find, got error: %v", err)
				}

				if aselect.HabitatId == nil || *aselect.HabitatId != h.Id {
					t.Errorf("Expected a update on id habitat, got : %v", aselect.HabitatId)
				}
				if aselect.Name != "Update Cat" {
					t.Errorf("Expected a update on name, got : %v", aselect.Name)
				}
			},
		},
		{
			desc: "Update_PersonJobs_Tx_Rollback",
			testCase: func(t *testing.T) {

				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Expected tx, got error: %v", err)
				}
				defer tx.Rollback()

				persons := []*Person{
					{Name: "Jhon"},
					{Name: "Laura"},
					{Name: "Luana"},
				}
				err = db.Person.Insert().OnTransaction(tx).All(true, persons)
				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected insert persons, got error: %v", err)
				}

				jobs := []*JobTitle{
					{Name: "Developer"},
					{Name: "Designer"},
				}
				err = db.JobTitle.Insert().OnTransaction(tx).All(true, jobs)
				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected insert jobs, got error: %v", err)
				}

				personJobs := []*PersonJobTitle{
					{PersonId: persons[0].Id, JobTitleId: jobs[0].Id, CreatedAt: time.Now()},
					{PersonId: persons[1].Id, JobTitleId: jobs[0].Id, CreatedAt: time.Now()},
					{PersonId: persons[2].Id, JobTitleId: jobs[1].Id, CreatedAt: time.Now()},
				}
				err = db.PersonJobTitle.Insert().OnTransaction(tx).All(false, personJobs)
				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected insert personJobs, got error: %v", err)
				}

				pj := []struct {
					JobTitle string
					Person   string
				}{}
				for row, err := range db.Person.Select().OnTransaction(tx).
					LeftJoin("id", db.PersonJobTitle.Field("person_id")).
					LeftJoin("job_title_id", db.JobTitle.Field("id")).
					Filter(goent.Equals(db.JobTitle.Field("id"), jobs[0].Id)).IterRows() {

					if err != nil {
						t.Fatalf("Expected a select, got error: %v", err)
					}
					pj = append(pj, struct {
						JobTitle string
						Person   string
					}{Person: row.Name})
				}

				if len(pj) != 2 {
					t.Errorf("Expected %v, got : %v", 2, len(pj))
				}
				err = db.PersonJobTitle.Update().OnTransaction(tx).Set(goent.Pair{Key: "job_title_id", Value: jobs[0].Id}).
					Filter(
						goent.And(
							goent.Equals(db.PersonJobTitle.Field("person_id"), persons[2].Id),
							goent.Equals(db.PersonJobTitle.Field("job_title_id"), jobs[1].Id),
						),
					).Exec()

				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected a update, got error: %v", err)
				}

				pj = nil
				for row, err := range db.Person.Select().OnTransaction(tx).
					LeftJoin("id", db.PersonJobTitle.Field("person_id")).
					LeftJoin("job_title_id", db.JobTitle.Field("id")).
					Filter(goent.Equals(db.JobTitle.Field("id"), jobs[0].Id)).IterRows() {

					if err != nil {
						t.Fatalf("Expected a select, got error: %v", err)
					}
					pj = append(pj, struct {
						JobTitle string
						Person   string
					}{Person: row.Name})
				}

				if len(pj) != 3 {
					t.Errorf("Expected %v, got : %v", 3, len(pj))
				}

				err = tx.Rollback()
				if err != nil {
					t.Fatalf("Expected Rollback, got error: %v", err)
				}

				pj = nil
				for row, err := range db.Person.Select().
					LeftJoin("id", db.PersonJobTitle.Field("person_id")).
					LeftJoin("job_title_id", db.JobTitle.Field("id")).
					Filter(goent.Equals(db.JobTitle.Field("id"), jobs[0].Id)).IterRows() {

					if err != nil {
						t.Fatalf("Expected a select, got error: %v", err)
					}
					pj = append(pj, struct {
						JobTitle string
						Person   string
					}{Person: row.Name})
				}

				if len(pj) != 0 {
					t.Errorf("Expected %v, got : %v", 0, len(pj))
				}
			},
		},
		{
			desc: "Update_Animal_Db_Tx_Commit",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				w := Weather{
					Name: "Warm",
				}
				h := Habitat{
					Id:   uuid.New(),
					Name: "City",
				}
				err = db.BeginTransaction(func(tx goent.Transaction) error {
					if err = db.Animal.Insert().OnTransaction(tx).One(&a); err != nil {
						t.Fatalf("Expected a insert animal, got error: %v", err)
					}

					as := Animal{
						Name: "Dog",
					}
					tx.BeginTransaction(func(tx2 goent.Transaction) error {
						if err = db.Animal.Insert().OnTransaction(tx2).One(&as); err != nil {
							t.Fatalf("Expected a insert animal, got error: %v", err)
						}
						if _, err = db.Animal.Select().OnTransaction(tx2).Match(as).One(); err != nil {
							t.Fatalf("Expected a find animal, got error: %v", err)
						}
						return errors.New("")
					})
					if _, err = db.Animal.Select().OnTransaction(tx).Match(as).One(); !errors.Is(err, goent.ErrNotFound) {
						t.Fatalf("Expected a goent.ErrNotFound, got: %v", err)
					}

					tx.BeginTransaction(func(tx3 goent.Transaction) error {
						if err = db.Animal.Insert().OnTransaction(tx3).One(&as); err != nil {
							t.Fatalf("Expected a insert animal, got error: %v", err)
						}
						if _, err = db.Animal.Select().OnTransaction(tx3).Match(as).One(); err != nil {
							t.Fatalf("Expected a find animal, got error: %v", err)
						}
						return nil
					})

					if _, err = db.Animal.Select().OnTransaction(tx).Match(as).One(); err != nil {
						t.Fatalf("Expected a find, got: %v", err)
					}

					err = db.Weather.Insert().OnTransaction(tx).One(&w)
					if err != nil {
						t.Fatalf("Expected a insert weather, got error: %v", err)
					}

					h.WeatherId = w.Id
					err = db.Habitat.Insert().OnTransaction(tx).One(&h)
					if err != nil {
						t.Fatalf("Expected a insert habitat, got error: %v", err)
					}

					a.HabitatId = &h.Id
					a.Name = "Update Cat"
					err = db.Animal.Save().OnTransaction(tx).One(&a)
					if err != nil {
						t.Fatalf("Expected a update, got error: %v", err)
					}

					_, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
					if !errors.Is(err, goent.ErrNotFound) {
						t.Fatalf("Expected a goent.ErrNotFound, got error: %v", err)
					}

					return nil
				})

				if err != nil {
					t.Fatalf("Expected tx, got error: %v", err)
				}

				var aselect *Animal
				aselect, err = db.Animal.Select().Match(Animal{Id: a.Id}).One()
				if err != nil {
					t.Fatalf("Expected find, got error: %v", err)
				}

				if aselect.HabitatId == nil || *aselect.HabitatId != h.Id {
					t.Errorf("Expected a update on id habitat, got : %v", aselect.HabitatId)
				}
				if aselect.Name != "Update Cat" {
					t.Errorf("Expected a update on name, got : %v", aselect.Name)
				}
			},
		},
		{
			desc: "Update_PersonJobs",
			testCase: func(t *testing.T) {
				persons := []*Person{
					{Name: "Jhon"},
					{Name: "Laura"},
					{Name: "Luana"},
				}
				err = db.Person.Insert().All(true, persons)
				if err != nil {
					t.Fatalf("Expected insert persons, got error: %v", err)
				}

				jobs := []*JobTitle{
					{Name: "Developer"},
					{Name: "Designer"},
				}
				err = db.JobTitle.Insert().All(true, jobs)
				if err != nil {
					t.Fatalf("Expected insert jobs, got error: %v", err)
				}

				personJobs := []*PersonJobTitle{
					{PersonId: persons[0].Id, JobTitleId: jobs[0].Id, CreatedAt: time.Now()},
					{PersonId: persons[1].Id, JobTitleId: jobs[0].Id, CreatedAt: time.Now()},
					{PersonId: persons[2].Id, JobTitleId: jobs[1].Id, CreatedAt: time.Now()},
				}
				err = db.PersonJobTitle.Insert().All(true, personJobs)
				if err != nil {
					t.Fatalf("Expected insert personJobs, got error: %v", err)
				}

				pj := []struct {
					JobTitle string
					Person   string
				}{}
				for row, err := range db.Person.Select().
					LeftJoin("id", db.PersonJobTitle.Field("person_id")).
					LeftJoin("job_title_id", db.JobTitle.Field("id")).
					Filter(goent.Equals(db.JobTitle.Field("id"), jobs[0].Id)).IterRows() {

					if err != nil {
						t.Fatalf("Expected a select, got error: %v", err)
					}
					pj = append(pj, struct {
						JobTitle string
						Person   string
					}{Person: row.Name})
				}

				if len(pj) != 2 {
					t.Errorf("Expected %v, got : %v", 2, len(pj))
				}

				err = db.PersonJobTitle.Update().Set(goent.Pair{Key: "job_title_id", Value: jobs[0].Id}).Filter(
					goent.And(goent.Equals(db.PersonJobTitle.Field("person_id"), persons[2].Id), goent.Equals(db.PersonJobTitle.Field("job_title_id"), jobs[1].Id))).Exec()
				if err != nil {
					t.Fatalf("Expected a update, got error: %v", err)
				}

				pj = nil
				for row, err := range db.Person.Select().
					LeftJoin("id", db.PersonJobTitle.Field("person_id")).
					LeftJoin("job_title_id", db.JobTitle.Field("id")).
					Filter(goent.Equals(db.JobTitle.Field("id"), jobs[0].Id)).IterRows() {

					if err != nil {
						t.Fatalf("Expected a select, got error: %v", err)
					}
					pj = append(pj, struct {
						JobTitle string
						Person   string
					}{Person: row.Name})
				}

				if len(pj) != 3 {
					t.Errorf("Expected %v, got : %v", 3, len(pj))
				}
			},
		},
		{
			desc: "Update_Context_Cancel",
			testCase: func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err = db.NewTransactionContext(ctx, sql.LevelDefault)
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Expected a context.Canceled, got error: %v", err)
				}
			},
		},
		{
			desc: "Update_Context_Timeout",
			testCase: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond*1)
				defer cancel()
				time.Sleep(time.Millisecond)
				_, err = db.NewTransactionContext(ctx, sql.LevelDefault)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected a context.DeadlineExceeded, got error: %v", err)
				}
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.testCase)
	}
}
