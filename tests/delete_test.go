package tests_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/azhai/goent"
)

func TestDelete(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected database, got error: %v", err)
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "Delete_Animal",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}

				err = db.Animal.Delete().Match(a).Exec()
				if err != nil {
					t.Fatalf("Expected a delete, got error: %v", err)
				}

				_, err = db.Animal.Select().Match(a).One()
				if !errors.Is(err, goent.ErrNotFound) {
					t.Fatalf("Expected a goent.ErrNotFound, got error: %v", err)
				}
			},
		},
		{
			desc: "Delete_Animal_Tx_Commit",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}

				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Expected a tx, got error: %v", err)
				}
				defer tx.Rollback()

				err = db.Animal.Delete().OnTransaction(tx).Match(a).Exec()
				if err != nil {
					t.Fatalf("Expected a delete, got error: %v", err)
				}

				_, err = db.Animal.Select().OnTransaction(tx).Match(a).One()
				if !errors.Is(err, goent.ErrNotFound) {
					t.Fatalf("Expected a goent.ErrNotFound, got error: %v", err)
				}

				err = tx.Commit()
				if err != nil {
					t.Fatalf("Expected a tx commit, got error: %v", err)
				}

				_, err = db.Animal.Select().Match(a).One()
				if !errors.Is(err, goent.ErrNotFound) {
					t.Fatalf("Expected a goent.ErrNotFound, got error: %v", err)
				}
			},
		},
		{
			desc: "Delete_Animal_Tx_Rollback",
			testCase: func(t *testing.T) {
				a := Animal{
					Name: "Cat",
				}
				err = db.Animal.Insert().One(&a)
				if err != nil {
					t.Fatalf("Expected a insert animal, got error: %v", err)
				}

				tx, err := db.NewTransaction()
				if err != nil {
					t.Fatalf("Expected a tx, got error: %v", err)
				}
				defer tx.Rollback()

				err = db.Animal.Delete().OnTransaction(tx).Match(a).Exec()
				if err != nil {
					t.Fatalf("Expected a delete, got error: %v", err)
				}

				err = tx.Rollback()
				if err != nil {
					t.Fatalf("Expected a tx rollback, got error: %v", err)
				}

				_, err = db.Animal.Select().Match(a).One()
				if err != nil {
					t.Fatalf("Expected a find, got error: %v", err)
				}
			},
		},
		{
			desc: "Delete_Animal_Filter",
			testCase: func(t *testing.T) {
				animals := []*Animal{
					{Name: "Cat"},
					{Name: "Dog"},
					{Name: "Forest Cat"},
					{Name: "Bear"},
					{Name: "Lion"},
					{Name: "Puma"},
					{Name: "Snake"},
					{Name: "Whale"},
				}
				err = db.Animal.Insert().All(true, animals)
				if err != nil {
					t.Fatalf("Expected insert animals, got error: %v", err)
				}

				err = db.Animal.Delete().Filter(goent.Like(db.Animal.Field("name"), "%Cat%")).Exec()
				if err != nil {
					t.Fatalf("Expected a delete, got error: %v", err)
				}

				count, err := db.Animal.Count("id")
				if err != nil {
					t.Fatalf("Expected a count, got error: %v", err)
				}

				if int(count) != 6 {
					t.Errorf("Expected %v, got : %v", 6, count)
				}
			},
		},
		{
			desc: "Delete_PersonJobs_Tx_Commit",
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
				err = db.PersonJobTitle.Insert().OnTransaction(tx).All(true, personJobs)
				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected insert personJobs, got error: %v", err)
				}

				err = db.PersonJobTitle.Delete().OnTransaction(tx).Filter(
					goent.And(
						goent.Equals(db.PersonJobTitle.Field("person_id"), persons[2].Id),
						goent.Equals(db.PersonJobTitle.Field("job_title_id"), jobs[1].Id),
					),
				).Exec()
				if err != nil {
					tx.Rollback()
					t.Fatalf("Expected a delete, got error: %v", err)
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

				err = tx.Commit()
				if err != nil {
					t.Fatalf("Expected a tx commit, got error: %v", err)
				}
			},
		},
		{
			desc: "Delete_Context_Cancel",
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
			desc: "Delete_Context_Timeout",
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
