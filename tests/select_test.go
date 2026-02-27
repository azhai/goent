package goent_test

import (
	"errors"
	"iter"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
	"github.com/google/uuid"
)

func TestSelect(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected database, got error: %v", err)
	}

	err = db.AnimalFood.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete AnimalFood, got error: %v", err)
	}
	err = db.Flag.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete flags, got error: %v", err)
	}
	err = db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete animals, got error: %v", err)
	}
	err = db.Food.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete foods, got error: %v", err)
	}
	err = db.Habitat.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete habitats, got error: %v", err)
	}
	err = db.Info.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete infos, got error: %v", err)
	}
	err = db.Status.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete status, got error: %v", err)
	}
	err = db.UserRole.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete user roles, got error: %v", err)
	}
	err = db.User.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete users, got error: %v", err)
	}
	err = db.Weather.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete weathers, got error: %v", err)
	}
	err = db.Person.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete persons, got error: %v", err)
	}
	err = db.JobTitle.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete job titles, got error: %v", err)
	}
	err = db.PersonJobTitle.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete person job titles, got error: %v", err)
	}
	err = db.Exam.Delete().Exec()
	if err != nil {
		t.Fatalf("Expected delete exams, got error: %v", err)
	}

	weathers := []*Weather{
		{Name: "Hot"},
		{Name: "Cold"},
		{Name: "Wind"},
		{Name: "Nice"},
		{Name: "Ocean"},
	}
	err = db.Weather.Insert().All(true, weathers)
	if err != nil {
		t.Fatalf("Expected insert weathers, got error: %v", err)
	}

	habitats := []*Habitat{
		{Id: uuid.New(), Name: "City", WeatherId: weathers[0].Id, NameWeather: "Test"},
		{Id: uuid.New(), Name: "Jungle", WeatherId: weathers[3].Id},
		{Id: uuid.New(), Name: "Savannah", WeatherId: weathers[0].Id},
		{Id: uuid.New(), Name: "Ocean", WeatherId: weathers[4].Id},
		{Id: uuid.New(), Name: "House", WeatherId: weathers[1].Id},
	}
	err = db.Habitat.Insert().All(true, habitats)
	if err != nil {
		t.Fatalf("Expected insert habitats, got error: %v", err)
	}

	animals := []*Animal{
		{Name: "Cat", HabitatId: &habitats[0].Id},
		{Name: "Dog", HabitatId: &habitats[1].Id},
		{Name: "Whale", HabitatId: &habitats[3].Id},
		{Name: "Lion", HabitatId: &habitats[2].Id},
		{Name: "Snake", HabitatId: &habitats[4].Id},
		{Name: "Bear", HabitatId: &habitats[0].Id},
		{Name: "Puma", HabitatId: &habitats[1].Id},
		{Name: "Forest Cat", HabitatId: &habitats[2].Id},
		{Name: "Bird", HabitatId: &habitats[3].Id},
		{Name: "Spider", HabitatId: &habitats[4].Id},
		{Name: "Turtle", HabitatId: &habitats[0].Id},
		{Name: "Shark", HabitatId: &habitats[3].Id},
		{Name: "Fish", HabitatId: &habitats[3].Id},
		{Name: "Dolphin", HabitatId: &habitats[3].Id},
		{Name: "Eagle", HabitatId: &habitats[1].Id},
		{Name: "Tiger", HabitatId: &habitats[2].Id},
		{Name: "Monkey", HabitatId: &habitats[1].Id},
		{Name: "Elephant", HabitatId: &habitats[2].Id},
		{Name: "Giraffe", HabitatId: &habitats[2].Id},
		{Name: "Zebra", HabitatId: &habitats[2].Id},
		{Name: "Penguin", HabitatId: &habitats[3].Id},
		{Name: "Polar Bear", HabitatId: &habitats[4].Id},
		{Name: "Panda", HabitatId: &habitats[1].Id},
		{Name: "Koala", HabitatId: &habitats[1].Id},
		{Name: "Kangaroo", HabitatId: &habitats[1].Id},
		{Name: "Crocodile", HabitatId: &habitats[2].Id},
		{Name: "Hippo", HabitatId: &habitats[2].Id},
		{Name: "Rhino", HabitatId: &habitats[2].Id},
		{Name: "Wolf", HabitatId: &habitats[0].Id},
		{Name: "Fox", HabitatId: &habitats[0].Id},
		{Name: "Deer", HabitatId: &habitats[1].Id},
		{Name: "Rabbit", HabitatId: &habitats[4].Id},
		{Name: "Squirrel", HabitatId: &habitats[1].Id},
		{Name: "Owl", HabitatId: &habitats[1].Id},
		{Name: "Frog", HabitatId: &habitats[0].Id},
		{Name: "Lizard", HabitatId: &habitats[2].Id},
		{Name: "Chameleon", HabitatId: &habitats[2].Id},
		{Name: "Parrot", HabitatId: &habitats[1].Id},
	}
	err = db.Animal.Insert().All(true, animals)
	if err != nil {
		t.Fatalf("Expected insert animals, got error: %v", err)
	}

	foods := []*Food{{Id: uuid.New(), Name: "Meat"}, {Id: uuid.New(), Name: "Grass"}}
	err = db.Food.Insert().All(true, foods)
	if err != nil {
		t.Fatalf("Expected insert foods, got error: %v", err)
	}

	animalFoods := []*AnimalFood{
		{FoodId: foods[0].Id, AnimalId: animals[0].Id},
		{FoodId: foods[1].Id, AnimalId: animals[0].Id},
		{FoodId: foods[0].Id, AnimalId: animals[1].Id}}
	err = db.AnimalFood.Insert().All(true, animalFoods)
	if err != nil {
		t.Fatalf("Expected insert animal foods, got error: %v", err)
	}

	runSelect := func(t *testing.T, iter iter.Seq2[*Animal, error]) []*Animal {
		var result []*Animal
		for a, err := range iter {
			if err != nil {
				t.Fatalf("Expected select, got error: %v", err)
			}
			result = append(result, a)
		}
		return result
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "Select",
			testCase: func(t *testing.T) {
				a := runSelect(t, db.Animal.Select().IterRows(nil))
				if len(a) != len(animals) {
					t.Errorf("Expected %v animals, got %v", len(animals), len(a))
				}
			},
		},
		{
			desc: "Select_Count",
			testCase: func(t *testing.T) {
				count, err := db.Animal.Count("id")
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if int(count) != len(animals) {
					t.Errorf("Expected %v got: %v", len(animals), count)
				}
			},
		},
		{
			desc: "Select_Count_Filter",
			testCase: func(t *testing.T) {
				count, err := db.Animal.Select().Filter(goent.Equals(db.Animal.Field("name"), "Cat")).Count("id")
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if count != 1 {
					t.Errorf("Expected 1, got: %v", count)
				}
			},
		},
		{
			desc: "Select_Match",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Match(Animal{Name: "Cat"}).One()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if a.Name != "Cat" {
					t.Errorf("Expected Cat, got: %v", a.Name)
				}
			},
		},
		{
			desc: "Select_Match_With_Schema",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Match(Animal{Name: "Cat"}).One()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if a.Name != "Cat" {
					t.Errorf("Expected Cat, got: %v", a.Name)
				}
			},
		},
		{
			desc: "Select_Match_Multiple_Fields",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Match(Animal{Name: "Cat", Id: animals[0].Id}).One()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if a.Name != "Cat" {
					t.Errorf("Expected Cat, got: %v", a.Name)
				}
			},
		},
		{
			desc: "Select_Match_Multiple_Fields_Not_Found",
			testCase: func(t *testing.T) {
				_, err := db.Animal.Select().Match(Animal{Name: "Cat", Id: 99999}).One()
				if !errors.Is(err, model.ErrNoRows) {
					t.Fatalf("Expected ErrNoRows, got: %v", err)
				}
			},
		},
		{
			desc: "Select_Match_Ptr",
			testCase: func(t *testing.T) {
				hId := habitats[0].Id
				a, err := db.Animal.Select().Match(Animal{HabitatId: &hId}).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) == 0 {
					t.Errorf("Expected results, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Match_Ptr_Nil",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.IsNull(db.Animal.Field("habitat_id"))).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				for _, animal := range a {
					if animal.HabitatId != nil {
						t.Errorf("Expected nil HabitatId, got: %v", animal.HabitatId)
					}
				}
			},
		},
		{
			desc: "Select_Match_Slice",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Match(Animal{Name: "Cat"}).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 1 {
					t.Errorf("Expected 1, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Match_Slice_Not_Found",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Match(Animal{Name: "NotExists"}).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 0 {
					t.Errorf("Expected 0, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_Equals",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.Equals(db.Animal.Field("name"), "Cat")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 1 {
					t.Errorf("Expected 1, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_NotEquals",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.NotEquals(db.Animal.Field("name"), "Cat")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-1 {
					t.Errorf("Expected %v, got: %v", len(animals)-1, len(a))
				}
			},
		},
		{
			desc: "Select_Where_Greater",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-1 {
					t.Errorf("Expected %v, got: %v", len(animals)-1, len(a))
				}
			},
		},
		{
			desc: "Select_Where_GreaterEquals",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.GreaterEquals(db.Animal.Field("id"), animals[0].Id)).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals) {
					t.Errorf("Expected %v, got: %v", len(animals), len(a))
				}
			},
		},
		{
			desc: "Select_Where_Less",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.Less(db.Animal.Field("id"), animals[3].Id)).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 3 {
					t.Errorf("Expected 3, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_LessEquals",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.LessEquals(db.Animal.Field("id"), animals[2].Id)).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 3 {
					t.Errorf("Expected 3, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_Like",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.Like(db.Animal.Field("name"), "%Cat%")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 2 {
					t.Errorf("Expected 2, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_NotLike",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.NotLike(db.Animal.Field("name"), "%Cat%")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-2 {
					t.Errorf("Expected %v, got: %v", len(animals)-2, len(a))
				}
			},
		},
		{
			desc: "Select_Where_ILike",
			testCase: func(t *testing.T) {
				if db.DriverName() == "SQLite" {
					t.Skip("SQLite does not support ILIKE")
				}
				a, err := db.Animal.Select().Filter(goent.ILike(db.Animal.Field("name"), "%cat%")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 2 {
					t.Errorf("Expected 2, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_NotILike",
			testCase: func(t *testing.T) {
				if db.DriverName() == "SQLite" {
					t.Skip("SQLite does not support ILIKE")
				}
				a, err := db.Animal.Select().Filter(goent.NotILike(db.Animal.Field("name"), "%cat%")).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-2 {
					t.Errorf("Expected %v, got: %v", len(animals)-2, len(a))
				}
			},
		},
		{
			desc: "Select_Where_In",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.In(db.Animal.Field("name"), []string{"Cat", "Dog"})).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 2 {
					t.Errorf("Expected 2, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_NotIn",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.NotIn(db.Animal.Field("name"), []string{"Cat", "Dog"})).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-2 {
					t.Errorf("Expected %v, got: %v", len(animals)-2, len(a))
				}
			},
		},
		{
			desc: "Select_Where_IsNull",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.IsNull(db.Animal.Field("info_id"))).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals) {
					t.Errorf("Expected %v, got: %v", len(animals), len(a))
				}
			},
		},
		{
			desc: "Select_Where_IsNotNull",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(goent.IsNotNull(db.Animal.Field("id"))).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals) {
					t.Errorf("Expected %v, got: %v", len(animals), len(a))
				}
			},
		},
		{
			desc: "Select_Where_And",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(
					goent.And(
						goent.Equals(db.Animal.Field("name"), "Cat"),
						goent.Greater(db.Animal.Field("id"), 0),
					),
				).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 1 {
					t.Errorf("Expected 1, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_Or",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(
					goent.Or(
						goent.Equals(db.Animal.Field("name"), "Cat"),
						goent.Equals(db.Animal.Field("name"), "Dog"),
					),
				).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 2 {
					t.Errorf("Expected 2, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_Not",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(
					goent.Not(
						goent.Equals(db.Animal.Field("name"), "Cat"),
					),
				).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != len(animals)-1 {
					t.Errorf("Expected %v, got: %v", len(animals)-1, len(a))
				}
			},
		},
		{
			desc: "Select_Where_Complex",
			testCase: func(t *testing.T) {
				a, err := db.Animal.Select().Filter(
					goent.And(
						goent.Or(
							goent.Equals(db.Animal.Field("name"), "Cat"),
							goent.Equals(db.Animal.Field("name"), "Dog"),
						),
						goent.Greater(db.Animal.Field("id"), 0),
					),
				).All()
				if err != nil {
					t.Fatalf("Expected select, got error: %v", err)
				}
				if len(a) != 2 {
					t.Errorf("Expected 2, got: %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_In_Query",
			testCase: func(t *testing.T) {
				t.Skip("Subquery in IN clause not yet implemented")
				a, err := db.Animal.Select().Filter(
					goent.And(
						goent.In(db.Animal.Field("name"), db.Animal.Select().Select("name").
							LeftJoin("id", db.AnimalFood.Field("animal_id")).
							LeftJoin("food_id", db.Food.Field("id")).
							Filter(
								goent.Or(goent.Equals(db.Animal.Field("name"), "Cat"), goent.In(db.Food.Field("name"), []string{foods[0].Name, foods[1].Name})))),
						goent.LessEquals(db.Animal.Field("id"), animals[1].Id)),
				).All()
				if err != nil {
					t.Fatalf("Expected a select where in, got error: %v", err)
				}

				if len(a) != 2 {
					t.Errorf("Expected 2, got %v", len(a))
				}
			},
		},
		{
			desc: "Select_Where_NotIn_Query",
			testCase: func(t *testing.T) {
				t.Skip("Subquery in NOT IN clause not yet implemented")
				a, err := db.Animal.Select().Filter(
					goent.And(
						goent.NotIn(db.Animal.Field("name"), db.Animal.Select().Select("name").
							LeftJoin("id", db.AnimalFood.Field("animal_id")).
							LeftJoin("food_id", db.Food.Field("id")).
							Filter(goent.Or(
								goent.Equals(db.Animal.Field("name"), "Cat"),
								goent.NotIn(db.Food.Field("name"), []string{foods[0].Name, foods[1].Name}),
							))),
						goent.LessEquals(db.Animal.Field("id"), animals[1].Id),
					)).All()
				if err != nil {
					t.Fatalf("Expected a select where in, got error: %v", err)
				}

				if len(a) != 1 {
					t.Errorf("Expected 1, got %v", len(a))
				}
			},
		},
		{
			desc: "List_As_Pagination",
			testCase: func(t *testing.T) {
				var p *goent.Pagination[Animal, Animal]
				p, err = db.Animal.Select().Pagination(1, 10)
				if err != nil {
					t.Fatalf("Expected pagination, got: %v", err)
				}

				if p.TotalValues != int64(len(animals)) {
					t.Errorf("Expected %v, got %v", len(animals), p.TotalValues)
				}

				if p.TotalPages != 4 {
					t.Errorf("Expected 4, got %v", p.TotalPages)
				}

				if p.PageSize != 10 {
					t.Errorf("Expected 10, got %v", p.PageSize)
				}

				if p.CurrentPage != 1 {
					t.Errorf("Expected 1, got %v", p.CurrentPage)
				}

				if p.NextPage != 2 {
					t.Errorf("Expected 2, got %v", p.NextPage)
				}

				if p.PreviousPage != 1 {
					t.Errorf("Expected 1, got %v", p.PreviousPage)
				}
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.testCase)
	}
}
