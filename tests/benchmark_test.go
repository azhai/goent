package tests_test

import (
	"context"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
	"github.com/google/uuid"
)

var animals []*Animal
var size int = 100

func BenchmarkSelect(b *testing.B) {
	db, _ := Setup()

	db.AnimalFood.Delete().Exec()
	db.Animal.Delete().Exec()

	animals = make([]*Animal, size)
	for i := 0; i < size; i++ {
		animals[i] = &Animal{Name: uuid.New().String()}
	}
	db.Animal.Insert().All(true, animals)

	for b.Loop() {
		result, _ := db.Animal.Select().All()
		_ = result
	}
}

func BenchmarkSelectRaw(b *testing.B) {
	db, _ := Setup()

	db.Animal.Delete().Exec()

	animals = make([]*Animal, size)
	for i := 0; i < size; i++ {
		animals[i] = &Animal{Name: uuid.New().String()}
	}
	db.Animal.Insert().All(true, animals)

	for b.Loop() {
		sql := "select a.id, a.name, a.info_id, a.habitat_id from animals a;"
		rows, _ := db.DB.RawQueryContext(context.Background(), sql)
		defer rows.Close()

		var a Animal
		for rows.Next() {
			rows.Scan(&a.Id, &a.Name, &a.InfoId, &a.HabitatId)
			_ = a
		}
	}
}

var foods []*Food

func BenchmarkJoin(b *testing.B) {
	db, _ := Setup()

	db.Weather.Delete().Exec()
	db.Habitat.Delete().Exec()
	db.AnimalFood.Delete().Exec()
	db.Animal.Delete().Exec()
	db.Food.Delete().Exec()

	w := Weather{Name: "Weather"}
	db.Weather.Insert().One(&w)

	h := Habitat{Id: uuid.New(), Name: "Habitat", WeatherId: w.Id}
	db.Habitat.Insert().One(&h)

	a := Animal{Name: "Animal", HabitatId: &h.Id}
	db.Animal.Insert().One(&a)

	f := Food{Id: uuid.New(), Name: "Food"}
	db.Food.Insert().One(&f)

	af := AnimalFood{AnimalId: a.Id, FoodId: f.Id}
	db.AnimalFood.Insert().One(&af)

	for b.Loop() {
		result, _ := db.Food.Select().
			Join(model.InnerJoin, db.AnimalFood.TableInfo, goent.Equals(db.Food.Field("id"), db.AnimalFood.Field("food_id"))).
			Join(model.InnerJoin, db.Animal.TableInfo, goent.Equals(db.AnimalFood.Field("animal_id"), db.Animal.Field("id"))).
			Join(model.InnerJoin, db.Habitat.TableInfo, goent.Equals(db.Animal.Field("habitat_id"), db.Habitat.Field("id"))).
			Join(model.InnerJoin, db.Weather.TableInfo, goent.Equals(db.Habitat.Field("weather_id"), db.Weather.Field("id"))).
			Filter(goent.And(
				goent.Equals(db.Food.Field("id"), f.Id),
				goent.Equals(db.Food.Field("name"), f.Name),
			)).All()
		foods = result
	}
}

func BenchmarkJoinSql(b *testing.B) {
	db, _ := Setup()

	db.Weather.Delete().Exec()
	db.Habitat.Delete().Exec()
	db.AnimalFood.Delete().Exec()
	db.Animal.Delete().Exec()
	db.Food.Delete().Exec()

	w := Weather{Name: "Weather"}
	db.Weather.Insert().One(&w)

	h := Habitat{Id: uuid.New(), Name: "Habitat", WeatherId: w.Id}
	db.Habitat.Insert().One(&h)

	a := Animal{Name: "Animal", HabitatId: &h.Id}
	db.Animal.Insert().One(&a)

	f := Food{Id: uuid.New(), Name: "Food"}
	db.Food.Insert().One(&f)

	af := AnimalFood{AnimalId: a.Id, FoodId: f.Id}
	db.AnimalFood.Insert().One(&af)

	for b.Loop() {
		sql := `select f.id, f.name from foods f
						join animal_foods af on f.id = af.food_id
						join animals a on af.animal_id = a.id
						join habitats h on a.habitat_id = h.id
						join weathers w on h.weather_id = w.id
						where f.id = $1 and f.name = $2;`
		rows, _ := db.DB.RawQueryContext(context.Background(), sql, f.Id, f.Name)
		defer rows.Close()

		foods = make([]*Food, 0)
		var food Food
		for rows.Next() {
			rows.Scan(&food.Id, &food.Name)
			foods = append(foods, &food)
		}
	}
}
