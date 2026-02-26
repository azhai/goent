package tests_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
	"github.com/google/uuid"
)

var (
	animals []*Animal
	size    = 100
)

func BenchmarkDelete(b *testing.B) {
	db, _ := Setup()
	db.Status.Delete().Exec()

	data := make([]*Status, size)
	for i := 0; i < size; i++ {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)

	for i := range b.N {
		_ = db.Status.Delete().Where("id > ?", i).Exec()
		// filter := goent.Greater(db.Status.Field("id"), i)
		// _ = db.Status.Delete().Filter(filter).Exec()
	}
}

func BenchmarkSelect(b *testing.B) {
	db, _ := Setup()
	db.Status.Delete().Exec()

	data := make([]*Status, size)
	for i := 0; i < size; i++ {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	// fetchTo := FetchStatus()

	for b.Loop() {
		query := db.Status.Select().Take(size)
		// result, _ := query.QueryRows(fetchTo)
		result, _ := query.All()
		_ = result
	}
}

func BenchmarkSelectUUID(b *testing.B) {
	db, _ := Setup()
	db.AnimalFood.Delete().Exec()
	db.Animal.Delete().Exec()

	animals = make([]*Animal, size)
	for i := 0; i < size; i++ {
		animals[i] = &Animal{Name: uuid.New().String()}
	}
	db.Animal.Insert().All(false, animals)

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
	db.Animal.Insert().All(false, animals)

	tableName := db.Animal.TableName
	sql := fmt.Sprintf("select a.id, a.name, a.info_id, a.habitat_id from %s a;", tableName)

	for b.Loop() {
		rows, err := db.DB.RawQueryContext(context.Background(), sql)
		if err != nil {
			panic(err)
		}
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

	sql := `select f.id, f.name from %s f
						join %s af on f.id = af.food_id
						join %s a on af.animal_id = a.id
						join %s h on a.habitat_id = h.id
						join %s w on h.weather_id = w.id
						where f.id = $1 and f.name = $2;`
	sql = fmt.Sprintf(sql, db.Food.TableName, db.AnimalFood.TableName,
		db.Animal.TableName, db.Habitat.TableName, db.Weather.TableName)

	for b.Loop() {
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
