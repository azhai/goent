package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/go-fuego/fuego"
	"github.com/go-fuego/fuego/option"
	"github.com/go-fuego/fuego/param"
)

type Animal struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Habitat string `json:"habitat"`
	Food    string `json:"food"`
	Emoji   string `json:"emoji"`
}

type Database struct {
	Animal *Animal
	*goent.DB
}

type RequestAnimal struct {
	Name    string `json:"name" validate:"required"`
	Emoji   string `json:"emoji" validate:"required"`
	Habitat string `json:"habitat" validate:"required"`
	Food    string `json:"food" validate:"required"`
}

func main() {
	var db *Database
	var err error
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if db, err = goent.Open[Database](sqlite.Open("one-file-crud.db", sqlite.NewConfig(sqlite.Config{Logger: logger}))); err != nil {
		log.Fatal(err)
	}
	slog.SetDefault(logger)

	goent.Migrate(db).AutoMigrate()

	s := fuego.NewServer()

	fuego.Get(s, "/animals/{id}", func(c fuego.ContextNoBody) (*Animal, error) {
		return goent.Find(db.Animal).ByID(Animal{ID: c.PathParamInt("id")})
	}, option.Path("id", "animal id", param.Integer()))

	fuego.Get(s, "/animals", func(c fuego.ContextNoBody) (*goent.Pagination[Animal], error) {
		return goent.List(db.Animal).OrderByAsc(&db.Animal.ID).Match(Animal{
			Name:    c.QueryParam("name"),
			Food:    c.QueryParam("food"),
			Habitat: c.QueryParam("habitat"),
		}).
			AsPagination(c.QueryParamInt("page"), c.QueryParamInt("size"))
	}, option.QueryInt("page", "current page"), option.QueryInt("size", "page size"),
		option.Query("name", "animal name"), option.Query("habitat", "animal habitat"), option.Query("food", "animal food"))

	fuego.Post(s, "/animals", func(c fuego.ContextWithBody[RequestAnimal]) (any, error) {
		request, err := c.Body()
		if err != nil {
			return nil, fuego.BadRequestError{}
		}
		return nil, goent.Insert(db.Animal).One(&Animal{
			Name: request.Name, Emoji: request.Emoji, Habitat: request.Habitat, Food: request.Food})
	})

	fuego.Put(s, "/animals/{id}", func(c fuego.ContextWithBody[RequestAnimal]) (any, error) {
		request, err := c.Body()
		if err != nil {
			return nil, fuego.BadRequestError{}
		}
		return nil, goent.Save(db.Animal).ByID(Animal{ID: c.PathParamInt("id"),
			Name: request.Name, Emoji: request.Emoji, Habitat: request.Habitat, Food: request.Food})
	}, option.Path("id", "animal id", param.Integer()))

	fuego.Delete(s, "/animals/{id}", func(c fuego.ContextNoBody) (any, error) {
		return nil, goent.Remove(db.Animal).ByID(Animal{ID: c.PathParamInt("id")})
	}, option.Path("id", "animal id", param.Integer()))

	s.Run()
}
