package data

import (
	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type Person struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Database struct {
	Person *Person
	*goent.DB
}

func NewDatabase(uri string) (*Database, error) {
	db, err := goent.Open[Database](sqlite.Open(uri, sqlite.NewConfig(sqlite.Config{})))
	if err != nil {
		return nil, err
	}

	err = goent.Migrate(db).AutoMigrate()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewMemoryDatabase() (*Database, error) {
	db, err := goent.Open[Database](sqlite.OpenInMemory(sqlite.NewConfig(sqlite.Config{})))
	if err != nil {
		return nil, err
	}

	err = goent.Migrate(db).AutoMigrate()
	if err != nil {
		return nil, err
	}
	return db, nil
}
