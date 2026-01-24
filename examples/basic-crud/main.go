package main

import (
	"os"

	"crud-basic/data"
	"crud-basic/framework"
	ginFramework "crud-basic/framework/gin"
	"crud-basic/framework/standard"

	"github.com/azhai/goent"
)

var frameworks map[string]func(db *data.Database) framework.Starter = map[string]func(db *data.Database) framework.Starter{
	"standard": standard.NewStarter,
	"gin":      ginFramework.NewStarter,
}

func main() {
	db, err := data.NewDatabase("crud-basic.db")
	if err != nil {
		panic(err)
	}
	defer goent.Close(db)

	starter := frameworks[os.Getenv("PK")]
	if starter == nil {
		panic("invalid package")
	}
	starter(db).Start(os.Getenv("PORT"))
}
