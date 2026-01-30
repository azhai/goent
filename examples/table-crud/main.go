package main

import (
	"fmt"

	"github.com/azhai/goent"
)

var (
	// dbType = "sqlite"
	// dbDSN  = "table-crud.db"
	dbType = "pgsql"
	dbDSN  = "postgres://dba:pass@127.0.0.1:5432/test?sslmode=disable"
)

func main() {
	newMain()
}

func oldMain() {
	db, err := Connect(dbType, dbDSN)
	if err != nil {
		panic(err)
	}
	defer goent.Close(db)

	if err = goent.Migrate(db).AutoMigrate(); err != nil {
		panic(err)
	}

	if err = seedData(db); err != nil {
		panic(err)
	}

	if err = createOrder(db, TestOrderNo); err != nil {
		fmt.Println(err)
	}

	var order *Order
	if order, err = findOrder(db, TestOrderNo); err != nil {
		fmt.Println(err)
	}
	if order != nil {
		fmt.Printf("\n%+v\n%+v\n", order, order.Products)
	}
}

func newMain() {
	db, err := ConnectII(dbType, dbDSN)
	if err != nil {
		panic(err)
	}
	defer goent.Close(db)

	if err = goent.Migrate(db).AutoMigrate(); err != nil {
		panic(err)
	}

	if err = seedDataII(db); err != nil {
		panic(err)
	}

	if err = createOrderII(db, TestOrderNo); err != nil {
		fmt.Println(err)
	}

	var order *Order
	if order, err = findOrderII(db, TestOrderNo); err != nil {
		fmt.Println(err)
	}
	if order != nil {
		fmt.Printf("\n%+v\n%+v\n", order, order.Products)
	}
}
