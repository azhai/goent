//go:generate goent-gen .

package models

import (
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
)

// Connect opens a database connection.
func Connect(dbDSN, logFile string) (*Database, error) {
	return goent.Open[Database](pgsql.OpenDSN(dbDSN), logFile)
}

// Database is the database connection with its driver.
type Database struct {
	PublicSchema `goe:"public;prefix:t_"`
	*goent.DB
}

// PublicSchema is the public schema of the database.
type PublicSchema struct {
	Category    *goent.Table[Category]
	Product     *goent.Table[Product]
	Order       *goent.Table[Order]
	OrderDetail *goent.Table[OrderDetail]
}

// Category is a category of products.
type Category struct {
	ID   int64
	Name string
}

// Product is a product in the store.
// CategoryID with `m2o` tag creates a foreign key to Category table.
// The Category field is a pointer to the related Category entity.
type Product struct {
	ID         int64
	CategoryID int64 `goe:"m2o"`
	Name       string
	Color      string
	Price      float64
	Category   *Category
}

// Order is an order in the store.
type Order struct {
	ID       int64
	OrderNo  string `goe:"unique"`
	Customer string
	Total    float64
	Status   string
	Created  time.Time
	Details  []*OrderDetail
	Products []*Product `goe:"-"`
}

func (t *Order) GetProductIds() (ids []int64) {
	for _, dt := range t.Details {
		ids = append(ids, dt.ProductID)
	}
	return
}

// OrderDetail is a detail of an order.
// OrderID and ProductID form a composite primary key with `pk;not_incr` tags.
// `m2o` tag on OrderID creates a foreign key to Order table.
// `o2o` tag on ProductID creates a one-to-one relationship with Product table.
type OrderDetail struct {
	OrderID   int64 `goe:"pk;not_incr;m2o"`
	ProductID int64 `goe:"pk;not_incr;o2o"`
	Quantity  int
	Price     float64
	Product   *Product
}

func (*OrderDetail) TableName() string {
	return "t_order_product"
}
