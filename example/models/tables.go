//go:generate goent-gen .

package models

import (
	"time"

	"github.com/azhai/goent"
)

// Category is a category of products.
type Category struct {
	ID   int64
	Name string
}

func (m *Category) GetID() int64 {
	return m.ID
}

func (m *Category) SetID(id int64) {
	m.ID = id
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

func (m *Product) GetID() int64 {
	return m.ID
}

func (m *Product) SetID(id int64) {
	m.ID = id
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

func (m *Order) GetID() int64 {
	return m.ID
}

func (m *Order) SetID(id int64) {
	m.ID = id
}

func (m *Order) GetProductIds() (ids []int64) {
	for _, dt := range m.Details {
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

// PublicSchema is the public schema of the database.
type PublicSchema struct {
	Category    *goent.Table[Category]
	Product     *goent.Table[Product]
	Order       *goent.Table[Order]
	OrderDetail *goent.Table[OrderDetail]
}
