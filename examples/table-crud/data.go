package main

import (
	"time"
)

const TestOrderNo = "20250801001"

type Category struct {
	ID   int
	Name string
}

func (*Category) TableName() string {
	return "t_category"
}

type Product struct {
	ID         int
	CategoryID int `goe:"o2m"`
	Name       string
	Color      string
	Price      float64
	Orders     []Order
}

func (*Product) TableName() string {
	return "t_product"
}

type Order struct {
	ID       int
	OrderNo  string `goe:"unique"`
	Customer string
	Total    float64
	Status   string
	Created  time.Time
	Details  []OrderDetail
	Products []Product
}

func (*Order) TableName() string {
	return "t_order"
}

func (m *Order) GetProductIds() (ids []int) {
	for _, dt := range m.Details {
		ids = append(ids, dt.ProductID)
	}
	return
}

type OrderDetail struct {
	OrderID   int `goe:"pk;not_incr"`
	ProductID int `goe:"pk;not_incr"`
	Quantity  int
	Price     float64
}

func (*OrderDetail) TableName() string {
	return "t_order_detail"
}

func dataCategories() []Category {
	return []Category{
		{ID: 1, Name: "Coat"},
		{ID: 2, Name: "Jeans"},
		{ID: 3, Name: "Shorts"},
		{ID: 4, Name: "T-Shirt"},
	}
}

func dataProducts() []Product {
	return []Product{
		{ID: 1, CategoryID: 1, Name: "Product 1", Color: "Red", Price: 200.0},
		{ID: 2, CategoryID: 2, Name: "Product 2", Color: "Blue", Price: 120.0},
		{ID: 3, CategoryID: 3, Name: "Product 3", Color: "Green", Price: 50.0},
		{ID: 4, CategoryID: 4, Name: "Product 4", Color: "Yellow", Price: 88.0},
		{ID: 5, CategoryID: 1, Name: "Product 5", Color: "White", Price: 330.0},
		{ID: 6, CategoryID: 2, Name: "Product 6", Color: "Blue", Price: 68.8},
		{ID: 7, CategoryID: 3, Name: "Product 7", Color: "Green", Price: 72.5},
	}
}

func dataOrder(orderNo string) Order {
	return Order{
		OrderNo:  orderNo,
		Customer: "Customer 1",
		Total:    0.0,
		Status:   "Paid",
		Created:  time.Now(),
	}
}

func dataOrderDetail(orderID int) []OrderDetail {
	return []OrderDetail{
		{OrderID: orderID, ProductID: 1, Quantity: 2, Price: 0.0},
		{OrderID: orderID, ProductID: 3, Quantity: 1, Price: 0.0},
		{OrderID: orderID, ProductID: 6, Quantity: 2, Price: 0.0},
	}
}
