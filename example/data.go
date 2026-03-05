package main

import (
	"time"

	"goent-example/models"
)

const TestOrderNo = "20250801098"

func DefaultDSN(dbType string) string {
	if dbType == "pgsql" || dbType == "postgres" {
		return "user=postgres password=postgres host=localhost port=5432 database=postgres sslmode=disable"
	}
	return "table-crud.db"
}

func GetProductIds(order *models.Order) (ids []int64) {
	for _, dt := range order.Details {
		ids = append(ids, dt.ProductID)
	}
	return
}

func DataCategories() []*models.Category {
	return []*models.Category{
		{ID: 1, Name: "Coat"},
		{ID: 2, Name: "Jeans"},
		{ID: 3, Name: "Shorts"},
		{ID: 4, Name: "T-Shirt"},
	}
}

func DataProducts() []*models.Product {
	return []*models.Product{
		{ID: 1, CategoryID: 1, Name: "Product 1", Color: "Red", Price: 200.0},
		{ID: 2, CategoryID: 2, Name: "Product 2", Color: "Blue", Price: 120.0},
		{ID: 3, CategoryID: 3, Name: "Product 3", Color: "Green", Price: 50.0},
		{ID: 4, CategoryID: 4, Name: "Product 4", Color: "Yellow", Price: 88.0},
		{ID: 5, CategoryID: 1, Name: "Product 5", Color: "White", Price: 330.0},
		{ID: 6, CategoryID: 2, Name: "Product 6", Color: "Blue", Price: 68.8},
		{ID: 7, CategoryID: 3, Name: "Product 7", Color: "Green", Price: 72.5},
	}
}

func DataOrder(orderNo string) *models.Order {
	return &models.Order{
		OrderNo:  orderNo,
		Customer: "Customer 1",
		Total:    0.0,
		Status:   "Paid",
		Created:  time.Now(),
	}
}

func DataOrderDetail(orderID int64) []*models.OrderDetail {
	return []*models.OrderDetail{
		{OrderID: orderID, ProductID: 1, Quantity: 1, Price: 0.0},
		{OrderID: orderID, ProductID: 3, Quantity: 5, Price: 0.0},
		{OrderID: orderID, ProductID: 6, Quantity: 2, Price: 0.0},
	}
}
