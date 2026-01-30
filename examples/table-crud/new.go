package main

import (
	"fmt"

	"github.com/azhai/goent"
	"github.com/azhai/goent/facade"
	"github.com/azhai/goent/query/where"
)

type DatabaseII struct {
	Category    *goent.Table[Category]
	Product     *goent.Table[Product]
	Order       *goent.Table[Order]
	OrderDetail *goent.Table[OrderDetail]
	*goent.DB
}

func ConnectII(dbType, dbDSN string) (*DatabaseII, error) {
	return facade.QuickOpen[DatabaseII](dbType, dbDSN, "stdout")
}

func CalcTotalPriceII(db *DatabaseII, orderID int) (float64, error) {
	order, err := db.Order.Find().ByValue(Order{ID: orderID})
	if err != nil || order == nil {
		return 0.0, err
	}
	// fmt.Printf("Output:\n%+v\n%+v\n%+v\n", order, order.Details, order.Products)

	filter := where.Equals(&db.OrderDetail.Model.OrderID, orderID)
	query := db.OrderDetail.List().OrderByAsc(&db.OrderDetail.Model.ProductID)
	order.Details, err = query.Where(filter).AsSlice()
	if err != nil {
		return 0.0, err
	}

	filter = where.In(&db.Product.Model.ID, order.GetProductIds())
	order.Products, err = db.Product.List().OrderByAsc(&db.Product.Model.ID).Where(filter).AsSlice()
	if err != nil {
		return 0.0, err
	}

	var total float64
	for i := range order.Details {
		if order.Details[i].ProductID != order.Products[i].ID {
			return total, err
		}
		order.Details[i].Price = order.Products[i].Price
		total += order.Details[i].Price * float64(order.Details[i].Quantity)
		err = db.OrderDetail.Save().One(order.Details[i])
		if err != nil {
			return total, err
		}
	}

	// filter = where.Equals(&db.Order.Model.ID, orderID)
	// change := update.Sets(&db.Order.Model.Total, total)
	// err = db.Order.Update().Sets(change).Where(filter)
	err = db.Order.Save().Sets(map[string]any{"total": total}).ByPK(orderID)

	fmt.Printf("Output:\n%+v\n%+v\n%+v\n", order, order.Details, order.Products)
	return total, err
}

func seedDataII(db *DatabaseII) error {
	count, err := db.Category.Count(&db.Category.Model.ID)
	if err != nil || count > 0 {
		return err
	}
	if err = db.Category.Insert().All(dataCategories()); err != nil {
		return err
	}
	if err = db.Product.Insert().All(dataProducts()); err != nil {
		return err
	}
	return nil
}

func createOrderII(db *DatabaseII, orderNo string) error {
	obj, err := db.Order.Find().ByValue(Order{OrderNo: orderNo})
	if err != nil && err != goent.ErrNotFound || obj != nil && obj.ID > 0 {
		return err
	}
	order := dataOrder(orderNo)
	if err = db.Order.Insert().One(&order); err != nil {
		return err
	}
	orderDetail := dataOrderDetail(order.ID)
	if err = db.OrderDetail.Insert().All(orderDetail); err != nil {
		return err
	}
	if _, err = CalcTotalPriceII(db, order.ID); err != nil {
		return err
	}
	return nil
}

func findOrderII(db *DatabaseII, orderNo string) (order *Order, err error) {
	order = &Order{OrderNo: orderNo}
	order, err = db.Order.Find().ByValue(*order)
	if err == nil && order.ID > 0 /*&& order.Total == 0.0*/ {
		order.Total, err = CalcTotalPriceII(db, order.ID)
	}
	return
}
