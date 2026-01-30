package main

import (
	"fmt"

	"github.com/azhai/goent"
	"github.com/azhai/goent/facade"
	"github.com/azhai/goent/query/update"
	"github.com/azhai/goent/query/where"
	"github.com/azhai/goent/utils"
)

type Database struct {
	Category    *Category
	Product     *Product
	Order       *Order
	OrderDetail *OrderDetail
	*goent.DB
}

func Connect(dbType, dbDSN string) (*Database, error) {
	drv, err := facade.OpenDSN(dbType, dbDSN)
	if err != nil {
		panic(err)
	}
	err = drv.AddLogger(utils.CreateLogger("stdout"))
	if err != nil {
		panic(err)
	}
	return goent.Open[Database](drv)
}

func CalcTotalPrice(db *Database, orderID int) (float64, error) {
	order, err := goent.Find(db.Order).ByValue(Order{ID: orderID})
	if err != nil || order == nil {
		return 0.0, err
	}
	fmt.Printf("Output:\n%+v\n%+v\n%+v\n", order, order.Details, order.Products)

	filter := where.Equals(&db.OrderDetail.OrderID, orderID)
	query := goent.List(db.OrderDetail).OrderByAsc(&db.OrderDetail.ProductID)
	order.Details, err = query.Where(filter).AsSlice()
	if err != nil {
		return 0.0, err
	}

	filter = where.In(&db.Product.ID, order.GetProductIds())
	order.Products, err = goent.List(db.Product).OrderByAsc(&db.Product.ID).Where(filter).AsSlice()
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
		err = goent.Save(db.OrderDetail).One(order.Details[i])
		if err != nil {
			return total, err
		}
	}

	filter = where.Equals(&db.Order.ID, orderID)
	change := update.Set(&db.Order.Total, total)
	err = goent.Update(db.Order).Set(change).Where(filter)

	fmt.Printf("Output:\n%+v\n%+v\n%+v\n", order, order.Details, order.Products)
	return total, err
}

func seedData(db *Database) error {
	count, err := goent.Count(&db.Category.ID)
	if err != nil || count > 0 {
		return err
	}
	if err = goent.Insert(db.Category).All(dataCategories()); err != nil {
		return err
	}
	if err = goent.Insert(db.Product).All(dataProducts()); err != nil {
		return err
	}
	return nil
}

func createOrder(db *Database, orderNo string) error {
	obj, err := goent.Find(db.Order).ByValue(Order{OrderNo: orderNo})
	if err != nil && err != goent.ErrNotFound || obj != nil && obj.ID > 0 {
		return err
	}
	order := dataOrder(orderNo)
	if err = goent.Insert(db.Order).One(&order); err != nil {
		return err
	}
	orderDetail := dataOrderDetail(order.ID)
	if err = goent.Insert(db.OrderDetail).All(orderDetail); err != nil {
		return err
	}
	if _, err = CalcTotalPrice(db, order.ID); err != nil {
		return err
	}
	return nil
}

func findOrder(db *Database, orderNo string) (*Order, error) {
	// order, err := goent.Find(db.Order).ByValue(Order{OrderNo: orderNo})
	query := goent.List(db.Order).Join(&db.Order.ID, &db.OrderDetail.OrderID)
	order, err := query.Match(Order{OrderNo: orderNo}).AsOne()
	if err == nil && order.ID > 0 && order.Total == 0.0 {
		order.Total, err = CalcTotalPrice(db, order.ID)
	}
	return &order, err
}
