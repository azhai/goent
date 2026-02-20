package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

const TestOrderNo = "20250801098"

var (
	// dbType = "sqlite"
	// dbDSN  = "table-crud.db"
	dbType = "pgsql"
	dbDSN  = "postgres://dba:pass@127.0.0.1:5432/test?sslmode=disable"
)

type Category struct {
	ID   int64
	Name string
}

func (*Category) TableName() string {
	return "t_category"
}

func (m *Category) GetID() int64 {
	return m.ID
}

func (m *Category) SetID(id int64) {
	m.ID = id
}

type Product struct {
	ID         int64
	CategoryID int64 `goe:"m2o"`
	Name       string
	Color      string
	Price      float64
	Category   *Category `goe:"-"`
}

func (*Product) TableName() string {
	return "t_product"
}

func (m *Product) GetID() int64 {
	return m.ID
}

func (m *Product) SetID(id int64) {
	m.ID = id
}

type Order struct {
	ID       int64
	OrderNo  string `goe:"unique"`
	Customer string
	Total    float64
	Status   string
	Created  time.Time
	Details  []*OrderDetail
	Products []*Product
}

func (*Order) TableName() string {
	return "t_order"
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

type OrderDetail struct {
	OrderID   int64 `goe:"pk;not_incr"`
	ProductID int64 `goe:"pk;not_incr"`
	Quantity  int
	Price     float64
	Product   *Product `goe:"-"`
}

func (*OrderDetail) TableName() string {
	return "t_order_detail"
}

type PublicSchema struct {
	Category    *goent.Table[Category]
	Product     *goent.Table[Product]
	Order       *goent.Table[Order]
	OrderDetail *goent.Table[OrderDetail]
}

type Database struct {
	PublicSchema `goe:"public"`
	*goent.DB
}

func main() {
	db, err := connect(dbType, dbDSN, "stdout")
	if err != nil {
		panic(err)
	}
	defer goent.Close(db)

	if err = goent.AutoMigrate(db); err != nil {
		panic(err)
	}

	addForeignKeys(db)

	if err = seedData(db); err != nil {
		panic(err)
	}

	order := &Order{OrderNo: TestOrderNo}
	fields := []any{"id", "order_no", "total", "status"}
	order, err = db.Order.Select(fields...).Match(*order).One()
	if order == nil {
		order, err = createOrder(db, TestOrderNo)
	}
	if err != nil {
		fmt.Println(err)
	}
	if order != nil {
		if order.ID > 0 /*&& order.Total == 0.0*/ {
			order.Total, err = CalcTotalPrice3(db, order)
		}
		fmt.Printf("\n%+v\n\n%#v\n\n%#v\n", order, db.Order.Cache, db.Order.TableInfo)
	}
}

func addForeignKeys(db *Database) {
	db.Product.Foreigns = map[string]*goent.Foreign{
		"t_category": {
			Type:       goent.M2O,
			MountField: "Category",
			ForeignKey: "category_id",
			Reference:  db.Category.Field("id"),
			Middle:     nil,
		},
	}
	db.Order.Foreigns = map[string]*goent.Foreign{
		"t_order_detail": {
			Type:       goent.O2M,
			MountField: "Details",
			ForeignKey: "id",
			Reference:  db.OrderDetail.Field("order_id"),
			Middle:     nil,
		},
		"t_product": {
			Type:       goent.M2M,
			MountField: "Products",
			ForeignKey: "id",
			Reference:  db.Product.Field("id"),
			Middle: &goent.ThirdParty{
				Table: new(OrderDetail).TableName(),
				Left:  "order_id",
				Right: "product_id",
				Where: goent.Condition{},
			},
		},
	}
	db.OrderDetail.Foreigns = map[string]*goent.Foreign{
		"t_product": {
			Type:       goent.O2O,
			MountField: "Product",
			ForeignKey: "product_id",
			Reference:  db.Product.Field("id"),
			Middle:     nil,
		},
	}
}

func connect(dbType, dbDSN, logFile string) (*Database, error) {
	var drv model.Driver
	if dbType == "pgsql" || dbType == "postgres" {
		drv = pgsql.OpenDSN(dbDSN)
	} else if dbType == "" && strings.HasPrefix(dbDSN, "postgres://") {
		drv = pgsql.OpenDSN(dbDSN)
	} else {
		_ = utils.MakeDirForFile(dbDSN)
		drv = sqlite.OpenDSN(dbDSN)
	}
	return goent.Open[Database](drv, logFile)
}

func dataCategories() []*Category {
	return []*Category{
		{ID: 1, Name: "Coat"},
		{ID: 2, Name: "Jeans"},
		{ID: 3, Name: "Shorts"},
		{ID: 4, Name: "T-Shirt"},
	}
}

func dataProducts() []*Product {
	return []*Product{
		{ID: 1, CategoryID: 1, Name: "Product 1", Color: "Red", Price: 200.0},
		{ID: 2, CategoryID: 2, Name: "Product 2", Color: "Blue", Price: 120.0},
		{ID: 3, CategoryID: 3, Name: "Product 3", Color: "Green", Price: 50.0},
		{ID: 4, CategoryID: 4, Name: "Product 4", Color: "Yellow", Price: 88.0},
		{ID: 5, CategoryID: 1, Name: "Product 5", Color: "White", Price: 330.0},
		{ID: 6, CategoryID: 2, Name: "Product 6", Color: "Blue", Price: 68.8},
		{ID: 7, CategoryID: 3, Name: "Product 7", Color: "Green", Price: 72.5},
	}
}

func dataOrder(orderNo string) *Order {
	return &Order{
		OrderNo:  orderNo,
		Customer: "Customer 1",
		Total:    0.0,
		Status:   "Paid",
		Created:  time.Now(),
	}
}

func dataOrderDetail(orderID int64) []*OrderDetail {
	return []*OrderDetail{
		{OrderID: orderID, ProductID: 1, Quantity: 1, Price: 0.0},
		{OrderID: orderID, ProductID: 3, Quantity: 5, Price: 0.0},
		{OrderID: orderID, ProductID: 6, Quantity: 2, Price: 0.0},
	}
}

func seedData(db *Database) error {
	count, err := db.Category.Count("*")
	if err != nil || count > 0 {
		return err
	}
	if err = db.Category.Insert().All(false, dataCategories()); err != nil {
		return err
	}
	if err = db.Product.Insert().All(false, dataProducts()); err != nil {
		return err
	}
	return nil
}

func createOrder(db *Database, orderNo string) (*Order, error) {
	obj, err := db.Order.Select("id", "status", "total").Match(Order{OrderNo: orderNo}).One()
	if err != nil && err != goent.ErrNotFound || obj != nil && obj.ID > 0 {
		return nil, err
	}
	order := dataOrder(orderNo)
	if err = db.Order.Insert().One(order); err != nil {
		return nil, err
	}
	orderDetail := dataOrderDetail(order.ID)
	if err = db.OrderDetail.Insert().All(true, orderDetail); err != nil {
		return nil, err
	}
	return order, nil
}

func CalcTotalPrice(db *Database, order *Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	query := db.OrderDetail.Select().OrderBy("product_id")
	order.Details, err = query.Filter(filter).All()
	if err != nil {
		return 0.0, err
	}

	filter = goent.In(db.Product.Field("id"), order.GetProductIds())
	order.Products, err = db.Product.Select().OrderBy("id").Filter(filter).All()
	if err != nil {
		return 0.0, err
	}

	productMap := make(map[int64]*Product, len(order.Products))
	for _, p := range order.Products {
		productMap[p.ID] = p
	}

	var total float64
	for i := range order.Details {
		product := productMap[order.Details[i].ProductID]
		if product == nil {
			return total, err
		}
		order.Details[i].Price = product.Price
		total += order.Details[i].Price * float64(order.Details[i].Quantity)
		err = db.OrderDetail.Save().One(order.Details[i])
		if err != nil {
			return total, err
		}
	}

	err = db.Order.Save().Map(goent.Dict{"id": order.ID, "total": total})
	return total, err
}

func CalcTotalPrice2(db *Database, order *Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	query := db.OrderDetail.Select().OrderBy("product_id").Filter(filter)
	order.Details, err = query.LeftJoin("product_id", db.Product.Field("id")).All()

	var total float64
	for _, detail := range order.Details {
		if detail.Product == nil {
			continue
		}
		detail.Price = detail.Product.Price
		total += detail.Price * float64(detail.Quantity)
		err = db.OrderDetail.Save().One(detail)
		if err != nil {
			return total, err
		}
	}

	err = db.Order.Save().Map(goent.Dict{"id": order.ID, "total": total})
	return total, err
}

func CalcTotalPrice3(db *Database, order *Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	change := goent.Pair{Key: "price", Value: db.Product.Field("price")}
	query := db.OrderDetail.Update().Filter(filter)
	err = query.LeftJoin("product_id", db.Product.Field("id")).Set(change).Exec()
	if err != nil {
		return 0.0, err
	}

	var total float64
	total, err = db.OrderDetail.Filter(filter).SumFloat("price * quantity")
	if err != nil {
		return 0.0, err
	}

	err = db.Order.Save().Map(goent.Dict{"id": order.ID, "total": total})
	return total, err
}
