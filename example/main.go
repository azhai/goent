package main

import (
	"fmt"

	"goent-example/models"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

func main() {
	env := utils.NewEnvWithFile("../.env")
	cfg := drivers.LoadConfig(env, "test.db")
	db, err := models.OpenDB(cfg)
	if err != nil {
		panic(err)
	}
	defer clearDatabase(db, false)

	// addForeignKeys(db)

	if err = seedData(db); err != nil {
		panic(err)
	}

	order := &models.Order{OrderNo: TestOrderNo}
	fields := []any{"id", "order_no", "total", "status"}
	order, err = db.Order.Select(fields...).Match(*order).One()
	if order == nil {
		order, err = createOrder(db, TestOrderNo)
	}
	if err != nil {
		fmt.Println(err)
	}
	if order != nil {
		if order.ID > 0 && order.Total == 0.0 {
			order.Total, err = CalcTotalPrice2(db, order)
		}
	}

	products, _ := ListAllProducts(db)
	if len(products) > 0 {
		fmt.Printf("%+v\n", products[0])
	}
	fmt.Printf("%+v\n", order)
}

func clearDatabase(db *models.Database, isDrop bool) {
	var err error
	if db != nil && isDrop {
		// sql := "DROP TABLE IF EXISTS public.t_order_product, public.t_order, public.t_product, public.t_category CASCADE"
		// err = db.RawExecContext(context.Background(), sql)
		if err = db.DropTables(); err != nil {
			panic(err)
		}
	}
	models.CloseDB()
}

func seedData(db *models.Database) error {
	count, err := db.Category.Count("*")
	if err != nil || count > 0 {
		return err
	}
	if err = db.Category.Insert().All(false, DataCategories()); err != nil {
		return err
	}
	if err = db.Product.Insert().All(false, DataProducts()); err != nil {
		return err
	}
	return nil
}

func createOrder(db *models.Database, orderNo string) (*models.Order, error) {
	obj, err := db.Order.Select("id", "status", "total").Match(models.Order{OrderNo: orderNo}).One()
	fmt.Printf("createOrder: obj=%v, err=%v, err==ErrNoRows=%v\n", obj, err, err == model.ErrNoRows)
	if err != nil && err != model.ErrNoRows || obj != nil && obj.ID > 0 {
		return nil, err
	}
	order := DataOrder(orderNo)
	fmt.Printf("createOrder: inserting order=%+v\n", order)
	if err = db.Order.Insert().One(order); err != nil {
		return nil, err
	}
	orderDetail := DataOrderDetail(order.ID)
	if err = db.OrderDetail.Insert().All(true, orderDetail); err != nil {
		return nil, err
	}
	return order, nil
}

func CalcTotalPrice(db *models.Database, order *models.Order) (float64, error) {
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

	filter = goent.In(db.Product.Field("id"), GetProductIds(order))
	products, err := db.Product.Select().OrderBy("id").Filter(filter).All()
	if err != nil {
		return 0.0, err
	}

	productMap := make(map[int64]*models.Product, len(products))
	for _, p := range products {
		productMap[p.ID] = p
	}

	var total float64
	for _, detail := range order.Details {
		product := productMap[detail.ProductID]
		if product == nil {
			return total, err
		}
		detail.Price = product.Price
		total += detail.Price * float64(detail.Quantity)
		err = db.OrderDetail.Save().One(detail)
		if err != nil {
			return total, err
		}
	}

	err = db.Order.Save().Map(goent.Dict{"id": order.ID, "total": total})
	return total, err
}

func CalcTotalPrice2(db *models.Database, order *models.Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	query := db.OrderDetail.Select().OrderBy("product_id").Filter(filter)
	order.Details, err = query.LeftJoin("product_id", db.Product.Field("id")).All()
	if err != nil {
		return 0.0, err
	}

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

func CalcTotalPrice3(db *models.Database, order *models.Order) (float64, error) {
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

func ListAllProducts(db *models.Database) ([]*models.Product, error) {
	filter := goent.LessEquals(db.Product.Field("price"), 100)
	query := db.Product.Select().Filter(filter)
	products, err := query.All()
	if err != nil {
		return nil, err
	}

	_ = goent.QueryForeignByName(db.Product, products, "Category")

	// var cateIds []int64
	// for _, p := range products {
	// 	cateIds = append(cateIds, p.CategoryID)
	// }
	// filter := goent.In(db.Product.Field("id"), cateIds)
	// _, err = db.Category.Select().Filter(filter).All()
	// if err != nil {
	// 	return nil, err
	// }

	return products, nil
}

func addForeignKeys(db *models.Database) {
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
			MountField: "OrderDetails",
			ForeignKey: "id",
			Reference:  db.OrderDetail.Field("order_id"),
			Middle:     nil,
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
