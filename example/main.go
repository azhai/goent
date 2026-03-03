package main

import (
	"fmt"
	"strings"

	"goent-example/models"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// Database is the database connection with its driver.
type Database struct {
	models.PublicSchema `goe:"public;prefix:t_"`
	*goent.DB
}

func main() {
	var dbDSN, logFile string
	env := utils.NewEnvWithFile("../.env")
	dbType := env.GetStr("GOE_DRIVER", "sqlite")
	if dbDSN = env.Get("GOE_DATABASE_DSN"); dbDSN == "" {
		dbDSN = models.DefaultDSN(dbType)
	}
	if logFile = env.Get("GOE_LOG_FILE"); logFile == "" {
		logFile = "stdout"
	}
	db, err := connect(dbType, dbDSN, logFile)
	if err != nil {
		panic(err)
	}
	defer clearDatabase(db, false)

	if err = goent.AutoMigrate(db); err != nil {
		panic(err)
	}

	// addForeignKeys(db)

	if err = seedData(db); err != nil {
		panic(err)
	}

	order := &models.Order{OrderNo: models.TestOrderNo}
	fields := []any{"id", "order_no", "total", "status"}
	order, err = db.Order.Select(fields...).Match(*order).One()
	if order == nil {
		order, err = createOrder(db, models.TestOrderNo)
	}
	if err != nil {
		fmt.Println(err)
	}
	if order != nil {
		if order.ID > 0 && order.Total == 0.0 {
			order.Total, err = CalcTotalPrice2(db, order)
		}
		// fmt.Printf("\n\nOrder:\n\n%+v\n\n%#v\n\n%#v\n", order, db.Order.Cache, db.Order.TableInfo)
	}

	products, _ := ListAllProducts(db)
	if len(products) > 0 {
		fmt.Printf("%+v\n", products[0])
	}
	fmt.Printf("%+v\n", order)
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

func clearDatabase(db *Database, isDrop bool) {
	var err error
	if isDrop {
		// sql := "DROP TABLE IF EXISTS public.t_order_product, public.t_order, public.t_product, public.t_category CASCADE"
		// err = db.RawExecContext(context.Background(), sql)
		err = db.DropTables()
	}
	_ = goent.Close(db)
	if err != nil {
		panic(err)
	}
}

func seedData(db *Database) error {
	count, err := db.Category.Count("*")
	if err != nil || count > 0 {
		return err
	}
	if err = db.Category.Insert().All(false, models.DataCategories()); err != nil {
		return err
	}
	if err = db.Product.Insert().All(false, models.DataProducts()); err != nil {
		return err
	}
	return nil
}

func createOrder(db *Database, orderNo string) (*models.Order, error) {
	obj, err := db.Order.Select("id", "status", "total").Match(models.Order{OrderNo: orderNo}).One()
	fmt.Printf("createOrder: obj=%v, err=%v, err==ErrNoRows=%v\n", obj, err, err == model.ErrNoRows)
	if err != nil && err != model.ErrNoRows || obj != nil && obj.ID > 0 {
		return nil, err
	}
	order := models.DataOrder(orderNo)
	fmt.Printf("createOrder: inserting order=%+v\n", order)
	if err = db.Order.Insert().One(order); err != nil {
		return nil, err
	}
	orderDetail := models.DataOrderDetail(order.ID)
	if err = db.OrderDetail.Insert().All(true, orderDetail); err != nil {
		return nil, err
	}
	return order, nil
}

func CalcTotalPrice(db *Database, order *models.Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	query := db.OrderDetail.Select().OrderBy("product_id")
	order.OrderDetails, err = query.Filter(filter).All()
	if err != nil {
		return 0.0, err
	}

	filter = goent.In(db.Product.Field("id"), order.GetProductIds())
	order.Products, err = db.Product.Select().OrderBy("id").Filter(filter).All()
	if err != nil {
		return 0.0, err
	}

	productMap := make(map[int64]*models.Product, len(order.Products))
	for _, p := range order.Products {
		productMap[p.ID] = p
	}

	var total float64
	for _, detail := range order.OrderDetails {
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

func CalcTotalPrice2(db *Database, order *models.Order) (float64, error) {
	var err error
	if order == nil {
		return 0.0, err
	}

	filter := goent.Equals(db.OrderDetail.Field("order_id"), order.ID)
	query := db.OrderDetail.Select().OrderBy("product_id").Filter(filter)
	order.OrderDetails, err = query.LeftJoin("product_id", db.Product.Field("id")).All()
	if err != nil {
		return 0.0, err
	}

	var total float64
	for _, detail := range order.OrderDetails {
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

func CalcTotalPrice3(db *Database, order *models.Order) (float64, error) {
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

func ListAllProducts(db *Database) ([]*models.Product, error) {
	filter := goent.LessEquals(db.Product.Field("price"), 100)
	query := db.Product.Select().Filter(filter)
	products, err := query.All()
	if err != nil {
		return nil, err
	}

	_ = goent.QueryForeign(db.Product, db.Category)
	_ = goent.QueryForeign(db.Order, db.OrderDetail)
	_ = goent.QueryForeign(db.Order, db.Product)
	_ = goent.QueryForeign(db.OrderDetail, db.Product)

	// var cateIds []int64
	// for _, p := range products {
	// 	cateIds = append(cateIds, p.CategoryID)
	// }
	// filter := goent.In(db.Product.Field("id"), cateIds)
	// _, err = db.Category.Select().Filter(filter).All()
	// if err != nil {
	// 	return nil, err
	// }

	fmt.Printf("\nCategory:\n%+v\n", db.Category.Cache)
	fmt.Printf("\nProduct:\n%+v\n", db.Product.Cache)
	return products, nil
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
