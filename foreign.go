package goent

import (
	"context"
	"maps"
	"reflect"
	"slices"

	"github.com/azhai/goent/model"
)

const (
	_   ForeignType = iota
	O2O             // one-to-one
	O2M             // one-to-many
	M2O             // many-to-one
	M2M             // many-to-many
)

// ForeignType represents the type of foreign key relationship
// Values: O2O (one-to-one), O2M (one-to-many), M2O (many-to-one), M2M (many-to-many)
type ForeignType uint

// ThirdParty represents an intermediate junction table for many-to-many relationships
// It contains the table name and the left/right column mappings
type ThirdParty struct {
	Table       string    // Junction table name
	Left, Right string    // Left and right column names
	Where       Condition // WHERE clause for filtering
}

// Foreign represents a foreign key relationship between two tables
// It contains the relationship type, mounting field, foreign key column,
// reference field, and optional middle table for many-to-many relationships
type Foreign struct {
	Type       ForeignType // Type of foreign key relationship
	MountField string      // Field name where the related object is mounted
	ForeignKey string      // Foreign key column name
	Reference  *Field      // Reference field in the related table
	Middle     *ThirdParty // Intermediate table for many-to-many relationships
	Where      Condition   // WHERE clause for filtering
}

// GetForeign retrieves the foreign key relationship between two tables
// It searches by table name first, then by table address
// Returns nil if no foreign key relationship is found
//
// Example:
//
//	foreign := GetForeign(userTable, addressTable)
//	if foreign != nil {
//		fmt.Println(foreign.Type) // prints O2O, O2M, M2O, or M2M
//	}
func GetForeign[T, R any](table *Table[T], refer *Table[R]) *Foreign {
	name := refer.TableInfo.TableName
	if foreign, ok := table.Foreigns[name]; ok {
		return foreign
	}
	tableAddr := refer.TableInfo.TableAddr
	for _, foreign := range table.Foreigns {
		if foreign.Reference.TableAddr == tableAddr {
			return foreign
		}
	}
	return nil
}

// QueryForeign queries and populates related records for a foreign key relationship
// It automatically determines the relationship type and calls the appropriate query method
// Returns nil if no foreign key relationship is found
//
// Example:
//
//	err := QueryForeign(orderTable, customerTable)
//	if err != nil {
//		log.Fatal(err)
//	}
//	for _, order := range orderTable.Cache.Each() {
//		fmt.Println(order.Customer) // populated Customer struct
//	}
func QueryForeign[T, R any](table *Table[T], refer *Table[R]) error {
	var foreign *Foreign
	if foreign = GetForeign(table, refer); foreign == nil {
		return nil
	}
	switch foreign.Type {
	default:
		return nil
	case O2O:
		_, err := QuerySome2One(foreign, table, refer)
		return err
	case M2O:
		_, err := QuerySome2One(foreign, table, refer)
		return err
	case O2M:
		_, err := QueryOne2Many(foreign, table, refer)
		return err
	case M2M:
		_, err := QueryMany2Many(foreign, table, refer)
		return err
	}
}

// QuerySome2One queries and populates many-to-one or one-to-one relationships
// It returns a map of foreign key IDs to referenced records
//
// Example:
//
//	results, err := QuerySome2One(foreign, orderTable, customerTable)
//	for id, customer := range results {
//		fmt.Printf("Order %d: %s\n", id, customer.Name)
//	}
func QuerySome2One[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64]*R, error) {
	col := table.ColumnInfo(foreign.ForeignKey)
	if col == nil {
		return nil, model.NewForeignKeyNotFoundError(foreign.ForeignKey)
	}
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64][]*T, table.Cache.Size())
	for _, row := range table.Cache.Each() {
		valueOf := reflect.ValueOf(row).Elem()
		fieldOf := valueOf.Field(col.FieldId)
		if key := fieldOf.Int(); key != 0 {
			reg[key] = append(reg[key], row)
		}
	}
	pkName := foreign.Reference.ColumnName
	pkIds := slices.Sorted(maps.Keys(reg))
	filter := And(foreign.Where, In(foreign.Reference, pkIds))
	data, err := refer.Select().Filter(filter).Map(pkName)
	if err != nil {
		return data, err
	}
	for id, rows := range reg {
		if val, ok := data[id]; ok {
			for _, row := range rows {
				elem := reflect.ValueOf(row).Elem()
				field := elem.FieldByName(foreign.MountField)
				if field.CanSet() {
					field.Set(reflect.ValueOf(val))
				}
			}
		}
	}
	return data, err
}

// QueryOne2Many queries and populates one-to-many relationships
// It returns a map of parent IDs to slices of child records
//
// Example:
//
//	results, err := QueryOne2Many(foreign, categoryTable, productTable)
//	for catId, products := range results {
//		fmt.Printf("Category %d has %d products\n", catId, len(products))
//	}
func QueryOne2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64][]*R, error) {
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64]*T, table.Cache.Size())
	for id, row := range table.Cache.Each() {
		reg[id] = row
		elem := reflect.ValueOf(row).Elem()
		field := elem.FieldByName(foreign.MountField)
		if field.CanSet() {
			sliceType := reflect.SliceOf(reflect.TypeFor[R]())
			field.Set(reflect.MakeSlice(sliceType, 0, 0))
		}
	}

	pkName := foreign.Reference.ColumnName
	pkIds := slices.Sorted(maps.Keys(reg))
	filter := And(foreign.Where, In(foreign.Reference, pkIds))
	data, err := refer.Select().Filter(filter).Rank(pkName)
	if err != nil {
		return data, err
	}
	for id, rows := range data {
		if row, ok := reg[id]; ok {
			elem := reflect.ValueOf(row).Elem()
			field := elem.FieldByName(foreign.MountField)
			if field.CanSet() {
				field.Set(reflect.ValueOf(rows))
			}
		}
	}
	return data, err
}

// QueryMany2Many queries and populates many-to-many relationships
// It uses the middle table to establish the relationship between two tables
// Returns a map of left-side IDs to right-side records
//
// Example:
//
//	results, err := QueryMany2Many(foreign, studentTable, courseTable)
//	for studentId, courses := range results {
//		fmt.Printf("Student %d is enrolled in %d courses\n", studentId, len(courses))
//	}
func QueryMany2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64]*R, error) {
	if foreign.Middle == nil {
		return nil, model.ErrMiddleTableNotSet
	}
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64]*T, table.Cache.Size())
	for id, row := range table.Cache.Each() {
		reg[id] = row
		elem := reflect.ValueOf(row).Elem()
		field := elem.FieldByName(foreign.MountField)
		if field.CanSet() {
			sliceType := reflect.SliceOf(reflect.TypeFor[R]())
			field.Set(reflect.MakeSlice(sliceType, 0, 0))
		}
	}

	middleData, err := QueryMiddleTable(foreign, table, foreign.Middle.Left, foreign.Middle.Right)
	if err != nil {
		return nil, err
	}

	rightIds := make([]int64, 0, len(middleData))
	for _, pids := range middleData {
		rightIds = append(rightIds, pids...)
	}
	slices.Sort(rightIds)
	rightIds = slices.Compact(rightIds)

	pkName := foreign.Reference.ColumnName
	filter := And(foreign.Where, In(foreign.Reference, rightIds))
	data, err := refer.Select().Filter(filter).Map(pkName)
	if err != nil {
		return data, err
	}

	for leftId, rightIdList := range middleData {
		if row, ok := reg[leftId]; ok {
			elem := reflect.ValueOf(row).Elem()
			field := elem.FieldByName(foreign.MountField)
			if !field.CanSet() {
				continue
			}
			products := reflect.MakeSlice(field.Type(), 0, len(rightIdList))
			for _, rightId := range rightIdList {
				if product, ok := data[rightId]; ok {
					products = reflect.Append(products, reflect.ValueOf(product))
				}
			}
			field.Set(products)
		}
	}
	return data, err
}

// QueryMiddleTable queries the middle junction table for many-to-many relationships
// It returns a map of left-side IDs to slices of right-side IDs
//
// Example:
//
//	mapping, err := QueryMiddleTable(foreign, studentTable, "student_id", "course_id")
//	for studentId, courseIds := range mapping {
//		fmt.Printf("Student %d is in courses: %v\n", studentId, courseIds)
//	}
func QueryMiddleTable[T any](foreign *Foreign, table *Table[T], leftCol, rightCol string) (map[int64][]int64, error) {
	if foreign.Middle == nil {
		return nil, model.ErrMiddleTableNotSet
	}

	size := table.Cache.Size()
	pkIds := make([]int64, 0, size)
	for id := range table.Cache.Each() {
		pkIds = append(pkIds, id)
	}
	if len(pkIds) == 0 {
		return nil, nil
	}
	slices.Sort(pkIds)

	leftField := &Field{ColumnName: leftCol}
	filter := And(foreign.Middle.Where, In(leftField, pkIds))

	builder := GetBuilder()
	defer PutBuilder(builder)
	builder.Type = model.SelectQuery
	builder.SetTable(table.TableInfo, table.db.driver)
	builder.Where, builder.Limit = filter, size
	builder.VisitFields = []*Field{
		{ColumnName: leftCol},
		{ColumnName: rightCol},
	}

	sqlQuery, args := builder.Build(false)
	rows, err := table.db.RawQueryContext(context.Background(), sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make(map[int64][]int64, size)
	for rows.Next() {
		var leftId, rightId int64
		if err = rows.Scan(&leftId, &rightId); err != nil {
			return nil, err
		}
		data[leftId] = append(data[leftId], rightId)
	}
	return data, nil
}
