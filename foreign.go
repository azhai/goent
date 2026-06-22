package goent

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"

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
	RefType    string      // Type name of the referenced struct (e.g. "Contributor" for AssigneeID)

	mountFieldIdx atomic.Int32 // Cached field index for MountField (-1 = not found, 0 = not cached, >0 = index+1)
}

// getMountFieldIdx returns the cached field index for MountField, looking it up on first access.
func (f *Foreign) getMountFieldIdx(valueOf reflect.Value) int {
	idx := f.mountFieldIdx.Load()
	if idx != 0 {
		return int(idx)
	}
	sf, ok := valueOf.Type().FieldByName(f.MountField)
	if ok {
		idx = int32(sf.Index[0] + 1)
	} else {
		idx = -1
	}
	f.mountFieldIdx.Store(idx)
	return int(idx)
}

// fieldByCachedIdx returns the reflect.Value at the cached field index.
func fieldByCachedIdx(valueOf reflect.Value, idx int) reflect.Value {
	if idx > 0 {
		return valueOf.Field(idx - 1)
	}
	panic("field index not cached")
}

// GetForeign retrieves the foreign key relationship between two tables
// It searches by table name first, then by table address
// Returns nil if no foreign key relationship is found
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

// QueryForeign queries and populates related records for a foreign key relationship.
// It automatically determines the relationship type and calls the appropriate query method.
// The rows parameter contains the records whose foreign fields should be populated.
// Returns nil if no foreign key relationship is found.
func QueryForeign[T, R any](table *Table[T], refer *Table[R], rows []*T) error {
	var foreign *Foreign
	if foreign = GetForeign(table, refer); foreign == nil {
		return nil
	}
	switch foreign.Type {
	default:
		return nil
	case O2O:
		_, err := QuerySome2One(foreign, table, refer, rows)
		return err
	case M2O:
		_, err := QuerySome2One(foreign, table, refer, rows)
		return err
	case O2M:
		_, err := QueryOne2Many(foreign, table, refer, rows)
		return err
	case M2M:
		_, err := QueryMany2Many(foreign, table, refer, rows)
		return err
	}
}

// QueryForeignByName queries and populates related records by foreign key name.
// It looks up the foreign relationship by name in the table's Foreigns map,
// then uses reflection to call the appropriate query method.
// The name can be the foreign table name, field name, or column name.
// The rows parameter contains the records whose foreign fields should be populated.
func QueryForeignByName[T any](table *Table[T], rows []*T, name string) error {
	return QueryForeignByNameContext(context.Background(), table, rows, name)
}

// QueryForeignByNameContext queries and populates related records by foreign key name using the given context.
// The context can carry a transaction for atomic operations.
// The rows parameter contains the records whose foreign fields should be populated.
func QueryForeignByNameContext[T any](ctx context.Context, table *Table[T], rows []*T, name string) error {
	foreign := findForeignByName(table.Foreigns, name)
	if foreign == nil {
		return nil
	}
	return queryForeignReflect(ctx, foreign, table, rows)
}

// QueryForeignsByName queries and populates multiple related records by foreign key names.
// The rows parameter contains the records whose foreign fields should be populated.
func QueryForeignsByName[T any](table *Table[T], rows []*T, names ...string) error {
	return QueryForeignsByNameContext(context.Background(), table, rows, names...)
}

// QueryForeignsByNameContext queries and populates multiple related records by foreign key names using the given context.
// The context can carry a transaction for atomic operations.
// The rows parameter contains the records whose foreign fields should be populated.
func QueryForeignsByNameContext[T any](ctx context.Context, table *Table[T], rows []*T, names ...string) error {
	for _, name := range names {
		if err := QueryForeignByNameContext(ctx, table, rows, name); err != nil {
			return err
		}
	}
	return nil
}

// findForeignByName searches for a foreign relationship by name.
// It checks the Foreigns map keys (table names), MountField, and ForeignKey.
// Matching is case-insensitive.
func findForeignByName(foreigns map[string]*Foreign, name string) *Foreign {
	if foreign, ok := foreigns[name]; ok {
		return foreign
	}
	lowerName := strings.ToLower(name)
	for key, foreign := range foreigns {
		if strings.ToLower(key) == lowerName {
			return foreign
		}
		if strings.ToLower(foreign.MountField) == lowerName || strings.ToLower(foreign.ForeignKey) == lowerName {
			return foreign
		}
	}
	return nil
}

// queryForeignReflect executes the appropriate foreign key query using reflection.
func queryForeignReflect[T any](ctx context.Context, foreign *Foreign, table *Table[T], rows []*T) error {
	if len(rows) == 0 {
		return nil
	}
	if foreign.Reference == nil {
		return nil
	}
	refInfo := GetTableInfo(foreign.Reference.TableAddr)
	if refInfo == nil {
		return nil
	}
	switch foreign.Type {
	default:
		return nil
	case O2O, M2O:
		return querySome2OneReflect(ctx, foreign, table, refInfo, rows)
	case O2M:
		return queryOne2ManyReflect(ctx, foreign, table, refInfo, rows)
	case M2M:
		return queryMany2ManyReflect(ctx, foreign, table, refInfo, rows)
	}
}

// fieldInt64 returns the int64 value of a reflect.Value, dereferencing pointers.
// Returns (0, false) for nil pointers.
func fieldInt64(v reflect.Value) (int64, bool) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	return v.Int(), true
}

// mapRowsByField indexes rows by an int64 field (FK or PK), returning a map of field value to row slice.
// Pointer fields are dereferenced; nil pointers are skipped.
func mapRowsByField[T any](rows []*T, col *Column) map[int64][]*T {
	reg := make(map[int64][]*T, len(rows))
	for _, row := range rows {
		valueOf := reflect.ValueOf(row).Elem()
		fieldOf := valueOf.Field(col.FieldId)
		if key, ok := fieldInt64(fieldOf); ok && key != 0 {
			reg[key] = append(reg[key], row)
		}
	}
	return reg
}

// mapRowsByPK indexes rows by their primary key, returning a map of PK value to row.
// Also initializes foreign slice fields on each row.
func mapRowsByPK[T any](rows []*T, table *Table[T], foreign *Foreign) map[int64]*T {
	reg := make(map[int64]*T, len(rows))
	pkCol := table.ColumnInfo(table.PrimaryKeys[0].ColumnName)
	if pkCol == nil {
		return reg
	}
	for _, row := range rows {
		valueOf := reflect.ValueOf(row).Elem()
		pkField := valueOf.Field(pkCol.FieldId)
		if id, ok := fieldInt64(pkField); ok && id != 0 {
			reg[id] = row
		}
		if foreign != nil {
			initForeignSlice(row, foreign)
		}
	}
	return reg
}

// querySome2OneReflect performs M2O/O2O query using reflection instead of generics for the refer table.
func querySome2OneReflect[T any](ctx context.Context, foreign *Foreign, table *Table[T], refInfo *TableInfo, rows []*T) error {
	col := table.ColumnInfo(foreign.ForeignKey)
	if col == nil {
		return model.NewForeignKeyNotFoundError(foreign.ForeignKey)
	}
	reg := mapRowsByField(rows, col)
	pkIds := slices.Sorted(maps.Keys(reg))
	filter := And(foreign.Where, InBatch(foreign.Reference, pkIds, 500))

	pkName := foreign.Reference.ColumnName
	data, err := selectReferMap(ctx, refInfo, filter, pkName)
	if err != nil {
		return err
	}
	for id, matchedRows := range reg {
		if val, ok := data[id]; ok {
			for _, row := range matchedRows {
				setForeignField(row, foreign.MountField, val.Interface())
			}
		}
	}
	return nil
}

// setForeignField sets a foreign relationship field on a row, using GenSetForeign if available.
func setForeignField(row any, mountField string, value any) {
	if setter, ok := row.(GenSetForeign); ok {
		setter.SetForeign(mountField, value)
		return
	}
	elem := reflect.ValueOf(row).Elem()
	foreign := &Foreign{MountField: mountField}
	idx := foreign.getMountFieldIdx(elem)
	field := fieldByCachedIdx(elem, idx)
	if field.CanSet() {
		field.Set(reflect.ValueOf(value))
	}
}

// initForeignSlice initializes a foreign relationship slice field to an empty slice.
func initForeignSlice(row any, foreign *Foreign) {
	if setter, ok := row.(GenSetForeign); ok {
		// Create an empty slice of the correct type using reflection so SetForeign
		// receives a non-nil empty slice instead of nil, matching the non-GenSetForeign path.
		elem := reflect.ValueOf(row).Elem()
		idx := foreign.getMountFieldIdx(elem)
		field := fieldByCachedIdx(elem, idx)
		emptySlice := reflect.MakeSlice(field.Type(), 0, 0).Interface()
		setter.SetForeign(foreign.MountField, emptySlice)
		return
	}
	elem := reflect.ValueOf(row).Elem()
	idx := foreign.getMountFieldIdx(elem)
	field := fieldByCachedIdx(elem, idx)
	if field.CanSet() {
		field.Set(reflect.MakeSlice(field.Type(), 0, 0))
	}
}

// queryOne2ManyReflect performs O2M query using reflection.
func queryOne2ManyReflect[T any](ctx context.Context, foreign *Foreign, table *Table[T], refInfo *TableInfo, rows []*T) error {
	reg := mapRowsByPK(rows, table, foreign)

	fkName := foreign.ForeignKey // e.g. "order_id" in the child table
	pkIds := slices.Sorted(maps.Keys(reg))

	// Build filter: WHERE order_id IN (5557, ...) using the FK column in the child table
	fkField := &Field{ColumnName: fkName}
	filter := And(foreign.Where, InBatch(fkField, pkIds, 500))

	data, err := selectReferRank(ctx, refInfo, filter, fkName)
	if err != nil {
		return err
	}
	for id, refRows := range data {
		if row, ok := reg[id]; ok {
			sliceType := reflect.SliceOf(reflect.PointerTo(refInfo.modelType))
			sliceVal := reflect.MakeSlice(sliceType, len(refRows), len(refRows))
			for i, r := range refRows {
				sliceVal.Index(i).Set(r)
			}
			setForeignField(row, foreign.MountField, sliceVal.Interface())
		}
	}
	return nil
}

// queryMany2ManyReflect performs M2M query using reflection.
func queryMany2ManyReflect[T any](ctx context.Context, foreign *Foreign, table *Table[T], refInfo *TableInfo, rows []*T) error {
	if foreign.Middle == nil {
		return model.ErrMiddleTableNotSet
	}
	reg := mapRowsByPK(rows, table, foreign)

	middleData, err := QueryMiddleTableContext(ctx, foreign, table, rows, foreign.Middle.Left, foreign.Middle.Right)
	if err != nil {
		return err
	}

	rightIds := make([]int64, 0, len(middleData))
	for _, pids := range middleData {
		rightIds = append(rightIds, pids...)
	}
	slices.Sort(rightIds)
	rightIds = slices.Compact(rightIds)

	pkName := foreign.Reference.ColumnName
	filter := And(foreign.Where, InBatch(foreign.Reference, rightIds, 500))
	data, err := selectReferMap(ctx, refInfo, filter, pkName)
	if err != nil {
		return err
	}

	for leftId, rightIdList := range middleData {
		if row, ok := reg[leftId]; ok {
			rowValues := make([]any, 0, len(rightIdList))
			for _, rightId := range rightIdList {
				if product, ok := data[rightId]; ok {
					rowValues = append(rowValues, product.Elem().Interface())
				}
			}
			setForeignField(row, foreign.MountField, rowValues)
		}
	}
	return nil
}

// selectReferMap performs a SELECT query on a refer table and returns results as a map by pkName.
// It uses reflection to scan rows into dynamically created structs.
func selectReferMap(ctx context.Context, refInfo *TableInfo, filter Condition, pkName string) (map[int64]reflect.Value, error) {
	builder := GetBuilder()
	defer PutBuilder(builder)
	builder.Type = model.SelectQuery
	builder.SetTable(refInfo)
	builder.core.Where = filter
	builder.VisitFields = refInfo.GetSortedFields()

	sqlQuery, args := builder.Build(false)
	rows, err := refInfo.db.RawQueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]reflect.Value)
	pkCol := refInfo.ColumnInfo(pkName)
	if pkCol == nil {
		return result, nil
	}
	for rows.Next() {
		val := reflect.New(refInfo.modelType)
		dest := AppendDestTable(refInfo, val.Elem())
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		// Use fieldInt64 to safely dereference pointer fields before calling .Int()
		pkField := val.Elem().Field(pkCol.FieldId)
		if id, ok := fieldInt64(pkField); ok && id != 0 {
			result[id] = val
		}
	}
	return result, rows.Err()
}

// selectReferRank performs a SELECT query on a refer table and returns results grouped by pkName.
func selectReferRank(ctx context.Context, refInfo *TableInfo, filter Condition, pkName string) (map[int64][]reflect.Value, error) {
	builder := GetBuilder()
	defer PutBuilder(builder)
	builder.Type = model.SelectQuery
	builder.SetTable(refInfo)
	builder.core.Where = filter
	builder.VisitFields = refInfo.GetSortedFields()

	sqlQuery, args := builder.Build(false)
	rows, err := refInfo.db.RawQueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64][]reflect.Value)
	pkCol := refInfo.ColumnInfo(pkName)
	if pkCol == nil {
		return result, nil
	}
	for rows.Next() {
		val := reflect.New(refInfo.modelType)
		dest := AppendDestTable(refInfo, val.Elem())
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		// Use fieldInt64 to safely dereference pointer fields before calling .Int()
		pkField := val.Elem().Field(pkCol.FieldId)
		if id, ok := fieldInt64(pkField); ok && id != 0 {
			result[id] = append(result[id], val)
		}
	}
	return result, rows.Err()
}

// QuerySome2One queries and populates M2O/O2O relationships.
// The rows parameter contains the records whose foreign fields should be populated.
// It returns a map of foreign key IDs to referenced records.
func QuerySome2One[T, R any](foreign *Foreign, table *Table[T], refer *Table[R], rows []*T) (map[int64]*R, error) {
	col := table.ColumnInfo(foreign.ForeignKey)
	if col == nil {
		return nil, model.NewForeignKeyNotFoundError(foreign.ForeignKey)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	reg := mapRowsByField(rows, col)
	pkName := foreign.Reference.ColumnName
	pkIds := slices.Sorted(maps.Keys(reg))
	filter := And(foreign.Where, InBatch(foreign.Reference, pkIds, 500))
	data, err := refer.Select().Filter(filter).Map(pkName)
	if err != nil {
		return data, err
	}
	for id, matchedRows := range reg {
		if val, ok := data[id]; ok {
			for _, row := range matchedRows {
				elem := reflect.ValueOf(row).Elem()
				idx := foreign.getMountFieldIdx(elem)
				field := fieldByCachedIdx(elem, idx)
				if field.CanSet() {
					field.Set(reflect.ValueOf(val))
				}
			}
		}
	}
	return data, err
}

// QueryOne2Many queries and populates one-to-many relationships.
// The rows parameter contains the records whose foreign fields should be populated.
// It returns a map of parent IDs to slices of child records.
func QueryOne2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R], rows []*T) (map[int64][]*R, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	reg := mapRowsByPK(rows, table, foreign)

	pkName := foreign.Reference.ColumnName
	pkIds := slices.Sorted(maps.Keys(reg))
	filter := And(foreign.Where, InBatch(foreign.Reference, pkIds, 500))
	data, err := refer.Select().Filter(filter).Rank(pkName)
	if err != nil {
		return data, err
	}
	for id, refRows := range data {
		if row, ok := reg[id]; ok {
			elem := reflect.ValueOf(row).Elem()
			idx := foreign.getMountFieldIdx(elem)
			field := fieldByCachedIdx(elem, idx)
			if field.CanSet() {
				field.Set(reflect.ValueOf(refRows))
			}
		}
	}
	return data, err
}

// QueryMany2Many queries and populates many-to-many relationships.
// It uses the middle table to establish the relationship between two tables.
// The rows parameter contains the records whose foreign fields should be populated.
// Returns a map of left-side IDs to right-side records.
func QueryMany2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R], rows []*T) (map[int64]*R, error) {
	if foreign.Middle == nil {
		return nil, model.ErrMiddleTableNotSet
	}
	if len(rows) == 0 {
		return nil, nil
	}
	reg := mapRowsByPK(rows, table, foreign)

	middleData, err := QueryMiddleTable(foreign, table, rows, foreign.Middle.Left, foreign.Middle.Right)
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
	filter := And(foreign.Where, InBatch(foreign.Reference, rightIds, 500))
	data, err := refer.Select().Filter(filter).Map(pkName)
	if err != nil {
		return data, err
	}

	for leftId, rightIdList := range middleData {
		if row, ok := reg[leftId]; ok {
			elem := reflect.ValueOf(row).Elem()
			idx := foreign.getMountFieldIdx(elem)
			field := fieldByCachedIdx(elem, idx)
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

// QueryMiddleTable queries the middle junction table for many-to-many relationships.
// The rows parameter contains the records whose primary keys are used to query the junction table.
// It returns a map of left-side IDs to slices of right-side IDs.
func QueryMiddleTable[T any](foreign *Foreign, table *Table[T], rows []*T, leftCol, rightCol string) (map[int64][]int64, error) {
	return QueryMiddleTableContext(context.Background(), foreign, table, rows, leftCol, rightCol)
}

// QueryMiddleTableContext queries the middle junction table with a specific context.
func QueryMiddleTableContext[T any](ctx context.Context, foreign *Foreign, table *Table[T], rows []*T, leftCol, rightCol string) (map[int64][]int64, error) {
	if foreign.Middle == nil {
		return nil, model.ErrMiddleTableNotSet
	}

	pkCol := table.ColumnInfo(table.PrimaryKeys[0].ColumnName)
	if pkCol == nil {
		return nil, nil
	}

	pkIds := make([]int64, 0, len(rows))
	for _, row := range rows {
		valueOf := reflect.ValueOf(row).Elem()
		pkField := valueOf.Field(pkCol.FieldId)
		if id, ok := fieldInt64(pkField); ok && id != 0 {
			pkIds = append(pkIds, id)
		}
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
	builder.SetTable(table.TableInfo)
	builder.core.Where, builder.core.Limit = filter, len(pkIds)
	builder.VisitFields = []*Field{
		{ColumnName: leftCol},
		{ColumnName: rightCol},
	}

	sqlQuery, args := builder.Build(false)
	dbRows, err := table.db.RawQueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	data := make(map[int64][]int64, len(pkIds))
	for dbRows.Next() {
		var leftId, rightId int64
		if err = dbRows.Scan(&leftId, &rightId); err != nil {
			return nil, err
		}
		data[leftId] = append(data[leftId], rightId)
	}
	if err := dbRows.Err(); err != nil {
		return nil, err
	}
	return data, nil
}
