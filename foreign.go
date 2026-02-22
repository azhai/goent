package goent

import (
	"context"
	"maps"
	"reflect"
	"slices"
)

const (
	_ ForeignType = iota
	O2O
	O2M
	M2O
	M2M
)

type ForeignType uint

type ThirdParty struct {
	Table       string
	Left, Right string
	Where       Condition
}

type Foreign struct {
	Type       ForeignType
	MountField string
	ForeignKey string
	Reference  *Field
	Middle     *ThirdParty
	Where      Condition
}

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

func QuerySome2One[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64]*R, error) {
	col := table.ColumnInfo(foreign.ForeignKey)
	if col == nil {
		return nil, NewForeignKeyNotFoundError(foreign.ForeignKey)
	}
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64][]*T)
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

func QueryOne2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64][]*R, error) {
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64]*T)
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

func QueryMany2Many[T, R any](foreign *Foreign, table *Table[T], refer *Table[R]) (map[int64]*R, error) {
	if foreign.Middle == nil {
		return nil, ErrMiddleTableNotSet
	}
	if table.Cache == nil || table.Cache.Size() == 0 {
		return nil, nil
	}
	reg := make(map[int64]*T)
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

	rightIds := make([]int64, 0)
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

func QueryMiddleTable[T any](foreign *Foreign, table *Table[T], leftCol, rightCol string) (map[int64][]int64, error) {
	if foreign.Middle == nil {
		return nil, ErrMiddleTableNotSet
	}

	pkIds := make([]int64, 0)
	for id := range table.Cache.Each() {
		pkIds = append(pkIds, id)
	}
	if len(pkIds) == 0 {
		return make(map[int64][]int64), nil
	}
	slices.Sort(pkIds)

	leftField := &Field{ColumnName: leftCol}
	filter := And(foreign.Middle.Where, In(leftField, pkIds))

	builder := GetBuilder()
	defer PutBuilder(builder)
	builder.Type = 1
	builder.Where = filter
	builder.Selects = []*Field{
		{ColumnName: leftCol},
		{ColumnName: rightCol},
	}

	sqlQuery, args := builder.Build(false)
	rows, err := table.db.RawQueryContext(context.Background(), sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make(map[int64][]int64)
	for rows.Next() {
		var leftId, rightId int64
		if err = rows.Scan(&leftId, &rightId); err != nil {
			return nil, err
		}
		data[leftId] = append(data[leftId], rightId)
	}
	return data, nil
}
