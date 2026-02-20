package goent

import (
	"fmt"
	"reflect"
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

func LoadForeigns[T, R any](name string, target *Table[T], refer *Table[R]) error {
	foreign, ok := target.Foreigns[name]
	if !ok {
		return nil
	}
	switch foreign.Type {
	default:
		return nil
	case O2O:
		_, err := QuerySome2One(foreign, target, refer)
		return err
	case M2O:
		_, err := QuerySome2One(foreign, target, refer)
		return err
	case O2M:
		_, err := QueryOne2Many(foreign, target, refer)
		return err
	case M2M:
		_, err := QueryMany2Many(foreign, target, refer)
		return err
	}
}

func QuerySome2One[T, R any](foreign *Foreign, target *Table[T], refer *Table[R]) (map[int64]*R, error) {
	values, data := make([]any, 0), make(map[int64]*R)
	col := target.Columns[foreign.ForeignKey]
	for _, row := range target.Cache.Each() {
		if val, ok := col.GetInt64(row); ok {
			values = append(values, val)
			data[val] = nil
			elem := reflect.ValueOf(row).Elem()
			elem.FieldByName(foreign.MountField).Set(reflect.ValueOf(data[val]).Addr())
		}
	}
	var err error
	query := refer.Select().OrderBy(foreign.Reference.ColumnName)
	filter := And(foreign.Where, In(foreign.Reference, values))
	data, err = query.Filter(filter).Map(foreign.ForeignKey)
	return data, err
}

func QueryOne2Many[T, R any](foreign *Foreign, target *Table[T], refer *Table[R]) (map[int64][]*R, error) {
	values, data := make([]any, 0), make(map[int64][]*R)
	for id, row := range target.Cache.Each() {
		values = append(values, id)
		data[id] = make([]*R, 0)
		elem := reflect.ValueOf(row).Elem()
		elem.FieldByName(foreign.MountField).Set(reflect.ValueOf(data[id]))
	}
	var err error
	query := refer.Select().OrderBy(foreign.Reference.ColumnName)
	filter := And(foreign.Where, In(foreign.Reference, values))
	data, err = query.Filter(filter).Rank(foreign.ForeignKey)
	fmt.Printf("%+v\n", data)
	return data, err
}

func QueryMany2Many[T, R any](foreign *Foreign, target *Table[T], refer *Table[R]) ([]*R, error) {
	var values []any
	for id := range target.Cache.Each() {
		values = append(values, id)
	}
	query := refer.Select().OrderBy(foreign.Reference.ColumnName)
	filter := And(foreign.Where, In(foreign.Reference, values))
	return query.Filter(filter).All()
}
