package goent

import (
	"reflect"
	"slices"

	"github.com/azhai/goent/model"
)

type StateUpdate[T any] struct {
	changes []model.Set
	table   *Table[T]
	others  []*Table[T]
	*StateWhere
}

// Exec executes the update query
func (s *StateUpdate[T]) Exec() error {
	// TODO: add table to builder
	return s.exec(s.table.db.driver)
}

func (s *StateUpdate[T]) OnTransaction(tx model.Transaction) *StateUpdate[T] {
	s.StateWhere.conn = tx
	return s
}

// Set one or more arguments for update
func (s *StateUpdate[T]) Set(sets ...model.Set) *StateUpdate[T] {
	for i := range sets {
		attr := getArg(sets[i].Attribute, addrMap.mapField, nil)
		newbie := set{attribute: attr, value: sets[i].Value}
		s.builder.sets = append(s.builder.sets, newbie)
	}
	return s
}

func (s *StateUpdate[T]) Filter(args ...Condition) *StateUpdate[T] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

func (s *StateUpdate[T]) Match(obj T) *StateUpdate[T] {
	s.StateWhere = MatchWhere[T](s.StateWhere, s.table, obj)
	return s
}

type argSave struct {
	sets        []set
	argsWhere   []any
	valuesWhere []any
	skip        bool
}

func getArgsSave[T any](addrMap map[uintptr]field, table *T, value T, only, ignore []string) argSave {
	if table == nil {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	tableOf := reflect.ValueOf(table).Elem()

	if tableOf.Kind() != reflect.Struct {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	valueOf := reflect.ValueOf(value)

	sets := make([]set, 0)
	pksWhere, valuesWhere := make([]any, 0, valueOf.NumField()), make([]any, 0, valueOf.NumField())

	fld, ok := field(nil), false
	for i := 0; i < valueOf.NumField(); i++ {
		if valueOf.Field(i).IsZero() {
			continue
		}
		fieldName := tableOf.Type().Field(i).Name
		if len(only) > 0 { // only update fields, opposite ignore
			if !slices.Contains(only, fieldName) {
				continue
			}
		} else if slices.Contains(ignore, fieldName) {
			continue
		}

		addr := uintptr(tableOf.Field(i).Addr().UnsafePointer())
		if fld, ok = addrMap[addr]; !ok || fld == nil {
			continue
		}
		if fld.isPrimaryKey() {
			pksWhere = append(pksWhere, tableOf.Field(i).Addr().Interface())
			valuesWhere = append(valuesWhere, valueOf.Field(i).Interface())
		} else {
			val := valueOf.Field(i).Interface()
			sets = append(sets, set{attribute: fld, value: val})
		}
	}
	if len(pksWhere) == 0 || len(valuesWhere) == 0 {
		return argSave{skip: true}
	}
	return argSave{sets: sets, argsWhere: pksWhere, valuesWhere: valuesWhere}
}
