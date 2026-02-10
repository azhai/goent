package goent

import (
	"fmt"
	"reflect"
)

type Field struct {
	Table  uintptr
	Column string
	Func   string
}

func (f *Field) String() string {
	res, err := f.Column, error(nil)
	if res != "*" {
		res, err = GetFieldName(f.Table, res)
		if err != nil {
			return ""
		}
	}
	if f.Func != "" {
		return fmt.Sprintf(f.Func, res)
	}
	return res
}

type Value struct {
	Type   reflect.Kind
	Args   []any
	Length int
}

func NewValue(value any) *Value {
	valueOf := reflect.ValueOf(value)
	kind, size := valueOf.Kind(), valueOf.Len()
	if kind == reflect.Slice {
		args := make([]any, size)
		for i := range args {
			args[i] = valueOf.Index(i).Interface()
		}
		return &Value{Type: kind, Args: args, Length: size}
	} else if kind == reflect.Pointer && valueOf.IsNil() {
		return &Value{Type: kind, Args: nil, Length: 0}
	} else {
		return &Value{Type: kind, Args: []any{value}, Length: 1}
	}
}

type Condition struct {
	Template string
	Fields   []*Field
	Values   []*Value
}

func (c Condition) IsEmpty() bool {
	return c.Template == ""
}

// And Example
//
//	where.And(
//		where.Equals(&db.Animal.Status, "Eating"),
//		where.Like(&db.Animal.Name, "%Cat%"),
//		where.GreaterThan(&db.Animal.Age, 3),
//	)
func And(branches ...Condition) Condition {
	var size int
	if size = len(branches); size == 0 {
		return Condition{}
	} else if size == 1 {
		return branches[0]
	}
	res := Condition{Template: "("}
	for i, cond := range branches {
		res.Template += cond.Template
		if i > 0 {
			res.Template += ") AND ("
		}
		res.Fields = append(res.Fields, cond.Fields...)
		res.Values = append(res.Values, cond.Values...)
	}
	res.Template += ")"
	return res
}

// Or Example
//
//	where.Or(
//		where.Equals(&db.Animal.Status, "Eating"),
//		where.Like(&db.Animal.Name, "%Cat%"),
//		where.LessThan(&db.Animal.Age, 1),
//	)
func Or(branches ...Condition) Condition {
	var size int
	if size = len(branches); size == 0 {
		return Condition{}
	} else if size == 1 {
		return branches[0]
	}
	res := Condition{Template: ""}
	for i, cond := range branches {
		res.Template += cond.Template
		if i > 0 {
			res.Template += " OR "
		}
		res.Fields = append(res.Fields, cond.Fields...)
		res.Values = append(res.Values, cond.Values...)
	}
	return res
}

// Equals
//
//	// Example: using Table with field name
//	where.Equals(&db.OrderDetail, "order_id", 1)
func Equals(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length == 0 {
		return Condition{Template: "%s IS NULL", Fields: []*Field{left}, Values: []*Value{}}
	} else {
		return Condition{Template: "%s = ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
}

func EqualsField(left, right *Field) Condition {
	return Condition{Template: "%s = %s", Fields: []*Field{left, right}}
}

func NotEquals(left *Field, value any) Condition {
	cond := Equals(left, value)
	if len(cond.Values) == 0 {
		cond.Template = "%s IS NOT NULL"
	} else {
		cond.Template = "%s != ?"
	}
	return cond
}

func NotEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s != %s", Fields: []*Field{left, right}}
}

func EqualsMap(left *Field, data map[string]any) Condition {
	var branches []Condition
	for key, value := range data {
		field := &Field{Table: left.Table, Column: key}
		branches = append(branches, Equals(field, value))
	}
	return And(branches...)
}

// Greater Example
//
//	// get all animals that was created after 09 of october 2024 at 11:50AM
//	Where(where.Greater(&db.Animal.CreateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Greater(left *Field, value any) Condition {
	return Condition{Template: "%s > ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func GreaterField(left, right *Field) Condition {
	return Condition{Template: "%s > %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// GreaterEquals Example
//
//	// get all animals that was created in or after 09 of october 2024 at 11:50AM
//	Where(where.GreaterEquals(&db.Animal.CreateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func GreaterEquals(left *Field, value any) Condition {
	return Condition{Template: "%s >= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func GreaterEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s >= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// Less Example
//
//	// get all animals that was updated before 09 of october 2024 at 11:50AM
//	Where(where.Less(&db.Animal.UpdateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Less(left *Field, value any) Condition {
	return Condition{Template: "%s < ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func LessField(left, right *Field) Condition {
	return Condition{Template: "%s < %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// LessEquals Example
//
//	// get all animals that was updated in or before 09 of october 2024 at 11:50AM
//	Where(where.LessEquals(&db.Animal.UpdateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func LessEquals(left *Field, value any) Condition {
	return Condition{Template: "%s <= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func LessEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s <= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

func In(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length <= 1 {
		return Equals(left, right)
	}
	return Condition{Template: "%s IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

func NotIn(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length <= 1 {
		return NotEquals(left, right)
	}
	return Condition{Template: "%s NOT IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

// Like
//
//	// Example: get all animals that has a "at" in his name
//	where.Like(&db.Animal.Name, "%at%")
func Like(left *Field, value string) Condition {
	return Condition{Template: "%s LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func NotLike(left *Field, value string) Condition {
	return Condition{Template: "%s NOT LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}
