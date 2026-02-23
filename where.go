package goent

import (
	"fmt"
	"reflect"
	"strings"
)

// Field represents a database field with its table reference and column name.
type Field struct {
	TableAddr  uintptr
	FieldId    int
	ColumnName string
	AliasName  string
	Function   string
}

func SameTable(field, another *Field) bool {
	return field.TableAddr == another.TableAddr
}

func (f *Field) Func(name string) *Field {
	f.Function = name
	return f
}

func (f *Field) GetFid() int {
	if f.FieldId < 0 && f.ColumnName != "*" {
		if col := GetTableColumn(f.TableAddr, f.ColumnName); col != nil {
			f.FieldId = col.FieldId
		}
	}
	return f.FieldId
}

func (f *Field) Simple() string {
	if f.Function != "" {
		return fmt.Sprintf(f.Function, f.ColumnName)
	}
	return f.ColumnName
}

func (f *Field) String() string {
	res, err := GetFieldName(f.TableAddr, f.ColumnName)
	if err != nil {
		return ""
	}
	if f.Function != "" {
		return fmt.Sprintf(f.Function, res)
	}
	return res
}

// Value represents a value or list of values for use in query conditions.
type Value struct {
	Type   reflect.Kind
	Args   []any
	Length int
}

func NewValue(value any) *Value {
	valueOf := reflect.ValueOf(value)
	kind := valueOf.Kind()
	if kind == reflect.Slice {
		size := valueOf.Len()
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

// Condition represents a SQL WHERE condition with a template and associated fields/values.
type Condition struct {
	Template string
	Fields   []*Field
	Values   []*Value
}

func (c Condition) IsEmpty() bool {
	return c.Template == ""
}

// Expr creates a condition with a custom template and associated values.
func Expr(where string, args ...any) Condition {
	var values []*Value
	for _, arg := range args {
		switch val := arg.(type) {
		default:
			values = append(values, NewValue(arg))
		case *Value:
			values = append(values, val)
		}
	}
	where = strings.ReplaceAll(where, "?", "%s")
	return Condition{Template: where, Values: values}
}

// And Example
//
//	goent.And(
//		goent.Equals(db.Animal.Field("status"), "Eating"),
//		goent.Like(db.Animal.Field("name"), "%Cat%"),
//		goent.GreaterThan(db.Animal.Field("age"), 3),
//	)
func And(branches ...Condition) Condition {
	var size int
	if size = len(branches); size == 0 {
		return Condition{}
	} else if size == 1 {
		return branches[0]
	}
	idx, res := 0, Condition{Template: ""}
	for _, cond := range branches {
		if cond.IsEmpty() {
			continue
		}
		if idx > 0 {
			res.Template += ") AND ("
		}
		res.Template += cond.Template
		res.Fields = append(res.Fields, cond.Fields...)
		res.Values = append(res.Values, cond.Values...)
		idx += 1
	}
	if idx >= 2 {
		res.Template = fmt.Sprintf("(%s)", res.Template)
	}
	return res
}

// Or Example
//
//	goent.Or(
//		goent.Equals(db.Animal.Field("status"), "Eating"),
//		goent.Like(db.Animal.Field("name"), "%Cat%"),
//		goent.LessThan(db.Animal.Field("age"), 1),
//	)
func Or(branches ...Condition) Condition {
	var size int
	if size = len(branches); size == 0 {
		return Condition{}
	} else if size == 1 {
		return branches[0]
	}
	idx, res := 0, Condition{Template: ""}
	for _, cond := range branches {
		if cond.IsEmpty() {
			continue
		}
		if idx > 0 {
			res.Template += " OR "
		}
		res.Template += cond.Template
		res.Fields = append(res.Fields, cond.Fields...)
		res.Values = append(res.Values, cond.Values...)
		idx += 1
	}
	return res
}

// Not creates a condition that negates another condition.
func Not(cond Condition) Condition {
	return Condition{Template: fmt.Sprintf("NOT (%s)", cond.Template), Fields: cond.Fields, Values: cond.Values}
}

// IsNull creates a condition that checks if a field is NULL.
func IsNull(left *Field) Condition {
	return Condition{Template: "%s IS NULL", Fields: []*Field{left}, Values: []*Value{}}
}

// IsNotNull creates a condition that checks if a field is NOT NULL.
func IsNotNull(left *Field) Condition {
	return Condition{Template: "%s IS NOT NULL", Fields: []*Field{left}, Values: []*Value{}}
}

// Equals creates a condition that checks if a field is equal to a value.
//
//	Example: using Table with field name
//	goent.Equals(db.OrderDetail.Field("order_id"), 1)
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
		field := &Field{TableAddr: left.TableAddr, ColumnName: key}
		if _, ok := value.(NilMarker); ok {
			branches = append(branches, IsNull(field))
		} else {
			branches = append(branches, Equals(field, value))
		}
	}
	return And(branches...)
}

// Greater Example
//
//	// get all animals that was created after 09 of october 2024 at 11:50AM
//	Filter(goent.Greater(db.Animal.Field("create_at"), time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Greater(left *Field, value any) Condition {
	return Condition{Template: "%s > ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

func GreaterField(left, right *Field) Condition {
	return Condition{Template: "%s > %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// GreaterEquals Example
//
//	// get all animals that was created in or after 09 of october 2024 at 11:50AM
//	Filter(goent.GreaterEquals(db.Animal.Field("create_at"), time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func GreaterEquals(left *Field, value any) Condition {
	return Condition{Template: "%s >= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// GreaterEqualsField creates a condition that checks if one field is greater than or equal to another.
func GreaterEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s >= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// Less creates a condition that checks if a field is less than a value.
//
// Example: get all animals that was updated before 09 of october 2024 at 11:50AM
//
//	Filter(goent.Less(db.Animal.Field("update_at"), time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Less(left *Field, value any) Condition {
	return Condition{Template: "%s < ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// LessField creates a condition that checks if one field is less than another.
func LessField(left, right *Field) Condition {
	return Condition{Template: "%s < %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// LessEquals creates a condition that checks if a field is less than or equal to a value.
//
// Example: get all animals that was updated in or before 09 of october 2024 at 11:50AM
//
//	Filter(goent.LessEquals(db.Animal.Field("update_at"), time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func LessEquals(left *Field, value any) Condition {
	return Condition{Template: "%s <= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// LessEqualsField creates a condition that checks if one field is less than or equal to another.
func LessEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s <= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// In creates a condition that checks if a field value is in a list of values.
func In(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length <= 1 {
		return Equals(left, value)
	}
	return Condition{Template: "%s IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

// NotIn creates a condition that checks if a field value is not in a list of values.
func NotIn(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length <= 1 {
		return NotEquals(left, value)
	}
	return Condition{Template: "%s NOT IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

// Like creates a condition that checks if a field matches a LIKE pattern.
//
// Example: get all animals that has a "at" in his name
//
//	goent.Like(db.Animal.Field("name"), "%at%")
func Like(left *Field, value string) Condition {
	return Condition{Template: "%s LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// NotLike creates a condition that checks if a field does not match a LIKE pattern.
func NotLike(left *Field, value string) Condition {
	return Condition{Template: "%s NOT LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// ILike creates a case-insensitive LIKE condition.
func ILike(left *Field, value string) Condition {
	return Condition{Template: "%s ILIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// NotILike creates a case-insensitive NOT LIKE condition.
func NotILike(left *Field, value string) Condition {
	return Condition{Template: "%s NOT ILIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}
