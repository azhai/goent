package goent

import (
	"fmt"
	"slices"
)

// Condition represents a SQL WHERE condition with a template and associated fields/values
// It is used to build WHERE clauses in queries
type Condition struct {
	Template string   // SQL template with placeholders
	Fields   []*Field // Fields referenced in the condition
	Values   []*Value // Values to bind to the placeholders
}

// IsEmpty returns true if the condition has no template (is empty)
// It checks if the condition is effectively empty
func (c Condition) IsEmpty() bool {
	return c.Template == ""
}

// Expr creates a condition with a custom template and associated values
// It allows for raw SQL conditions with placeholders
//
// Example:
//
//	cond := goent.Expr("age > ? AND status = ?", 18, "active")
//	users, _ := db.User.Filter(cond).Select().All()
func Expr(where string, args ...any) Condition {
	if len(args) == 0 {
		return Condition{Template: where, Values: nil}
	}
	values := make([]*Value, 0, len(args))
	for _, arg := range args {
		switch val := arg.(type) {
		default:
			values = append(values, NewValue(arg))
		case *Value:
			values = append(values, val)
		}
	}
	return Condition{Template: where, Values: values}
}

// And combines multiple conditions with AND logic
// It creates a compound condition where all branches must be true
//
// Example:
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
	totalFields, totalValues := 0, 0
	for _, cond := range branches {
		if cond.IsEmpty() {
			continue
		}
		totalFields += len(cond.Fields)
		totalValues += len(cond.Values)
	}
	res := Condition{
		Fields: make([]*Field, 0, totalFields),
		Values: make([]*Value, 0, totalValues),
	}
	idx := 0
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

// Or combines multiple conditions with OR logic
// It creates a compound condition where at least one branch must be true
//
// Example:
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
	totalFields, totalValues := 0, 0
	for _, cond := range branches {
		if cond.IsEmpty() {
			continue
		}
		totalFields += len(cond.Fields)
		totalValues += len(cond.Values)
	}
	res := Condition{
		Fields: make([]*Field, 0, totalFields),
		Values: make([]*Value, 0, totalValues),
	}
	idx := 0
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

// Not creates a condition that negates another condition
// It wraps the condition in a NOT clause
//
// Example:
//
//	cond := goent.Not(goent.Equals(field, "active"))
func Not(cond Condition) Condition {
	return Condition{Template: fmt.Sprintf("NOT (%s)", cond.Template), Fields: cond.Fields, Values: cond.Values}
}

// IsNull creates a condition that checks if a field is NULL
// It generates an IS NULL clause for the field
//
// Example:
//
//	cond := goent.IsNull(db.User.Field("deleted_at"))
func IsNull(left *Field) Condition {
	return Condition{Template: "%s IS NULL", Fields: []*Field{left}, Values: nil}
}

// IsNotNull creates a condition that checks if a field is NOT NULL
// It generates an IS NOT NULL clause for the field
//
// Example:
//
//	cond := goent.IsNotNull(db.User.Field("email"))
func IsNotNull(left *Field) Condition {
	return Condition{Template: "%s IS NOT NULL", Fields: []*Field{left}, Values: nil}
}

// Equals creates a condition that checks if a field is equal to a value
// It generates an equality check, handling NULL values appropriately
//
// Example: using Table with field name
//
//	goent.Equals(db.OrderDetail.Field("order_id"), 1)
func Equals(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length == 0 {
		return Condition{Template: "%s IS NULL", Fields: []*Field{left}, Values: []*Value{}}
	} else {
		return Condition{Template: "%s = ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
}

// EqualsField creates a condition that checks if one field is equal to another
// It generates an equality check between two fields
func EqualsField(left, right *Field) Condition {
	return Condition{Template: "%s = %s", Fields: []*Field{left, right}}
}

// NotEquals creates a condition that checks if a field is not equal to a value
// It generates an inequality check, handling NULL values appropriately
//
// Example:
//
//	cond := goent.NotEquals(db.User.Field("status"), "deleted")
func NotEquals(left *Field, value any) Condition {
	cond := Equals(left, value)
	if len(cond.Values) == 0 {
		cond.Template = "%s IS NOT NULL"
	} else {
		cond.Template = "%s != ?"
	}
	return cond
}

// NotEqualsField creates a condition that checks if one field is not equal to another
// It generates an inequality check between two fields
func NotEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s != %s", Fields: []*Field{left, right}}
}

// EqualsMap creates a condition that checks if multiple fields equal specified values
// It generates AND conditions for each key-value pair in the map
//
// Example:
//
//	cond := goent.EqualsMap(db.User.Field("status"), map[string]any{"active": true, "pending": nil})
func EqualsMap(left *Field, data map[string]any) Condition {
	if len(data) == 0 {
		return Condition{}
	}
	branches := make([]Condition, 0, len(data))
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

// Greater creates a condition that checks if a field is greater than a value
// It generates a greater than comparison
//
// Example:
//
//	// get all animals that was created after this year
//	thisYear, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z08:00")
//	Filter(goent.Greater(db.Animal.Field("create_at"), thisYear))
func Greater(left *Field, value any) Condition {
	return Condition{Template: "%s > ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// GreaterField creates a condition that checks if one field is greater than another
// It generates a greater than comparison between two fields
func GreaterField(left, right *Field) Condition {
	return Condition{Template: "%s > %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// GreaterEquals creates a condition that checks if a field is greater than or equal to a value
// It generates a greater than or equal comparison
func GreaterEquals(left *Field, value any) Condition {
	return Condition{Template: "%s >= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// GreaterEqualsField creates a condition that checks if one field is greater than or equal to another
// It generates a greater than or equal comparison between two fields
func GreaterEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s >= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// Less creates a condition that checks if a field is less than a value
// It generates a less than comparison
func Less(left *Field, value any) Condition {
	return Condition{Template: "%s < ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// LessField creates a condition that checks if one field is less than another
// It generates a less than comparison between two fields
func LessField(left, right *Field) Condition {
	return Condition{Template: "%s < %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// LessEquals creates a condition that checks if a field is less than or equal to a value
// It generates a less than or equal comparison
func LessEquals(left *Field, value any) Condition {
	return Condition{Template: "%s <= ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// LessEqualsField creates a condition that checks if one field is less than or equal to another
// It generates a less than or equal comparison between two fields
func LessEqualsField(left, right *Field) Condition {
	return Condition{Template: "%s <= %s", Fields: []*Field{left, right}, Values: []*Value{}}
}

// In creates a condition that checks if a field value is in a list of values
// It generates an IN clause for the field
// For large lists, use InBatch to automatically split into batches
func In(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length == 0 {
		return Expr("1 = 0")
	}
	if right.Length == 1 {
		return Condition{Template: "%s = ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
	return Condition{Template: "%s IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

// InBatch creates a condition that checks if a field value is in a list of values,
// automatically splitting large lists into batches to avoid SQL limitations.
// Most databases have a limit on the number of parameters in an IN clause
// (SQLite: 999 by default, PostgreSQL: 65535).
//
// Example:
//
//	ids := make([]int64, 5000)
//	cond := goent.InBatch(db.User.Field("id"), ids, 500)
//	// Generates: (id IN (1,...,500) OR id IN (501,...,1000) OR ...)
func InBatch(left *Field, value any, batchSize int) Condition {
	right := NewValue(value)
	if right.Length == 0 {
		return Expr("1 = 0")
	}
	if right.Length == 1 {
		return Condition{Template: "%s = ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	if right.Length <= batchSize {
		return Condition{Template: "%s IN ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
	var branches []Condition
	for i := 0; i < right.Length; i += batchSize {
		end := i + batchSize
		if end > right.Length {
			end = right.Length
		}
		batch := &Value{Args: right.Args[i:end], Length: end - i}
		branches = append(branches, Condition{
			Template: "%s IN ?",
			Fields:   []*Field{left},
			Values:   []*Value{batch},
		})
	}
	return Or(branches...)
}

// NotIn creates a condition that checks if a field value is not in a list of values
// It generates a NOT IN clause for the field
func NotIn(left *Field, value any) Condition {
	right := NewValue(value)
	if right.Length == 0 {
		return Expr("1 = 1")
	}
	if right.Length == 1 {
		return Condition{Template: "%s != ?", Fields: []*Field{left}, Values: []*Value{right}}
	}
	return Condition{Template: "%s NOT IN ?", Fields: []*Field{left}, Values: []*Value{right}}
}

// Like creates a condition that checks if a field matches a LIKE pattern
// It generates a LIKE clause for pattern matching
func Like(left *Field, value string) Condition {
	return Condition{Template: "%s LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// NotLike creates a condition that checks if a field does not match a LIKE pattern
// It generates a NOT LIKE clause for pattern matching
func NotLike(left *Field, value string) Condition {
	return Condition{Template: "%s NOT LIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// ILike creates a case-insensitive LIKE condition
// It generates an ILIKE clause for case-insensitive pattern matching
func ILike(left *Field, value string) Condition {
	return Condition{Template: "%s ILIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// NotILike creates a case-insensitive NOT LIKE condition
// It generates a NOT ILIKE clause for case-insensitive pattern matching
func NotILike(left *Field, value string) Condition {
	return Condition{Template: "%s NOT ILIKE ?", Fields: []*Field{left}, Values: []*Value{NewValue(value)}}
}

// applyFilter combines existing conditions with new conditions using AND logic
func applyFilter(w *Condition, conds ...Condition) Condition {
	if len(conds) == 0 || len(conds) == 1 && conds[0].IsEmpty() {
		return *w
	}
	if !w.IsEmpty() {
		conds = append(conds, *w)
	}
	return And(conds...)
}

// applyWhere combines existing conditions with a raw WHERE clause
func applyWhere(w *Condition, where string, args ...any) Condition {
	cond := Expr(where, args...)
	if !w.IsEmpty() {
		return And(*w, cond)
	}
	return cond
}

// isConditionDuplicated checks if a condition already exists in a slice
func isConditionDuplicated(conds []Condition, target Condition) bool {
	return slices.ContainsFunc(conds, func(c Condition) bool {
		return c.Template == target.Template && len(c.Fields) == len(target.Fields) && len(c.Values) == len(target.Values)
	})
}
