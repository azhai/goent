package goent

import (
	"fmt"
	"reflect"
)

// Field represents a database field with its table reference and column name
// It is used to reference columns in queries and conditions
type Field struct {
	TableAddr  uintptr // Table address for table identification
	FieldId    int     // Field ID for quick lookup
	ColumnName string  // Database column name
	AliasName  string  // Alias name for the field
	Function   string  // SQL function to apply to the field
}

// SameTable checks if two fields belong to the same table
// It compares the TableAddr of both fields
func SameTable(field, another *Field) bool {
	return field.TableAddr == another.TableAddr
}

// Func applies a SQL function to the field (e.g., "UPPER", "LOWER", "COUNT")
// It sets the function to be applied when the field is used in queries
//
// Example:
//
//	field := goent.Expr("UPPER(name)").(*goent.Field)
//	users, _ := db.User.Filter(goent.Equals(field, "JOHN")).Select().All()
func (f *Field) Func(name, alias string) *Field {
	return &Field{
		TableAddr: f.TableAddr, FieldId: -1,
		ColumnName: f.ColumnName,
		AliasName:  alias, Function: name,
	}
}

// GetFid returns the field ID, resolving it from the table metadata if needed
// It looks up the field ID from the table registry if not already set
func (f *Field) GetFid() int {
	if f.FieldId < 0 && f.ColumnName != "*" {
		col, _ := GetTableColumn(f.TableAddr, f.ColumnName)
		if col != nil {
			f.FieldId = col.FieldId
		}
	}
	return f.FieldId
}

// Simple returns the column name with any SQL function applied
// It does not include the table name
func (f *Field) Simple() string {
	if f.Function != "" {
		return fmt.Sprintf(f.Function, f.ColumnName)
	}
	return f.ColumnName
}

// String returns the qualified field name (table.column) with any SQL function applied
// It includes the table name for unambiguous reference
func (f *Field) String() string {
	res, err := GetTableFieldName(f.TableAddr, f.ColumnName)
	if err != nil {
		return ""
	}
	if f.Function != "" {
		return fmt.Sprintf(f.Function, res)
	}
	return res
}

// Value represents a value or list of values for use in query conditions
// It handles both single values and slices for IN conditions
type Value struct {
	Args   []any // Slice of values for IN conditions
	Length int   // Length of the value slice
	single any   // Single value storage to avoid slice allocation
}

// NewValue creates a new Value from a Go value
// It handles both single values and slices
//
// Example:
//
//	value := NewValue([]int{1, 2, 3}) // creates IN (1, 2, 3)
//	value := NewValue(42)              // creates single value
func NewValue(value any) *Value {
	switch v := value.(type) {
	default:
		return NewValueReflect(value)
	case nil:
		return &Value{Args: nil, Length: 0}
	case []any:
		return &Value{Args: v, Length: len(v)}
	case []int64:
		args := make([]any, len(v))
		for i, x := range v {
			args[i] = x
		}
		return &Value{Args: args, Length: len(v)}
	case []int:
		args := make([]any, len(v))
		for i, x := range v {
			args[i] = x
		}
		return &Value{Args: args, Length: len(v)}
	case []string:
		args := make([]any, len(v))
		for i, x := range v {
			args[i] = x
		}
		return &Value{Args: args, Length: len(v)}
	case []float64:
		args := make([]any, len(v))
		for i, x := range v {
			args[i] = x
		}
		return &Value{Args: args, Length: len(v)}
	case int64:
		return &Value{single: v, Length: 1}
	case int:
		return &Value{single: v, Length: 1}
	case float64:
		return &Value{single: v, Length: 1}
	case bool:
		return &Value{single: v, Length: 1}
	}
}

func NewValueReflect(value any) *Value {
	valueOf := reflect.ValueOf(value)
	if valueOf.Kind() != reflect.Slice {
		return &Value{single: value, Length: 1}
	}
	size := valueOf.Len()
	args := make([]any, size)
	for i := range args {
		args[i] = valueOf.Index(i).Interface()
	}
	return &Value{Args: args, Length: size}
}
