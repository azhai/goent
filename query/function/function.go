package function

import (
	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

// ToUpper uses database Function to converts the target string to uppercase
//
// # Example
//
//	goent.Select(&struct {
//		UpperName *Function[string]
//	}{
//		UpperName: function.ToUpper(&db.Animal.Name),
//	})
func ToUpper(target *string) *Function[string] {
	return &Function[string]{Field: target, Type: enum.UpperFunction}
}

// ToLower uses database Function to converts the target string to lowercase
//
// # Example
//
//	goent.Select(&struct {
//		LowerName *Function[string]
//	}{
//		LowerName: function.ToLower(&db.Animal.Name),
//	})
func ToLower(target *string) *Function[string] {
	return &Function[string]{Field: target, Type: enum.LowerFunction}
}

// Argument is used to pass a value to a Function inside a where clause
//
// # Example
//
//	goent.Select(db.Animal).Where(where.Equals(function.ToUpper(&db.Animal.Name), function.Argument("CAT"))).AsSlice()
func Argument[T any](value T) Function[T] {
	return Function[T]{Value: value}
}

type Function[T any] struct {
	Field *T
	Type  enum.FunctionType
	Value T
}

func (f Function[T]) GetValue() any {
	return f.Value
}

func (f Function[T]) GetType() enum.FunctionType {
	return f.Type
}

func (f Function[T]) Attribute(b model.Body) model.Attribute {
	return model.Attribute{
		Table:        b.Table,
		Name:         b.Name,
		FunctionType: f.Type,
	}
}

func (f Function[T]) GetField() any {
	return f.Field
}
