package where

import (
	"reflect"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type valueOperation struct {
	value any
}

func (vo valueOperation) GetValue() any {
	if result, ok := vo.value.(model.ValueOperation); ok {
		return result.GetValue()
	}
	return vo.value
}

// Equals Example
//
//	// match Food with Id fc1865b4-6f2d-4cc6-b766-49c2634bf5c4
//	Where(where.Equals(&db.Food.Id, "fc1865b4-6f2d-4cc6-b766-49c2634bf5c4"))
//
//	// generate: WHERE "animals"."idhabitat" IS NULL
//	var idHabitat *string
//	Where(where.Equals(&db.Animal.IdHabitat, idHabitat))
//
//	// Using Table with field name
//	Where(where.EqualsTable(&db.OrderDetail, "order_id", 1))
func Equals[T any, A *T | **T](a A, v T) model.Operation {
	if reflect.ValueOf(v).Kind() == reflect.Pointer && reflect.ValueOf(v).IsNil() {
		return model.Operation{Arg: a, Operator: enum.Is, Type: enum.OperationIsWhere}
	}
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Equals, Type: enum.OperationWhere}
}

// EqualsTable creates an Equals condition using a Table and field name
func EqualsTable(table any, fieldName string, v any) model.Operation {
	tableValue := reflect.ValueOf(table)
	if tableValue.Kind() != reflect.Pointer {
		return model.Operation{}
	}
	fieldMethod := tableValue.MethodByName("Field")
	if !fieldMethod.IsValid() {
		return model.Operation{}
	}
	results := fieldMethod.Call([]reflect.Value{reflect.ValueOf(fieldName)})
	if len(results) == 0 {
		return model.Operation{}
	}
	field := results[0].Interface()
	if reflect.ValueOf(v).Kind() == reflect.Pointer && reflect.ValueOf(v).IsNil() {
		return model.Operation{Arg: field, Operator: enum.Is, Type: enum.OperationIsWhere}
	}
	return model.Operation{Arg: field, Value: valueOperation{value: v}, Operator: enum.Equals, Type: enum.OperationWhere}
}

func EqualsMap(table any, data map[string]any) model.Operation {
	if len(data) == 0 {
		return model.Operation{}
	}
	ops := make([]model.Operation, 0, len(data))
	for fieldName, value := range data {
		ops = append(ops, EqualsTable(table, fieldName, value))
	}
	if len(ops) == 1 {
		return ops[0]
	}
	result := ops[0]
	for i := 1; i < len(ops); i++ {
		result = And(result, ops[i])
	}
	return result
}

// NotEquals Example
//
//	// match all foods that name are not Cookie
//	Where(where.NotEquals(&db.Food.Name, "Cookie"))
//
//	// generate: WHERE "animals"."idhabitat" IS NOT NULL
//	var idHabitat *string
//	Where(where.NotEquals(&db.Animal.IdHabitat, idHabitat))
func NotEquals[T any, A *T | **T](a A, v T) model.Operation {
	if reflect.ValueOf(v).Kind() == reflect.Pointer && reflect.ValueOf(v).IsNil() {
		return model.Operation{Arg: a, Operator: enum.IsNot, Type: enum.OperationIsWhere}
	}
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.NotEquals, Type: enum.OperationWhere}
}

// Greater Example
//
//	// get all animals that was created after 09 of october 2024 at 11:50AM
//	Where(where.Greater(&db.Animal.CreateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Greater[T any, A *T | **T](a A, v T) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Greater, Type: enum.OperationWhere}
}

// GreaterEquals Example
//
//	// get all animals that was created in or after 09 of october 2024 at 11:50AM
//	Where(where.GreaterEquals(&db.Animal.CreateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func GreaterEquals[T any, A *T | **T](a A, v T) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.GreaterEquals, Type: enum.OperationWhere}
}

// Less Example
//
//	// get all animals that was updated before 09 of october 2024 at 11:50AM
//	Where(where.Less(&db.Animal.UpdateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func Less[T any, A *T | **T](a A, v T) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Less, Type: enum.OperationWhere}
}

// LessEquals Example
//
//	// get all animals that was updated in or before 09 of october 2024 at 11:50AM
//	Where(where.LessEquals(&db.Animal.UpdateAt, time.Date(2024, time.October, 9, 11, 50, 00, 00, time.Local)))
func LessEquals[T any, A *T | **T](a A, v T) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.LessEquals, Type: enum.OperationWhere}
}

// Like Example
//
//	// get all animals that has a "at" in his name
//	Where(where.Like(&db.Animal.Name, "%at%"))
func Like[T any](a *T, v string) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Like, Type: enum.OperationWhere}
}

// NotLike Example
//
//	// get all animals that has a "at" in his name
//	Where(where.Like(&db.Animal.Name, "%at%"))
func NotLike[T any](a *T, v string) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.NotLike, Type: enum.OperationWhere}
}

// In Example
//
//	// where in using a slice
//	Where(where.In(&db.Animal.Name, []string{"Cat", "Dog"}))
//
//	// AsQuery for get the query result from a select query
//	querySelect := goent.Select[any](&struct{ Name *string }{Name: &db.Animal.Name}).AsQuery()
//
//	// Use querySelect on in
//	rows, err := goent.Select(db.Animal).Where(where.In(&db.Animal.Name, querySelect).AsSlice()
func In[T any, V []T | model.Query](a *T, mq V) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: mq}, Operator: enum.In, Type: enum.OperationInWhere}
}

// InTable creates an In condition using a Table and field name
func InTable(table any, fieldName string, v any) model.Operation {
	tableValue := reflect.ValueOf(table)
	if tableValue.Kind() != reflect.Pointer {
		return model.Operation{}
	}
	fieldMethod := tableValue.MethodByName("Field")
	if !fieldMethod.IsValid() {
		return model.Operation{}
	}
	results := fieldMethod.Call([]reflect.Value{reflect.ValueOf(fieldName)})
	if len(results) == 0 {
		return model.Operation{}
	}
	field := results[0].Interface()
	return model.Operation{Arg: field, Value: valueOperation{value: v}, Operator: enum.In, Type: enum.OperationInWhere}
}

// NotIn Example
//
//	// where not in using a slice
//	Where(where.NotIn(&db.Animal.Name, []string{"Cat", "Dog"}))
//
//	// AsQuery for get the query result from a select query
//	querySelect, err := goent.Select(&struct{ Name *string }{Name: &db.Animal.Name}).AsQuery()
//
//	// Use querySelect on not in
//	rows, err := goent.Select(db.Animal).Where(where.NotIn(&db.Animal.Name, querySelect).AsSlice()
func NotIn[T any, V []T | model.Query](a *T, mq V) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: mq}, Operator: enum.NotIn, Type: enum.OperationInWhere}
}

// And Example
//
//	Where(
//		where.And(
//			where.Equals(&db.Animal.Status, "Eating"),
//			where.Like(&db.Animal.Name, "%Cat%"),
//			where.GreaterThan(&db.Animal.Age, 3),
//		),
//	)
func And(left, right model.Operation, others ...model.Operation) model.Operation {
	branches := []model.Operation{left, right}
	return model.Operation{
		Operator: enum.And,
		Type:     enum.LogicalWhere,
		Branches: append(branches, others...),
	}
}

// Or Example
//
//	Where(
//		where.Or(
//			where.Equals(&db.Animal.Status, "Eating"),
//			where.Like(&db.Animal.Name, "%Cat%"),
//			where.LessThan(&db.Animal.Age, 1),
//		),
//	)
func Or(left, right model.Operation, others ...model.Operation) model.Operation {
	branches := []model.Operation{left, right}
	return model.Operation{
		Operator: enum.Or,
		Type:     enum.LogicalWhere,
		Branches: append(branches, others...),
	}
}

// EqualsArg Example
//
//	// implicit join using EqualsArg
//	goent.Select(db.Animal).
//	Where(
//		where.And(
//			where.EqualsArg[int](&db.Animal.Id, &db.AnimalFood.IdAnimal),
//			where.EqualsArg[uuid.UUID](&db.Food.Id, &db.AnimalFood.IdFood),
//		),
//	).AsSlice()
func EqualsArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Equals, Type: enum.OperationAttributeWhere}
}

// NotEqualsArg Example
//
//	Where(where.NotEqualsArg(&db.Job.Id, &db.Person.Id))
func NotEqualsArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.NotEquals, Type: enum.OperationAttributeWhere}
}

// GreaterArg Example
//
//	Where(where.GreaterArg(&db.Stock.Minimum, &db.Drinks.Stock))
func GreaterArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Greater, Type: enum.OperationAttributeWhere}
}

// GreaterEqualsArg Example
//
//	Where(where.GreaterEqualsArg(&db.Drinks.Reorder, &db.Drinks.Stock))
func GreaterEqualsArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.GreaterEquals, Type: enum.OperationAttributeWhere}
}

// LessArg Example
//
//	Where(where.LessArg(&db.Exam.Score, &db.Data.Minimum))
func LessArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.Less, Type: enum.OperationAttributeWhere}
}

// LessEqualsArg Example
//
//	Where(where.LessEqualsArg(&db.Exam.Score, &db.Data.Minimum))
func LessEqualsArg[T any, A *T | **T](a A, v A) model.Operation {
	return model.Operation{Arg: a, Value: valueOperation{value: v}, Operator: enum.LessEquals, Type: enum.OperationAttributeWhere}
}
