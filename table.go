package goent

import (
	"context"
	"reflect"
	"sync"

	"github.com/azhai/goent/query/aggregate"
	"github.com/azhai/goent/query/function"
	"github.com/azhai/goent/utils"
)

type Table[T any] struct {
	Model    *T
	fields   *utils.CoMap
	exists   sync.Map
	deleteds sync.Map
	newbies  []*T
}

// ------------------------------
// NewTable ...
// ------------------------------

func NewTable[T any]() *Table[T] {
	return NewTableModel(new(T))
}

func NewTableModel[T any](m *T) *Table[T] {
	return &Table[T]{
		Model:    m,
		fields:   utils.NewCoMap(),
		exists:   sync.Map{},
		deleteds: sync.Map{},
	}
}

func NewTableReflect(typeOf reflect.Type) reflect.Value {
	tb := reflect.New(typeOf)
	modelType := tb.Elem().FieldByName("Model").Type().Elem()
	tb.Elem().FieldByName("Model").Set(reflect.New(modelType))
	return tb
}

// ------------------------------
// Insert ...
// ------------------------------

func (t *Table[T]) Insert() StateInsert[T] {
	return t.InsertContext(context.Background())
}

func (t *Table[T]) InsertContext(ctx context.Context) StateInsert[T] {
	return InsertTableContext(ctx, t)
}

// ------------------------------
// Delete ...
// ------------------------------

func (t *Table[T]) Delete() StateDelete {
	return t.DeleteContext(context.Background())
}

func (t *Table[T]) DeleteContext(ctx context.Context) StateDelete {
	return DeleteTableContext(ctx, t)
}

func (t *Table[T]) Remove() StateRemove[T] {
	return t.RemoveContext(context.Background())
}

func (t *Table[T]) RemoveContext(ctx context.Context) StateRemove[T] {
	return RemoveContext(ctx, t.Model)
}

// ------------------------------
// Update ...
// ------------------------------

func (t *Table[T]) Update() StateUpdate[T] {
	return t.UpdateContext(context.Background())
}

func (t *Table[T]) UpdateContext(ctx context.Context) StateUpdate[T] {
	return UpdateContext(ctx, t.Model)
}

func (t *Table[T]) Save() StateSave[T] {
	return t.SaveContext(context.Background())
}

func (t *Table[T]) SaveContext(ctx context.Context) StateSave[T] {
	return SaveTableContext(ctx, t)
}

// ------------------------------
// Select ...
// ------------------------------

func (t *Table[T]) List() StateSelect[T] {
	return t.ListContext(context.Background())
}

func (t *Table[T]) ListContext(ctx context.Context) StateSelect[T] {
	return ListTableContext(ctx, t)
}

func (t *Table[T]) Find() StateFind[T] {
	return t.FindContext(context.Background())
}

func (t *Table[T]) FindContext(ctx context.Context) StateFind[T] {
	return FindTableContext(ctx, t)
}

// ------------------------------
// Count ...
// ------------------------------

type ResultCount struct {
	Count int64
}

func (t *Table[T]) Count(col any) (int64, error) {
	return t.CountContext(context.Background(), col)
}

func (t *Table[T]) CountContext(ctx context.Context, col any) (int64, error) {
	result, err := SelectContext[ResultCount](ctx, aggregate.Count(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// ------------------------------
// Max/Min/Sum/Avg ...
// ------------------------------

type ResultAggr struct {
	Aggr float64
}

func (t *Table[T]) Max(col any) (float64, error) {
	return t.MaxContext(context.Background(), col)
}

func (t *Table[T]) MaxContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Max(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func (t *Table[T]) Min(col any) (float64, error) {
	return t.MinContext(context.Background(), col)
}

func (t *Table[T]) MinContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Min(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func (t *Table[T]) Sum(col any) (float64, error) {
	return t.SumContext(context.Background(), col)
}

func (t *Table[T]) SumContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Sum(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func (t *Table[T]) Avg(col any) (float64, error) {
	return t.AvgContext(context.Background(), col)
}

func (t *Table[T]) AvgContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Avg(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

// ------------------------------
// ToUpper/ToLower ...
// ------------------------------

type FuncString *function.Function[string]

func (t *Table[T]) ToUpper(col *string) (string, error) {
	return t.ToUpperContext(context.Background(), col)
}

func (t *Table[T]) ToUpperContext(ctx context.Context, col *string) (string, error) {
	result, err := SelectContext[FuncString](ctx, function.ToUpper(col)).AsOne()
	if err != nil {
		return "", err
	}
	return result.Value, nil
}

func (t *Table[T]) ToLower(col *string) (string, error) {
	return t.ToLowerContext(context.Background(), col)
}

func (t *Table[T]) ToLowerContext(ctx context.Context, col *string) (string, error) {
	result, err := SelectContext[FuncString](ctx, function.ToLower(col)).AsOne()
	if err != nil {
		return "", err
	}
	return result.Value, nil
}
