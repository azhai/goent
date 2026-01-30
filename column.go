package goent

import (
	"context"

	"github.com/azhai/goent/query/aggregate"
	"github.com/azhai/goent/query/function"
)

// ------------------------------
// Count ...
// ------------------------------

type ResultCount struct {
	Count int64
}

func Count(col any) (int64, error) {
	return CountContext(context.Background(), col)
}

func CountContext(ctx context.Context, col any) (int64, error) {
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

func Max(col any) (float64, error) {
	return MaxContext(context.Background(), col)
}

func MaxContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Max(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func Min(col any) (float64, error) {
	return MinContext(context.Background(), col)
}

func MinContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Min(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func Sum(col any) (float64, error) {
	return SumContext(context.Background(), col)
}

func SumContext(ctx context.Context, col any) (float64, error) {
	result, err := SelectContext[ResultAggr](ctx, aggregate.Sum(col)).AsOne()
	if err != nil {
		return 0, err
	}
	return result.Aggr, nil
}

func Avg(col any) (float64, error) {
	return AvgContext(context.Background(), col)
}

func AvgContext(ctx context.Context, col any) (float64, error) {
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

func ToUpper(col *string) ([]string, error) {
	return ToUpperContext(context.Background(), col)
}

func ToUpperContext(ctx context.Context, col *string) (res []string, err error) {
	var row FuncString
	for row, err = range SelectContext[FuncString](ctx, function.ToUpper(col)).Rows() {
		if err != nil {
			return
		}
		res = append(res, row.Value)
	}
	return
}

func ToLower(col *string) ([]string, error) {
	return ToLowerContext(context.Background(), col)
}

func ToLowerContext(ctx context.Context, col *string) (res []string, err error) {
	var row FuncString
	for row, err = range SelectContext[FuncString](ctx, function.ToLower(col)).Rows() {
		if err != nil {
			return
		}
		res = append(res, row.Value)
	}
	return
}
