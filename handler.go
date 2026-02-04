package goent

import (
	"context"
	"iter"
	"reflect"
	"time"

	"github.com/azhai/goent/model"
)

func handlerValues(ctx context.Context, conn model.Connection, query model.Query, dc *model.DatabaseConfig) error {
	query.Header.Err = wrapperExec(ctx, conn, &query)
	if query.Header.Err != nil {
		return dc.ErrorQueryHandler(ctx, query)
	}
	dc.InfoHandler(ctx, query)
	return nil
}

func handlerValuesReturning(ctx context.Context, conn model.Connection, query model.Query, value reflect.Value, pkFieldId int, dc *model.DatabaseConfig) error {
	row := wrapperQueryRow(ctx, conn, &query)

	query.Header.Err = row.Scan(value.Field(pkFieldId).Addr().Interface())
	if query.Header.Err != nil {
		return dc.ErrorQueryHandler(ctx, query)
	}
	dc.InfoHandler(ctx, query)
	return nil
}

func handlerValuesReturningBatch(ctx context.Context, conn model.Connection, query model.Query, value reflect.Value, pkFieldId int, dc *model.DatabaseConfig) error {
	var rows model.Rows
	rows, query.Header.Err = wrapperQuery(ctx, conn, &query)

	if query.Header.Err != nil {
		return dc.ErrorQueryHandler(ctx, query)
	}
	defer rows.Close()
	dc.InfoHandler(ctx, query)

	i := 0
	for rows.Next() {
		query.Header.Err = rows.Scan(value.Index(i).Field(pkFieldId).Addr().Interface())
		if query.Header.Err != nil {
			// TODO: add infos about row
			return dc.ErrorQueryHandler(ctx, query)
		}
		i++
	}
	return nil
}

func handlerResult[T any](ctx context.Context, conn model.Connection, query model.Query, numFields int, dc *model.DatabaseConfig) iter.Seq2[T, error] {
	var rows model.Rows
	rows, query.Header.Err = wrapperQuery(ctx, conn, &query)

	var entity T
	if query.Header.Err != nil {
		return func(yield func(T, error) bool) {
			yield(entity, dc.ErrorQueryHandler(ctx, query))
		}
	}
	dc.InfoHandler(ctx, query)

	dest := make([]any, numFields)
	value := reflect.ValueOf(&entity).Elem()
	for i := range dest {
		dest[i] = value.Field(i).Addr().Interface()
	}

	return func(yield func(T, error) bool) {
		defer rows.Close()

		for rows.Next() {
			query.Header.Err = rows.Scan(dest...)
			if query.Header.Err != nil {
				// TODO: add infos about row
				yield(entity, dc.ErrorQueryHandler(ctx, query))
				return
			}
			if !yield(entity, nil) {
				return
			}
		}
	}
}

func wrapperQuery(ctx context.Context, conn model.Connection, query *model.Query) (model.Rows, error) {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.QueryContext(ctx, query)
}

func wrapperQueryRow(ctx context.Context, conn model.Connection, query *model.Query) model.Row {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.QueryRowContext(ctx, query)
}

func wrapperExec(ctx context.Context, conn model.Connection, query *model.Query) error {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.ExecContext(ctx, query)
}
