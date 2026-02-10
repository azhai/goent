package goent

import (
	"context"
	"iter"
	"reflect"
	"time"

	"github.com/azhai/goent/model"
)

type Handler struct {
	ctx  context.Context
	conn model.Connection
	cfg  *model.DatabaseConfig
}

func NewHandler(ctx context.Context, conn model.Connection, cfg *model.DatabaseConfig) *Handler {
	return &Handler{ctx: ctx, conn: conn, cfg: cfg}
}

func (h *Handler) ExecuteNoReturn(query model.Query) error {
	startTime := time.Now()
	query.Err = h.conn.ExecContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		return h.cfg.ErrorQueryHandler(h.ctx, query)
	}
	h.cfg.InfoHandler(h.ctx, query)
	return nil
}

func (h *Handler) ExecuteReturning(query model.Query, valueOf reflect.Value, returnFid int) error {
	var row model.Row
	startTime := time.Now()
	row = h.conn.QueryRowContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	fieldOf := valueOf.Field(returnFid)
	query.Err = row.Scan(fieldOf.Addr().Interface())
	if query.Err != nil {
		return h.cfg.ErrorQueryHandler(h.ctx, query)
	}
	h.cfg.InfoHandler(h.ctx, query)
	return nil
}

func (h *Handler) BatchReturning(query model.Query, valueOf reflect.Value, returnFid int) error {
	var rows model.Rows
	startTime := time.Now()
	rows, query.Err = h.conn.QueryContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		return h.cfg.ErrorQueryHandler(h.ctx, query)
	}
	defer rows.Close()
	h.cfg.InfoHandler(h.ctx, query)

	i := 0
	for rows.Next() {
		fieldOf := valueOf.Index(i).Field(returnFid)
		query.Err = rows.Scan(fieldOf.Addr().Interface())
		if query.Err != nil {
			// TODO: add infos about row
			return h.cfg.ErrorQueryHandler(h.ctx, query)
		}
		i++
	}
	return nil
}

func QueryResult[R any](hd *Handler, query model.Query) iter.Seq2[*R, error] {
	var rows model.Rows
	startTime := time.Now()
	rows, query.Err = hd.conn.QueryContext(hd.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		return func(yield func(*R, error) bool) {
			yield(nil, hd.cfg.ErrorQueryHandler(hd.ctx, query))
		}
	}
	hd.cfg.InfoHandler(hd.ctx, query)

	return func(yield func(*R, error) bool) {
		defer rows.Close()

		for rows.Next() {
			entity := new(R)
			valueOf := reflect.ValueOf(entity).Elem()
			dest := FlattenDest(valueOf)
			query.Err = rows.Scan(dest...)
			if query.Err != nil {
				yield(entity, hd.cfg.ErrorQueryHandler(hd.ctx, query))
				return
			}
			if !yield(entity, nil) {
				return
			}
		}
	}
}

// FlattenDest returns a slice of pointers to the fields of a struct
func FlattenDest(valueOf reflect.Value) []any {
	var dest []any
	valueType := valueOf.Type()
	for i := range valueOf.NumField() {
		if geoTag := valueType.Field(i).Tag.Get("goe"); geoTag == "-" {
			continue
		}
		fieldOf := valueOf.Field(i)
		if fieldOf.Kind() == reflect.Slice {
			continue
		}
		if fieldOf.Kind() == reflect.Ptr && fieldOf.Elem().Kind() == reflect.Struct {
			continue
		}
		dest = append(dest, fieldOf.Addr().Interface())
	}
	return dest
}

func wrapperQuery(ctx context.Context, conn model.Connection, query *model.Query) (model.Rows, error) {
	queryStart := time.Now()
	defer func() { query.QueryDuration = time.Since(queryStart) }()
	return conn.QueryContext(ctx, query)
}

func wrapperQueryRow(ctx context.Context, conn model.Connection, query *model.Query) model.Row {
	queryStart := time.Now()
	defer func() { query.QueryDuration = time.Since(queryStart) }()
	return conn.QueryRowContext(ctx, query)
}

func wrapperExec(ctx context.Context, conn model.Connection, query *model.Query) error {
	queryStart := time.Now()
	defer func() { query.QueryDuration = time.Since(queryStart) }()
	return conn.ExecContext(ctx, query)
}
