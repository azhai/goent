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

func (h *Handler) QueryResult(query model.Query) (model.Rows, error) {
	var rows model.Rows
	startTime := time.Now()
	rows, query.Err = h.conn.QueryContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		err := h.cfg.ErrorQueryHandler(h.ctx, query)
		return rows, err
	}
	h.cfg.InfoHandler(h.ctx, query)
	return rows, nil
}

func FetchResult[R any](hd *Handler, query model.Query, to FetchFunc) iter.Seq2[*R, error] {
	rows, err := hd.QueryResult(query)
	if err != nil {
		return func(yield func(*R, error) bool) {
			yield(nil, err)
		}
	}

	return func(yield func(*R, error) bool) {
		defer rows.Close()
		for rows.Next() {
			target := new(R)
			dest := to(target)
			query.Err = rows.Scan(dest...)
			if query.Err != nil {
				err = hd.cfg.ErrorQueryHandler(hd.ctx, query)
				yield(target, err)
				return
			}
			if !yield(target, nil) {
				return
			}
		}
	}
}

// AppendDestFields returns a slice of pointers to the fields of a struct
func AppendDestFields(fields []*Field, valueOf reflect.Value, foreign *Foreign) []any {
	var dest []any
	for _, fld := range fields {
		if fld.ColumnName == "*" {
			if info := GetTableInfo(fld.TableAddr); info != nil {
				dest = append(dest, AppendDestTable(*info, valueOf)...)
			}
		} else {
			fieldOf := valueOf.Field(fld.FieldId)
			dest = append(dest, fieldOf.Addr().Interface())
		}
	}
	return dest
}

// AppendDestTable returns a slice of pointers to the fields of a struct
func AppendDestTable(info TableInfo, valueOf reflect.Value) []any {
	size := min(len(info.Columns), valueOf.NumField())
	dest := make([]any, size)
	for i := range size {
		dest[i] = valueOf.Field(i).Addr().Interface()
	}
	return dest
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
		if fieldOf.Kind() == reflect.Pointer && fieldOf.Elem().Kind() == reflect.Struct {
			continue
		}
		dest = append(dest, fieldOf.Addr().Interface())
	}
	return dest
}
