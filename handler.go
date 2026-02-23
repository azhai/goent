package goent

import (
	"context"
	"iter"
	"reflect"
	"slices"
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

func (h *Handler) InfoHandler(query model.Query) {
	h.cfg.InfoHandler(h.ctx, query)
}

func (h *Handler) ErrHandler(query model.Query) error {
	return h.cfg.ErrorQueryHandler(h.ctx, query)
}

func (h *Handler) ExecuteNoReturn(query model.Query) error {
	startTime := time.Now()
	query.Err = h.conn.ExecContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		return h.ErrHandler(query)
	}
	h.InfoHandler(query)
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
		return h.ErrHandler(query)
	}
	h.InfoHandler(query)
	return nil
}

func (h *Handler) BatchReturning(query model.Query, valueOf reflect.Value, returnFid int) error {
	var rows model.Rows
	startTime := time.Now()
	rows, query.Err = h.conn.QueryContext(h.ctx, &query)
	query.QueryDuration = time.Since(startTime)
	if query.Err != nil {
		return h.ErrHandler(query)
	}
	defer rows.Close()
	h.InfoHandler(query)

	i := 0
	for rows.Next() {
		fieldOf := valueOf.Index(i).Field(returnFid)
		query.Err = rows.Scan(fieldOf.Addr().Interface())
		if query.Err != nil {
			// TODO: add infos about row
			return h.ErrHandler(query)
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
		err := h.ErrHandler(query)
		return rows, err
	}
	h.InfoHandler(query)
	return rows, nil
}

type FetchFunc func(target any) []any
type FetchCreator func(TableInfo, []*Field, *Foreign) FetchFunc

type Fetcher[R any] struct {
	NewTarget func() *R
	FetchTo   FetchFunc
	*Handler
}

func NewFetcher[R any](hd *Handler, newTarget func() *R) *Fetcher[R] {
	fet := &Fetcher[R]{Handler: hd, NewTarget: newTarget}
	if fet.NewTarget == nil {
		fet.NewTarget = func() *R { return new(R) }
	}
	return fet
}

func (f *Fetcher[R]) FetchRows(rows model.Rows, err error, limit int) ([]*R, error) {
	if err != nil {
		return nil, err
	}
	objs := make([]*R, 0, limit)
	defer rows.Close()
	for rows.Next() {
		target := f.NewTarget()
		err = rows.Scan(f.FetchTo(target)...)
		if err != nil {
			return nil, err
		}
		objs = append(objs, target)
	}
	return objs, nil
}

func (f *Fetcher[R]) FetchResult(query model.Query) iter.Seq2[*R, error] {
	rows, err := f.QueryResult(query)
	if err != nil {
		return func(yield func(*R, error) bool) {
			yield(nil, err)
		}
	}

	return func(yield func(*R, error) bool) {
		defer rows.Close()
		for rows.Next() {
			target := f.NewTarget()
			query.Err = rows.Scan(f.FetchTo(target)...)
			if query.Err != nil {
				yield(target, f.ErrHandler(query))
				return
			}
			if !yield(target, nil) {
				return
			}
		}
	}
}

func CreateFetchOne(tblInfo TableInfo, fields []*Field, foreign *Foreign) FetchFunc {
	return func(target any) []any {
		valueOf := reflect.ValueOf(target).Elem()
		return []any{valueOf.Field(0).Addr().Interface()}
	}
}

func CreateFetchFunc(tblInfo TableInfo, fields []*Field, foreign *Foreign) FetchFunc {
	return func(target any) []any {
		valueOf := reflect.ValueOf(target).Elem()
		if len(fields) > 0 && fields[0].Function != "" {
			return FlattenDest(valueOf)
		}
		if len(fields) > 0 {
			return AppendDestFields(valueOf, fields, foreign)
		}
		dest := AppendDestTable(tblInfo, valueOf)
		if foreign != nil {
			dest = append(dest, CreateForeignDest(valueOf, foreign)...)
		}
		return dest
	}
}

func CreateForeignDest(valueOf reflect.Value, foreign *Foreign) []any {
	frnInfo := GetTableInfo(foreign.Reference.TableAddr)
	if typeOf, ok := valueOf.Type().FieldByName(foreign.MountField); ok {
		fieldOf := reflect.New(typeOf.Type.Elem())
		valueOf.FieldByName(foreign.MountField).Set(fieldOf)
		return AppendDestTable(*frnInfo, fieldOf.Elem())
	}
	return nil
}

// AppendDestFields returns a slice of pointers to the fields of a struct
func AppendDestFields(valueOf reflect.Value, fields []*Field, foreign *Foreign) []any {
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
	columns := make([]*Column, 0, len(info.Columns))
	for _, col := range info.Columns {
		columns = append(columns, col)
	}
	slices.SortFunc(columns, func(a, b *Column) int {
		return a.FieldId - b.FieldId
	})
	dest := make([]any, len(columns))
	for i, col := range columns {
		fieldOf := valueOf.Field(col.FieldId)
		dest[i] = fieldOf.Addr().Interface()
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
