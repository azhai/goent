package goent

import (
	"context"
	"iter"
	"reflect"

	"github.com/azhai/goent/model"
)

// GenScanFields is an interface for Model which is modifiable by goent-gen.
type GenScanFields interface {
	ScanFields() []any
}

// FetchFunc is a function type that returns a slice of pointers to struct fields for scanning.
type FetchFunc func(target any) []any

// FetchValue returns a slice of pointers to the first field of the target struct.
func FetchValue(target any) []any {
	valueOf := reflect.ValueOf(target).Elem()
	return []any{valueOf.Field(0).Addr().Interface()}
}

// FetchCreator is a function type that creates a FetchFunc based on table info, fields, and foreign.
type FetchCreator func(TableInfo, []*Field, []*Foreign) FetchFunc

// CreateFetchFunc creates a FetchFunc based on the specified fields and foreign relationships.
// It handles both aggregate function results and regular table field scanning.
func CreateFetchFunc(tblInfo TableInfo, fields []*Field, foreigns []*Foreign) FetchFunc {
	ctx := &fetchContext{
		tblInfo:  tblInfo,
		fields:   fields,
		foreigns: foreigns,
	}
	if len(fields) > 0 {
		ctx.destSize = len(fields)
	} else {
		ctx.destSize = len(tblInfo.GetSortedFields())
	}
	return func(target any) []any {
		valueOf := reflect.ValueOf(target).Elem()
		if len(fields) > 0 && fields[0].Function != "" {
			return FlattenDest(valueOf)
		}
		if len(fields) > 0 {
			return ctx.buildDest(valueOf)
		}
		dest := AppendDestTable(tblInfo, valueOf)
		for _, foreign := range foreigns {
			dest = append(dest, CreateForeignDest(valueOf, foreign)...)
		}
		return dest
	}
}

// Handler handles database query execution and result processing.
type Handler struct {
	ctx  context.Context
	conn model.Connection
	cfg  *model.DatabaseConfig
}

// NewHandler creates a new Handler with the given context, connection, and database config.
func NewHandler(ctx context.Context, conn model.Connection, cfg *model.DatabaseConfig) *Handler {
	return &Handler{ctx: ctx, conn: conn, cfg: cfg}
}

// ExecuteReturning executes a query with RETURNING clause and scans the result into the specified field.
// It is used for INSERT statements that return auto-generated values like IDs.
//
// Example:
//
//	err := hd.ExecuteReturning(query, valueOf, returnFid)
//	if err != nil {
//		return err
//	}
//	fmt.Println(valueOf.Field(returnFid).Int()) // printed generated ID
func (h *Handler) ExecuteReturning(query model.Query, valueOf reflect.Value, returnFid int) error {
	row, err := query.WrapQueryRow(h.ctx, h.conn, h.cfg)
	if err != nil {
		return err
	}
	fieldOf := valueOf.Field(returnFid)
	query.Err = row.Scan(fieldOf.Addr().Interface())
	if query.Err != nil {
		return h.cfg.ErrorQueryHandler(h.ctx, query)
	}
	return nil
}

// BatchReturning executes a query with RETURNING clause for batch inserts.
// It scans multiple returned rows into successive elements of the slice.
//
// Example:
//
//	err := hd.BatchReturning(query, valueOf, returnFid)
//	if err != nil {
//		return err
//	}
//	fmt.Println(valueOf.Len()) // number of inserted records
func (h *Handler) BatchReturning(query model.Query, valueOf reflect.Value, returnFid int) error {
	rows, err := query.WrapQuery(h.ctx, h.conn, h.cfg)
	if err != nil {
		return err
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		elem := valueOf.Index(i)
		if elem.Kind() == reflect.Pointer {
			elem = elem.Elem()
		}
		fieldOf := elem.Field(returnFid)
		query.Err = rows.Scan(fieldOf.Addr().Interface())
		if query.Err != nil {
			return h.cfg.ErrorQueryHandler(h.ctx, query)
		}
		i++
	}
	return nil
}

// Fetcher handles fetching query results into typed structs.
type Fetcher[R any] struct {
	NewTarget func() *R
	FetchTo   FetchFunc
	*Handler
}

// NewFetcher creates a new Fetcher with the given handler and target constructor.
//
// Example:
//
//	fetcher := NewFetcher(handler, func() *User { return &User{} })
func NewFetcher[R any](hd *Handler, newTarget func() *R) *Fetcher[R] {
	fet := &Fetcher[R]{Handler: hd, NewTarget: newTarget}
	if fet.NewTarget == nil {
		fet.NewTarget = func() *R { return new(R) }
	}
	return fet
}

// FetchResult returns an iterator that yields typed results from the query result set.
// This is memory-efficient for processing large result sets.
//
// Example:
//
//	for user, err := range fetcher.FetchResult(query) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(user.Name)
//	}
func (f *Fetcher[R]) FetchResult(query model.Query) iter.Seq2[*R, error] {
	rows, err := query.WrapQuery(f.ctx, f.conn, f.cfg)
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
				yield(target, f.cfg.ErrorQueryHandler(f.ctx, query))
				return
			}
			if !yield(target, nil) {
				return
			}
		}
	}
}

type fieldScanInfo struct {
	fieldId    int
	tableAddr  uintptr
	isWildcard bool
}

type fetchContext struct {
	tblInfo   TableInfo
	fields    []*Field
	foreigns  []*Foreign
	scanInfos []fieldScanInfo
	destSize  int
}

func (ctx *fetchContext) buildDest(valueOf reflect.Value) []any {
	dest := make([]any, 0, ctx.destSize)
	foreignValues := make(map[uintptr]reflect.Value, len(ctx.foreigns))
	var mainTableAddr uintptr
	if len(ctx.fields) > 0 {
		mainTableAddr = ctx.fields[0].TableAddr
	}
	for _, foreign := range ctx.foreigns {
		if typeOf, ok := valueOf.Type().FieldByName(foreign.MountField); ok {
			if typeOf.Type.Kind() == reflect.Slice {
				continue
			}
			foreignValue := reflect.New(typeOf.Type.Elem())
			valueOf.FieldByName(foreign.MountField).Set(foreignValue)
			frnInfo := GetTableInfo(foreign.Reference.TableAddr)
			if frnInfo != nil {
				foreignValues[frnInfo.TableAddr] = foreignValue
			}
		}
	}
	var dummy any
	for _, fld := range ctx.fields {
		if fld.ColumnName == "*" {
			if info := GetTableInfo(fld.TableAddr); info != nil {
				dest = append(dest, AppendDestTable(*info, valueOf)...)
			}
		} else if fld.TableAddr != mainTableAddr && fld.TableAddr != 0 {
			if fv, ok := foreignValues[fld.TableAddr]; ok {
				fieldOf := fv.Elem().Field(fld.FieldId)
				dest = append(dest, fieldOf.Addr().Interface())
			} else {
				dest = append(dest, &dummy)
			}
		} else if fld.TableAddr == mainTableAddr {
			fieldOf := valueOf.Field(fld.FieldId)
			dest = append(dest, fieldOf.Addr().Interface())
		}
	}
	return dest
}

// CreateForeignDest creates destination pointers for foreign key relationship fields.
// It initializes the related struct field and returns pointers to its columns for scanning.
func CreateForeignDest(valueOf reflect.Value, foreign *Foreign) []any {
	frnInfo := GetTableInfo(foreign.Reference.TableAddr)
	if typeOf, ok := valueOf.Type().FieldByName(foreign.MountField); ok {
		fieldOf := reflect.New(typeOf.Type.Elem())
		valueOf.FieldByName(foreign.MountField).Set(fieldOf)
		return AppendDestTable(*frnInfo, fieldOf.Elem())
	}
	return nil
}

// AppendDestTable returns a slice of pointers to the fields of a struct
func AppendDestTable(info TableInfo, valueOf reflect.Value) []any {
	if method := valueOf.MethodByName("ScanFields"); method.IsValid() {
		return method.Call([]reflect.Value{})[0].Interface().([]any)
	}
	fields := info.GetSortedFields()
	dest := make([]any, len(fields))
	for i, fld := range fields {
		fieldOf := valueOf.Field(fld.FieldId)
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

// AppendDestFields returns a slice of pointers to the fields of a struct for database scanning.
// It handles both regular fields and wildcard (*) fields that expand to all table columns.
//
// Example:
//
//	dest := AppendDestFields(valueOf, fields, nil)
//	rows.Scan(dest...) // scan into struct fields
// func AppendDestFields(valueOf reflect.Value, fields []*Field, foreigns []*Foreign) []any {
// 	var dest []any
// 	foreignValues := make(map[uintptr]reflect.Value, len(foreigns))
// 	var mainTableAddr uintptr
// 	if len(fields) > 0 {
// 		mainTableAddr = fields[0].TableAddr
// 	}
// 	for _, foreign := range foreigns {
// 		if typeOf, ok := valueOf.Type().FieldByName(foreign.MountField); ok {
// 			if typeOf.Type.Kind() == reflect.Slice {
// 				continue
// 			}
// 			foreignValue := reflect.New(typeOf.Type.Elem())
// 			valueOf.FieldByName(foreign.MountField).Set(foreignValue)
// 			frnInfo := GetTableInfo(foreign.Reference.TableAddr)
// 			if frnInfo != nil {
// 				foreignValues[frnInfo.TableAddr] = foreignValue
// 			}
// 		}
// 	}
// 	var dummy any
// 	for _, fld := range fields {
// 		if fld.ColumnName == "*" {
// 			if info := GetTableInfo(fld.TableAddr); info != nil {
// 				dest = append(dest, AppendDestTable(*info, valueOf)...)
// 			}
// 		} else if fld.TableAddr != mainTableAddr && fld.TableAddr != 0 {
// 			if fv, ok := foreignValues[fld.TableAddr]; ok {
// 				fieldOf := fv.Elem().Field(fld.FieldId)
// 				dest = append(dest, fieldOf.Addr().Interface())
// 			} else {
// 				dest = append(dest, &dummy)
// 			}
// 		} else if fld.TableAddr == mainTableAddr {
// 			fieldOf := valueOf.Field(fld.FieldId)
// 			dest = append(dest, fieldOf.Addr().Interface())
// 		}
// 	}
// 	return dest
// }
