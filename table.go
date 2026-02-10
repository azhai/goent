package goent

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// TableInfo contains metadata about a database table including columns, primary keys, and indexes.
type TableInfo struct {
	TableAddr   uintptr
	FieldName   string
	TableId     int
	TableName   string
	SchemaId    int
	SchemaName  string
	PrimaryKeys []*Index
	Indexes     []*Index
	Columns     map[string]*Column
	Foreigns    map[string]*Foreign
	Ignores     []string
}

func (t TableInfo) String() string {
	return t.TableName
	// return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
}

func (t TableInfo) Table() *model.Table {
	schemaName := t.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}
	return &model.Table{
		Schema: &schemaName,
		Name:   t.TableName,
	}
}

func (t TableInfo) Field(col string) *Field {
	if fid, ok := t.Check(col); ok {
		return &Field{Table: t.TableAddr, FieldId: fid, Column: col}
	}
	panic(fmt.Sprintf("column %s not found in table %s", col, t.TableName))
}

func (t TableInfo) Check(col string) (int, bool) {
	if col == "*" {
		return -1, true
	}
	if info, ok := t.Columns[col]; ok {
		return info.FieldId, ok
	}
	return -1, false
}

func (t TableInfo) GetPrimaryInfo() (int, string, []string) {
	pkFid, pkName := -1, ""
	pkeys := make([]string, 0, len(t.PrimaryKeys))
	for _, pkey := range t.PrimaryKeys {
		pkeys = append(pkeys, pkey.ColumnName)
		if pkey.Column.isAutoIncr {
			pkFid = pkey.Column.FieldId
			pkName = pkey.ColumnName
		}
	}
	sort.Strings(pkeys)
	return pkFid, pkName, pkeys
}

// Table represents a database table with its model and metadata.
// It provides methods for querying, inserting, updating, and deleting records.
type Table[T any] struct {
	Model   *T
	newbies []*T
	exists  sync.Map
	db      *DB
	state   *StateWhere
	TableInfo
}

// NewTableReflect creates a new Table instance using reflection.
// It analyzes the struct type to extract table metadata including columns, primary keys, and indexes.
func NewTableReflect(db *DB, typeOf reflect.Type, fieldName, schema string, schemaId, tableId int) (reflect.Value, TableInfo) {
	tb := reflect.New(typeOf)
	modelField := tb.Elem().FieldByName("Model")
	if !modelField.IsValid() {
		return tb, TableInfo{}
	}

	modelType := modelField.Type().Elem()
	modelValue := reflect.New(modelType)
	tb.Elem().FieldByName("Model").Set(modelValue)
	tableName := utils.TableNameMethod(modelValue)
	if tableName == "" {
		tableName = utils.TableNamePattern(fieldName)
	}
	info := TableInfo{
		SchemaId: schemaId, SchemaName: schema,
		TableId: tableId, TableName: tableName,
		FieldName: fieldName, Columns: make(map[string]*Column),
		Foreigns: make(map[string]*Foreign),
	}

	// var attr field
	modelValue = modelValue.Elem()
	for i := 0; i < modelValue.NumField(); i++ {
		fieldOf := modelValue.Type().Field(i)
		fieldKind := fieldOf.Type.Kind()
		geoTag := fieldOf.Tag.Get("goe")
		if geoTag == "-" || fieldKind == reflect.Slice ||
			fieldKind == reflect.Interface || fieldKind == reflect.Func {
			continue
		}
		addr := modelValue.Field(i).Addr()
		columnName := utils.ToSnakeCase(fieldOf.Name)
		_, exists := utils.GetTagValue(geoTag, "default")
		column := &Column{
			FieldAddr:  uintptr(addr.UnsafePointer()),
			FieldName:  fieldOf.Name,
			ColumnName: columnName,
			ColumnType: fieldOf.Type.String(),
			AllowNull:  fieldKind == reflect.Ptr,
			HasDefault: exists,
			FieldId:    i,
			tableName:  tableName,
			schemaName: &schema,
			db:         db,
		}
		info.Columns[columnName] = column

		if strings.EqualFold(fieldOf.Name, "id") || utils.HasTagValue(geoTag, "pk") {
			isAutoIncr := !utils.HasTagValue(geoTag, "not_incr")
			column.isAutoIncr = isAutoIncr
			// attr = createPkFromColumn(db, column, tableId, isAutoIncr)
			info.PrimaryKeys = append(info.PrimaryKeys, &Index{
				IsUnique:   true,
				IsAutoIncr: isAutoIncr,
				Column:     column,
			})
		} else {
			// attr = createAttFromColumn(db, column, tableId)
			if utils.HasTagValue(geoTag, "unique") {
				info.Indexes = append(info.Indexes, &Index{
					IsUnique:   true,
					IsAutoIncr: false,
					Column:     column,
				})
			} else if utils.HasTagValue(geoTag, "index") {
				info.Indexes = append(info.Indexes, &Index{
					IsUnique:   false,
					IsAutoIncr: false,
					Column:     column,
				})
			}
		}
		// addrMap.set(column.FieldAddr, attr)
	}

	tb.Elem().FieldByName("TableInfo").Set(reflect.ValueOf(info))
	return tb, info
}

func (t *Table[T]) SetDB(db *DB) {
	t.db = db
	for _, col := range t.Columns {
		col.db = db
	}
}

func (t *Table[T]) Load(foreign *Foreign) {
	switch foreign.Type {
	default:
		return
	case O2O:
		return
	case O2M:
		return
	case M2O:
		return
	case M2M:
		return
	}
}

// func (t *Table[T]) FieldInfo(name string) *Column {
// 	if col, ok := t.Columns[name]; ok {
// 		return col
// 	}
// 	for _, col := range t.Columns {
// 		if strings.EqualFold(col.ColumnName, name) {
// 			return col
// 		}
// 	}
// 	return nil
// }

// func (t *Table[T]) Field(name string) field {
// 	return t.FieldInfo(name)
// }

func (t *Table[T]) Dest() (*T, []any) {
	obj, size := new(T), len(t.Columns)
	dest := make([]any, size)
	value := reflect.ValueOf(obj).Elem()
	for i := range size {
		dest[i] = value.Field(i).Addr().Interface()
	}
	return obj, dest
}

// ------------------------------
// Filter ...
// ------------------------------

func (t *Table[T]) Filter(args ...Condition) *Table[T] {
	return t.FilterContext(context.Background(), args...)
}

func (t *Table[T]) FilterContext(ctx context.Context, args ...Condition) *Table[T] {
	if t.state == nil {
		t.state = NewStateWhere(ctx)
	}
	if err := t.state.Filter(args...); err != nil {
		panic(err)
	}
	return t
}

func (t *Table[T]) Drop() error {
	if t.db != nil {
		return Migrate(t.db).OnTable(t.TableName).DropTable()
	}
	return fmt.Errorf("db not found")
}

// ------------------------------
// Delete ...
// ------------------------------

func (t *Table[T]) Delete() *StateDelete[T] {
	return t.DeleteContext(context.Background())
}

func (t *Table[T]) DeleteContext(ctx context.Context) *StateDelete[T] {
	var s *StateWhere
	if s = t.state; s == nil {
		s = NewStateWhere(ctx)
		s.builder.Type = enum.DeleteQuery
	}
	return &StateDelete[T]{table: t, StateWhere: s}
}

// ------------------------------
// Insert/Save ...
// ------------------------------

func (t *Table[T]) Insert() *StateInsert[T] {
	return t.InsertContext(context.Background())
}

func (t *Table[T]) InsertContext(ctx context.Context) *StateInsert[T] {
	s := NewStateWhere(ctx)
	s.builder.Type = enum.InsertQuery
	return &StateInsert[T]{table: t, StateWhere: s}
}

func (t *Table[T]) Save() *StateSave[T] {
	return t.SaveContext(context.Background())
}

func (t *Table[T]) SaveContext(ctx context.Context) *StateSave[T] {
	s := NewStateWhere(ctx)
	return &StateSave[T]{table: t, StateWhere: s}
}

// ------------------------------
// Update ...
// ------------------------------

func (t *Table[T]) Update() *StateUpdate[T] {
	return t.UpdateContext(context.Background())
}

func (t *Table[T]) UpdateContext(ctx context.Context) *StateUpdate[T] {
	var s *StateWhere
	if s = t.state; s == nil {
		s = NewStateWhere(ctx)
		s.builder.Type = enum.UpdateQuery
	}
	return &StateUpdate[T]{table: t, StateWhere: s}
}

// ------------------------------
// Select ...
// ------------------------------

func (t *Table[T]) Select(cols ...any) *StateSelect[T, T] {
	return t.SelectContext(context.Background(), cols...)
}

func (t *Table[T]) SelectContext(ctx context.Context, cols ...any) *StateSelect[T, T] {
	state := NewStateSelect[T, T](ctx, t)
	if len(cols) > 0 {
		state = state.Select(cols...)
	}
	return state
}

// ------------------------------
// Count ...
// ------------------------------

func (s *StateSelect[T, R]) Count(col string) (int64, error) {
	return s.CountContext(context.Background(), col)
}

func (s *StateSelect[T, R]) CountContext(ctx context.Context, col string) (int64, error) {
	fld := &Field{Table: s.table.TableAddr, Column: col, Function: "COUNT(%s)"}
	query := NewStateSelect[T, ResultCount](ctx, s.table).Select(fld)
	return FetchCountResult(query)
}

func (t *Table[T]) Count(col string) (int64, error) {
	return t.CountContext(context.Background(), col)
}

func (t *Table[T]) CountContext(ctx context.Context, col string) (int64, error) {
	return NewStateSelect[T, ResultCount](ctx, t).CountContext(ctx, col)
}

// ------------------------------
// Max/Min/Sum/Avg ...
// ------------------------------

func (t *Table[T]) Max(col string) (float64, error) {
	return t.MaxContext(context.Background(), col)
}

func (t *Table[T]) MaxContext(ctx context.Context, col string) (float64, error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "MAX(%s)"}
	query := NewStateSelect[T, ResultAggr](ctx, t).Select(fld)
	return FetchAggrResult(query)
}

func (t *Table[T]) Min(col string) (float64, error) {
	return t.MinContext(context.Background(), col)
}

func (t *Table[T]) MinContext(ctx context.Context, col string) (float64, error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "MIN(%s)"}
	query := NewStateSelect[T, ResultAggr](ctx, t).Select(fld)
	return FetchAggrResult(query)
}

func (t *Table[T]) Sum(col string) (float64, error) {
	return t.SumContext(context.Background(), col)
}

func (t *Table[T]) SumContext(ctx context.Context, col string) (float64, error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "SUM(%s)"}
	query := NewStateSelect[T, ResultAggr](ctx, t).Select(fld)
	return FetchAggrResult(query)
}

func (t *Table[T]) Avg(col string) (float64, error) {
	return t.AvgContext(context.Background(), col)
}

func (t *Table[T]) AvgContext(ctx context.Context, col string) (float64, error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "AVG(%s)"}
	query := NewStateSelect[T, ResultAggr](ctx, t).Select(fld)
	return FetchAggrResult(query)
}

// ------------------------------
// ToUpper/ToLower ...
// ------------------------------

func (t *Table[T]) ToUpper(col string) ([]string, error) {
	return t.ToUpperContext(context.Background(), col)
}

func (t *Table[T]) ToUpperContext(ctx context.Context, col string) (res []string, err error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "UPPER(%s)"}
	query := NewStateSelect[T, FuncStr](ctx, t).Select(fld)
	return FetchFuncResult(query)
}

func (t *Table[T]) ToLower(col string) ([]string, error) {
	return t.ToLowerContext(context.Background(), col)
}

func (t *Table[T]) ToLowerContext(ctx context.Context, col string) (res []string, err error) {
	fld := &Field{Table: t.TableAddr, Column: col, Function: "LOWER(%s)"}
	query := NewStateSelect[T, FuncStr](ctx, t).Select(fld)
	return FetchFuncResult(query)
}
