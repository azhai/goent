package goent

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// Entity is the interface for entities that have an ID.
type Entity interface {
	GetID() int64
	SetID(int64)
}

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
	simpleTable bool
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
	col = strings.TrimSpace(col)
	if fid, ok := t.Check(col); ok {
		return &Field{TableAddr: t.TableAddr, FieldId: fid, ColumnName: col}
	}
	panic(fmt.Sprintf("column %s not found in table %s", col, t.TableName))
}

func (t TableInfo) Check(col string) (int, bool) {
	col = strings.TrimSpace(col)
	if t.simpleTable || col == "" || col == "*" ||
			strings.ContainsAny(col, " ,+*/%()") {
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
	Model *T
	Cache *utils.CoMap[int64, T]
	State *StateWhere
	db    *DB
	TableInfo
}

// SimpleTable creates a new Table instance for a simple table without foreign keys.
// It is useful for tables that do not have complex relationships with other tables.
func SimpleTable[T any](db *DB, tableName, SchemaName string) *Table[T] {
	return &Table[T]{
		db: db,
		TableInfo: TableInfo{
			TableName:   tableName,
			SchemaName:  SchemaName,
			simpleTable: true,
		},
	}
}

// NewTableReflect creates a new Table instance using reflection.
// It analyzes the struct type to extract table metadata including columns, primary keys, and indexes.
func NewTableReflect(db *DB, typeOf reflect.Type, addr uintptr, fieldName, schema string,
		schemaId, tableId int) (reflect.Value, TableInfo) {
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
		TableAddr: addr, FieldName: fieldName,
		Columns:  make(map[string]*Column),
		Foreigns: make(map[string]*Foreign),
	}

	// var attr field
	modelValue = modelValue.Elem()
	for i := range modelValue.NumField() {
		fieldOf := modelValue.Type().Field(i)
		fieldKind := fieldOf.Type.Kind()
		geoTag := fieldOf.Tag.Get("goe")
		if geoTag == "-" || fieldKind == reflect.Slice ||
				fieldKind == reflect.Interface || fieldKind == reflect.Func {
			continue
		}
		columnName := utils.ToSnakeCase(fieldOf.Name)
		_, exists := utils.GetTagValue(geoTag, "default")
		column := &Column{
			FieldName:  fieldOf.Name,
			ColumnName: columnName,
			ColumnType: fieldOf.Type.Kind().String(),
			AllowNull:  fieldKind == reflect.Pointer,
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
// LoadOne/LoadAll ...
// ------------------------------

func (t *Table[T]) CacheOne(row any) {
	var id int64
	if row, ok := row.(Entity); ok {
		id = row.GetID()
	} else {
		return
	}
	if t.Cache == nil {
		t.Cache = utils.NewCoMap[int64, T]()
	}
	t.Cache.Set(id, row.(*T))
}

// ------------------------------
// Filter ...
// ------------------------------

func (t *Table[T]) Filter(args ...Condition) *Table[T] {
	return t.FilterContext(context.Background(), args...)
}

func (t *Table[T]) FilterContext(ctx context.Context, args ...Condition) *Table[T] {
	if t.State == nil {
		t.State = NewStateWhere(ctx)
	}
	t.State = t.State.Filter(args...)
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
	if s = t.State; s == nil {
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
	if s = t.State; s == nil {
		s = NewStateWhere(ctx)
		s.builder.Type = enum.UpdateQuery
	}
	return &StateUpdate[T]{table: t, StateWhere: s}
}

// ------------------------------
// Select ...
// ------------------------------

func (t *Table[T]) Select(fields ...any) *StateSelect[T, T] {
	return t.SelectContext(context.Background(), fields...)
}

func (t *Table[T]) SelectContext(ctx context.Context, fields ...any) *StateSelect[T, T] {
	var s *StateWhere
	if s = t.State; s == nil {
		s = NewStateWhere(ctx)
	}
	state := NewStateSelectFrom[T, T](s, t)
	if len(fields) > 0 {
		state.Select(fields...)
	} else {
		state.sameModel = true
	}
	return state
}
