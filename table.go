package goent

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

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

func getColumnTypeName(t reflect.Type) string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		return "[]" + t.Elem().Kind().String()
	}
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	if t.Kind() == reflect.Array {
		return fmt.Sprintf("[%d]%s", t.Len(), t.Elem().Kind().String())
	}
	return t.Kind().String()
}

func (t TableInfo) String() string {
	return t.TableName
	// return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
}

func (t TableInfo) Table() *model.Table {
	var schemaName *string
	if t.SchemaName != "" {
		schemaName = &t.SchemaName
	}
	return &model.Table{
		Schema: schemaName,
		Name:   t.TableName,
	}
}

func (t TableInfo) ColumnInfo(name string) *Column {
	if col, ok := t.Columns[name]; ok {
		return col
	}
	for _, col := range t.Columns {
		if strings.EqualFold(col.ColumnName, name) {
			return col
		}
	}
	return nil
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
	if info := t.ColumnInfo(col); info != nil {
		return info.FieldId, true
	}
	return -1, false
}

func (t TableInfo) GetPrimaryInfo() (int, string, []string) {
	pkFid, pkName := -1, ""
	pkeys := make([]string, 0, len(t.PrimaryKeys))
	for _, pkey := range t.PrimaryKeys {
		pkeys = append(pkeys, pkey.ColumnName)
	}
	if len(t.PrimaryKeys) == 1 {
		pkFid = t.PrimaryKeys[0].Column.FieldId
		pkName = t.PrimaryKeys[0].ColumnName
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

// GetSortedFields returns the table's columns sorted by FieldId for SELECT queries.
func (t *Table[T]) GetSortedFields() []*Field {
	columns := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		columns = append(columns, col)
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].FieldId < columns[j].FieldId
	})
	fields := make([]*Field, len(columns))
	for i, col := range columns {
		fields[i] = &Field{
			TableAddr:  t.TableAddr,
			ColumnName: col.ColumnName,
			FieldId:    col.FieldId,
		}
	}
	return fields
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
		if geoTag == "-" || fieldKind == reflect.Interface || fieldKind == reflect.Func {
			continue
		}

		columnName := utils.ToSnakeCase(fieldOf.Name)
		defaultValue, hasDefault := utils.GetTagValue(geoTag, "default")

		if fieldKind == reflect.Slice {
			if utils.HasTagValue(geoTag, "o2m") {
				fkCol, _ := utils.GetTagValue(geoTag, "fk")
				info.Foreigns[columnName] = &Foreign{
					Type:       O2M,
					MountField: fieldOf.Name,
					ForeignKey: fkCol,
					Reference:  nil,
				}
				continue
			} else if utils.HasTagValue(geoTag, "m2m") {
				middle, _ := utils.GetTagValue(geoTag, "middle")
				leftCol, _ := utils.GetTagValue(geoTag, "left")
				rightCol, _ := utils.GetTagValue(geoTag, "right")
				var middleInfo *ThirdParty
				if middle != "" {
					middleInfo = &ThirdParty{
						Table: middle,
						Left:  leftCol,
						Right: rightCol,
					}
				}
				info.Foreigns[columnName] = &Foreign{
					Type:       M2M,
					MountField: fieldOf.Name,
					ForeignKey: "",
					Reference:  nil,
					Middle:     middleInfo,
				}
				continue
			}
			if fieldOf.Type.Elem().Kind() != reflect.Uint8 {
				continue
			}
		}

		column := &Column{
			FieldName:    fieldOf.Name,
			ColumnName:   columnName,
			ColumnType:   getColumnTypeName(fieldOf.Type),
			AllowNull:    fieldKind == reflect.Pointer,
			HasDefault:   hasDefault,
			DefaultValue: defaultValue,
			FieldId:      i,
			tableName:    tableName,
			schemaName:   &schema,
		}
		info.Columns[columnName] = column

		if strings.EqualFold(fieldOf.Name, "id") || utils.HasTagValue(geoTag, "pk") {
			isAutoIncr := !utils.HasTagValue(geoTag, "not_incr") && strings.Contains(fieldOf.Type.Kind().String(), "int")
			column.isAutoIncr = isAutoIncr
			info.PrimaryKeys = append(info.PrimaryKeys, &Index{
				IsUnique:   true,
				IsAutoIncr: isAutoIncr,
				Column:     column,
			})
		} else if utils.HasTagValue(geoTag, "m2o") {
			mountField := strings.TrimSuffix(fieldOf.Name, "ID")
			mountField = strings.TrimSuffix(mountField, "Id")
			if mountField == fieldOf.Name {
				mountField = strings.TrimSuffix(columnName, "_id")
				mountField = utils.TitleCase(mountField)
			}
			info.Foreigns[columnName] = &Foreign{
				Type:       M2O,
				MountField: mountField,
				ForeignKey: columnName,
				Reference:  nil,
			}
		} else if utils.HasTagValue(geoTag, "o2o") {
			mountField := strings.TrimSuffix(fieldOf.Name, "ID")
			mountField = strings.TrimSuffix(mountField, "Id")
			if mountField == fieldOf.Name {
				mountField = strings.TrimSuffix(columnName, "_id")
				mountField = utils.TitleCase(mountField)
			}
			info.Foreigns[columnName] = &Foreign{
				Type:       O2O,
				MountField: mountField,
				ForeignKey: columnName,
				Reference:  nil,
			}
		} else {
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
	}

	tb.Elem().FieldByName("TableInfo").Set(reflect.ValueOf(info))
	return tb, info
}

func (t *Table[T]) SetDB(db *DB) {
	t.db = db
}

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
// Filter/Where ...
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

func (t *Table[T]) Where(where string, args ...any) *Table[T] {
	return t.WhereContext(context.Background(), where, args...)
}

func (t *Table[T]) WhereContext(ctx context.Context, where string, args ...any) *Table[T] {
	if t.State == nil {
		t.State = NewStateWhere(ctx)
	}
	t.State = t.State.Where(where, args...)
	return t
}

func (t *Table[T]) Drop() error {
	if t.db != nil {
		return Migrate(t.db).OnTable(t.TableName).DropTable()
	}
	return ErrDBNotFound
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
		s.builder.Type = model.DeleteQuery
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
	s.builder.Type = model.InsertQuery
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
		s.builder.Type = model.UpdateQuery
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
