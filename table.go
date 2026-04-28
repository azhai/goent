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

// TableInfo contains metadata about a database table including columns, primary keys, and indexes.
// It is automatically populated when creating a Table using reflection.
type TableInfo struct {
	TableAddr  uintptr // TableAddr is the unique address of the table type.
	FieldName  string  // FieldName is the name of the field in the entity struct.
	TableId    int     // TableId is the unique identifier for the table.
	TableName  string  // TableName is the name of the table in the database schema.
	SchemaId   int     // SchemaId is the unique identifier for the schema.
	SchemaName string  // SchemaName is the name of the schema in the database.

	PrimaryKeys []*Index            // PrimaryKeys is a list of primary key indexes.
	Indexes     []*Index            // Indexes is a list of non-primary key indexes.
	ColumnNames []string            // ColumnNames is a list of column names in the table.
	Columns     map[string]*Column  // Columns is a map of column names to Column metadata.
	Foreigns    map[string]*Foreign // Foreigns is a map of foreign key column names to Foreign metadata.
	Ignores     []string            // Ignores is a list of column names to ignore.

	simpleTable   bool     // simpleTable is true if the table has a single primary key.
	sortedFields  []*Field // sortedFields is a list of columns sorted by field ID.
	selectByPKSql string   // selectByPKSql is the cached SELECT BY primary key SQL.
	deleteByPKSql string   // deleteByPKSql is the cached DELETE BY primary key SQL.
	pkField       *Field   // pkField is the cached primary key field.
}

// String returns the table name as a string representation.
func (info TableInfo) String() string {
	return info.TableName
	// return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
}

// Table returns a model.Table representation with schema and table name.
func (info TableInfo) Table() *model.Table {
	var schemaName *string
	if info.SchemaName != "" {
		schemaName = &info.SchemaName
	}
	return &model.Table{
		Schema: schemaName,
		Name:   info.TableName,
	}
}

// ColumnInfo returns the Column metadata for a given column name.
// It performs case-insensitive lookup.
func (info TableInfo) ColumnInfo(name string) *Column {
	if col, ok := info.Columns[name]; ok {
		return col
	}
	for _, col := range info.Columns {
		if strings.EqualFold(col.ColumnName, name) {
			return col
		}
	}
	return nil
}

// Field returns a Field pointer for the specified column name.
// It panics if the column is not found in the table.
// Uses caching to reduce allocations.
func (info *TableInfo) Field(name string) *Field {
	name = strings.TrimSpace(name)
	if info.simpleTable || name == "" || name == "*" ||
		strings.ContainsAny(name, " ,+*/%()") {
		return &Field{TableAddr: info.TableAddr, FieldId: -1, ColumnName: name}
	}
	var col *Column
	if col = info.ColumnInfo(name); col == nil {
		panic(fmt.Sprintf("column %s not found in table %s", name, info.TableName))
	}
	return info.sortedFields[col.FieldId]
}

// GetPrimaryInfo returns the primary key field ID, column name, and all primary key column names.
// It only returns valid field ID and column name for single-column primary keys.
func (info TableInfo) GetPrimaryInfo() (int, string, []string) {
	pkFid, pkName := -1, ""
	size := len(info.PrimaryKeys)
	pkeys := make([]string, 0, size)
	for _, pkey := range info.PrimaryKeys {
		pkeys = append(pkeys, pkey.ColumnName)
	}
	if size == 1 {
		pkFid = info.PrimaryKeys[0].Column.FieldId
		pkName = info.PrimaryKeys[0].ColumnName
	}
	sort.Strings(pkeys)
	return pkFid, pkName, pkeys
}

// GetPKField returns the primary key field for single-PK tables.
// Returns nil for tables with no primary key or composite primary keys.
func (info *TableInfo) GetPKField() *Field {
	if info.pkField != nil {
		return info.pkField
	}
	if len(info.PrimaryKeys) != 1 {
		return nil
	}
	info.pkField = info.sortedFields[info.PrimaryKeys[0].Column.FieldId]
	return info.pkField
}

// GetSortedFields returns the table's columns sorted by FieldId for SELECT queries.
// This ensures consistent column ordering between queries and struct fields.
// Example:
//
//	fields := table.GetSortedFields()
//	for _, field := range fields {
//		fmt.Println(field.ColumnName)
//	}
func (info TableInfo) GetSortedFields() []*Field {
	return info.sortedFields
}

func (info TableInfo) getFullName() string {
	if info.SchemaName != "" {
		return info.SchemaName + "." + info.TableName
	}
	return info.TableName
}

func (info *TableInfo) GetSelectByPKSql() string {
	if info.selectByPKSql != "" {
		return info.selectByPKSql
	}
	if len(info.PrimaryKeys) != 1 {
		return ""
	}
	pkName := info.PrimaryKeys[0].ColumnName
	info.selectByPKSql = "SELECT * FROM " + info.getFullName() + " WHERE " + pkName + " = $1"
	return info.selectByPKSql
}

func (info *TableInfo) GetDeleteByPKSql() string {
	if info.deleteByPKSql != "" {
		return info.deleteByPKSql
	}
	if len(info.PrimaryKeys) != 1 {
		return ""
	}
	pkName := info.PrimaryKeys[0].ColumnName
	info.deleteByPKSql = "DELETE FROM " + info.getFullName() + " WHERE " + pkName + " = $1"
	return info.deleteByPKSql
}

func (info TableInfo) getRefTableName(foreign *Foreign, fkName string) (string, bool) {
	switch foreign.Type {
	case M2O, O2O:
		if _, ok := info.Columns[fkName]; !ok {
			return "", false
		}
		return strings.TrimSuffix(fkName, "_id"), true
	case O2M:
		return strings.TrimSuffix(foreign.ForeignKey, "_id"), true
	case M2M:
		if foreign.Middle == nil {
			return "", false
		}
		return strings.TrimSuffix(foreign.Middle.Right, "_id"), true
	}
	return "", false
}

func (info TableInfo) setForeignReference(foreign *Foreign, refTableName string) (*Foreign, bool) {
	if strings.EqualFold(info.TableName, refTableName) ||
		strings.EqualFold(info.FieldName, refTableName) ||
		strings.HasSuffix(strings.ToLower(info.TableName), "_"+strings.ToLower(refTableName)) {
		foreign.Reference = &Field{
			TableAddr:  info.TableAddr,
			ColumnName: "id",
		}
		for _, pk := range info.PrimaryKeys {
			if pk.IsAutoIncr {
				foreign.Reference.FieldId = pk.FieldId
				break
			}
		}
		return foreign, true
	}
	return foreign, false
}

// Table represents a database table with its model and metadata.
// It provides methods for querying, inserting, updating, and deleting records.
type Table[T any] struct {
	Model       *T
	Cache       *utils.CoMap[int64, T]
	State       *StateWhere
	StateDelete *StateDeleteWhere
	db          *DB
	TableInfo
}

// SimpleTable creates a new Table instance for a simple table without foreign keys.
// It is useful for tables that do not have complex relationships with other tables.
//
// Example:
//
//	type SimpleRecord struct {
//		ID   int64
//		Name string
//	}
//	table := SimpleTable[SimpleRecord](db, "simple_record", "")
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
//
// Example:
//
//	value, info := NewTableReflect(db, reflect.TypeFor[User](), addr, "User", "", 0, 0)
//	fmt.Println(info.TableName) // prints "user"
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

	// Parse schema and table names from tags
	schemaName, prefix := utils.ParseSchemaTag(schema)
	tableName := utils.TableNameMethod(modelValue)
	if tableName == "" {
		tableName = prefix + utils.TableNamePattern(fieldName)
	}

	info := TableInfo{
		SchemaId: schemaId, SchemaName: schemaName,
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

		if fieldKind == reflect.Pointer {
			elemType := fieldOf.Type.Elem()
			if elemType.Kind() == reflect.Struct && elemType.PkgPath() != "" {
				if isTableTypeField(elemType) {
					continue
				}
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
		info.ColumnNames = append(info.ColumnNames, columnName)
		info.sortedFields = append(info.sortedFields, &Field{
			TableAddr:  info.TableAddr,
			ColumnName: columnName,
			FieldId:    column.FieldId,
		})

		if strings.EqualFold(fieldOf.Name, "id") || utils.HasTagValue(geoTag, "pk") {
			isAutoIncr := !utils.HasTagValue(geoTag, "not_incr") && strings.Contains(fieldOf.Type.Kind().String(), "int")
			column.isAutoIncr = isAutoIncr
			info.PrimaryKeys = append(info.PrimaryKeys, &Index{
				IsUnique:   true,
				IsAutoIncr: isAutoIncr,
				Column:     column,
			})
		}
		if utils.HasTagValue(geoTag, "m2o") || utils.HasTagValue(geoTag, "o2o") {
			fkType := M2O
			if utils.HasTagValue(geoTag, "o2o") {
				fkType = O2O
			}
			mountField := strings.TrimSuffix(fieldOf.Name, "ID")
			mountField = strings.TrimSuffix(mountField, "Id")
			if mountField == fieldOf.Name {
				mountField = strings.TrimSuffix(columnName, "_id")
				mountField = utils.ToTitleCase(mountField)
			}
			info.Foreigns[columnName] = &Foreign{
				Type:       fkType,
				MountField: mountField,
				ForeignKey: columnName,
				Reference:  nil,
			}
		}
		if !utils.HasTagValue(geoTag, "pk") && !strings.EqualFold(fieldOf.Name, "id") &&
			!utils.HasTagValue(geoTag, "m2o") && !utils.HasTagValue(geoTag, "o2o") {
			if utils.HasTagValue(geoTag, "unique") || utils.HasTagValue(geoTag, "index") {
				info.Indexes = append(info.Indexes, &Index{
					IsUnique:   utils.HasTagValue(geoTag, "unique"),
					IsAutoIncr: false,
					Column:     column,
				})
			}
		}
	}

	tb.Elem().FieldByName("TableInfo").Set(reflect.ValueOf(info))
	return tb, info
}

// SetDB sets the database connection for the table.
func (t *Table[T]) SetDB(db *DB) {
	t.db = db
}

// Dest returns a new instance of T and a slice of destination pointers for scanning.
// The destination slice is sized to hold all columns.
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

// CacheOne caches a single row in the table's cache using its ID as the key.
func (t *Table[T]) CacheOne(obj any) (id int64) {
	if row, ok := obj.(Entity); ok {
		id = row.GetID()
	} else {
		return
	}
	if t.Cache == nil {
		t.Cache = utils.NewCoMap[int64, T]()
	}
	t.Cache.Set(id, obj.(*T))
	return
}

// ------------------------------
// Filter/Where ...
// ------------------------------

// Filter adds filter conditions to the table's query using the default context.
//
// Example:
//
//	users, err := db.User.Filter(goent.Equals("status", "active")).Select().All()
func (t *Table[T]) Filter(args ...Condition) *Table[T] {
	return t.FilterContext(context.Background(), args...)
}

// FilterContext adds filter conditions to the table's query with a specific context.
func (t *Table[T]) FilterContext(ctx context.Context, args ...Condition) *Table[T] {
	if t.State == nil {
		t.State = NewStateWhere(ctx)
	}
	t.State = t.State.Filter(args...)
	return t
}

// Where adds a WHERE clause to the table's query using the default context.
//
// Example:
//
//	users, err := db.User.Where("age > ?", 18).Select().All()
func (t *Table[T]) Where(where string, args ...any) *Table[T] {
	return t.WhereContext(context.Background(), where, args...)
}

// WhereContext adds a WHERE clause to the table's query with a specific context.
func (t *Table[T]) WhereContext(ctx context.Context, where string, args ...any) *Table[T] {
	if t.State == nil {
		t.State = NewStateWhere(ctx)
	}
	t.State = t.State.Where(where, args...)
	return t
}

// Drop drops (deletes) the table from the database.
//
// Example:
//
//	err := db.User.Drop()
//	if err != nil {
//		log.Fatal(err)
//	}
func (t *Table[T]) Drop() error {
	if t.db != nil {
		return Migrate(t.db).OnTable(t.TableName).DropTable()
	}
	return model.ErrDBNotFound
}

// ------------------------------
// Delete ...
// ------------------------------

// Delete creates a new StateDelete for deleting records from the table.
//
// Example:
//
//	err := db.User.Filter(goent.Equals("status", "deleted")).Delete().Exec()
func (t *Table[T]) Delete() *StateDelete[T] {
	return t.DeleteContext(context.Background())
}

// DeleteContext creates a new StateDelete with a specific context.
func (t *Table[T]) DeleteContext(ctx context.Context) *StateDelete[T] {
	var s *StateDeleteWhere
	if s = t.StateDelete; s == nil {
		s = NewStateDeleteWhere(ctx)
	}
	return &StateDelete[T]{table: t, StateDeleteWhere: s}
}

// ------------------------------
// Insert/Save ...
// ------------------------------

// Insert creates a new StateInsert for inserting records into the table.
//
// Example:
//
//	user := &User{Name: "John", Email: "john@example.com"}
//	err := db.User.Insert().One(user)
func (t *Table[T]) Insert() *StateInsert[T] {
	return t.InsertContext(context.Background())
}

// InsertContext creates a new StateInsert with a specific context.
func (t *Table[T]) InsertContext(ctx context.Context) *StateInsert[T] {
	s := NewStateWhere(ctx)
	s.builder.Type = model.InsertQuery
	return &StateInsert[T]{table: t, StateWhere: s}
}

// Save creates a new StateSave for saving (insert or update) records to the table.
//
// Example:
//
//	user := &User{ID: 1, Name: "John"} // ID > 0 means update
//	err := db.User.Save().One(user)
func (t *Table[T]) Save() *StateSave[T] {
	return t.SaveContext(context.Background())
}

// SaveContext creates a new StateSave with a specific context.
func (t *Table[T]) SaveContext(ctx context.Context) *StateSave[T] {
	s := NewStateWhere(ctx)
	return &StateSave[T]{table: t, StateWhere: s}
}

// ------------------------------
// Update ...
// ------------------------------

// Update creates a new StateUpdate for updating records in the table.
//
// Example:
//
//	change := goent.Pair{Key: "name", Value: "John"}
//	err := db.User.Filter(goent.Equals("status", "active")).Update().Set(change).Exec()
func (t *Table[T]) Update() *StateUpdate[T] {
	return t.UpdateContext(context.Background())
}

// UpdateContext creates a new StateUpdate with a specific context.
func (t *Table[T]) UpdateContext(ctx context.Context) *StateUpdate[T] {
	s := NewStateWhere(ctx)
	s.builder.Type = model.UpdateQuery
	return &StateUpdate[T]{table: t, StateWhere: s}
}

// ------------------------------
// Select ...
// ------------------------------

// Select creates a new StateSelect for querying records from the table.
//
// Example:
//
//	users, err := db.User.Select("id", "name", "email").All()
func (t *Table[T]) Select(fields ...any) *StateSelect[T, T] {
	return t.SelectContext(context.Background(), fields...)
}

// SelectContext creates a new StateSelect with a specific context.
func (t *Table[T]) SelectContext(ctx context.Context, fields ...any) *StateSelect[T, T] {
	s := NewStateWhere(ctx)
	state := NewStateSelectFrom[T, T](s, t)
	if len(fields) > 0 {
		state.builder.VisitFields = make([]*Field, 0, len(fields))
		state.Select(fields...)
	} else {
		state.sameModel = true
	}
	return state
}

// getColumnTypeName returns the string representation of a column type.
// It handles pointer, slice, array, and custom types.
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

func isTableTypeField(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	ptrType := reflect.PointerTo(t)
	if method, ok := ptrType.MethodByName("TableName"); ok {
		if method.Type.NumIn() == 0 && method.Type.NumOut() == 1 {
			return true
		}
	}
	for _, info := range tableRegistry {
		if info.FieldName == t.Name() {
			return true
		}
	}
	return false
}
