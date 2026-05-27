package goent

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

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

	simpleTable   bool         // simpleTable is true if the table has a single primary key.
	sortedFields  []*Field     // sortedFields is a list of columns sorted by field ID.
	selectByPKSql string       // selectByPKSql is the cached SELECT BY primary key SQL.
	deleteByPKSql string       // deleteByPKSql is the cached DELETE BY primary key SQL.
	pkField       *Field       // pkField is the cached primary key field.
	modelType     reflect.Type // modelType is the reflect.Type of the table's model struct.
	driver        model.Driver // driver is the database driver for this table.
	db            *DB          // db is the database connection for this table.

	// formattedName is the cached driver-formatted full table name (e.g. "public"."status").
	formattedName string
	// modelTable is the cached model.Table to avoid allocation on each SetTable call.
	modelTable *model.Table
	// cachedConn is the cached Connection to avoid allocation on each Prepare call.
	cachedConn model.Connection
	// cachedCfg is the cached DatabaseConfig pointer to avoid repeated GetDatabaseConfig calls.
	cachedCfg *model.DatabaseConfig

	// fetchByPK is the cached FetchFunc for FindByPK fast path.
	fetchByPK FetchFunc
	// connByPK is the cached Connection for FindByPK fast path.
	connByPK model.Connection
	// cfgByPK is the cached DatabaseConfig for FindByPK fast path.
	cfgByPK *model.DatabaseConfig
	// cachedPkeys is the cached sorted primary key column names.
	cachedPkeys []string
	// insertOneSql is the cached INSERT SQL for single-record inserts.
	insertOneSql string
	// insertOneFields is the cached field list for INSERT (non-PK, non-default columns).
	insertOneFields []*Field
	// insertOneOnce ensures insertOneSql is initialized only once (concurrent-safe).
	insertOneOnce sync.Once
}

// String returns the table name as a string representation.
func (info TableInfo) String() string {
	return info.TableName
	// return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
}

// Table returns a model.Table representation with schema and table name.
func (info *TableInfo) Table() *model.Table {
	if info.modelTable != nil {
		return info.modelTable
	}
	var schemaName *string
	if info.SchemaName != "" {
		schemaName = &info.SchemaName
	}
	info.modelTable = &model.Table{
		Schema: schemaName,
		Name:   info.TableName,
	}
	return info.modelTable
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
// The pkeys slice is cached to avoid allocation on repeated calls.
func (info *TableInfo) GetPrimaryInfo() (int, string, []string) {
	if info.cachedPkeys != nil {
		pkFid, pkName := -1, ""
		if len(info.PrimaryKeys) == 1 {
			pkFid = info.PrimaryKeys[0].Column.FieldId
			pkName = info.PrimaryKeys[0].ColumnName
		}
		return pkFid, pkName, info.cachedPkeys
	}
	size := len(info.PrimaryKeys)
	pkeys := make([]string, 0, size)
	for _, pkey := range info.PrimaryKeys {
		pkeys = append(pkeys, pkey.ColumnName)
	}
	sort.Strings(pkeys)
	info.cachedPkeys = pkeys
	pkFid, pkName := -1, ""
	if size == 1 {
		pkFid = info.PrimaryKeys[0].Column.FieldId
		pkName = info.PrimaryKeys[0].ColumnName
	}
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

// GetFormattedName returns the driver-formatted full table name, caching the result.
func (info *TableInfo) GetFormattedName() string {
	if info.formattedName != "" {
		return info.formattedName
	}
	if info.driver != nil {
		info.formattedName = info.driver.FormatTableName(info.SchemaName, info.TableName)
	} else {
		info.formattedName = info.getFullName()
	}
	return info.formattedName
}

// GetConnection returns a cached Connection, creating one if necessary.
func (info *TableInfo) GetConnection() model.Connection {
	if info.cachedConn != nil {
		return info.cachedConn
	}
	info.cachedConn = info.driver.NewConnection()
	return info.cachedConn
}

// GetConfig returns a cached DatabaseConfig pointer.
func (info *TableInfo) GetConfig() *model.DatabaseConfig {
	if info.cachedCfg != nil {
		return info.cachedCfg
	}
	info.cachedCfg = info.driver.GetDatabaseConfig()
	return info.cachedCfg
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
	Model *T
	db    *DB
	TableInfo
	// fetchAll is the cached FetchFunc for simple Select (sameModel, no joins).
	fetchAll FetchFunc
	// fetchAllOnce ensures fetchAll is initialized only once (concurrent-safe).
	fetchAllOnce sync.Once
	// fetchByPKOnce ensures fetchByPK/connByPK/cfgByPK are initialized only once.
	fetchByPKOnce sync.Once
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
			driver:      db.driver,
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
		Columns:   make(map[string]*Column),
		Foreigns:  make(map[string]*Foreign),
		modelType: modelType,
		driver:    db.driver,
		db:        db,
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
				// Find the element type name for reference matching
				refType := ""
				elemType := fieldOf.Type.Elem()
				if elemType.Kind() == reflect.Pointer {
					elemType = elemType.Elem()
				}
				refType = elemType.Name()
				info.Foreigns[columnName] = &Foreign{
					Type:       O2M,
					MountField: fieldOf.Name,
					ForeignKey: fkCol,
					Reference:  nil,
					RefType:    refType,
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
				// Find the element type name for reference matching
				refType := ""
				elemType := fieldOf.Type.Elem()
				if elemType.Kind() == reflect.Pointer {
					elemType = elemType.Elem()
				}
				refType = elemType.Name()
				info.Foreigns[columnName] = &Foreign{
					Type:       M2M,
					MountField: fieldOf.Name,
					ForeignKey: "",
					Reference:  nil,
					Middle:     middleInfo,
					RefType:    refType,
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
			column.IsPK = true
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
			// Find the type name of the mount field for reference matching
			refType := ""
			if mf, ok := modelValue.Type().FieldByName(mountField); ok {
				t := mf.Type
				if t.Kind() == reflect.Pointer {
					t = t.Elem()
				}
				if t.Kind() == reflect.Slice {
					t = t.Elem()
					if t.Kind() == reflect.Pointer {
						t = t.Elem()
					}
				}
				refType = t.Name()
			}
			info.Foreigns[columnName] = &Foreign{
				Type:       fkType,
				MountField: mountField,
				ForeignKey: columnName,
				Reference:  nil,
				RefType:    refType,
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
// TableQuery ...
// ------------------------------

// TableQuery represents a conditional query builder for a table.
// It is created by Table.Filter() or Table.Where() and supports chaining
// aggregate methods, Select(), and Delete().
//
// Example:
//
//	count, err := db.User.Filter(goent.Equals("status", "active")).Count("id")
//	users, err := db.User.Where("age > ?", 18).Select().All()
//	err := db.User.Filter(goent.Equals("status", "deleted")).Delete().Exec()
type TableQuery[T any] struct {
	table *Table[T]
	state *StateWhere
}

func newTableQuery[T any](table *Table[T], ctx context.Context) *TableQuery[T] {
	return &TableQuery[T]{table: table, state: NewStateWhere(ctx)}
}

// Filter adds filter conditions to the query.
func (q *TableQuery[T]) Filter(args ...Condition) *TableQuery[T] {
	q.state = q.state.Filter(args...)
	return q
}

// Where adds a raw WHERE clause to the query.
func (q *TableQuery[T]) Where(where string, args ...any) *TableQuery[T] {
	q.state = q.state.Where(where, args...)
	return q
}

// OnTransaction sets the transaction for the query.
func (q *TableQuery[T]) OnTransaction(tx model.Transaction) *TableQuery[T] {
	q.state = q.state.OnTransaction(tx)
	return q
}

// Select creates a StateSelect from this query's conditions.
// This allows further chaining with Select-specific methods like All(), One(), etc.
func (q *TableQuery[T]) Select() *StateSelect[T, T] {
	return NewStateSelectFrom[T, T](q.state, q.table)
}

// Delete creates a StateDelete from this query's conditions.
func (q *TableQuery[T]) Delete() *StateDelete[T] {
	s := NewStateDeleteWhere(q.state.ctx)
	s.builder.core.Where = q.state.builder.core.Where
	s.conn = q.state.conn
	return &StateDelete[T]{table: q.table, StateDeleteWhere: s}
}

// Count counts the number of rows matching the query conditions.
func (q *TableQuery[T]) Count(col string) (int64, error) {
	return aggInt[T](q.state, q.table, col, "COUNT(%s)")
}

func (q *TableQuery[T]) Max(col string) (int64, error) {
	return aggInt[T](q.state, q.table, col, "MAX(%s)")
}

func (q *TableQuery[T]) Min(col string) (int64, error) {
	return aggInt[T](q.state, q.table, col, "MIN(%s)")
}

func (q *TableQuery[T]) Sum(col string) (int64, error) {
	return aggInt[T](q.state, q.table, col, "SUM(%s)")
}

func (q *TableQuery[T]) Avg(col string) (int64, error) {
	return aggInt[T](q.state, q.table, col, "AVG(%s)")
}

func (q *TableQuery[T]) MaxFloat(col string) (float64, error) {
	return aggFloat[T](q.state, q.table, col, "MAX(%s)")
}

func (q *TableQuery[T]) MinFloat(col string) (float64, error) {
	return aggFloat[T](q.state, q.table, col, "MIN(%s)")
}

func (q *TableQuery[T]) SumFloat(col string) (float64, error) {
	return aggFloat[T](q.state, q.table, col, "SUM(%s)")
}

func (q *TableQuery[T]) AvgFloat(col string) (float64, error) {
	return aggFloat[T](q.state, q.table, col, "AVG(%s)")
}

func (q *TableQuery[T]) ToUpper(col string) ([]string, error) {
	return aggStr[T](q.state, q.table, col, "UPPER(%s)")
}

func (q *TableQuery[T]) ToLower(col string) ([]string, error) {
	return aggStr[T](q.state, q.table, col, "LOWER(%s)")
}

// ------------------------------
// Filter/Where ...
// ------------------------------

// Filter creates a conditional query builder with the specified filter conditions.
//
// Example:
//
//	count, err := db.User.Filter(goent.Equals("status", "active")).Count("id")
//	users, err := db.User.Filter(goent.Equals("status", "active")).Select().All()
func (t *Table[T]) Filter(args ...Condition) *TableQuery[T] {
	return t.FilterContext(context.Background(), args...)
}

// FilterContext creates a conditional query builder with the specified context and filter conditions.
func (t *Table[T]) FilterContext(ctx context.Context, args ...Condition) *TableQuery[T] {
	q := newTableQuery[T](t, ctx)
	q.state = q.state.Filter(args...)
	return q
}

// Where creates a conditional query builder with the specified WHERE clause.
//
// Example:
//
//	count, err := db.User.Where("age > ?", 18).Count("id")
//	users, err := db.User.Where("age > ?", 18).Select().All()
func (t *Table[T]) Where(where string, args ...any) *TableQuery[T] {
	return t.WhereContext(context.Background(), where, args...)
}

// WhereContext creates a conditional query builder with the specified context and WHERE clause.
func (t *Table[T]) WhereContext(ctx context.Context, where string, args ...any) *TableQuery[T] {
	q := newTableQuery[T](t, ctx)
	q.state = q.state.Where(where, args...)
	return q
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
		return t.db.driver.DropTable("", t.TableName)
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
	s := NewStateDeleteWhere(ctx)
	return &StateDelete[T]{table: t, StateDeleteWhere: s}
}

// Truncate removes all rows from the table and resets auto-increment counters.
// For PostgreSQL it executes TRUNCATE TABLE ... RESTART IDENTITY;
// For SQLite it executes DELETE FROM ... and resets the sqlite_sequence.
//
// Example:
//
//	err := db.User.Truncate()
func (t *Table[T]) Truncate() error {
	return t.TruncateContext(context.Background())
}

// TruncateContext removes all rows from the table and resets auto-increment counters with a specific context.
func (t *Table[T]) TruncateContext(ctx context.Context) error {
	var sql string
	tableName := t.getFullName()
	switch t.db.DriverName() {
	case "PostgreSQL":
		sql = "TRUNCATE TABLE " + tableName + " RESTART IDENTITY"
	default:
		sql = "DELETE FROM " + tableName
	}
	conn := t.db.driver.NewConnection()
	cfg := t.db.driver.GetDatabaseConfig()
	qr := model.CreateQuery(sql, nil)
	err := qr.WrapExec(ctx, conn, cfg)
	if err != nil {
		return err
	}
	if t.db.DriverName() == "SQLite" {
		seqSQL := "DELETE FROM sqlite_sequence WHERE name = ?"
		qr2 := model.CreateQuery(seqSQL, []any{t.TableName})
		err = qr2.WrapExec(ctx, conn, cfg)
	}
	return err
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

// InsertOne inserts a single record using a fast path that bypasses Builder creation.
// It uses cached SQL and pre-computed field positions to minimize allocations.
// Falls back to the standard Insert().One() path for complex cases.
func (t *Table[T]) InsertOne(obj *T) error {
	return t.InsertOneContext(context.Background(), obj)
}

// InsertOneContext inserts a single record with a specific context using a fast path.
func (t *Table[T]) InsertOneContext(ctx context.Context, obj *T) error {
	// Try fast path: single auto-increment PK, no defaults on non-PK columns
	if len(t.PrimaryKeys) == 1 && t.PrimaryKeys[0].IsAutoIncr {
		if sql, fields := t.getInsertOneSql(); sql != "" {
			return t.insertOneFastPath(ctx, sql, fields, obj)
		}
	}
	// Fallback to standard path
	return t.InsertContext(ctx).One(obj)
}

// getInsertOneSql returns the cached INSERT SQL and field list for the fast path.
// Returns empty string if the fast path is not applicable.
func (t *Table[T]) getInsertOneSql() (string, []*Field) {
	t.insertOneOnce.Do(func() {
		// Only cache for simple tables: single auto-incr PK, no default values on non-PK columns
		pkName := t.PrimaryKeys[0].ColumnName
		var fields []*Field
		applicable := true
		for _, col := range t.Columns {
			if col.IsPK && col.isAutoIncr {
				continue // Skip auto-increment PK
			}
			if col.HasDefault {
				// Columns with defaults need runtime check, can't use fast path
				applicable = false
				break
			}
			fields = append(fields, t.sortedFields[col.FieldId])
		}
		if !applicable {
			return // insertOneSql stays empty, marking fast path not applicable
		}
		// Build the INSERT SQL
		var buf bytes.Buffer
		buf.WriteString("INSERT INTO ")
		buf.WriteString(t.GetFormattedName())
		buf.WriteString(" (")
		for i, fld := range fields {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fld.Simple())
		}
		buf.WriteString(") VALUES (")
		for i := range fields {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteByte('$')
			buf.WriteString(strconv.Itoa(i + 1))
		}
		buf.WriteByte(')')
		// Add RETURNING clause if supported
		if t.db.driver.SupportsReturning() {
			buf.WriteString(" RETURNING ")
			buf.WriteString(pkName)
		}
		t.insertOneSql = buf.String()
		t.insertOneFields = fields
	})
	return t.insertOneSql, t.insertOneFields
}

// insertOneFastPath executes the INSERT using cached SQL, bypassing Builder.
func (t *Table[T]) insertOneFastPath(ctx context.Context, sql string, fields []*Field, obj *T) error {
	var args []any
	if gen, ok := any(obj).(GenInsertValues); ok {
		args = gen.InsertValues()
	} else {
		valueOf := reflect.ValueOf(obj).Elem()
		args = make([]any, len(fields))
		for i, fld := range fields {
			args[i] = valueOf.Field(fld.FieldId).Interface()
		}
	}

	conn := t.GetConnection()
	qr := model.Query{RawSql: sql, Arguments: args}

	if t.db.driver.SupportsReturning() {
		row := conn.QueryRowContext(ctx, &qr)
		if row == nil {
			return model.ErrNoRows
		}
		pkFid := t.PrimaryKeys[0].Column.FieldId
		valueOf := reflect.ValueOf(obj).Elem()
		fieldOf := valueOf.Field(pkFid)
		return row.Scan(fieldOf.Addr().Interface())
	}

	err := conn.ExecContext(ctx, &qr)
	if err != nil {
		return err
	}
	// SQLite: get last insert rowid
	if t.PrimaryKeys[0].IsAutoIncr {
		valueOf := reflect.ValueOf(obj).Elem()
		return t.getLastInsertIdFastPath(ctx, conn, valueOf)
	}
	return nil
}

func (t *Table[T]) getLastInsertIdFastPath(ctx context.Context, conn model.Connection, valueOf reflect.Value) error {
	qr := model.Query{RawSql: "SELECT last_insert_rowid()"}
	row := conn.QueryRowContext(ctx, &qr)
	if row == nil {
		return model.ErrNoRows
	}
	pkFid := t.PrimaryKeys[0].Column.FieldId
	fieldOf := valueOf.Field(pkFid)
	return row.Scan(fieldOf.Addr().Interface())
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

// FindByPK selects a single row by primary key using a fast path that bypasses Builder creation.
// This is the most optimized path for simple primary key lookups - no Builder, no StateSelect,
// no object allocation beyond the result struct. Uses cached SQL and cached FetchFunc.
// Only works for tables with a single primary key column.
func (t *Table[T]) FindByPK(id int64) (*T, error) {
	return t.FindByPKContext(context.Background(), id)
}

// FindByPKContext selects a single row by primary key with a specific context.
func (t *Table[T]) FindByPKContext(ctx context.Context, id int64) (*T, error) {
	sql := t.GetSelectByPKSql()
	if sql == "" {
		return nil, model.ErrNoPrimaryKey
	}
	t.fetchByPKOnce.Do(func() {
		t.fetchByPK = t.newFetchByPKFunc()
		t.connByPK = t.db.driver.NewConnection()
		t.cfgByPK = t.db.driver.GetDatabaseConfig()
	})
	args := []any{id}
	qr := model.Query{RawSql: sql, Arguments: args}
	row := t.connByPK.QueryRowContext(ctx, &qr)
	if row == nil {
		return nil, model.ErrNoRows
	}
	target := new(T)
	if err := row.Scan(t.fetchByPK(target)...); err != nil {
		return nil, err
	}
	return target, nil
}

// newFetchByPKFunc creates a FetchFunc for the FindByPK fast path.
// It uses GenScanDest if available, otherwise falls back to reflection.
func (t *Table[T]) newFetchByPKFunc() FetchFunc {
	if _, ok := any(new(T)).(GenScanDest); ok {
		return func(target any) []any {
			return target.(GenScanDest).ScanDest()
		}
	}
	fields := t.GetSortedFields()
	return func(target any) []any {
		valueOf := reflect.ValueOf(target).Elem()
		dest := make([]any, len(fields))
		for i, fld := range fields {
			dest[i] = valueOf.Field(fld.FieldId).Addr().Interface()
		}
		return dest
	}
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
		// Cache the FetchFunc for simple Select (sameModel, no joins)
		t.fetchAllOnce.Do(func() {
			t.fetchAll = t.newFetchByPKFunc() // same logic: reflect over sortedFields
		})
	}
	return state
}

// getColumnTypeName returns the string representation of a column type.
// It handles pointer, slice, array, and custom types.
func getColumnTypeName(t reflect.Type) string {
	return resolveTypeName(t)
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
