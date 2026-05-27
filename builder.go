package goent

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/azhai/goent/model"
)

// TakeNoLimit is a constant indicating no limit on query results
const TakeNoLimit = -1

// Dict is a type alias for a map of string keys to any values
type Dict = map[string]any

// maxBufferSize is the maximum buffer size retained after Reset.
// Buffers larger than this are discarded to prevent memory bloat.
const maxBufferSize = 4096

// BuilderCore contains the shared fields between Builder and DeleteBuilder.
// It is a pure data struct without methods.
type BuilderCore struct {
	Table    *model.Table
	fullName string
	Where    Condition
	Limit    int
	argNo    int
	holders  []string
	buf      *bytes.Buffer
}

func (c *BuilderCore) resetBuf() {
	if c.buf != nil {
		c.buf.Reset()
		if c.buf.Cap() > maxBufferSize {
			c.buf = new(bytes.Buffer)
		}
	} else {
		c.buf = new(bytes.Buffer)
	}
}

func (c *BuilderCore) resetHolders() {
	if cap(c.holders) > 64 {
		c.holders = nil
	} else {
		c.holders = c.holders[:0]
	}
}

// DeleteBuilder builds SQL DELETE statements
type DeleteBuilder struct {
	core BuilderCore
}

func CreateDeleteBuilder() DeleteBuilder {
	return DeleteBuilder{
		core: BuilderCore{Limit: TakeNoLimit, buf: new(bytes.Buffer)},
	}
}

// NewDeleteBuilder creates a new DeleteBuilder pointer
// It initializes the builder with default values
func NewDeleteBuilder() any {
	builder := CreateDeleteBuilder()
	return &builder
}

var deleteBuilderPool = sync.Pool{
	New: NewDeleteBuilder,
}

// GetDeleteBuilder retrieves a DeleteBuilder from the pool.
func GetDeleteBuilder() *DeleteBuilder {
	return deleteBuilderPool.Get().(*DeleteBuilder)
}

// PutDeleteBuilder resets and returns a DeleteBuilder to the pool.
func PutDeleteBuilder(b *DeleteBuilder) {
	b.Reset()
	deleteBuilderPool.Put(b)
}

// CoreWhere returns a copy of the WHERE condition for inspection.
func (b *DeleteBuilder) CoreWhere() Condition { return b.core.Where }

// CoreLimit returns the current LIMIT value.
func (b *DeleteBuilder) CoreLimit() int { return b.core.Limit }

// CoreTable returns the current table reference.
func (b *DeleteBuilder) CoreTable() *model.Table { return b.core.Table }

func (b *DeleteBuilder) Reset() {
	c := &b.core
	c.Table = nil
	c.Where = Condition{}
	c.Limit = TakeNoLimit
	c.argNo = 0
	c.resetHolders()
	c.fullName = ""
	c.resetBuf()
}

// SetTable sets the table for the DeleteBuilder
// It uses cached formatted name from TableInfo to avoid allocation.
func (b *DeleteBuilder) SetTable(table *TableInfo) *DeleteBuilder {
	c := &b.core
	c.Table = table.Table()
	c.fullName = table.GetFormattedName()
	return b
}

// SetTableName sets the full table name directly without a driver.
// This is useful for testing builder pool behavior without a database connection.
func (b *DeleteBuilder) SetTableName(name string) *DeleteBuilder {
	b.core.fullName = name
	return b
}

// BuildHead builds the DELETE statement head
// It writes "DELETE FROM table_name" to the buffer
func (b *DeleteBuilder) buildHead() []any {
	c := &b.core
	c.buf.WriteString("DELETE FROM ")
	if c.fullName != "" {
		c.buf.WriteString(c.fullName)
	}
	return nil
}

// BuildTail builds the DELETE statement tail
// It adds the LIMIT clause if specified
func (b *DeleteBuilder) buildTail() []any {
	c := &b.core
	if c.Limit > 0 {
		c.buf.WriteString(" LIMIT ")
		c.buf.WriteString(strconv.Itoa(c.Limit))
	}
	return nil
}

// BuildWhere builds the WHERE clause for the DELETE statement
// It processes the conditions and returns the query arguments
func (b *DeleteBuilder) buildWhere(full bool) []any {
	c := &b.core
	if c.Where.IsEmpty() {
		return nil
	}
	var args []any
	c.buf.WriteString(" WHERE ")
	c.argNo = b.buildTemplate(c.Where, &args, c.argNo, full)
	return args
}

func (b *DeleteBuilder) buildTemplate(cond Condition, args *[]any, startIdx int, full bool) int {
	c := &b.core
	fi, vi := 0, 0
	template := cond.Template
	name, last := "", len(template)-1
	for idx := 0; idx <= last; idx++ {
		curr := template[idx]
		if idx < last && curr == '%' && template[idx+1] == 's' {
			if fi < len(cond.Fields) {
				if full {
					name = cond.Fields[fi].String()
				} else {
					name = cond.Fields[fi].Simple()
				}
				c.buf.WriteString(name)
				fi++
			}
			idx++
		} else if curr == '?' {
			if vi < len(cond.Values) {
				val := cond.Values[vi]
				startIdx = b.appendValueParam(val, startIdx, args)
				vi++
			}
		} else {
			c.buf.WriteByte(curr)
		}
	}
	return startIdx
}

func (b *DeleteBuilder) appendValueParam(val *Value, startIdx int, args *[]any) int {
	c := &b.core
	if len(val.Args) > 0 {
		c.buf.WriteByte('(')
		for j, arg := range val.Args {
			startIdx++
			if j > 0 {
				c.buf.WriteByte(',')
			}
			c.writeParam(startIdx)
			*args = append(*args, arg)
		}
		c.buf.WriteByte(')')
	} else if val.Length > 0 {
		startIdx++
		c.writeParam(startIdx)
		if val.single != nil {
			*args = append(*args, val.single)
		} else {
			*args = append(*args, val.Args[0])
		}
	}
	return startIdx
}

func (b *DeleteBuilder) Build() (sql string, args []any) {
	b.core.argNo = 0
	_ = b.buildHead()
	args = b.buildWhere(false)
	_ = b.buildTail()
	sql = b.core.buf.String()
	if sql == "" {
		panic(fmt.Sprintf("goent: DeleteBuilder.Build returned empty SQL (fullName=%q, Where=%v)",
			b.core.fullName, !b.core.Where.IsEmpty()))
	}
	return
}

// Builder builds SQL statements for SELECT, INSERT, UPDATE, and DELETE operations
// It handles complex query construction including joins, conditions, and pagination
type Builder struct {
	Type  model.QueryType // The type of query (SELECT, INSERT, UPDATE, DELETE)
	Joins []*JoinTable    // JOIN clauses for the query

	InsertValues [][]any        // Values for batch INSERT operations
	VisitFields  []*Field       // Fields to select or insert
	Changes      map[*Field]any // Changes for UPDATE operations

	Orders []*Order // ORDER BY clauses
	Groups []*Group // GROUP BY clauses
	Offset int      // OFFSET clause value

	Returning string // RETURNING clause for INSERT/UPDATE operations
	RollUp    string // ROLL UP clause for GROUP BY operations
	ForUpdate bool   // FOR UPDATE clause for transactional operations

	cachedSortedChanges []*Field // Cached sorted changes to avoid re-sorting
	visitFieldsShared   bool     // Whether VisitFields is shared (needs clone before append)

	core BuilderCore // Shared core fields (composition, not embedding)
}

// NewBuilder creates a new Builder instance
// It initializes the builder with default values and an empty changes map
func NewBuilder() any {
	return &Builder{
		Changes: make(map[*Field]any),
		core:    BuilderCore{Limit: TakeNoLimit, buf: new(bytes.Buffer)},
	}
}

var builderPool = sync.Pool{
	New: NewBuilder,
}

// GetBuilder retrieves a Builder from the pool.
func GetBuilder() *Builder {
	return builderPool.Get().(*Builder)
}

// PutBuilder resets and returns a Builder to the pool.
func PutBuilder(b *Builder) {
	b.Reset()
	builderPool.Put(b)
}

// CoreWhere returns a copy of the WHERE condition for inspection.
func (b *Builder) CoreWhere() Condition { return b.core.Where }

// CoreLimit returns the current LIMIT value.
func (b *Builder) CoreLimit() int { return b.core.Limit }

// CoreTable returns the current table reference.
func (b *Builder) CoreTable() *model.Table { return b.core.Table }

func (b *Builder) Reset() {
	b.ResetForSave()
	b.Type = 0
	b.core.Table = nil
	b.core.fullName = ""
	b.Joins = nil
	b.core.Where = Condition{}
	b.Orders = nil
	b.Groups = nil
	b.Offset = 0
	b.RollUp = ""
	b.visitFieldsShared = false
	b.core.resetBuf()
}

// ResetForSave resets the Builder for INSERT/UPDATE operations
// It clears changes, insert values, visit fields, and other operation-specific settings
func (b *Builder) ResetForSave() {
	clear(b.Changes)
	b.InsertValues = nil
	b.VisitFields = nil
	b.core.Limit = -1
	b.Returning = ""
	b.ForUpdate = false
	b.core.argNo = 0
	b.core.resetHolders()
	b.cachedSortedChanges = nil
}

// IsJoinQuery checks if the query is a join query
func (b *Builder) IsJoinQuery() bool {
	return b.Type == model.SelectJoinQuery || b.Type == model.UpdateJoinQuery || len(b.Joins) > 0
}

// IsInsertQuery checks if the query is an insert query
func (b *Builder) IsInsertQuery() bool {
	return b.Type == model.InsertQuery || b.Type == model.InsertAllQuery
}

// sortedChanges returns the changes sorted by FieldId.
// The result is cached after the first call and cleared by ResetForSave.
func (b *Builder) sortedChanges() []*Field {
	if b.cachedSortedChanges != nil {
		return b.cachedSortedChanges
	}
	fields := make([]*Field, 0, len(b.Changes))
	for f := range b.Changes {
		fields = append(fields, f)
	}
	slices.SortFunc(fields, func(a, b *Field) int {
		if a.FieldId != b.FieldId {
			return a.FieldId - b.FieldId
		}
		return strings.Compare(a.ColumnName, b.ColumnName)
	})
	b.cachedSortedChanges = fields
	return fields
}

// SetTable sets the table for the query builder
// It uses cached formatted name from TableInfo to avoid allocation.
func (b *Builder) SetTable(table *TableInfo) *Builder {
	c := &b.core
	c.Table = table.Table()
	c.fullName = table.GetFormattedName()
	return b
}

// SetTableName sets the full table name directly without a driver.
// This is useful for testing builder pool behavior without a database connection.
func (b *Builder) SetTableName(name string) *Builder {
	b.core.fullName = name
	return b
}

// BuildHead builds the SQL statement head for SELECT, INSERT, UPDATE, or DELETE operations
// It handles different query types and returns the initial query arguments
func (b *Builder) buildHead() []any {
	c := &b.core
	var args []any
	switch b.Type {
	default:
		c.buf.WriteString("SELECT ")
		if len(b.VisitFields) == 0 {
			c.buf.WriteString("*")
		} else if b.Type == model.SelectJoinQuery {
			c.buf.WriteString(b.VisitFields[0].String())
			for _, f := range b.VisitFields[1:] {
				c.buf.WriteByte(',')
				c.buf.WriteString(f.String())
			}
		} else {
			c.buf.WriteString(b.VisitFields[0].Simple())
			for _, f := range b.VisitFields[1:] {
				c.buf.WriteByte(',')
				c.buf.WriteString(f.Simple())
			}
		}
		c.buf.WriteString(" FROM ")
		if c.fullName != "" {
			c.buf.WriteString(c.fullName)
		} else if c.Table != nil && c.Table.Name != "" {
			c.buf.WriteString(c.Table.Name)
		} else {
			panic(fmt.Sprintf("goent: buildHead called with empty table name for query (Type=%d, fullName=%q, Table=%v) - missing SetTable() call",
				b.Type, c.fullName, c.Table))
		}
	case model.DeleteQuery:
		c.buf.WriteString("DELETE FROM ")
		if c.fullName != "" {
			c.buf.WriteString(c.fullName)
		}
	case model.InsertQuery:
		c.buf.WriteString("INSERT INTO ")
		if c.fullName != "" {
			c.buf.WriteString(c.fullName)
		} else if c.Table != nil && c.Table.Name != "" {
			c.buf.WriteString(c.Table.Name)
		} else {
			panic("goent: buildHead called with empty table name for INSERT query - missing SetTable() call")
		}
		c.buf.WriteByte('(')
		for i, f := range b.sortedChanges() {
			v := b.Changes[f]
			c.argNo += 1
			if i > 0 {
				c.holders = append(c.holders, "$"+strconv.Itoa(c.argNo))
				c.buf.WriteString(", ")
			} else {
				c.holders = append(c.holders, "$"+strconv.Itoa(c.argNo))
			}
			args = append(args, v)
			c.buf.WriteString(f.Simple())
		}
	case model.InsertAllQuery:
		c.buf.WriteString("INSERT INTO ")
		if c.fullName != "" {
			c.buf.WriteString(c.fullName)
		} else if c.Table != nil && c.Table.Name != "" {
			c.buf.WriteString(c.Table.Name)
		}
		c.buf.WriteByte('(')
		columns := make([]string, len(b.VisitFields))
		for i, f := range b.VisitFields {
			columns[i] = f.ColumnName
		}
		c.buf.WriteString(strings.Join(columns, ", "))
	case model.UpdateQuery, model.UpdateJoinQuery:
		c.buf.WriteString("UPDATE ")
		if c.fullName != "" {
			c.buf.WriteString(c.fullName)
		}
	}
	return args
}

// BuildDoing builds the SET clause for UPDATE or VALUES clause for INSERT operations
// It processes the changes or values and returns the query arguments
func (b *Builder) buildDoing() []any {
	c := &b.core
	var args []any
	switch b.Type {
	default:
		return args
	case model.InsertQuery:
		c.buf.WriteString(") VALUES (")
		for i, holder := range c.holders {
			if i > 0 {
				c.buf.WriteByte(',')
			}
			c.buf.WriteString(holder)
		}
		c.buf.WriteByte(')')
	case model.InsertAllQuery:
		if len(b.InsertValues) == 0 {
			return args
		}
		size, last := len(b.VisitFields), len(b.InsertValues)-1
		c.buf.WriteString(") VALUES (")
		for i, row := range b.InsertValues {
			c.holders = make([]string, size)
			for j := range size {
				c.argNo += 1
				c.holders[j] = "$" + strconv.Itoa(c.argNo)
			}
			args = append(args, row...)
			for j, holder := range c.holders {
				if j > 0 {
					c.buf.WriteByte(',')
				}
				c.buf.WriteString(holder)
			}
			if i != last {
				c.buf.WriteString("), (")
			}
		}
		c.buf.WriteByte(')')
	case model.UpdateQuery:
		if len(b.Changes) == 0 {
			return args
		}
		c.buf.WriteString(" SET ")
		for i, f := range b.sortedChanges() {
			v := b.Changes[f]
			c.argNo += 1
			if i > 0 {
				c.buf.WriteString(", ")
			}
			c.buf.WriteString(f.Simple())
			c.buf.WriteByte('=')
			c.writeParam(c.argNo)
			args = append(args, v)
		}
	case model.UpdateJoinQuery:
		if len(b.Changes) == 0 {
			return args
		}
		c.buf.WriteString(" SET ")
		for i, f := range b.sortedChanges() {
			v := b.Changes[f]
			if i > 0 {
				c.buf.WriteString(", ")
			}
			if fld, ok := v.(*Field); ok {
				c.buf.WriteString(f.Simple())
				c.buf.WriteByte('=')
				c.buf.WriteString(fld.String())
			} else {
				c.argNo += 1
				c.buf.WriteString(f.Simple())
				c.buf.WriteByte('=')
				c.writeParam(c.argNo)
				args = append(args, v)
			}
		}
	}

	return args
}

// BuildTail builds the tail part of the query (GROUP BY, ORDER BY, LIMIT, OFFSET, RETURNING)
// It handles the trailing clauses for different query types
func (b *Builder) buildTail() []any {
	c := &b.core
	var args []any

	if b.Type == model.DeleteQuery {
		if c.Limit > 0 {
			c.buf.WriteString(" LIMIT ")
			c.buf.WriteString(strconv.Itoa(c.Limit))
		}
	}

	if b.Type == model.SelectQuery || b.Type == model.SelectJoinQuery {
		if len(b.Groups) != 0 {
			c.buf.WriteByte(' ')
			gp := b.Groups[0]
			c.buf.WriteString("GROUP BY ")
			c.buf.WriteString(gp.String())
			for _, gp = range b.Groups[1:] {
				c.buf.WriteByte(',')
				c.buf.WriteString(gp.String())
			}
		}

		if len(b.Orders) != 0 {
			c.buf.WriteString(" ORDER BY ")
			for i, ob := range b.Orders {
				if i > 0 {
					c.buf.WriteString(", ")
				}
				c.buf.WriteString(ob.String())
				if ob.Desc {
					c.buf.WriteString(" DESC")
				}
			}
		}

		if c.Limit > 0 {
			c.buf.WriteString(" LIMIT ")
			c.buf.WriteString(strconv.Itoa(c.Limit))
		}
		if b.Offset > 0 {
			c.buf.WriteString(" OFFSET ")
			c.buf.WriteString(strconv.Itoa(b.Offset))
		}
	}

	if b.Returning != "" && b.IsInsertQuery() {
		c.buf.WriteString(" RETURNING ")
		c.buf.WriteString(b.Returning)
	}

	return args
}

// BuildJoins builds the JOIN clauses for the query
// It processes all join tables and returns the query arguments
func (b *Builder) buildJoins() []any {
	c := &b.core
	if len(b.Joins) == 0 {
		return nil
	}
	if b.Type == model.UpdateJoinQuery {
		c.buf.WriteString(" FROM ")
		c.buf.WriteString(b.Joins[0].Table.String())
		return nil
	}

	var args []any
	for _, j := range b.Joins {
		c.buf.WriteString(" ")
		c.buf.WriteString(string(j.JoinType) + " ")
		if j.fullName != "" {
			c.buf.WriteString(j.fullName)
		} else if j.Table != nil {
			c.buf.WriteString(j.Table.String())
		}
		c.buf.WriteString(" ON ")
		if j.On.Template != "" {
			c.argNo = b.buildTemplate(j.On, &args, c.argNo, true)
		}
	}
	return args
}

// buildTemplate delegates to the core's template building logic
func (b *Builder) buildTemplate(cond Condition, args *[]any, startIdx int, full bool) int {
	c := &b.core
	fi, vi := 0, 0
	template := cond.Template
	name, last := "", len(template)-1
	for idx := 0; idx <= last; idx++ {
		curr := template[idx]
		if idx < last && curr == '%' && template[idx+1] == 's' {
			if fi < len(cond.Fields) {
				if full {
					name = cond.Fields[fi].String()
				} else {
					name = cond.Fields[fi].Simple()
				}
				c.buf.WriteString(name)
				fi++
			}
			idx++
		} else if curr == '?' {
			if vi < len(cond.Values) {
				val := cond.Values[vi]
				startIdx = b.appendValueParam(val, startIdx, args)
				vi++
			}
		} else {
			c.buf.WriteByte(curr)
		}
	}
	return startIdx
}

// writeParam writes a parameter placeholder ($N) directly to the buffer.
// It avoids string concatenation by writing directly to the buffer.
func (c *BuilderCore) writeParam(n int) {
	c.buf.WriteByte('$')
	c.buf.WriteString(strconv.Itoa(n))
}

// appendValueParam writes value parameters to the buffer for Builder.
func (b *Builder) appendValueParam(val *Value, startIdx int, args *[]any) int {
	c := &b.core
	if len(val.Args) > 0 {
		c.buf.WriteByte('(')
		for j, arg := range val.Args {
			startIdx++
			if j > 0 {
				c.buf.WriteByte(',')
			}
			c.writeParam(startIdx)
			*args = append(*args, arg)
		}
		c.buf.WriteByte(')')
	} else if val.Length > 0 {
		startIdx++
		c.writeParam(startIdx)
		if val.single != nil {
			*args = append(*args, val.single)
		} else {
			*args = append(*args, val.Args[0])
		}
	}
	return startIdx
}

// BuildWhere builds the WHERE clause for the Builder
func (b *Builder) buildWhere(full bool) []any {
	c := &b.core
	if c.Where.IsEmpty() {
		return nil
	}
	var args []any
	c.buf.WriteString(" WHERE ")
	c.argNo = b.buildTemplate(c.Where, &args, c.argNo, full)
	return args
}

func (b *Builder) Build(destroy bool) (sql string, args []any) {
	c := &b.core
	c.argNo = 0
	args = b.buildHead()
	if doArgs := b.buildDoing(); len(doArgs) > 0 {
		args = append(args, doArgs...)
	}
	if joinArgs := b.buildJoins(); len(joinArgs) > 0 {
		args = append(args, joinArgs...)
	}
	if whereArgs := b.buildWhere(b.IsJoinQuery()); len(whereArgs) > 0 {
		args = append(args, whereArgs...)
	}
	if tailArgs := b.buildTail(); len(tailArgs) > 0 {
		args = append(args, tailArgs...)
	}
	sql = c.buf.String()
	if sql == "" {
		panic(fmt.Sprintf("goent: Build returned empty SQL (Type=%d, fullName=%q, Changes=%d, Where=%v)",
			b.Type, c.fullName, len(b.Changes), !c.Where.IsEmpty()))
	}
	return
}

// CollectFields collects primary key and non-primary key fields from a struct value
// It sets the builder's returning information for auto-increment primary keys
// and returns a map of primary key column names to their values
func CollectFields[T any](builder *Builder, table *Table[T], valueOf reflect.Value, ignores []string) (Dict, int) {
	pkFid, pkName, _ := table.TableInfo.GetPrimaryInfo()
	var primary Dict
	for _, col := range table.Columns {
		if len(ignores) > 0 && slices.Contains(ignores, col.FieldName) {
			continue
		}
		fieldOf := valueOf.Field(col.FieldId)
		if col.IsPK {
			if primary == nil {
				primary = make(Dict, len(table.PrimaryKeys))
			}
			if !fieldOf.IsZero() {
				primary[col.ColumnName] = fieldOf.Interface()
			} else if col.HasDefault && col.DefaultValue != "" {
				setDefaultValue(fieldOf, col.DefaultValue)
				primary[col.ColumnName] = fieldOf.Interface()
			}
			continue
		}
		if fieldOf.Kind() == reflect.Pointer && fieldOf.IsNil() {
			continue
		}
		if col.HasDefault && fieldOf.IsZero() {
			continue
		}
		builder.Changes[table.sortedFields[col.FieldId]] = fieldOf.Interface()
	}
	if pkName != "" && len(primary) == 0 {
		for _, pk := range table.PrimaryKeys {
			if pk.Column.HasDefault || pk.IsAutoIncr {
				builder.Returning = pkName
				break
			}
		}
	}
	return primary, pkFid
}

func setDefaultValue(fieldOf reflect.Value, defaultValue string) {
	if fieldOf.Kind() == reflect.String {
		defaultValue = strings.Trim(defaultValue, "'")
		fieldOf.SetString(defaultValue)
	}
}
