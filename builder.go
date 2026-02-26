package goent

import (
	"bytes"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/azhai/goent/model"
)

const TakeNoLimit = -1

type Dict = map[string]any

// JoinTable represents a JOIN clause with the join type, target table, and ON condition.
type JoinTable struct {
	JoinType model.JoinType
	Table    *model.Table
	fullName string
	On       Condition
}

// Pair represents a key-value pair.
type Pair struct {
	Key   string
	Value any
}

// Order represents an ORDER BY clause with a field and descending flag.
type Order struct {
	Desc bool
	*Field
}

// Group represents a GROUP BY clause with a field and optional HAVING condition.
type Group struct {
	Having Condition
	*Field
}

var bufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}

type DeleteBuilder struct {
	Table *model.Table
	Where Condition
	Limit int

	argNo    int
	holders  []string
	fullName string

	buf *bytes.Buffer
}

func CreateDeleteBuilder() DeleteBuilder {
	return DeleteBuilder{
		Limit: TakeNoLimit,
		buf:   bufPool.Get().(*bytes.Buffer),
	}
}

func NewDeleteBuilder() *DeleteBuilder {
	builder := CreateDeleteBuilder()
	return &builder
}

func (b *DeleteBuilder) SetTable(table TableInfo, driver model.Driver) *DeleteBuilder {
	b.Table = table.Table()
	var schema string
	if b.Table.Schema != nil {
		schema = *b.Table.Schema
	}
	b.fullName = driver.FormatTableName(schema, b.Table.Name)
	return b
}

func (b *DeleteBuilder) BuildHead() []any {
	b.buf.WriteString("DELETE FROM ")
	if b.fullName != "" {
		b.buf.WriteString(b.fullName)
	}
	return nil
}

func (b *DeleteBuilder) BuildTail() []any {
	if b.Limit > 0 {
		b.buf.WriteString(" LIMIT " + strconv.Itoa(b.Limit))
	}
	return nil
}

func (b *DeleteBuilder) BuildWhere(full bool) []any {
	if b.Where.IsEmpty() {
		return nil
	}
	var args []any
	b.buf.WriteString(" WHERE ")
	b.argNo = b.buildTemplate(b.Where, &args, b.argNo, full)
	return args
}

func (b *DeleteBuilder) buildTemplate(cond Condition, args *[]any, startIdx int, full bool) int {
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
				b.buf.WriteString(name)
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
			b.buf.WriteByte(curr)
		}
	}
	return startIdx
}

func (b *DeleteBuilder) appendValueParam(val *Value, startIdx int, args *[]any) int {
	if val.Type == reflect.Slice && len(val.Args) > 0 {
		b.buf.WriteString("(")
		for j, arg := range val.Args {
			startIdx++
			if j > 0 {
				b.buf.WriteString(", $" + strconv.Itoa(startIdx))
			} else {
				b.buf.WriteString("$" + strconv.Itoa(startIdx))
			}
			*args = append(*args, arg)
		}
		b.buf.WriteByte(')')
	} else if len(val.Args) > 0 {
		startIdx++
		b.buf.WriteString("$" + strconv.Itoa(startIdx))
		*args = append(*args, val.Args[0])
	}
	return startIdx
}

func (b *DeleteBuilder) Build() (sql string, args []any) {
	_ = b.BuildHead()
	args = b.BuildWhere(false)
	_ = b.BuildTail()
	sql = b.buf.String()
	b.buf.Reset()
	bufPool.Put(b.buf)
	return
}

type Builder struct {
	Type  model.QueryType
	Joins []*JoinTable

	InsertValues [][]any
	VisitFields  []*Field
	Changes      map[*Field]any

	Orders []*Order
	Groups []*Group
	Offset int

	Returning string
	RollUp    string
	ForUpdate bool

	DeleteBuilder
}

func NewBuilder() *Builder {
	return &Builder{
		Changes:       make(map[*Field]any),
		DeleteBuilder: CreateDeleteBuilder(),
	}
}

// var builderPool = sync.Pool{
// 	New: func() any {
// 		buf := bufPool.Get().(*bytes.Buffer)
// 		buf.Reset()
// 		return &Builder{
// 			Changes: make(map[*Field]any),
// 			buf:     buf,
// 		}
// 	},
// }

// // GetBuilder retrieves a Builder from the pool.
// func GetBuilder() *Builder {
// 	return builderPool.Get().(*Builder)
// }

// // PutBuilder resets and returns a Builder to the pool.
// func PutBuilder(b *Builder) {
// 	bufPool.Put(b.buf)
// 	b.Reset()
// 	builderPool.Put(b)
// }

// // Reset resets the Builder to its initial state.
// func (b *Builder) Reset() {
// 	b.ResetForSave()
// 	b.Type = 0
// 	b.Table = nil
// 	b.fullName = ""
// 	b.Joins = make([]*JoinTable, 0)
// 	b.Where = Condition{}
// 	b.Orders = make([]*Order, 0)
// 	b.Groups = make([]*Group, 0)
// 	b.Offset = 0
// 	b.RollUp = ""
// }

// ResetForSave resets the Builder for INSERT/UPDATE operations.
func (b *Builder) ResetForSave() {
	b.Changes = make(map[*Field]any)
	b.InsertValues = make([][]any, 0)
	b.VisitFields = make([]*Field, 0)
	b.Limit = -1
	b.Returning = ""
	b.ForUpdate = false
	b.argNo = 0
	b.holders = make([]string, 0)
}

func (b *Builder) IsJoinQuery() bool {
	return b.Type == model.SelectJoinQuery || b.Type == model.UpdateJoinQuery
}

func (b *Builder) IsInsertQuery() bool {
	return b.Type == model.InsertQuery || b.Type == model.InsertAllQuery
}

// SetTable sets the table for the query builder.
func (b *Builder) SetTable(table TableInfo, driver model.Driver) *Builder {
	b.DeleteBuilder.SetTable(table, driver)
	return b
}

// BuildHead builds the SELECT, INSERT, UPDATE, or DELETE statement head (e.g., "SELECT * FROM table").
func (b *Builder) BuildHead() []any {
	var args []any
	switch b.Type {
	default:
		b.buf.WriteString("SELECT ")
		if len(b.VisitFields) == 0 {
			b.buf.WriteString("*")
		} else if b.Type == model.SelectJoinQuery {
			b.buf.WriteString(b.VisitFields[0].String())
			for _, f := range b.VisitFields[1:] {
				b.buf.WriteByte(',')
				b.buf.WriteString(f.String())
			}
		} else {
			b.buf.WriteString(b.VisitFields[0].Simple())
			for _, f := range b.VisitFields[1:] {
				b.buf.WriteByte(',')
				b.buf.WriteString(f.Simple())
			}
		}
		b.buf.WriteString(" FROM ")
		if b.fullName != "" {
			b.buf.WriteString(b.fullName)
		}
	case model.DeleteQuery:
		_ = b.DeleteBuilder.BuildHead()
	case model.InsertQuery:
		b.buf.WriteString("INSERT INTO ")
		if b.fullName != "" {
			b.buf.WriteString(b.fullName)
		}
		b.buf.WriteByte('(')
		var columns []string
		for f, v := range b.Changes {
			b.argNo += 1
			b.holders = append(b.holders, "$"+strconv.Itoa(b.argNo))
			args = append(args, v)
			columns = append(columns, f.Simple())
		}
		b.buf.WriteString(strings.Join(columns, ", "))
	case model.InsertAllQuery:
		b.buf.WriteString("INSERT INTO ")
		if b.fullName != "" {
			b.buf.WriteString(b.fullName)
		}
		b.buf.WriteByte('(')
		columns := make([]string, len(b.VisitFields))
		for i, f := range b.VisitFields {
			columns[i] = f.ColumnName
		}
		b.buf.WriteString(strings.Join(columns, ", "))
	case model.UpdateQuery, model.UpdateJoinQuery:
		b.buf.WriteString("UPDATE ")
		if b.fullName != "" {
			b.buf.WriteString(b.fullName)
		}
	}
	return args
}

// BuildDoing builds the SET clause for UPDATE or VALUES clause for INSERT.
func (b *Builder) BuildDoing() []any {
	var args []any
	switch b.Type {
	default:
		return args
	case model.InsertQuery:
		b.buf.WriteString(") VALUES (")
		b.buf.WriteString(strings.Join(b.holders, ", "))
		b.buf.WriteByte(')')
	case model.InsertAllQuery:
		size, last := len(b.VisitFields), len(b.InsertValues)-1
		b.buf.WriteString(") VALUES (")
		for i, row := range b.InsertValues {
			b.holders = make([]string, size)
			for j := range size {
				b.argNo += 1
				b.holders[j] = "$" + strconv.Itoa(b.argNo)
			}
			args = append(args, row...)
			b.buf.WriteString(strings.Join(b.holders, ", "))
			if i != last {
				b.buf.WriteString("), (")
			}
		}
		b.buf.WriteByte(')')
	case model.UpdateQuery:
		b.buf.WriteString(" SET ")
		for f, v := range b.Changes {
			b.argNo += 1
			if b.argNo > 1 {
				b.buf.WriteString(", ")
			}
			b.buf.WriteString(f.Simple() + "=$" + strconv.Itoa(b.argNo))
			args = append(args, v)
		}
	case model.UpdateJoinQuery:
		b.buf.WriteString(" SET ")
		isFirst := true
		for f, v := range b.Changes {
			if !isFirst {
				b.buf.WriteString(", ")
			}
			if fld, ok := v.(*Field); ok {
				b.buf.WriteString(f.Simple() + "=" + fld.String())
			} else {
				b.argNo += 1
				b.buf.WriteString(f.Simple() + "=$" + strconv.Itoa(b.argNo))
				args = append(args, v)
			}
			isFirst = false
		}
	}

	return args
}

// BuildTail builds the tail part of the query (GROUP BY, ORDER BY, LIMIT, OFFSET, RETURNING).
func (b *Builder) BuildTail() []any {
	var args []any

	if b.Type == model.DeleteQuery {
		b.DeleteBuilder.BuildTail()
	}

	if b.Type == model.SelectQuery {
		if len(b.Groups) != 0 {
			b.buf.WriteString(" ")
			gp := b.Groups[0]
			b.buf.WriteString("GROUP BY " + gp.String())
			for _, gp = range b.Groups[1:] {
				b.buf.WriteString("," + gp.String())
			}
		}

		if len(b.Orders) != 0 {
			b.buf.WriteString(" ")
			b.buf.WriteString("ORDER BY ")
			for i, ob := range b.Orders {
				if i > 0 {
					b.buf.WriteString(", ")
				}
				b.buf.WriteString(ob.String())
				if ob.Desc {
					b.buf.WriteString(" DESC")
				}
			}
		}

		if b.Limit > 0 {
			b.buf.WriteString(" LIMIT " + strconv.Itoa(b.Limit))
		}
		if b.Offset > 0 {
			b.buf.WriteString(" OFFSET " + strconv.Itoa(b.Offset))
		}
	}

	if b.Returning != "" && b.IsInsertQuery() {
		b.buf.WriteString(" RETURNING ")
		b.buf.WriteString(b.Returning)
	}

	return args
}

// BuildJoins builds the JOIN clauses for the query.
func (b *Builder) BuildJoins() []any {
	if len(b.Joins) == 0 {
		return nil
	}
	if b.Type == model.UpdateJoinQuery {
		b.buf.WriteString(" FROM ")
		b.buf.WriteString(b.Joins[0].Table.String())
		return nil
	}

	var args []any
	for _, j := range b.Joins {
		b.buf.WriteString(" ")
		b.buf.WriteString(string(j.JoinType) + " ")
		if j.fullName != "" {
			b.buf.WriteString(j.fullName)
		} else if j.Table != nil {
			b.buf.WriteString(j.Table.String())
		}
		b.buf.WriteString(" ON ")
		if j.On.Template != "" {
			b.buildTemplate(j.On, &args, len(args), true)
		}
	}
	return args
}

// Build assembles the complete SQL query and returns it along with query arguments.
func (b *Builder) Build(destroy bool) (sql string, args []any) {
	args = b.BuildHead()
	if doArgs := b.BuildDoing(); len(doArgs) > 0 {
		args = append(args, doArgs...)
	}
	if joinArgs := b.BuildJoins(); len(joinArgs) > 0 {
		args = append(args, joinArgs...)
	}
	if whereArgs := b.BuildWhere(b.IsJoinQuery()); len(whereArgs) > 0 {
		args = append(args, whereArgs...)
	}
	if tailArgs := b.BuildTail(); len(tailArgs) > 0 {
		args = append(args, tailArgs...)
	}
	sql = b.buf.String()
	b.buf.Reset()
	bufPool.Put(b.buf)
	// if destroy {
	// 	PutBuilder(b)
	// }
	return
}

// CollectFields collects primary key and non-primary key fields from a struct value.
// It sets the builder's returning information for auto-increment primary keys
// and returns a map of primary key column names to their values.
func CollectFields[T any](builder *Builder, table *Table[T], valueOf reflect.Value, ignores []string) (Dict, int) {
	pkFid, pkName, pkeys := table.TableInfo.GetPrimaryInfo()
	primary := make(Dict)
	for _, col := range table.Columns {
		if len(ignores) > 0 && slices.Contains(ignores, col.FieldName) {
			continue
		}
		name := col.ColumnName
		fieldOf := valueOf.FieldByName(col.FieldName)
		if slices.Contains(pkeys, name) {
			if !fieldOf.IsZero() {
				primary[name] = fieldOf.Interface()
			} else if col.HasDefault && col.DefaultValue != "" {
				setDefaultValue(fieldOf, col.DefaultValue)
				primary[name] = fieldOf.Interface()
			}
			continue
		}
		if fieldOf.Kind() == reflect.Pointer && fieldOf.IsNil() {
			continue
		}
		if col.HasDefault && fieldOf.IsZero() {
			continue
		}
		fld := table.Field(name)
		builder.Changes[fld] = fieldOf.Interface()
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
