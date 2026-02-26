package goent

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/azhai/goent/model"
)

var builderPool = sync.Pool{
	New: func() any {
		return &Builder{
			Changes: make(map[*Field]any),
		}
	},
}

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

// Builder constructs SQL queries with support for SELECT, INSERT, UPDATE, and DELETE operations.
type Builder struct {
	Type  model.QueryType
	Table *model.Table
	Joins []*JoinTable

	InsertValues [][]any
	VisitFields  []*Field
	Changes      map[*Field]any
	Where        Condition

	Orders []*Order
	Groups []*Group
	Limit  int
	Offset int

	Returning string
	RollUp    string
	ForUpdate bool

	argNo    int
	holders  []string
	fullName string

	strings.Builder
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

// Reset resets the Builder to its initial state.
func (b *Builder) Reset() {
	b.Builder.Reset()
	b.ResetForSave()
	b.Type = 0
	b.Table = nil
	b.fullName = ""
	b.Joins = make([]*JoinTable, 0)
	b.Where = Condition{}
	b.Orders = make([]*Order, 0)
	b.Groups = make([]*Group, 0)
	b.Limit = 0
	b.Offset = 0
	b.RollUp = ""
}

// ResetForSave resets the Builder for INSERT/UPDATE operations.
func (b *Builder) ResetForSave() {
	b.Changes = make(map[*Field]any)
	b.InsertValues = make([][]any, 0)
	b.VisitFields = make([]*Field, 0)
	b.argNo = 0
	b.holders = make([]string, 0)
	b.Returning = ""
	b.ForUpdate = false
}

// SetTable sets the table for the query builder.
func (b *Builder) SetTable(table TableInfo, driver model.Driver) *Builder {
	b.Table = table.Table()
	var schema string
	if b.Table.Schema != nil {
		schema = *b.Table.Schema
	}
	b.fullName = driver.FormatTableName(schema, b.Table.Name)
	return b
}

// BuildHead builds the SELECT, INSERT, UPDATE, or DELETE statement head (e.g., "SELECT * FROM table").
func (b *Builder) BuildHead() []any {
	var args []any
	switch b.Type {
	default:
		b.WriteString("SELECT ")
		if len(b.VisitFields) == 0 {
			b.WriteString("*")
		} else if b.Type == model.SelectJoinQuery {
			b.WriteString(b.VisitFields[0].String())
			for _, f := range b.VisitFields[1:] {
				b.WriteByte(',')
				b.WriteString(f.String())
			}
		} else {
			b.WriteString(b.VisitFields[0].Simple())
			for _, f := range b.VisitFields[1:] {
				b.WriteByte(',')
				b.WriteString(f.Simple())
			}
		}
		b.WriteString(" FROM ")
		if b.fullName != "" {
			b.WriteString(b.fullName)
		}
	case model.InsertQuery:
		b.WriteString("INSERT INTO ")
		if b.fullName != "" {
			b.WriteString(b.fullName)
		}
		b.WriteByte('(')
		var columns []string
		for f, v := range b.Changes {
			b.argNo += 1
			b.holders = append(b.holders, "$"+strconv.Itoa(b.argNo))
			args = append(args, v)
			columns = append(columns, f.Simple())
		}
		b.WriteString(strings.Join(columns, ", "))
	case model.InsertAllQuery:
		b.WriteString("INSERT INTO ")
		if b.fullName != "" {
			b.WriteString(b.fullName)
		}
		b.WriteByte('(')
		columns := make([]string, len(b.VisitFields))
		for i, f := range b.VisitFields {
			columns[i] = f.ColumnName
		}
		b.WriteString(strings.Join(columns, ", "))
	case model.UpdateQuery, model.UpdateJoinQuery:
		b.WriteString("UPDATE ")
		if b.fullName != "" {
			b.WriteString(b.fullName)
		}
	case model.DeleteQuery:
		b.WriteString("DELETE FROM ")
		if b.fullName != "" {
			b.WriteString(b.fullName)
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
		b.WriteString(") VALUES (")
		b.WriteString(strings.Join(b.holders, ", "))
		b.WriteByte(')')
	case model.InsertAllQuery:
		size, last := len(b.VisitFields), len(b.InsertValues)-1
		b.WriteString(") VALUES (")
		for i, row := range b.InsertValues {
			b.holders = make([]string, size)
			for j := range size {
				b.argNo += 1
				b.holders[j] = "$" + strconv.Itoa(b.argNo)
			}
			args = append(args, row...)
			b.WriteString(strings.Join(b.holders, ", "))
			if i != last {
				b.WriteString("), (")
			}
		}
		b.WriteByte(')')
	case model.UpdateQuery:
		b.WriteString(" SET ")
		for f, v := range b.Changes {
			b.argNo += 1
			if b.argNo > 1 {
				b.WriteString(", ")
			}
			b.WriteString(f.Simple() + "=$" + strconv.Itoa(b.argNo))
			args = append(args, v)
		}
	case model.UpdateJoinQuery:
		b.WriteString(" SET ")
		isFirst := true
		for f, v := range b.Changes {
			if !isFirst {
				b.WriteString(", ")
			}
			if fld, ok := v.(*Field); ok {
				b.WriteString(f.Simple() + "=" + fld.String())
			} else {
				b.argNo += 1
				b.WriteString(f.Simple() + "=$" + strconv.Itoa(b.argNo))
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

	if b.Type == model.SelectQuery {
		if len(b.Groups) != 0 {
			b.WriteString(" ")
			gp := b.Groups[0]
			b.WriteString("GROUP BY " + gp.String())
			for _, gp = range b.Groups[1:] {
				b.WriteString("," + gp.String())
			}
		}

		if len(b.Orders) != 0 {
			b.WriteString(" ")
			b.WriteString("ORDER BY ")
			for i, ob := range b.Orders {
				if i > 0 {
					b.WriteString("")
				}
				b.WriteString(ob.String())
				if ob.Desc {
					b.WriteString(" DESC")
				}
			}
		}

		if b.Limit != 0 {
			b.WriteString(" LIMIT " + strconv.Itoa(b.Limit))
		}
		if b.Offset != 0 {
			b.WriteString(" OFFSET " + strconv.Itoa(b.Offset))
		}
	}

	if b.Returning != "" && (b.Type == model.InsertQuery || b.Type == model.InsertAllQuery) {
		b.WriteString(" RETURNING ")
		b.WriteString(b.Returning)
	}

	return args
}

// BuildWhere builds the WHERE clause for the query.
func (b *Builder) BuildWhere() []any {
	if b.Where.IsEmpty() {
		return nil
	}

	var args []any
	b.WriteString(" WHERE ")
	b.argNo = b.buildTemplate(b.Where, b.argNo, func(fld *Field) string {
		if b.Type == model.SelectJoinQuery || b.Type == model.UpdateJoinQuery {
			return fld.String()
		}
		return fld.Simple()
	}, &args)
	return args
}

func (b *Builder) buildTemplate(cond Condition, startIdx int, fieldFmt func(*Field) string, args *[]any) int {
	fi, vi := 0, 0
	template := cond.Template
	last := len(template) - 1
	for idx := 0; idx <= last; idx++ {
		curr := template[idx]
		if idx < last && curr == '%' && template[idx+1] == 's' {
			if fi < len(cond.Fields) {
				b.WriteString(fieldFmt(cond.Fields[fi]))
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
			b.WriteByte(curr)
		}
	}
	return startIdx
}

func (b *Builder) appendValueParam(val *Value, startIdx int, args *[]any) int {
	if val.Type == reflect.Slice && len(val.Args) > 0 {
		b.WriteString("(")
		for j, arg := range val.Args {
			startIdx++
			if j > 0 {
				b.WriteString(", $" + strconv.Itoa(startIdx))
			} else {
				b.WriteString("$" + strconv.Itoa(startIdx))
			}
			*args = append(*args, arg)
		}
		b.WriteByte(')')
	} else if len(val.Args) > 0 {
		startIdx++
		b.WriteString("$" + strconv.Itoa(startIdx))
		*args = append(*args, val.Args[0])
	}
	return startIdx
}

// BuildJoins builds the JOIN clauses for the query.
func (b *Builder) BuildJoins() []any {
	if len(b.Joins) == 0 {
		return nil
	}
	if b.Type == model.UpdateJoinQuery {
		b.WriteString(" FROM ")
		b.WriteString(b.Joins[0].Table.String())
		return nil
	}

	var args []any
	for _, j := range b.Joins {
		b.WriteString(" ")
		b.WriteString(string(j.JoinType) + " ")
		if j.fullName != "" {
			b.WriteString(j.fullName)
		} else if j.Table != nil {
			b.WriteString(j.Table.String())
		}
		b.WriteString(" ON ")
		if j.On.Template != "" {
			b.buildTemplate(j.On, len(args), func(fld *Field) string {
				return fld.String()
			}, &args)
		}
	}
	return args
}

// Build assembles the complete SQL query and returns it along with query arguments.
func (b *Builder) Build(destroy bool) (sql string, args []any) {
	args = append(args, b.BuildHead()...)
	args = append(args, b.BuildDoing()...)
	args = append(args, b.BuildJoins()...)
	args = append(args, b.BuildWhere()...)
	args = append(args, b.BuildTail()...)
	sql = b.String()
	if destroy {
		PutBuilder(b)
	}
	return
}

// BuildForDelete
func (b *Builder) BuildForDelete(destroy bool) (sql string, args []any) {
	args = append(args, b.BuildHead()...)
	args = append(args, b.BuildWhere()...)
	sql = b.String()
	if destroy {
		PutBuilder(b)
	}
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
