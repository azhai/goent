package goent

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

var builderPool = sync.Pool{
	New: func() any {
		return &Builder{
			Changes: map[*Field]any{},
		}
	},
}

type Dict = map[string]any

// JoinTable represents a JOIN clause with the join type, target table, and ON condition.
type JoinTable struct {
	JoinType enum.JoinType
	Table    *model.Table
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
	argNo     int
	Type      enum.QueryType
	Table     *model.Table
	Joins     []*JoinTable
	Changes   map[*Field]any
	MoreRows  [][]any
	Where     Condition
	Selects   []*Field
	Orders    []*Order
	Groups    []*Group
	Limit     int
	Offset    int
	Returning string
	RollUp    string
	ForUpdate bool
	// Clause    *Builder
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

func (b *Builder) Reset() {
	b.Builder.Reset()
	b.ResetForSave()
	b.Type = 0
	b.Table = nil
	b.Joins = nil
	b.Where = Condition{}
	b.Selects = nil
	b.Orders = nil
	b.Groups = nil
	b.Limit = 0
	b.Offset = 0
	b.RollUp = ""
}

func (b *Builder) ResetForSave() {
	b.Changes = make(map[*Field]any)
	b.MoreRows = make([][]any, 0)
	b.argNo = 0
	b.Returning = ""
}

func (b *Builder) SetTable(table TableInfo) *Builder {
	b.Table = table.Table()
	return b
}

func (b *Builder) BuildHead() []any {
	var args []any

	switch b.Type {
	case enum.SelectQuery:
		b.WriteString("SELECT ")
		if len(b.Selects) == 0 {
			b.WriteString("*")
		} else {
			b.WriteString(b.Selects[0].String())
			for _, f := range b.Selects[1:] {
				b.WriteByte(',')
				b.WriteString(f.String())
			}
		}
		b.WriteString(" FROM ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
	case enum.InsertQuery:
		b.WriteString("INSERT INTO ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
		b.WriteByte('(')
		var columns, holders []string
		for f, v := range b.Changes {
			b.argNo += 1
			holders = append(holders, "$"+strconv.Itoa(b.argNo))
			args = append(args, v)
			columns = append(columns, f.String())
		}
		b.WriteString(strings.Join(columns, ", "))
		b.WriteString(") VALUES (")
		b.WriteString(strings.Join(holders, ", "))
		b.WriteByte(')')
	case enum.InsertAllQuery:
		b.WriteString("INSERT INTO ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
		b.WriteByte('(')
		size, last := len(b.Changes), len(b.MoreRows)-1
		columns := make([]string, size)
		for f, v := range b.Changes {
			columns[v.(int)] = f.Column
		}
		b.WriteString(strings.Join(columns, ", "))
		b.WriteString(") VALUES (")
		for i, row := range b.MoreRows {
			holders := make([]string, size)
			for j := range size {
				b.argNo += 1
				holders[j] = "$" + strconv.Itoa(b.argNo)
			}
			args = append(args, row...)
			b.WriteString(strings.Join(holders, ", "))
			if i != last {
				b.WriteString("), (")
			}
		}
		b.WriteByte(')')
	case enum.UpdateQuery:
		b.WriteString("UPDATE ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
		b.WriteString(" SET ")
		for f, v := range b.Changes {
			b.argNo += 1
			if b.argNo > 1 {
				b.WriteString(", ")
			}
			b.WriteString(f.String() + "=$" + strconv.Itoa(b.argNo))
			args = append(args, v)
		}
	case enum.DeleteQuery:
		b.WriteString("DELETE FROM ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
	}

	return args
}

func (b *Builder) BuildTail() []any {
	var args []any

	if b.Type == enum.SelectQuery {
		if len(b.Groups) != 0 {
			b.WriteByte('\n')
			gp := b.Groups[0]
			b.WriteString("GROUP BY " + gp.String())
			for _, gp = range b.Groups[1:] {
				b.WriteString("," + gp.String())
			}
		}

		if len(b.Orders) != 0 {
			b.WriteByte('\n')
			ob := b.Orders[0]
			if ob.Desc {
				b.WriteString("ORDER BY " + ob.String() + " DESC")
			} else {
				b.WriteString("ORDER BY " + ob.String() + " ASC")
			}
			for _, ob = range b.Orders[1:] {
				if ob.Desc {
					b.WriteString("," + ob.String() + " DESC")
				} else {
					b.WriteString("," + ob.String() + " ASC")
				}
			}
		}

		if b.Limit != 0 {
			b.WriteByte('\n')
			b.WriteString("LIMIT " + strconv.Itoa(b.Limit))
		}
		if b.Offset != 0 {
			b.WriteByte('\n')
			b.WriteString("OFFSET " + strconv.Itoa(b.Offset))
		}
	}

	if b.Returning != "" && b.Type == enum.InsertQuery {
		b.WriteString(" RETURNING ")
		b.WriteString(b.Returning)
	}

	return args
}

func (b *Builder) BuildWhere() []any {
	var args []any

	if b.Where.Template == "" {
		return nil
	}

	b.WriteString("\nWHERE ")

	template := b.Where.Template
	fieldIndex, valueIndex := 0, 0

	for i := 0; i < len(template); i++ {
		if i+1 < len(template) && template[i:i+2] == "%s" {
			if fieldIndex < len(b.Where.Fields) {
				b.WriteString(b.Where.Fields[fieldIndex].String())
				fieldIndex++
			}
			i++
		} else if template[i] == '?' {
			if valueIndex < len(b.Where.Values) {
				val := b.Where.Values[valueIndex]
				if val.Type == reflect.Slice && len(val.Args) > 0 {
					b.WriteString("(")
					for j, arg := range val.Args {
						b.argNo += 1
						if j > 0 {
							b.WriteString(",$" + strconv.Itoa(b.argNo))
						} else {
							b.WriteString("$" + strconv.Itoa(b.argNo))
						}
						args = append(args, arg)
					}
					b.WriteByte(')')
				} else if len(val.Args) > 0 {
					b.argNo += 1
					b.WriteString("$" + strconv.Itoa(b.argNo))
					args = append(args, val.Args[0])
				}
				valueIndex++
			}
		} else {
			b.WriteByte(template[i])
		}
	}

	return args
}

func (b *Builder) BuildJoins() []any {
	var args []any

	if len(b.Joins) == 0 {
		return nil
	}

	for _, j := range b.Joins {
		b.WriteByte('\n')
		b.WriteString(string(j.JoinType) + " ")
		if j.Table != nil {
			b.WriteString(j.Table.String())
		}
		b.WriteString(" ON ")
		if j.On.Template != "" {
			template := j.On.Template
			fieldIndex := 0
			valueIndex := 0
			paramIndex := 1

			for i := 0; i < len(template); i++ {
				if i+1 < len(template) && template[i:i+2] == "%s" {
					if fieldIndex < len(j.On.Fields) {
						b.WriteString(j.On.Fields[fieldIndex].String())
						fieldIndex++
					}
					i++
				} else if template[i] == '?' {
					if valueIndex < len(j.On.Values) {
						val := j.On.Values[valueIndex]
						if val.Type == reflect.Slice && len(val.Args) > 0 {
							b.WriteString("(")
							for j, arg := range val.Args {
								if j > 0 {
									b.WriteString(",$" + strconv.Itoa(paramIndex))
								} else {
									b.WriteString("$" + strconv.Itoa(paramIndex))
								}
								args = append(args, arg)
								paramIndex++
							}
							b.WriteByte(')')
						} else if len(val.Args) > 0 {
							b.WriteString("$" + strconv.Itoa(paramIndex))
							args = append(args, val.Args[0])
							paramIndex++
						}
						valueIndex++
					}
				} else {
					b.WriteByte(template[i])
				}
			}
		}
	}

	return args
}

func (b *Builder) Build() (sql string, args []any) {
	args = append(args, b.BuildHead()...)
	args = append(args, b.BuildJoins()...)
	args = append(args, b.BuildWhere()...)
	args = append(args, b.BuildTail()...)
	sql = b.String()
	PutBuilder(b)
	return
}

// CollectFields collects primary key and non-primary key fields from a struct value.
// It sets the builder's returning information for auto-increment primary keys
// and returns a map of primary key column names to their values.
func CollectFields[T any](builder *Builder, table *Table[T], valueOf reflect.Value) (Dict, int) {
	pkFid, pkName, pkeys := table.TableInfo.GetPrimaryInfo()
	primary := make(Dict)
	for _, col := range table.Columns {
		name := col.ColumnName
		fieldOf := valueOf.FieldByName(col.FieldName)
		if slices.Contains(pkeys, name) {
			if !fieldOf.IsZero() {
				primary[name] = fieldOf.Interface()
			}
			continue
		}
		if fieldOf.Kind() == reflect.Ptr && fieldOf.IsNil() {
			continue
		}
		fld := table.Field(name)
		builder.Changes[fld] = fieldOf.Interface()
	}
	if pkName != "" && len(primary) == 0 {
		builder.Returning = pkName
	}
	return primary, pkFid
}
