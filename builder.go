package goent

import (
	"reflect"
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

type JoinTable struct {
	JoinType enum.JoinType
	Table    *model.Table
	On       Condition
}

type Order struct {
	Desc bool
	*Field
}

type Group struct {
	Having Condition
	*Field
}

type Builder struct {
	Type      enum.QueryType
	Table     *model.Table
	Joins     []*JoinTable
	Selects   []*Field
	Changes   map[*Field]any
	Where     Condition
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

func GetBuilder() *Builder {
	return builderPool.Get().(*Builder)
}

func PutBuilder(b *Builder) {
	b.Reset()
	builderPool.Put(b)
}

func (b *Builder) Reset() {
	b.Builder.Reset()
	b.Type = 0
	b.Table = nil
	b.Joins = nil
	b.Selects = nil
	b.Changes = map[*Field]any{}
	b.Where = Condition{}
	b.Orders = nil
	b.Groups = nil
	b.Limit = 0
	b.Offset = 0
	b.Returning = ""
	b.RollUp = ""
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
		i := 0
		for f := range b.Changes {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(f.String())
			i++
		}
		b.WriteString(") VALUES (")
		i = 1
		for range b.Changes {
			if i > 1 {
				b.WriteString(",$" + strconv.Itoa(i))
			} else {
				b.WriteString("$" + strconv.Itoa(i))
			}
			i++
		}
		b.WriteByte(')')
		for _, v := range b.Changes {
			args = append(args, v)
		}
	case enum.UpdateQuery:
		b.WriteString("UPDATE ")
		if b.Table != nil {
			b.WriteString(b.Table.String())
		}
		b.WriteString(" SET ")
		i := 1
		for f, v := range b.Changes {
			if i > 1 {
				b.WriteByte(',')
			}
			b.WriteString(f.String() + "=$" + strconv.Itoa(i))
			args = append(args, v)
			i++
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

	b.WriteString("WHERE ")

	template := b.Where.Template
	fieldIndex := 0
	valueIndex := 0
	paramIndex := 1

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

	return args
}

func (b *Builder) BuildJoins() []any {
	var args []any

	if len(b.Joins) == 0 {
		return nil
	}

	joinTypes := map[enum.JoinType]string{
		enum.Join:      "JOIN",
		enum.LeftJoin:  "LEFT JOIN",
		enum.RightJoin: "RIGHT JOIN",
	}

	for _, j := range b.Joins {
		b.WriteByte('\n')
		joinType := joinTypes[j.JoinType]
		if joinType == "" {
			joinType = "JOIN"
		}
		b.WriteString(joinType + " ")
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
