package goent

import (
	"context"
	"iter"
	"math"
	"reflect"
	"strings"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/query/aggregate"
	"github.com/azhai/goent/query/function"
	"github.com/azhai/goent/query/where"
)

type StateSelect[T, R any] struct {
	fields []fieldSelect
	table  *Table[T]
	others []*Table[T]
	*StateWhere
}

func NewStateSelect[T, R any](ctx context.Context, table *Table[T], col any) *StateSelect[T, R] {
	s := NewStateWhere(ctx)
	s.builder.query.Type = enum.SelectQuery
	s.builder.table = &table.TableInfo
	sel := &StateSelect[T, R]{table: table, StateWhere: s}
	if col != nil {
		if a, ok := col.(model.Aggregate); ok {
			if f, ok := a.(interface{ GetField() any }).GetField().(*Column); ok {
				agg := createAggregateFromTable(table, f, a.Aggregate())
				sel.fields = append(sel.fields, agg)
				s.builder.fieldsSelect = append(s.builder.fieldsSelect, agg)
			}
		}
	}
	return sel
}

func (s *StateSelect[T, R]) CopyFrom(ob builder, conn model.Connection) *StateSelect[T, R] {
	// copy joins
	s.builder.joins, s.builder.joinsArgs = ob.joins, ob.joinsArgs
	// copy operations
	s.builder.brs = ob.brs
	s.builder.filters = ob.filters
	// copy connection/transaction
	s.conn = conn
	return s
}

// AsQuery return a [model.Query] for use inside a [where.In].
func (s *StateSelect[T, R]) AsQuery() model.Query {
	s.builder.buildSqlSelect()
	return s.builder.query
}

// Rows return a iterator on rows.
func (s *StateSelect[T, R]) Rows() iter.Seq2[R, error] {
	s.builder.buildSqlSelect()
	dc, size := s.prepare(s.table.db.driver), len(s.builder.fieldsSelect)
	return handlerResult[R](s.ctx, s.conn, s.builder.query, size, dc)
}

func (s *StateSelect[T, R]) One() (*R, error) {
	for row, err := range s.Take(1).Rows() {
		return &row, err
	}
	return nil, ErrNotFound
}

func (s *StateSelect[T, R]) All() ([]*R, error) {
	rows := make([]*R, 0, s.builder.query.Limit)
	for row, err := range s.Rows() {
		if err != nil {
			return nil, err
		}
		rows = append(rows, &row)
	}
	return rows, nil
}

func (s *StateSelect[T, R]) RollUP() *StateSelect[T, R] {
	return s
}

func (s *StateSelect[T, R]) OnTransaction(tx model.Transaction) *StateSelect[T, R] {
	s.builder.query.ForUpdate = true
	s.StateWhere.conn = tx
	return s
}

func (s *StateSelect[T, R]) Filter(args ...model.Operation) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

func (s *StateSelect[T, R]) Match(obj T) *StateSelect[T, R] {
	s.StateWhere = MatchWhere[T](s.StateWhere, s.table, obj)
	return s
}

// OrderBy makes a ordained by args query
func (s *StateSelect[T, R]) OrderBy(args ...string) *StateSelect[T, R] {
	var desc bool
	for _, arg := range args {
		pieces := strings.Fields(arg)
		if len(pieces) == 2 && strings.ToLower(pieces[1]) == "desc" {
			arg, desc = pieces[0], true
		}
		var table string
		if pieces = strings.SplitN(arg, ".", 2); len(pieces) == 2 {
			table, arg = pieces[0], pieces[1]
		} else {
			table = s.table.TableName
		}
		attr := model.Attribute{Table: table, Name: arg}
		ord := model.OrderBy{Attribute: attr, Desc: desc}
		s.builder.query.OrderBy = append(s.builder.query.OrderBy, ord)
	}
	return s
}

// GroupBy makes a group by args query
func (s *StateSelect[T, R]) GroupBy(args ...any) *StateSelect[T, R] {
	s.builder.query.GroupBy = make([]model.GroupBy, len(args))
	for i := range args {
		if a, ok := getAttribute(args[i], addrMap.mapField); ok {
			s.builder.query.GroupBy[i].Attribute = a
		}
	}
	return s
}

// Take takes i elements
func (s *StateSelect[T, R]) Take(i int) *StateSelect[T, R] {
	s.builder.query.Limit = i
	return s
}

// Skip skips i elements
func (s *StateSelect[T, R]) Skip(i int) *StateSelect[T, R] {
	s.builder.query.Offset = i
	return s
}

func (s *StateSelect[T, R]) Join(left, right any) *StateSelect[T, R] {
	s.builder.buildSelectJoins(enum.Join, getArgsJoin(addrMap.mapField, left, right))
	return s
}

func (s *StateSelect[T, R]) LeftJoin(left, right any) *StateSelect[T, R] {
	s.builder.buildSelectJoins(enum.LeftJoin, getArgsJoin(addrMap.mapField, left, right))
	return s
}

func (s *StateSelect[T, R]) RightJoin(left, right any) *StateSelect[T, R] {
	s.builder.buildSelectJoins(enum.RightJoin, getArgsJoin(addrMap.mapField, left, right))
	return s
}

type Pagination[T, R any] struct {
	TotalValues int64 `json:"totalValues"`
	TotalPages  int   `json:"totalPages"`

	PageValues int `json:"pageValues"`
	PageSize   int `json:"pageSize"`

	CurrentPage     int  `json:"currentPage"`
	HasPreviousPage bool `json:"hasPreviousPage"`
	PreviousPage    int  `json:"previousPage"`
	HasNextPage     bool `json:"hasNextPage"`
	NextPage        int  `json:"nextPage"`

	StartIndex int  `json:"startIndex"`
	EndIndex   int  `json:"endIndex"`
	Values     []*R `json:"values"`
}

// Pagination return a paginated query as [Pagination].
//
// Default values for page and size are 1 and 10 respectively.
func (s *StateSelect[T, R]) Pagination(page, size int) (*Pagination[T, R], error) {
	if size <= 0 {
		size = 10
	}
	if page <= 0 {
		page = 1
	}

	counter := NewStateSelect[T, ResultCount](s.ctx, s.table, aggregate.Count(s.table))
	counter.CopyFrom(s.builder, s.conn)
	count, err := FetchCountResult(counter)
	if err != nil {
		return nil, err
	}

	s.builder.query.Offset = size * (page - 1)
	s.builder.query.Limit = size

	p := new(Pagination[T, R])

	p.Values, err = s.All()
	if err != nil {
		return nil, err
	}

	p.TotalValues = count

	p.TotalPages = int(math.Ceil(float64(count) / float64(size)))
	p.CurrentPage = page

	if page == p.TotalPages || p.TotalPages == 0 {
		p.NextPage = page
	} else {
		p.NextPage = page + 1
		p.HasNextPage = true
	}

	if page == 1 {
		p.PreviousPage = page
	} else {
		p.PreviousPage = page - 1
		p.HasPreviousPage = true
	}

	p.PageSize = size
	p.PageValues = len(p.Values)

	p.StartIndex = (page-1)*size + 1

	if !p.HasNextPage {
		p.EndIndex = int(p.TotalValues)
	} else {
		p.EndIndex = size * page
	}

	return p, nil
}

type getArgs struct {
	addrMap   map[uintptr]field
	value     any
	tableArgs []any
}

func getNonZeroFields(a getArgs) ([]any, []any, bool) {
	args, values := make([]any, 0), make([]any, 0)

	valueOf := reflect.ValueOf(a.value)
	for i := 0; i < valueOf.NumField(); i++ {
		if !valueOf.Field(i).IsZero() {
			args = append(args, a.tableArgs[i])
			values = append(values, valueOf.Field(i).Interface())
		}
	}

	if len(args) == 0 {
		return nil, nil, true
	}
	return args, values, false
}

func andList(args, values []any, eq func(f, a any) model.Operation) model.Operation {
	size := len(args)
	if size == 0 {
		return model.Operation{}
	} else if size == 1 {
		return eq(args[0], values[0])
	}

	var others []model.Operation
	left, right := eq(args[0], values[0]), eq(args[1], values[1])
	for i := 2; i < size; i++ {
		others = append(others, eq(args[i], values[i]))
	}
	return where.And(left, right, others...)
}

func operations(args, values []any) model.Operation {
	return andList(args, values, equals)
}

func equals(f, a any) model.Operation {
	return where.Equals(&f, a)
}

func operationsList(args, values []any) model.Operation {
	return andList(args, values, equalsOrLike)
}

func equalsOrLike(f, a any) model.Operation {
	v, ok := a.(string)

	if !ok {
		return where.Equals(&f, a)
	}

	return where.Like(function.ToUpper(f.(*string)), strings.ToUpper("%"+v+"%"))
}

type argsSelect struct {
	fields    []fieldSelect
	tableArgs []any
}

func createFunction(field field, a any) fieldSelect {
	if f, ok := a.(model.FunctionType); ok {
		return functionResult{
			tableName:     field.table(),
			schemaName:    field.schema(),
			tableId:       field.getTableId(),
			db:            field.getDb(),
			attributeName: field.getAttributeName(),
			functionType:  f.GetType()}
	}

	return nil
}

func createAggregate(field field, a any) fieldSelect {
	if ag, ok := a.(model.Aggregate); ok {
		return aggregateResult{
			tableName:     field.table(),
			schemaName:    field.schema(),
			tableId:       field.getTableId(),
			db:            field.getDb(),
			attributeName: field.getAttributeName(),
			aggregateType: ag.Aggregate()}
	}

	return nil
}

func createAggregateFromTable[T any](table *Table[T], col *Column, aggType enum.AggregateType) fieldSelect {
	schemaName := table.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}
	return aggregateResult{
		tableName:     table.TableName,
		schemaName:    &schemaName,
		tableId:       table.TableId,
		db:            table.db,
		attributeName: col.ColumnName,
		aggregateType: aggType}
}

func getArgsJoin(addrMap map[uintptr]field, args ...any) []field {
	fields := make([]field, 2)
	var ptr uintptr
	var valueOf reflect.Value
	var f field
	for i := range args {
		valueOf = reflect.ValueOf(args[i])
		if valueOf.Kind() == reflect.Pointer {
			ptr = uintptr(valueOf.UnsafePointer())
			f = addrMap[ptr]
			if f != nil {
				fields[i] = f
			}
			continue
		}
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	if fields[0] == nil || fields[1] == nil {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}
	return fields
}

func getArgFunction(arg any, addrMap map[uintptr]field, operation *model.Operation) field {
	value := reflect.ValueOf(arg)
	if value.IsNil() {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	if fun, ok := value.Elem().Interface().(model.Attributer); ok {
		operation.Function = fun.Attribute(model.Body{}).FunctionType
		return getArg(fun.GetField(), addrMap, nil)
	}
	return getArg(arg, addrMap, nil)
}

func getArg(arg any, addrMap map[uintptr]field, operation *model.Operation) field {
	v := reflect.ValueOf(arg)
	if v.Kind() != reflect.Pointer {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	if operation != nil {
		return getArgFunction(arg, addrMap, operation)
	}

	addr := uintptr(v.UnsafePointer())
	if addrMap[addr] != nil {
		return addrMap[addr]
	}
	// any as pointer, used on StateSave2, StateFind, StateRemove and list
	return getAnyArg(v, addrMap)
}

// used only inside getArg
func getAnyArg(value reflect.Value, addrMap map[uintptr]field) field {
	if value.IsNil() {
		return nil
	}

	value = reflect.ValueOf(value.Elem().Interface())
	if value.Kind() != reflect.Pointer {
		return nil
	}

	addr := uintptr(value.UnsafePointer())
	if addrMap[addr] != nil {
		return addrMap[addr]
	}
	return nil
}

func getAttribute(arg any, addrMap map[uintptr]field) (model.Attribute, bool) {
	v := reflect.ValueOf(arg)
	if v.Kind() != reflect.Pointer {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	f := addrMap[uintptr(v.UnsafePointer())]
	if f != nil {
		return model.Attribute{Table: f.table(), Name: f.getAttributeName()}, true
	}

	if a, ok := v.Elem().Interface().(model.Attributer); ok {
		f = addrMap[uintptr(reflect.ValueOf(a.GetField()).UnsafePointer())]
		if f != nil {
			return a.Attribute(model.Body{
				Table: f.table(),
				Name:  f.getAttributeName(),
			}), true
		}
	}

	return model.Attribute{}, false
}

func helperWhere(builder *builder, addrMap map[uintptr]field, br model.Operation) {
	switch br.Type {
	case enum.OperationWhere, enum.OperationInWhere:
		a := getArg(br.Arg, addrMap, &br)
		if a == nil {
			return
		}
		br.Table = model.Table{Schema: a.schema(), Name: a.table()}
		br.TableId = a.getTableId()
		br.Attribute = a.getAttributeName()

		builder.brs = append(builder.brs, br)
	case enum.OperationAttributeWhere:
		a, b := getArg(br.Arg, addrMap, nil), getArg(br.Value.GetValue(), addrMap, nil)
		if a == nil || b == nil {
			return
		}
		br.Table = model.Table{Schema: a.schema(), Name: a.table()}
		br.TableId = a.getTableId()
		br.Attribute = a.getAttributeName()

		br.AttributeValue = b.getAttributeName()
		br.AttributeValueTable = model.Table{Schema: b.schema(), Name: b.table()}
		br.AttributeTableId = b.getTableId()
		builder.brs = append(builder.brs, br)
	case enum.OperationIsWhere:
		a := getArg(br.Arg, addrMap, nil)
		if a == nil {
			return
		}
		br.Table = model.Table{Schema: a.schema(), Name: a.table()}
		br.TableId = a.getTableId()
		br.Attribute = a.getAttributeName()

		builder.brs = append(builder.brs, br)
	case enum.LogicalWhere:
		for i, op := range br.Branches {
			if i > 0 {
				builder.brs = append(builder.brs, br)
			}
			helperWhere(builder, addrMap, op)
		}
	}
}

func helperFilter(builder *builder, addrMap map[uintptr]field, br model.Operation) bool {
	switch br.Type {
	case enum.OperationWhere, enum.OperationInWhere:
		if !reflect.ValueOf(br.Value.GetValue()).IsZero() {
			a := getArg(br.Arg, addrMap, &br)
			br.Table = model.Table{Schema: a.schema(), Name: a.table()}
			br.TableId = a.getTableId()
			br.Attribute = a.getAttributeName()

			builder.filters = append(builder.filters, br)
			return true
		}
	case enum.OperationAttributeWhere:
		a, b := getArg(br.Arg, addrMap, nil), getArg(br.Value.GetValue(), addrMap, nil)
		br.Table = model.Table{Schema: a.schema(), Name: a.table()}
		br.TableId = a.getTableId()
		br.Attribute = a.getAttributeName()

		br.AttributeValue = b.getAttributeName()
		br.AttributeValueTable = model.Table{Schema: b.schema(), Name: b.table()}
		br.AttributeTableId = b.getTableId()
		builder.filters = append(builder.filters, br)
		return true
	case enum.LogicalWhere:
		var currFlag, lastFlag bool
		for i, op := range br.Branches {
			currFlag = helperFilter(builder, addrMap, op)
			if i > 0 && currFlag && lastFlag {
				builder.filters = append(builder.filters, br)
			}
			lastFlag = currFlag
		}
		return true
	}
	return false
}

func getArgsSelect(args ...any) argsSelect {
	addrMap := addrMap.mapField
	fields := make([]fieldSelect, 0, len(args))

	for _, arg := range args {
		if f, ok := arg.(field); ok {
			fields = append(fields, f)
			continue
		}
		if a, ok := arg.(model.Aggregate); ok {
			if f, ok := a.(interface{ GetField() any }).GetField().(field); ok {
				fields = append(fields, createAggregate(f, a))
				continue
			}
		}
		if a, ok := arg.(model.FunctionType); ok {
			if f, ok := a.(interface{ GetField() any }).GetField().(field); ok {
				fields = append(fields, createFunction(f, a))
				continue
			}
		}
		if a, ok := arg.(model.Attributer); ok {
			if f, ok := a.GetField().(field); ok {
				fields = append(fields, f)
				continue
			}
		}
		fieldOf := reflect.ValueOf(arg)
		if fieldOf.Kind() == reflect.Ptr && !fieldOf.IsNil() {
			f := addrMap[uintptr(fieldOf.UnsafePointer())]
			if f != nil {
				fields = append(fields, f)
				continue
			}
		}
	}

	if len(fields) == 0 {
		panic("goent: invalid argument. try sending a pointer to a database mapped argument")
	}

	return argsSelect{fields: fields, tableArgs: args}
}

func getArgsList(args ...any) argsSelect {
	addrMap := addrMap.mapField
	fields := make([]fieldSelect, 0, len(args))
	tableArgs := make([]any, 0, len(args))

	for _, arg := range args {
		structOf := reflect.ValueOf(arg).Elem()
		var fieldOf reflect.Value
		for i := 0; i < structOf.NumField(); i++ {
			fieldOf = structOf.Field(i)
			if f := addrMap[uintptr(fieldOf.Addr().UnsafePointer())]; f != nil {
				fields = append(fields, f)
				tableArgs = append(tableArgs, fieldOf.Addr().Interface())
			}
		}
	}

	if len(fields) == 0 {
		panic("goent: invalid argument. try sending a pointer to a database mapped argument")
	}

	return argsSelect{fields: fields, tableArgs: tableArgs}
}
