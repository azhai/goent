package goent

import (
	"context"
	"iter"
	"math"
	"reflect"
	"strings"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type FetchFunc func(target any) []any

// StateSelect represents a SELECT query state with type parameters for table and result types.
type StateSelect[T, R any] struct {
	table     *Table[T]
	sameModel bool
	FetchRow  FetchFunc
	*StateWhere
}

// NewStateSelect creates a new StateSelect for querying data from a table.
func NewStateSelect[T, R any](ctx context.Context, table *Table[T]) *StateSelect[T, R] {
	state := NewStateWhere(ctx)
	return NewStateSelectFrom[T, R](state, table)
}

// NewStateSelectFrom creates a new StateSelect for querying data from a table
func NewStateSelectFrom[T, R any](state *StateWhere, table *Table[T]) *StateSelect[T, R] {
	if state == nil {
		ctx := context.Background()
		state = NewStateWhere(ctx)
	}
	state.builder.Type = enum.SelectQuery
	state.builder.SetTable(table.TableInfo)
	return &StateSelect[T, R]{table: table, StateWhere: state}
}

func (s *StateSelect[T, R]) CopyFrom(ob *Builder, conn model.Connection) *StateSelect[T, R] {
	// copy joins
	// s.builder.Selects = ob.Selects
	s.builder.Joins = ob.Joins
	// copy operations
	s.builder.Where = ob.Where
	s.builder.Orders = ob.Orders
	s.builder.Groups = ob.Groups
	s.builder.Limit = ob.Limit
	s.builder.Offset = ob.Offset
	s.builder.RollUp = ob.RollUp
	// copy connection/transaction
	s.conn = conn
	return s
}

func (s *StateSelect[T, R]) Select(fields ...any) *StateSelect[T, R] {
	var fld *Field
	for _, one := range fields {
		if col, ok := one.(string); ok {
			fld = s.table.Field(col)
		} else if fld, ok = one.(*Field); !ok {
			continue
		}
		s.builder.Selects = append(s.builder.Selects, fld)
	}
	return s
}

func (s *StateSelect[T, R]) GetFetchFunc() FetchFunc {
	fields, foreign := s.builder.Selects, s.GetForeign()
	return func(target any) []any {
		valueOf := reflect.ValueOf(target).Elem()
		if len(fields) > 0 && fields[0].Function != "" {
			return FlattenDest(valueOf)
		}
		if len(fields) > 0 {
			return AppendDestFields(fields, valueOf, foreign)
		}
		dest := AppendDestTable(s.table.TableInfo, valueOf)
		if foreign != nil {
			info := GetTableInfo(foreign.Reference.TableAddr)
			if typeOf, ok := valueOf.Type().FieldByName(foreign.MountField); ok {
				fieldOf := reflect.New(typeOf.Type.Elem())
				valueOf.FieldByName(foreign.MountField).Set(fieldOf)
				dest = append(dest, AppendDestTable(*info, fieldOf.Elem())...)
			}
		}
		return dest
	}
}

// Rows return a iterator on rows.
func (s *StateSelect[T, R]) Rows() iter.Seq2[*R, error] {
	qr := model.CreateQuery(s.builder.Build(false))
	defer PutBuilder(s.builder)
	hd := s.Prepare(s.table.db.driver)
	if s.FetchRow == nil {
		s.FetchRow = s.GetFetchFunc()
	}
	return FetchResult[R](hd, qr, s.FetchRow)
}

func (s *StateSelect[T, R]) One() (*R, error) {
	limit := -1
	if s.sameModel {
		limit = 1
	}
	for row, err := range s.Take(limit).Rows() {
		if s.sameModel {
			s.table.CacheOne(row)
		}
		return row, err
	}
	return nil, ErrNotFound
}

func (s *StateSelect[T, R]) All() ([]*R, error) {
	rows := make([]*R, 0, s.builder.Limit)
	for row, err := range s.Rows() {
		if err != nil {
			return nil, err
		}
		if s.sameModel {
			s.table.CacheOne(row)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *StateSelect[T, R]) Map(key string) (map[int64]*R, error) {
	res := make(map[int64]*R)
	col := s.table.Columns[key]
	for row, err := range s.Rows() {
		if err != nil {
			return nil, err
		}
		if s.sameModel {
			s.table.CacheOne(row)
		}
		if val, ok := col.GetInt64(row); ok {
			res[val] = row
		}
	}
	return res, nil
}

func (s *StateSelect[T, R]) Rank(key string) (map[int64][]*R, error) {
	res := make(map[int64][]*R)
	col := s.table.Columns[key]
	for row, err := range s.Rows() {
		if err != nil {
			return nil, err
		}
		if s.sameModel {
			s.table.CacheOne(row)
		}
		if val, ok := col.GetInt64(row); ok {
			res[val] = append(res[val], row)
		}
	}
	return res, nil
}

func (s *StateSelect[T, R]) RollUP() *StateSelect[T, R] {
	return s
}

func (s *StateSelect[T, R]) OnTransaction(tx model.Transaction) *StateSelect[T, R] {
	s.builder.ForUpdate = true
	s.StateWhere.conn = tx
	return s
}

func (s *StateSelect[T, R]) Filter(args ...Condition) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

func (s *StateSelect[T, R]) Match(obj T) *StateSelect[T, R] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// OrderBy makes a ordained by args query
func (s *StateSelect[T, R]) OrderBy(args ...string) *StateSelect[T, R] {
	var desc bool
	for _, arg := range args {
		pieces := strings.Fields(arg)
		if len(pieces) == 2 && strings.ToUpper(pieces[1]) == "DESC" {
			arg, desc = pieces[0], true
		}
		ord := &Order{Field: s.table.Field(arg), Desc: desc}
		s.builder.Orders = append(s.builder.Orders, ord)
	}
	return s
}

// GroupBy makes a group by args query
func (s *StateSelect[T, R]) GroupBy(args ...string) *StateSelect[T, R] {
	for _, arg := range args {
		grp := &Group{Field: s.table.Field(arg), Having: Condition{}}
		s.builder.Groups = append(s.builder.Groups, grp)
	}
	return s
}

// Take takes i elements
func (s *StateSelect[T, R]) Take(i int) *StateSelect[T, R] {
	if i >= 0 {
		s.builder.Limit = i
	}
	return s
}

// Skip skips i elements
func (s *StateSelect[T, R]) Skip(i int) *StateSelect[T, R] {
	s.builder.Offset = i
	return s
}

func (s *StateSelect[T, R]) GetForeign() *Foreign {
	if len(s.builder.Joins) == 0 {
		return nil
	}
	tableName := s.builder.Joins[0].Table.Name
	if foreign, ok := s.table.Foreigns[tableName]; ok {
		return foreign
	}
	return nil
}

// Join joins another table with a condition
func (s *StateSelect[T, R]) Join(joinType enum.JoinType, info TableInfo, on Condition) *StateSelect[T, R] {
	s.builder.Type = enum.SelectJoinQuery
	s.builder.Joins = append(s.builder.Joins, &JoinTable{
		JoinType: joinType, Table: info.Table(), On: on,
	})
	return s
}

// LeftJoin joins another table with a condition on left table
func (s *StateSelect[T, R]) LeftJoin(fkey string, refer *Field) *StateSelect[T, R] {
	info := GetTableInfo(refer.TableAddr)
	return s.Join(enum.LeftJoin, *info, EqualsField(s.table.Field(fkey), refer))
}

// Pagination holds paginated query results with metadata.
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

	fld := &Field{TableAddr: s.table.TableAddr, ColumnName: "*", Function: "COUNT(%s)"}
	counter := NewStateSelect[T, ResultLong](s.ctx, s.table).Select(fld)
	counter.CopyFrom(s.builder, s.conn)
	count, err := FetchSingleResult(counter)
	if err != nil {
		return nil, err
	}

	s.builder.Offset = size * (page - 1)
	s.builder.Limit = size

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
