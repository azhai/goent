package goent

import (
	"context"
	"iter"
	"math"
	"strings"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

// StateSelect represents a SELECT query state with type parameters for table and result types.
type StateSelect[T, R any] struct {
	fields []fieldSelect
	table  *Table[T]
	others []*Table[T]
	*StateWhere
}

// NewStateSelect creates a new StateSelect for querying data from a table.
func NewStateSelect[T, R any](ctx context.Context, table *Table[T]) *StateSelect[T, R] {
	s := NewStateWhere(ctx)
	s.builder.Type = enum.SelectQuery
	s.builder.SetTable(table.TableInfo)
	return &StateSelect[T, R]{table: table, StateWhere: s}
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

func (s *StateSelect[T, R]) Select(args ...any) *StateSelect[T, R] {
	for _, arg := range args {
		if col, ok := arg.(*Field); ok {
			s.builder.Selects = append(s.builder.Selects, col)
		} else if col, ok := arg.(string); ok {
			fld := s.table.Field(col)
			s.builder.Selects = append(s.builder.Selects, fld)
		}
	}
	return s
}

// AsQuery return a [model.Query] for use inside a [where.In].
// func (s *StateSelect[T, R]) AsQuery() model.Query {
// 	sql, args := s.builder.Build()
// 	return model.Query{
// 		Type:       s.builder.Type,
// 		RawSql:     sql,
// 		Arguments:  args,
// 		WhereIndex: 1,
// 	}
// }

// Rows return a iterator on rows.
func (s *StateSelect[T, R]) Rows() iter.Seq2[*R, error] {
	qr := model.CreateQuery(s.builder.Build())
	hd := s.Prepare(s.table.db.driver)
	return QueryResult[R](hd, qr)
}

func (s *StateSelect[T, R]) One() (*R, error) {
	for row, err := range s.Take(1).Rows() {
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
		rows = append(rows, row)
	}
	return rows, nil
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
	s.builder.Limit = i
	return s
}

// Skip skips i elements
func (s *StateSelect[T, R]) Skip(i int) *StateSelect[T, R] {
	s.builder.Offset = i
	return s
}

// Join joins another table with a condition
func (s *StateSelect[T, R]) Join(joinType enum.JoinType, on Condition) *StateSelect[T, R] {
	s.builder.Joins = append(s.builder.Joins, &JoinTable{
		JoinType: joinType, On: on,
		Table: s.table.TableInfo.Table(),
	})
	return s
}

// LeftJoin joins another table with a condition on left table
func (s *StateSelect[T, R]) LeftJoin(table *Table[T], left, right string) *StateSelect[T, R] {
	return s.Join(enum.LeftJoin, EqualsField(s.table.Field(left), table.Field(right)))
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

	fld := &Field{Table: s.table.TableAddr, Column: "*", Function: "COUNT(%s)"}
	counter := NewStateSelect[T, ResultCount](s.ctx, s.table).Select(fld)
	counter.CopyFrom(s.builder, s.conn)
	count, err := FetchCountResult(counter)
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
