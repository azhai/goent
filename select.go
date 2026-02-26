package goent

import (
	"context"
	"iter"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/azhai/goent/model"
)

// StateSelect represents a SELECT query state with type parameters for table and result types.
type StateSelect[T, R any] struct {
	table     *Table[T]
	sameModel bool
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
	state.builder.Type = model.SelectQuery
	state.builder.SetTable(table.TableInfo, table.db.driver)
	state.builder.VisitFields = table.GetSortedFields()
	return &StateSelect[T, R]{table: table, StateWhere: state}
}

// CopyFrom copies the query builder state from another builder and connection.
func (s *StateSelect[T, R]) CopyFrom(ob *Builder, conn model.Connection) *StateSelect[T, R] {
	// copy joins
	// s.builder.VisitFields = ob.VisitFields
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

// Select specifies the fields to select from the table.
func (s *StateSelect[T, R]) Select(fields ...any) *StateSelect[T, R] {
	var fld *Field
	for _, one := range fields {
		if col, ok := one.(string); ok {
			fld = s.table.Field(col)
		} else if fld, ok = one.(*Field); !ok {
			continue
		}
		s.builder.VisitFields = append(s.builder.VisitFields, fld)
	}
	return s
}

// Query returns a Fetcher and a Query.
func (s *StateSelect[T, R]) Query(creator FetchCreator) (*Fetcher[R], model.Query) {
	var to FetchFunc
	if s.sameModel && len(s.builder.Joins) == 0 {
		if obj, ok := any(new(R)).(GenScanFields); ok {
			to = func(_ any) []any { return obj.ScanFields() }
		}
	}
	if to == nil {
		info, fields := s.table.TableInfo, slices.Clone(s.builder.VisitFields)
		to = creator(info, fields, s.GetJoinForeigns())
	}
	return s.QueryFetch(to)
}

// QueryFetch returns a Fetcher and a Query.
func (s *StateSelect[T, R]) QueryFetch(to FetchFunc) (*Fetcher[R], model.Query) {
	qr := model.CreateQuery(s.builder.Build(false))
	// defer PutBuilder(s.builder)
	fet := &Fetcher[R]{
		Handler:   s.Prepare(s.table.db.driver),
		NewTarget: func() *R { return new(R) },
		FetchTo:   to,
	}
	return fet, qr
}

// QueryRows executes the query and returns all rows as a slice.
func (s *StateSelect[T, R]) QueryRows(to FetchFunc) (data []*R, err error) {
	var rows model.Rows
	limit := s.builder.Limit
	fet, qr := s.QueryFetch(to)
	if rows, err = fet.QueryResult(qr); err != nil {
		return
	}
	data, qr.Err = fet.FetchRows(rows, err, limit)
	if qr.Err != nil {
		err = fet.ErrHandler(qr)
	}
	return
}

// IterRows return a iterator on rows.
func (s *StateSelect[T, R]) IterRows() iter.Seq2[*R, error] {
	fet, qr := s.Query(CreateFetchFunc)
	return fet.FetchResult(qr)
}

// One executes the query and returns the first row as a single result.
func (s *StateSelect[T, R]) One() (*R, error) {
	limit := TakeNoLimit
	if s.sameModel {
		limit = 1
	}
	for row, err := range s.Take(limit).IterRows() {
		if s.sameModel && row != nil {
			s.table.CacheOne(row)
		}
		return row, err
	}
	return nil, ErrNotFound
}

// All executes the query and returns all rows as a slice.
func (s *StateSelect[T, R]) All() ([]*R, error) {
	var rows []*R
	if s.builder.Limit > 0 {
		rows = make([]*R, 0, s.builder.Limit)
	} else {
		rows = make([]*R, 0)
	}
	for row, err := range s.IterRows() {
		if err != nil {
			return rows, err
		}
		if row != nil && s.sameModel {
			s.table.CacheOne(row)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Map executes the query and returns results as a map keyed by the specified column.
func (s *StateSelect[T, R]) Map(key string) (map[int64]*R, error) {
	var col *Column
	if col = s.table.ColumnInfo(key); col == nil {
		return nil, NewColumnNotFoundError(key)
	}
	res := make(map[int64]*R)
	for row, err := range s.IterRows() {
		if err != nil {
			return nil, err
		}
		if s.sameModel && row != nil {
			s.table.CacheOne(row)
		}
		if val, ok := col.GetInt64(row); ok {
			res[val] = row
		}
	}
	return res, nil
}

// Rank executes the query and returns results as a map keyed by the specified column, with each key mapping to a slice of results.
func (s *StateSelect[T, R]) Rank(key string) (map[int64][]*R, error) {
	var col *Column
	if col = s.table.ColumnInfo(key); col == nil {
		return nil, NewColumnNotFoundError(key)
	}
	res := make(map[int64][]*R)
	for row, err := range s.IterRows() {
		if err != nil {
			return nil, err
		}
		if s.sameModel && row != nil {
			s.table.CacheOne(row)
		}
		if val, ok := col.GetInt64(row); ok {
			res[val] = append(res[val], row)
		}
	}
	return res, nil
}

// RollUP enables rollup for aggregation queries.
//
// Example:
//
//	results, _ := db.Order.Select("status", "total").GroupBy("status").RollUP().All()
func (s *StateSelect[T, R]) RollUP() *StateSelect[T, R] {
	return s
}

// OnTransaction sets a transaction for the select query and enables FOR UPDATE lock.
func (s *StateSelect[T, R]) OnTransaction(tx model.Transaction) *StateSelect[T, R] {
	s.builder.ForUpdate = true
	s.StateWhere.conn = tx
	return s
}

// Filter adds filter conditions to the select query.
func (s *StateSelect[T, R]) Filter(args ...Condition) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

// Where adds a WHERE clause to the select query.
func (s *StateSelect[T, R]) Where(where string, args ...any) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Where(where, args...)
	return s
}

// Match sets the WHERE conditions based on the non-zero fields of the given object.
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
	if i >= TakeNoLimit {
		s.builder.Limit = i
	}
	return s
}

// Skip skips i elements
func (s *StateSelect[T, R]) Skip(i int) *StateSelect[T, R] {
	if i >= 0 {
		s.builder.Offset = i
	}
	return s
}

// GetJoinForeign returns the foreign key relationship for the joined table.
func (s *StateSelect[T, R]) GetJoinForeign() *Foreign {
	if len(s.builder.Joins) == 0 {
		return nil
	}
	tableName := s.builder.Joins[0].Table.Name
	if foreign, ok := s.table.Foreigns[tableName]; ok {
		return foreign
	}
	return nil
}

// GetJoinForeigns returns all foreign key relationships for the joined tables.
func (s *StateSelect[T, R]) GetJoinForeigns() []*Foreign {
	if len(s.builder.Joins) == 0 {
		return nil
	}
	foreigns := make([]*Foreign, 0, len(s.builder.Joins))
	valueType := reflect.TypeFor[R]()
	for _, join := range s.builder.Joins {
		if foreign, ok := s.table.Foreigns[join.Table.Name]; ok {
			foreigns = append(foreigns, foreign)
			continue
		}
		info := findTableInfoByName(join.Table.Name)
		if info != nil {
			if field, ok := valueType.FieldByName(info.FieldName); ok {
				if field.Type.Kind() == reflect.Slice {
					continue
				}
				foreigns = append(foreigns, &Foreign{
					Type:       O2O,
					MountField: info.FieldName,
					Reference:  &Field{TableAddr: info.TableAddr},
				})
			}
		}
	}
	return foreigns
}

// Join joins another table with a condition
func (s *StateSelect[T, R]) Join(joinType model.JoinType, info TableInfo, on Condition) *StateSelect[T, R] {
	s.builder.Type = model.SelectJoinQuery
	jt := &JoinTable{
		JoinType: joinType,
		Table:    info.Table(),
		On:       on,
	}
	if s.table.db != nil && s.table.db.driver != nil {
		var schema string
		if jt.Table.Schema != nil {
			schema = *jt.Table.Schema
		}
		jt.fullName = s.table.db.driver.FormatTableName(schema, jt.Table.Name)
	}
	s.builder.Joins = append(s.builder.Joins, jt)
	return s
}

// LeftJoin joins another table with a condition on left table
func (s *StateSelect[T, R]) LeftJoin(fkey string, refer *Field) *StateSelect[T, R] {
	info := GetTableInfo(refer.TableAddr)
	var leftField *Field
	if len(s.builder.Joins) > 0 {
		lastJoin := s.builder.Joins[len(s.builder.Joins)-1]
		leftTableInfo := findTableInfoByName(lastJoin.Table.Name)
		if leftTableInfo == nil {
			panic("table info not found for join table: " + lastJoin.Table.Name)
		}
		leftField = leftTableInfo.Field(fkey)
	} else {
		leftField = s.table.Field(fkey)
	}
	s.Join(model.LeftJoin, *info, EqualsField(leftField, refer))
	s.builder.VisitFields = append(s.builder.VisitFields, info.GetSortedFields()...)
	return s
}

func findTableInfoByName(name string) *TableInfo {
	tableRegLock.RLock()
	defer tableRegLock.RUnlock()
	for _, info := range tableRegistry {
		if info.TableName == name {
			return info
		}
	}
	return nil
}

// Pagination holds paginated query results with metadata.
type Pagination[T, R any] struct {
	TotalValues int64 `json:"totalValues"` // TotalValues is the total number of values in the query result.
	TotalPages  int   `json:"totalPages"`  // TotalPages is the total number of pages in the pagination.

	PageValues int `json:"pageValues"` // PageValues is the number of values on the current page.
	PageSize   int `json:"pageSize"`   // PageSize is the maximum number of values per page.

	CurrentPage     int  `json:"currentPage"`     // CurrentPage is the current page number.
	HasPreviousPage bool `json:"hasPreviousPage"` // HasPreviousPage is true if there is a previous page.
	PreviousPage    int  `json:"previousPage"`    // PreviousPage is the number of the previous page.
	HasNextPage     bool `json:"hasNextPage"`     // HasNextPage is true if there is a next page.
	NextPage        int  `json:"nextPage"`        // NextPage is the number of the next page.

	StartIndex int  `json:"startIndex"` // StartIndex is the index of the first value on the current page.
	EndIndex   int  `json:"endIndex"`   // EndIndex is the index of the last value on the current page.
	Values     []*R `json:"values"`     // Values is the slice of values on the current page.
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
	counter := NewStateSelect[T, ResultLong](s.ctx, s.table)
	counter.builder.VisitFields = nil
	counter.Select(fld)
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
