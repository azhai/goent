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

// StateSelect represents a SELECT query state with type parameters for table and result types
// It provides methods for building and executing SELECT queries with various options
type StateSelect[T, R any] struct {
	table       *Table[T] // The table to query from
	sameModel   bool      // Whether the result type is the same as the table model
	*StateWhere           // Embedded StateWhere for WHERE clause construction
}

// NewStateSelect creates a new StateSelect for querying data from a table
// It initializes the query state with the provided context and table
func NewStateSelect[T, R any](ctx context.Context, table *Table[T]) *StateSelect[T, R] {
	state := NewStateWhere(ctx)
	return NewStateSelectFrom[T, R](state, table)
}

// NewStateSelectFrom creates a new StateSelect from an existing StateWhere
// It sets up the query state for the specified table
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

// CopyFrom copies the query builder state from another builder and connection
// It copies joins, conditions, orders, groups, limits, and offset
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

// Select specifies the fields to select from the table
// It accepts field names as strings or Field objects
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

// getFetchFunc returns a FetchFunc for the query
// It uses generated ScanFields if available, otherwise creates a reflection-based fetcher
func (s *StateSelect[T, R]) getFetchFunc() FetchFunc {
	if s.sameModel && len(s.builder.Joins) == 0 {
		if obj, ok := any(new(R)).(GenScanFields); ok {
			return func(_ any) []any { return obj.ScanFields() }
		}
	}
	info, fields := s.table.TableInfo, slices.Clone(s.builder.VisitFields)
	return CreateFetchFunc(info, fields, s.GetJoinForeigns())
}

// FetchRow executes the query and returns a single row using the provided FetchFunc
// It handles the query execution and row scanning
func (s *StateSelect[T, R]) FetchRow(to FetchFunc) (*R, error) {
	if to == nil {
		to = s.getFetchFunc()
	}
	qr := model.CreateQuery(s.builder.Build(false))
	defer PutBuilder(s.builder)
	conn, cfg := s.Prepare(s.table.db.driver)
	row, err := qr.WrapQueryRow(s.ctx, conn, cfg)
	if err != nil || row == nil {
		return nil, err
	}
	target := new(R)
	err = row.Scan(to(target)...)
	return target, err
}

// One executes the query and returns the first row as a single result
// It automatically limits the query to one row for same-model queries
func (s *StateSelect[T, R]) One() (obj *R, err error) {
	if s.sameModel {
		s = s.Take(1)
	}
	obj, err = s.FetchRow(nil)
	if s.sameModel && err == nil {
		s.table.CacheOne(obj)
	}
	return
}

// IterRows returns an iterator over the query results
// It yields each row along with any error encountered
func (s *StateSelect[T, R]) IterRows(to FetchFunc) iter.Seq2[*R, error] {
	if to == nil {
		to = s.getFetchFunc()
	}
	qr := model.CreateQuery(s.builder.Build(false))
	defer PutBuilder(s.builder)
	conn, cfg := s.Prepare(s.table.db.driver)
	fet := &Fetcher[R]{
		Handler:   NewHandler(s.ctx, conn, cfg),
		NewTarget: func() *R { return new(R) },
		FetchTo:   to,
	}
	return fet.FetchResult(qr)
}

// All executes the query and returns all rows as a slice
// It pre-allocates the slice capacity if a limit is specified
func (s *StateSelect[T, R]) All() (res []*R, err error) {
	size := max(s.builder.Limit, 0)
	res = make([]*R, 0, size)
	var obj *R
	for obj, err = range s.IterRows(nil) {
		if err != nil {
			return
		}
		if obj != nil && s.sameModel {
			s.table.CacheOne(obj)
		}
		res = append(res, obj)
	}
	return
}

// Map executes the query and returns results as a map keyed by the specified column
// The key must be an integer column that exists in the table
func (s *StateSelect[T, R]) Map(key string) (map[int64]*R, error) {
	var col *Column
	if col = s.table.ColumnInfo(key); col == nil {
		return nil, model.NewColumnNotFoundError(key)
	}
	size := max(s.builder.Limit, 0)
	res := make(map[int64]*R, size)
	id, ok := int64(0), false
	for row, err := range s.IterRows(nil) {
		if err != nil {
			return nil, err
		}
		if s.sameModel && row != nil {
			id = s.table.CacheOne(row)
		}
		if id > 0 {
			res[id] = row
		} else if id, ok = col.GetInt64(row); ok {
			res[id] = row
		}
	}
	return res, nil
}

// Rank executes the query and returns results as a map keyed by the specified column
// Each key maps to a slice of results with that key value
func (s *StateSelect[T, R]) Rank(key string) (map[int64][]*R, error) {
	var col *Column
	if col = s.table.ColumnInfo(key); col == nil {
		return nil, model.NewColumnNotFoundError(key)
	}
	size := max(s.builder.Limit, 0)
	res := make(map[int64][]*R, size)
	id, ok := int64(0), false
	for row, err := range s.IterRows(nil) {
		if err != nil {
			return nil, err
		}
		if s.sameModel && row != nil {
			id = s.table.CacheOne(row)
		}
		if id > 0 {
			res[id] = append(res[id], row)
		} else if id, ok = col.GetInt64(row); ok {
			res[id] = append(res[id], row)
		}
	}
	return res, nil
}

// RollUP enables rollup for aggregation queries
// It adds ROLL UP clause to GROUP BY operations
//
// Example:
//
//	results, _ := db.Order.Select("status", "total").GroupBy("status").RollUP().All()
func (s *StateSelect[T, R]) RollUP() *StateSelect[T, R] {
	return s
}

// OnTransaction sets a transaction for the select query and enables FOR UPDATE lock
// It ensures the query runs within the specified transaction
func (s *StateSelect[T, R]) OnTransaction(tx model.Transaction) *StateSelect[T, R] {
	s.builder.ForUpdate = true
	s.StateWhere.conn = tx
	return s
}

// Filter adds filter conditions to the select query
// It appends the specified conditions to the WHERE clause
func (s *StateSelect[T, R]) Filter(args ...Condition) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Filter(args...)
	return s
}

// Where adds a WHERE clause to the select query
// It accepts a raw SQL WHERE clause with optional arguments
func (s *StateSelect[T, R]) Where(where string, args ...any) *StateSelect[T, R] {
	s.StateWhere = s.StateWhere.Where(where, args...)
	return s
}

// Match sets the WHERE conditions based on the non-zero fields of the given object
// It automatically generates conditions for fields with non-zero values
func (s *StateSelect[T, R]) Match(obj T) *StateSelect[T, R] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// OrderBy adds ORDER BY clauses to the query
// It accepts field names with optional DESC keyword for descending order
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

// GroupBy adds GROUP BY clauses to the query
// It groups results by the specified fields
func (s *StateSelect[T, R]) GroupBy(args ...string) *StateSelect[T, R] {
	for _, arg := range args {
		grp := &Group{Field: s.table.Field(arg), Having: Condition{}}
		s.builder.Groups = append(s.builder.Groups, grp)
	}
	return s
}

// Take limits the number of rows returned by the query
// It sets the LIMIT clause to the specified value
func (s *StateSelect[T, R]) Take(i int) *StateSelect[T, R] {
	if i >= TakeNoLimit {
		s.builder.Limit = i
	}
	return s
}

// Skip skips the specified number of rows in the result
// It sets the OFFSET clause to the specified value
func (s *StateSelect[T, R]) Skip(i int) *StateSelect[T, R] {
	if i >= 0 {
		s.builder.Offset = i
	}
	return s
}

// GetJoinForeign returns the foreign key relationship for the first joined table
// It looks up the foreign relationship in the table's foreign key registry
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

// GetJoinForeigns returns all foreign key relationships for the joined tables
// It looks up foreign relationships and creates them for joined tables
func (s *StateSelect[T, R]) GetJoinForeigns() []*Foreign {
	size := len(s.builder.Joins)
	if size == 0 {
		return nil
	}
	foreigns := make([]*Foreign, 0, size)
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
// It adds a JOIN clause with the specified join type and condition
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

// LeftJoin performs a LEFT JOIN with another table
// It automatically adds the joined table's columns to the select list
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

// Pagination holds paginated query results with metadata
// It provides information about total values, pages, and current page details
type Pagination[T, R any] struct {
	TotalValues int64 `json:"totalValues"` // Total number of values in the query result
	TotalPages  int   `json:"totalPages"`  // Total number of pages in the pagination

	PageValues int `json:"pageValues"` // Number of values on the current page
	PageSize   int `json:"pageSize"`   // Maximum number of values per page

	CurrentPage     int  `json:"currentPage"`     // Current page number
	HasPreviousPage bool `json:"hasPreviousPage"` // Whether there is a previous page
	PreviousPage    int  `json:"previousPage"`    // Previous page number
	HasNextPage     bool `json:"hasNextPage"`     // Whether there is a next page
	NextPage        int  `json:"nextPage"`        // Next page number

	StartIndex int  `json:"startIndex"` // Index of the first value on the current page
	EndIndex   int  `json:"endIndex"`   // Index of the last value on the current page
	Values     []*R `json:"values"`     // Slice of values on the current page
}

// Pagination returns a paginated query result
// It calculates total values, pages, and returns the current page data
// Default values for page and size are 1 and 10 respectively
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
