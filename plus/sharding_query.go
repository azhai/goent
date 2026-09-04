package plus

import (
	"reflect"
	"sort"

	"github.com/azhai/goent"
)

// SelectAll queries all matched sub-tables and returns combined results.
func (s *ShardedTable[T]) SelectAll() ([]*T, error) {
	var all []*T
	err := s.EachTable(func(tbl *goent.Table[T]) error {
		rows, err := tbl.Select().All()
		if err != nil {
			return err
		}
		all = append(all, rows...)
		return nil
	})
	return all, err
}

// CountAll counts total rows across all matched sub-tables.
func (s *ShardedTable[T]) CountAll() (int64, error) {
	var total int64
	err := s.EachTable(func(tbl *goent.Table[T]) error {
		count, err := tbl.Select().Count("id")
		if err != nil {
			return err
		}
		total += count
		return nil
	})
	return total, err
}

// PageResult holds paginated query results across sharded tables.
type PageResult[T any] struct {
	Data  []*T
	Total int64
	Page  int
	Size  int
}

// SelectPage performs paginated query across all matched sub-tables.
// It collects results from each table, sorts by ID, then applies pagination.
func (s *ShardedTable[T]) SelectPage(page, size int) (*PageResult[T], error) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 10
	}

	all, err := s.SelectAll()
	if err != nil {
		return nil, err
	}

	total := int64(len(all))
	start := (page - 1) * size
	end := start + size
	if start >= len(all) {
		return &PageResult[T]{Data: []*T{}, Total: total, Page: page, Size: size}, nil
	}
	if end > len(all) {
		end = len(all)
	}

	sorted := sortById(all)
	paged := sorted[start:end]
	return &PageResult[T]{Data: paged, Total: total, Page: page, Size: size}, nil
}

func sortById[T any](items []*T) []*T {
	sorted := make([]*T, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		idI := getPrimaryKeyValue(sorted[i])
		idJ := getPrimaryKeyValue(sorted[j])
		return idI < idJ
	})
	return sorted
}

type pkGetter interface {
	GetID() int64
}

// getPrimaryKeyValue returns the primary key of item for ordering purposes.
//
// It first tries the GetID() int64 method and falls back to reflecting over an
// "ID" field. Returning 0 for every row would make SelectPage silently return
// an arbitrary order, so the fallback supports the common int/uint/string-free
// int primary keys.
func getPrimaryKeyValue(item any) int64 {
	if g, ok := item.(pkGetter); ok {
		return g.GetID()
	}
	return reflectIDValue(item)
}

func reflectIDValue(item any) int64 {
	val := reflect.ValueOf(item)
	for val.Kind() == reflect.Pointer {
		if val.IsNil() {
			return 0
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return 0
	}
	field := val.FieldByName("ID")
	if !field.IsValid() {
		return 0
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(field.Uint())
	}
	return 0
}
