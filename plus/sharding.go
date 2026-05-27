package plus

import (
	"context"
	"reflect"
	"sync"

	"github.com/azhai/goent"
)

// ShardedTable provides automatic table routing based on a shard key.
// It manages multiple sub-tables lazily and routes operations to the correct one.
type ShardedTable[T any] struct {
	db       *goent.DB
	baseName string
	shardKey string
	strategy ShardingStrategy
	cache    *TableCache
	tables   sync.Map
}

// NewShardedTable creates a new ShardedTable instance.
func NewShardedTable[T any](db *goent.DB, baseName, shardKey string,
	strategy ShardingStrategy, cache *TableCache) *ShardedTable[T] {
	return &ShardedTable[T]{
		db:       db,
		baseName: baseName,
		shardKey: shardKey,
		strategy: strategy,
		cache:    cache,
	}
}

// ResolveTable resolves and returns the sub-table for the given shard value.
func (s *ShardedTable[T]) ResolveTable(shardValue any) *goent.Table[T] {
	tableName := s.strategy.ResolveTableName(s.baseName, shardValue)
	if v, ok := s.tables.Load(tableName); ok {
		return v.(*goent.Table[T])
	}
	tbl := goent.SimpleTable[T](s.db, tableName, "")
	actual, _ := s.tables.LoadOrStore(tableName, tbl)
	return actual.(*goent.Table[T])
}

// BaseName returns the base table name (without suffix).
func (s *ShardedTable[T]) BaseName() string {
	return s.baseName
}

// Strategy returns the sharding strategy.
func (s *ShardedTable[T]) Strategy() ShardingStrategy {
	return s.strategy
}

// Cache returns the table metadata cache.
func (s *ShardedTable[T]) Cache() *TableCache {
	return s.cache
}

// RefreshCache refreshes the table metadata cache from the database.
func (s *ShardedTable[T]) RefreshCache(ctx context.Context) error {
	return s.cache.Refresh(ctx, s.db)
}

// EachTable iterates over all matched sub-tables and calls fn for each.
func (s *ShardedTable[T]) EachTable(fn func(*goent.Table[T]) error) error {
	pattern := s.strategy.MatchPattern(s.baseName)
	tableNames := s.cache.Match(pattern)
	for _, name := range tableNames {
		tbl := s.getOrCreate(name)
		if err := fn(tbl); err != nil {
			return err
		}
	}
	return nil
}

// Insert inserts a row into the correct sub-table determined by the shard key value.
func (s *ShardedTable[T]) Insert(row *T) error {
	shardValue := s.extractShardValue(row)
	tbl := s.ResolveTable(shardValue)
	return tbl.Insert().One(row)
}

// FindByPK finds a row by primary key in the sub-table for the given shard value.
func (s *ShardedTable[T]) FindByPK(shardValue any, pk any) (*T, error) {
	tbl := s.ResolveTable(shardValue)
	return tbl.Select().Filter(goent.Equals(tbl.Field("id"), pk)).One()
}

// DeleteByPK deletes a row by primary key from the sub-table for the given shard value.
func (s *ShardedTable[T]) DeleteByPK(shardValue any) error {
	tbl := s.ResolveTable(shardValue)
	return tbl.Delete().Filter(goent.Equals(tbl.Field("id"), shardValue)).Exec()
}

// Update updates a row in the correct sub-table by primary key.
func (s *ShardedTable[T]) Update(shardValue any, pairs ...goent.Pair) error {
	tbl := s.ResolveTable(shardValue)
	return tbl.Update().Set(pairs...).Filter(goent.Equals(tbl.Field("id"), shardValue)).Exec()
}

// FindOne finds a single row matching filters in the sub-table for the given shard value.
func (s *ShardedTable[T]) FindOne(shardValue any, filters ...goent.Condition) (*T, error) {
	tbl := s.ResolveTable(shardValue)
	return tbl.Select().Filter(filters...).One()
}

func (s *ShardedTable[T]) getOrCreate(tableName string) *goent.Table[T] {
	if v, ok := s.tables.Load(tableName); ok {
		return v.(*goent.Table[T])
	}
	tbl := goent.SimpleTable[T](s.db, tableName, "")
	actual, _ := s.tables.LoadOrStore(tableName, tbl)
	return actual.(*goent.Table[T])
}

func (s *ShardedTable[T]) extractShardValue(row *T) any {
	val := reflect.ValueOf(row).Elem()
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		colName := toSnakeCase(field.Name)
		if colName == s.shardKey {
			fv := val.Field(i)
			if fv.CanInterface() {
				return fv.Interface()
			}
		}
	}
	return nil
}

func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				prev := s[i-1]
				if prev >= 'a' && prev <= 'z' {
					result = append(result, '_')
				} else if i+1 < len(s) && s[i+1] >= 'a' && s[i+1] <= 'z' {
					result = append(result, '_')
				}
			}
			result = append(result, r+32)
		} else {
			result = append(result, r)
		}
	}
	return string(result)
}
