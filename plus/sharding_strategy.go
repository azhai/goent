package plus

import (
	"fmt"
	"reflect"
	"time"
)

// ShardingStrategy defines how to resolve a shard table name from a shard value.
type ShardingStrategy interface {
	ResolveTableName(base string, shardValue any) string
	MatchPattern(base string) string
}

// ModuloHex shards tables by hex remainder of the shard key.
// Table name format: base_XX where XX is lowercase hex (2 digits).
// Bits=4 → 16 tables (_00.._0f), Bits=6 → 64 (_00.._3f), Bits=8 → 256 (_00.._ff).
type ModuloHex struct {
	Bits int
}

func (m ModuloHex) ResolveTableName(base string, shardValue any) string {
	n := toInt64(shardValue)
	mask := int64(1<<m.Bits) - 1
	return fmt.Sprintf("%s_%02x", base, n&mask)
}

func (m ModuloHex) MatchPattern(base string) string {
	return base + "_*"
}

// TimeGranularity defines time-based sharding granularity.
type TimeGranularity int

const (
	ByDay   TimeGranularity = iota // base_YYYYMMDD
	ByMonth                        // base_YYYYMM
	ByYear                         // base_YYYY
)

func (g TimeGranularity) ResolveTableName(base string, shardValue any) string {
	tm := toTime(shardValue)
	switch g {
	case ByDay:
		return base + "_" + tm.Format("20060102")
	case ByMonth:
		return base + "_" + tm.Format("200601")
	case ByYear:
		return base + "_" + tm.Format("2006")
	}
	return base
}

func (g TimeGranularity) MatchPattern(base string) string {
	return base + "_*"
}

func toInt64(val any) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	default:
		rv := reflect.ValueOf(val)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(rv.Uint())
		case reflect.Float32:
			return int64(rv.Float())
		case reflect.Float64:
			return int64(rv.Float())
		}
		return 0
	}
}

func toTime(val any) time.Time {
	switch v := val.(type) {
	case time.Time:
		return v
	case nil:
		return time.Time{}
	default:
		rv := reflect.ValueOf(val)
		if !rv.IsValid() {
			return time.Time{}
		}
		if rv.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return rv.Convert(reflect.TypeOf(time.Time{})).Interface().(time.Time)
		}
		return time.Time{}
	}
}
