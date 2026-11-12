//go:build nopgsql

package goent

import (
	"context"

	"github.com/azhai/goent/model"
)

// newPgSchemaDriver 是 `-tags nopgsql` 下的空实现，返回 nil。
//
// 这条分支在该构建里取不到：没有 pgsql 驱动，DriverName() 不会是 "PostgreSQL"
// （除非调用方自己塞了一个 PG 驱动进来 —— 那种用法要换回默认构建）。
func newPgSchemaDriver(_ string,
	_ func(ctx context.Context, sql string, args ...any) (model.Rows, error),
	_ func(ctx context.Context, sql string, args ...any) error) model.Structurer {
	return nil
}
