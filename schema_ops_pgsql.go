//go:build !nopgsql

package goent

import (
	"context"

	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/model"
)

// newPgSchemaDriver 造 PostgreSQL 的 schema 驱动（读列/索引/主键/外键）。
//
// 与 drivers.openPgDialect 一样单独成文件，供 `-tags nopgsql` 整体换掉，
// 免得这个包也把 pgx 拖进二进制。默认构建行为不变。
func newPgSchemaDriver(schema string,
	queryFn func(ctx context.Context, sql string, args ...any) (model.Rows, error),
	execFn func(ctx context.Context, sql string, args ...any) error) model.Structurer {
	return pgsql.NewPgSchemaDriver(schema, queryFn, execFn)
}
