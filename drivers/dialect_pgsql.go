//go:build !nopgsql

package drivers

import (
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/model"
)

// openPgDialect 打开 PostgreSQL 驱动。
//
// 单独成文件只为让 `-tags nopgsql` 能换成另一个实现（dialect_nopgsql.go）：
// pgsql 驱动链着 pgx（约 1.1MB 机器码），只连 sqlite 的程序没必要背它。
// 默认构建（不带该标签）行为不变，仍是这个实现。
func openPgDialect(dsn string) model.Driver { return pgsql.OpenDSN(dsn) }
