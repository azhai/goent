//go:build nopgsql

package drivers

import "github.com/azhai/goent/model"

// openPgDialect 是 `-tags nopgsql` 下的空实现：这个构建里没有 pgsql 驱动，
// 于是 pgx 不被链进二进制。返回 nil 表示「打不开」，调用方本就判 nil
// （见 Connect）；传 pgsql/postgres 类型到这个构建属于配置错误，会明确报错。
func openPgDialect(_ string) model.Driver { return nil }
