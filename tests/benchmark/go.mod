module github.com/azhai/goent/tests/benchmark

go 1.24

require (
	github.com/azhai/goent v0.0.0
	github.com/azhai/gobus v0.2.1
	github.com/go-goe/goe v0.7.2
	github.com/go-goe/postgres v0.5.2
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/shopspring/decimal v1.6.0
	modernc.org/sqlite v1.60.1
)

replace github.com/azhai/goent => ../..

replace github.com/azhai/gobus => ../../../gobus
