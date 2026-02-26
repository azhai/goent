package benchmark

import (
	"testing"

	"github.com/azhai/goent/tests/benchmark/tools"
)

// Benchmark interface was inspired by https://github.com/efectn/go-orm-benchmarks/blob/master/helper/suite.go.
type Benchmark interface {
	Init() error
	Close() error
	Insert(b *testing.B)
	InsertBulk(b *testing.B)
	Update(b *testing.B)
	Delete(b *testing.B)
	FindByID(b *testing.B)
	FindPage(b *testing.B)
}

func BeforeBenchmark() {
	tools.RecreateDatabase()
}

type ResultWrapper struct {
	Orm        string
	Benchmarks map[string]testing.BenchmarkResult
	Err        error
}
