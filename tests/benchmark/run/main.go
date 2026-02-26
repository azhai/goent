package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/azhai/goent/tests/benchmark"
)

const (
	all = "all"

	insertOp     = "insert"
	insertBulkOp = "insert-bulk"
	updateOp     = "update"
	deleteOp     = "delete"
	selectOne    = "select-one"
	selectPage   = "select-page"

	rawName   = "raw"
	goentName = "goent"
	goeName   = "goe"
)

var (
	benchmarksMap   = map[string]benchmark.Benchmark{}
	validOperations = []string{insertOp, insertBulkOp, updateOp, deleteOp, selectOne, selectPage}
)

func loadBenchmarks() {
	benchmarksMap[rawName] = benchmark.NewRawBenchmark()
	benchmarksMap[goentName] = benchmark.NewGoentBenchmark()
	benchmarksMap[goeName] = benchmark.NewGoeBenchmark()
}

func main() {
	operation := flag.String("operation", selectOne, "Specify the operation to run")
	orm := flag.String("orm", "", "Specify the orm to run")
	format := flag.String("format", "both", "Output format: table or markdown")
	flag.Parse()

	if operation == nil || *operation != all && !slices.Contains(validOperations, *operation) {
		log.Fatal("define a valid orm or operation")
		return
	}
	run(*orm, *operation, *format)
}

func run(orm, operation, format string) {
	loadBenchmarks()
	shuffleBenchmarksMap()
	var results []benchmark.ResultWrapper
	if orm != "" {
		res := doExecuteBenchmarks(benchmarksMap[orm], orm, operation)
		results = append(results, res)
	} else {
		results = executeBenchmarks(operation)
	}
	if format == "both" || format == "table" {
		printBenchmark(results, operation)
	}
	if format == "both" || format == "markdown" {
		sortResultsForMarkdown(&results)
		printMarkdown(results, validOperations...)
	}
}

func shuffleBenchmarksMap() {
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	keys := make([]string, 0, len(benchmarksMap))
	for key := range benchmarksMap {
		keys = append(keys, key)
	}
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	shuffledMap := make(map[string]benchmark.Benchmark)
	for _, key := range keys {
		shuffledMap[key] = benchmarksMap[key]
	}
	benchmarksMap = shuffledMap
}

func executeBenchmarks(operation string) []benchmark.ResultWrapper {
	var results []benchmark.ResultWrapper
	for ormName, b := range benchmarksMap {
		results = append(results, doExecuteBenchmarks(b, ormName, operation))
	}
	return results
}

func doExecuteBenchmarks(b benchmark.Benchmark, orm, operation string) benchmark.ResultWrapper {
	benchmark.BeforeBenchmark()
	wrapper := benchmark.ResultWrapper{}
	wrapper.Orm = orm
	err := b.Init()
	if err != nil {
		wrapper.Err = err
		return wrapper
	}
	resultMap := make(map[string]testing.BenchmarkResult)
	operations := map[string]func(*testing.B){
		insertOp:     b.Insert,
		insertBulkOp: b.InsertBulk,
		updateOp:     b.Update,
		deleteOp:     b.Delete,
		selectOne:    b.FindByID,
		selectPage:   b.FindPage,
	}
	if operation == all {
		for op, f := range operations {
			resultMap[op] = testing.Benchmark(f)
		}
		wrapper.Benchmarks = resultMap
		return wrapper
	}
	f, ok := operations[operation]
	if !ok || f == nil {
		wrapper.Err = fmt.Errorf("invalid operation: %s", operation)
		return wrapper
	}
	wrapper.Benchmarks = map[string]testing.BenchmarkResult{
		operation: testing.Benchmark(f),
	}
	return wrapper
}

func printBenchmark(results []benchmark.ResultWrapper, operation string) {
	table := new(tabwriter.Writer)
	table.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	if operation == all {
		doPrintBenchmark(table, results, validOperations...)
	} else {
		doPrintBenchmark(table, results, operation)
	}
}

func doPrintBenchmark(table *tabwriter.Writer, results []benchmark.ResultWrapper, operations ...string) {
	for _, op := range operations {
		_, _ = fmt.Fprint(table, "\n")
		_, _ = fmt.Fprintf(table, "Operation: %s\n", op)

		for _, r := range results {
			result, ok := r.Benchmarks[op]
			if !ok {
				continue
			}
			_, _ = fmt.Fprintf(table, "%s:\t%d\t%d ns/op\t%d B/op\t%d allocs/op\n",
				r.Orm,
				result.N,
				result.NsPerOp(),
				result.AllocedBytesPerOp(),
				result.AllocsPerOp(),
			)
		}

		_ = table.Flush()
	}
}

func sortResultsForMarkdown(results *[]benchmark.ResultWrapper) {
	ormOrder := map[string]int{"raw": 0, "goent": 1, "goe": 2}
	slices.SortFunc(*results, func(a, b benchmark.ResultWrapper) int {
		orderA := ormOrder[a.Orm]
		orderB := ormOrder[b.Orm]
		if orderA != orderB {
			return orderA - orderB
		}
		return 0
	})
}

func printMarkdown(results []benchmark.ResultWrapper, operations ...string) {
	fmt.Println("\n| Operation       | Package |    N    | Avg ns/op |  Avg B/op | Avg allocs/op |  percent  |")
	fmt.Println("|-----------------|---------|--------:|----------:|----------:|--------------:|----------:|")
	tpl := "| %-15s | %-7s | %7d | %9d | %9d | %13d | %9s |\n"
	for _, op := range operations {
		var npo int64
		for i, r := range results {
			result, ok := r.Benchmarks[op]
			if !ok {
				continue
			}
			first, opStr := false, ""
			if i == 0 {
				first, npo = true, 0
				opStr = fmt.Sprintf("**%s**", op)
			}
			var values []any
			npo, values = getRowValues(result, first, npo)
			values = append([]any{opStr, r.Orm}, values...)
			fmt.Printf(tpl, values...)
		}
	}
	fmt.Println("")
}

func getRowValues(result testing.BenchmarkResult, first bool, npo int64) (int64, []any) {
	var centStr string
	if first {
		centStr, npo = "=", result.NsPerOp()
	} else if npo > 0 {
		kilo := result.NsPerOp() * 1000 / npo
		centStr = fmt.Sprintf("%6.1f%%", float64(kilo-1000)/10.0)
	}
	values := []any{result.N, result.NsPerOp(),
		result.AllocedBytesPerOp(),
		result.AllocsPerOp(), centStr}
	return npo, values
}
