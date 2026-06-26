package goent_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/azhai/gobus/environ"
	"github.com/azhai/goent"
	"github.com/jackc/pgx/v5/pgxpool"
)

// =============================================
// Builder Pool Benchmarks (no DB I/O)
// =============================================

func BenchmarkBuilderPoolGetPut(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		builder := goent.GetBuilder()
		goent.PutBuilder(builder)
	}
}

func BenchmarkBuilderPoolGetOnly(b *testing.B) {
	b.ReportAllocs()
	builders := make([]*goent.Builder, 0, b.N)
	for b.Loop() {
		builders = append(builders, goent.GetBuilder())
	}
	_ = builders
}

func BenchmarkDeleteBuilderPoolGetPut(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		builder := goent.GetDeleteBuilder()
		goent.PutDeleteBuilder(builder)
	}
}

// =============================================
// Condition Building Benchmarks (no DB I/O)
// =============================================

func BenchmarkConditionEquals(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	field := db.Status.Field("id")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = goent.Equals(field, 42)
	}
}

func BenchmarkConditionAnd(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	f1 := db.Status.Field("id")
	f2 := db.Status.Field("name")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = goent.And(
			goent.Equals(f1, 1),
			goent.Like(f2, "%test%"),
		)
	}
}

func BenchmarkConditionOr(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	f1 := db.Status.Field("id")
	f2 := db.Status.Field("name")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = goent.Or(
			goent.Equals(f1, 1),
			goent.Like(f2, "%test%"),
		)
	}
}

func BenchmarkConditionIn(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	field := db.Status.Field("id")
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = goent.In(field, vals)
	}
}

func BenchmarkConditionComplex(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	f1 := db.Status.Field("id")
	f2 := db.Status.Field("name")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = goent.And(
			goent.Or(
				goent.Equals(f1, 1),
				goent.Equals(f1, 2),
			),
			goent.Like(f2, "%test%"),
			goent.Greater(f1, 0),
		)
	}
}

// =============================================
// NewValue Benchmarks
// =============================================

func BenchmarkNewValueInt64(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = goent.NewValue(int64(42))
	}
}

func BenchmarkNewValueString(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = goent.NewValue("hello world")
	}
}

func BenchmarkNewValueSliceInt64(b *testing.B) {
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	b.ReportAllocs()
	for b.Loop() {
		_ = goent.NewValue(vals)
	}
}

func BenchmarkNewValueSliceAny(b *testing.B) {
	vals := []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	b.ReportAllocs()
	for b.Loop() {
		_ = goent.NewValue(vals)
	}
}

func BenchmarkNewValueReflect(b *testing.B) {
	type customStruct struct{ X int }
	val := customStruct{X: 42}
	b.ReportAllocs()
	for b.Loop() {
		_ = goent.NewValueReflect(val)
	}
}

// =============================================
// Field Access Benchmarks
// =============================================

func BenchmarkFieldAccess(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.Field("id")
		_ = db.Status.Field("name")
	}
}

func BenchmarkFieldAccessParallel(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = db.Status.Field("id")
			_ = db.Status.Field("name")
		}
	})
}

// =============================================
// TableQuery Chain Benchmarks (no DB I/O for building)
// =============================================

func BenchmarkTableQueryFilter(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.Filter(goent.Equals(db.Status.Field("id"), 1))
	}
}

func BenchmarkTableQueryChainedFilter(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.
			Filter(goent.Greater(db.Status.Field("id"), 0)).
			Filter(goent.Less(db.Status.Field("id"), 100))
	}
}

func BenchmarkTableQueryWhere(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.Where("id > ?", 0)
	}
}

// =============================================
// Database Operation Benchmarks
// =============================================

func BenchmarkInsertOne(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s := &Status{Name: "Test"}
		_ = db.Status.Insert().One(s)
	}
}

func BenchmarkInsertOneFastPath(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s := &Status{Name: "Test"}
		_ = db.Status.InsertOne(s)
	}
}

func BenchmarkInsertBatch(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.Insert().All(false, data)
	}
}

func BenchmarkSelectOne(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	s := &Status{Name: "Test"}
	db.Status.Insert().One(s)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.Select().ByPK(s.ID)
	}
}

func BenchmarkFindByPK(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	s := &Status{Name: "Test"}
	db.Status.Insert().One(s)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.FindByPK(s.ID)
	}
}

func BenchmarkSelectFilter(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.Select().Filter(goent.Greater(db.Status.Field("id"), 0)).Take(10).All()
	}
}

func BenchmarkTableQueryCount(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.Count("id")
	}
}

func BenchmarkTableQueryFilterCount(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.Filter(goent.Greater(db.Status.Field("id"), 50)).Count("id")
	}
}

func BenchmarkTableQueryFilterSelect(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = db.Status.Filter(goent.Greater(db.Status.Field("id"), 50)).Select().Take(10).All()
	}
}

func BenchmarkUpdateOne(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	s := &Status{Name: "Test"}
	db.Status.Insert().One(s)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = db.Status.Update().
			Set(goent.Pair{Key: "name", Value: "Updated"}).
			Filter(goent.Equals(db.Status.Field("id"), s.ID)).
			Exec()
	}
}

func BenchmarkDeleteOne(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		db.Status.Delete().Exec()
		s := &Status{Name: "Test"}
		db.Status.Insert().One(s)
		b.StartTimer()
		_ = db.Status.Delete().ByPK(s.ID)
	}
}

// =============================================
// Concurrent Benchmarks
// =============================================

func BenchmarkConcurrentSelect(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = db.Status.Select().Take(10).All()
		}
	})
}

func BenchmarkConcurrentFilterCount(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	db.Status.Delete().Exec()
	data := make([]*Status, 100)
	for i := range data {
		data[i] = &Status{Name: fmt.Sprintf("Status %d", i)}
	}
	db.Status.Insert().All(false, data)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = db.Status.Filter(goent.Greater(db.Status.Field("id"), 50)).Count("id")
		}
	})
}

func BenchmarkConcurrentBuilderPool(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			builder := goent.GetBuilder()
			goent.PutBuilder(builder)
		}
	})
}

func BenchmarkConcurrentConditionBuild(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	f1 := db.Status.Field("id")
	f2 := db.Status.Field("name")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = goent.And(
				goent.Equals(f1, 1),
				goent.Like(f2, "%test%"),
			)
		}
	})
}

// =============================================
// ScanDest vs Reflection Benchmarks
// =============================================

func BenchmarkScanDestGenerated(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		s := &Status{Name: "Test"}
		_ = s.ScanDest()
	}
}

func BenchmarkScanDestReflection(b *testing.B) {
	db, err := Setup()
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	info := db.Status.TableInfo
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s := &Status{Name: "Test"}
		valueOf := reflect.ValueOf(s).Elem()
		_ = goent.AppendDestTable(info, valueOf)
	}
}

// =============================================
// Stress Benchmarks
// =============================================

func BenchmarkBuilderPoolStress(b *testing.B) {
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.GetBuilder()
			goent.PutBuilder(builder)
		}()
	}
	wg.Wait()
}

func BenchmarkDeleteBuilderPoolStress(b *testing.B) {
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			builder := goent.GetDeleteBuilder()
			goent.PutDeleteBuilder(builder)
		}()
	}
	wg.Wait()
}

// =============================================
// Raw pgxpool comparison for select-one
// =============================================

func getDSN() string {
	env := environ.NewEnvWithFile("../.env")
	dsn := env.Get("POSTGRES_DSN")
	if dsn == "" {
		dsn = env.Get("DB_DSN")
	}
	return dsn
}

func BenchmarkRawPgxPoolSelectOne(b *testing.B) {
	dsn := getDSN()
	if dsn == "" {
		b.Skip("Skipping: no DSN")
		return
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return
	}
	defer pool.Close()

	// Insert a test row
	var id int64
	err = pool.QueryRow(context.Background(),
		"INSERT INTO status (name) VALUES ($1) RETURNING id", "Test").Scan(&id)
	if err != nil {
		b.Fatal(err)
	}

	sql := "SELECT * FROM status WHERE id = $1"
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var s Status
		err := pool.QueryRow(context.Background(), sql, id).Scan(&s.ID, &s.Name)
		if err != nil {
			b.Fatal(err)
		}
	}
}
