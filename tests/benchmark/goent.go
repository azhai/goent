package benchmark

import (
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/tests/benchmark/models"
	"github.com/azhai/goent/tests/benchmark/tools"
)

type PublicSchema struct {
	Book *goent.Table[models.Book]
}

type GoentDatabase struct {
	PublicSchema `goe:"public"`
	*goent.DB
}

type GoentBenchmark struct {
	db *GoentDatabase
}

func NewGoentBenchmark() Benchmark {
	return &GoentBenchmark{}
}

func (o *GoentBenchmark) Init() (err error) {
	driver := pgsql.OpenDSN(tools.PostgresDSN)
	o.db, err = goent.Open[GoentDatabase](driver, "")
	// if err == nil {
	// 	err = goent.AutoMigrate(o.db)
	// }
	return err
}

func (o *GoentBenchmark) Close() error {
	return goent.Close(o.db)
}

func (o *GoentBenchmark) Insert(b *testing.B) {
	book := models.NewBook()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		book.ID = 0
		b.StartTimer()

		err := o.db.Book.Insert().One(book)

		b.StopTimer()
		if err != nil {
			return
		}
		b.StartTimer()
	}
}

func (o *GoentBenchmark) InsertBulk(b *testing.B) {
	books := models.NewBooks(tools.BulkInsertNumber)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for i := range books {
			books[i].ID = 0
		}
		b.StartTimer()

		err := o.db.Book.Insert().All(false, books)

		b.StopTimer()
		if err != nil {
			return
		}
		b.StartTimer()
	}
}

func (o *GoentBenchmark) Update(b *testing.B) {
	book := models.NewBook()

	err := o.db.Book.Insert().One(book)
	if err != nil {
		return
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = o.db.Book.Save().One(book)

		b.StopTimer()
		if err != nil {
			return
		}
		b.StartTimer()
	}
}

func (o *GoentBenchmark) Delete(b *testing.B) {
	if o.db == nil || o.db.Book == nil {
		return
	}
	n := b.N
	books := models.NewBooks(n)

	err := o.db.Book.Insert().All(false, books)
	if err != nil {
		return
	}

	b.ReportAllocs()
	b.ResetTimer()

	var bookID int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bookID = books[i].ID
		b.StartTimer()

		filter := goent.Greater(o.db.Book.Field("id"), bookID)
		err = o.db.Book.Delete().Filter(filter).Exec()

		b.StopTimer()
		if err != nil {
			return
		}
		b.StartTimer()
	}
}

func (o *GoentBenchmark) FindByID(b *testing.B) {
	book := models.NewBook()

	err := o.db.Book.Insert().One(book)
	if err != nil {
		return
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for range tools.FindOneLoop {
			_, err = o.db.Book.Select().Match(models.Book{ID: book.ID}).One()

			b.StopTimer()
			if err != nil {
				return
			}
			b.StartTimer()
		}
	}
}

func (o *GoentBenchmark) FindPage(b *testing.B) {
	books := models.NewBooks(tools.BulkInsertPageNumber)

	err := o.db.Book.Insert().All(false, books)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	// fetchTo := models.FetchBook()

	for i := 0; i < b.N; i++ {
		for s := int64(0); s < tools.BulkInsertPageNumber; s = s + tools.PageSize {
			filter := goent.Greater(o.db.Book.Field("id"), s)
			query := o.db.Book.Select().Filter(filter).Take(tools.PageSize)
			// _, err = query.QueryRows(fetchTo)
			_, err = query.All()

			b.StopTimer()
			if err != nil {
				b.Error(err)
			}
			b.StartTimer()
		}
	}
}
