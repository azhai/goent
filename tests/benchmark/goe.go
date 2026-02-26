package benchmark

import (
	"testing"

	"github.com/azhai/goent/tests/benchmark/models"
	"github.com/azhai/goent/tests/benchmark/tools"
	"github.com/go-goe/goe"
	"github.com/go-goe/goe/query/where"
	"github.com/go-goe/postgres"
)

type Database struct {
	Book *models.Book
	*goe.DB
}

type GoeBenchmark struct {
	db *Database
}

func NewGoeBenchmark() Benchmark {
	return &GoeBenchmark{}
}

func (o *GoeBenchmark) Init() (err error) {
	o.db, err = goe.Open[Database](postgres.Open(tools.PostgresDSN, postgres.NewConfig(postgres.Config{})))
	return err
}

func (o *GoeBenchmark) Close() error {
	return goe.Close(o.db)
}

func (o *GoeBenchmark) Insert(b *testing.B) {
	book := models.NewBook()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		book.ID = 0
		b.StartTimer()

		err := goe.Insert(o.db.Book).One(book)

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()
	}
}

func (o *GoeBenchmark) InsertBulk(b *testing.B) {
	books := models.NewBooksNoPtr(tools.BulkInsertNumber)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for i := range books {
			books[i].ID = 0
		}
		b.StartTimer()

		err := goe.Insert(o.db.Book).All(books)

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()
	}
}

func (o *GoeBenchmark) Update(b *testing.B) {
	book := models.NewBook()

	err := goe.Insert(o.db.Book).One(book)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err = goe.Save(o.db.Book).One(*book)

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()
	}
}

func (o *GoeBenchmark) Delete(b *testing.B) {
	n := b.N
	books := models.NewBooksNoPtr(n)

	err := goe.Insert(o.db.Book).All(books)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var bookID int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bookID = books[i].ID
		b.StartTimer()

		err = goe.Delete(o.db.Book).Where(where.Equals(&o.db.Book.ID, bookID))

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()
	}
}

func (o *GoeBenchmark) FindByID(b *testing.B) {
	book := models.NewBook()

	err := goe.Insert(o.db.Book).One(book)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for range tools.FindOneLoop {
			_, err = goe.Find(o.db.Book).ByValue(models.Book{ID: book.ID})

			b.StopTimer()
			if err != nil {
				b.Error(err)
			}
			b.StartTimer()
		}
	}
}

func (o *GoeBenchmark) FindPage(b *testing.B) {
	books := models.NewBooksNoPtr(tools.BulkInsertPageNumber)

	err := goe.Insert(o.db.Book).All(books)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for s := int64(0); s < tools.BulkInsertPageNumber; s = s + tools.PageSize {
			_, err = goe.List(o.db.Book).Take(tools.PageSize).Where(where.Greater(&o.db.Book.ID, s)).AsSlice()

			b.StopTimer()
			if err != nil {
				b.Error(err)
			}
			b.StartTimer()
		}
	}
}
