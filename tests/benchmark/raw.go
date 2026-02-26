package benchmark

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/azhai/goent/tests/benchmark/models"
	tools2 "github.com/azhai/goent/tests/benchmark/tools"
)

type RawBenchmark struct {
	db *sql.DB
}

func NewRawBenchmark() Benchmark {
	return &RawBenchmark{}
}

func (r *RawBenchmark) Init() error {
	var err error
	r.db, err = sql.Open("pgx", tools2.PostgresDSN)
	return err
}

func (r *RawBenchmark) Close() error {
	return r.db.Close()
}

func (r *RawBenchmark) Insert(b *testing.B) {
	book := models.NewBook()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := r.db.Exec(tools2.InsertQuery,
			book.ISBN, book.Title, book.Author, book.Genre, book.Quantity, book.PublicizedAt)

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()
	}
}

func (r *RawBenchmark) InsertBulk(b *testing.B) {
	books := models.NewBooks(tools2.BulkInsertNumber)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := r.doInsertBulk(books)

		if err != nil {
			b.Error(err)
		}
	}
}

func (r *RawBenchmark) Update(b *testing.B) {
	book := models.NewBook()
	var id int64
	err := r.db.QueryRow(tools2.InsertReturningIDQuery,
		book.ISBN, book.Title, book.Author, book.Genre, book.Quantity, book.PublicizedAt).Scan(&id)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = r.db.Exec(tools2.UpdateQuery,
			book.ISBN, book.Title, book.Author, book.Genre, book.Quantity, book.PublicizedAt, id)

		if err != nil {
			b.Error(err)
		}
	}
}

func (r *RawBenchmark) Delete(b *testing.B) {
	n := b.N
	book := models.NewBook()
	bookIDs := make([]int64, 0, n)
	for range n {
		var id int64
		err := r.db.QueryRow(tools2.InsertReturningIDQuery,
			book.ISBN, book.Title, book.Author, book.Genre, book.Quantity, book.PublicizedAt).Scan(&id)
		if err != nil {
			b.Error(err)
		}
		bookIDs = append(bookIDs, id)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var bookID int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		bookID = bookIDs[i]
		b.StartTimer()

		_, err := r.db.Exec(tools2.DeleteQuery, bookID)

		if err != nil {
			b.Error(err)
		}
	}
}

func (r *RawBenchmark) FindByID(b *testing.B) {
	book := models.NewBook()
	var id int64
	err := r.db.QueryRow(tools2.InsertReturningIDQuery,
		book.ISBN, book.Title, book.Author, book.Genre, book.Quantity, book.PublicizedAt).Scan(&id)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for range tools2.FindOneLoop {
			var foundBook models.Book
			err := r.db.QueryRow(tools2.SelectByIDQuery, id).Scan(
				&foundBook.ID,
				&foundBook.ISBN,
				&foundBook.Title,
				&foundBook.Author,
				&foundBook.Genre,
				&foundBook.Quantity,
				&foundBook.PublicizedAt,
			)

			// checking the error will count on raw benchmarks
			if err != nil {
				b.Error(err)
			}
		}
	}
}

var booksPage []models.Book

func (r *RawBenchmark) FindPage(b *testing.B) {
	books := models.NewBooks(tools2.BulkInsertPageNumber)
	batches := models.Chunk(books, tools2.BatchSize)
	for _, batch := range batches {
		if err := r.doInsertBulk(batch); err != nil {
			b.Error(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for s := 0; s < tools2.BulkInsertPageNumber; s = s + tools2.PageSize {
			// making slices will count on raw benchmarks
			booksPage = make([]models.Book, 0, tools2.PageSize)

			rows, err := r.db.Query(tools2.SelectPaginatingQuery, s, tools2.PageSize)

			// checking the error will count on raw benchmarks
			if err != nil {
				b.Error(err)
			}

			for rows.Next() {
				var book models.Book
				if err = rows.Scan(
					&book.ID,
					&book.ISBN,
					&book.Title,
					&book.Author,
					&book.Genre,
					&book.Quantity,
					&book.PublicizedAt,
				); err != nil {
					b.Error(err)
				}
				booksPage = append(booksPage, book)
			}
		}
	}
}

func (r *RawBenchmark) doInsertBulk(books []*models.Book) error {
	valueStrings := make([]string, 0, len(books))
	valueArgs := make([]any, 0, len(books)*6)

	start := 1

	for _, book := range books {
		placeholders := make([]string, 0, 6)
		for range 6 {
			placeholders = append(placeholders, fmt.Sprintf("$%d", start))
			start++
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ",")+")")
		valueArgs = append(valueArgs, book.ISBN)
		valueArgs = append(valueArgs, book.Title)
		valueArgs = append(valueArgs, book.Author)
		valueArgs = append(valueArgs, book.Genre)
		valueArgs = append(valueArgs, book.Quantity)
		valueArgs = append(valueArgs, book.PublicizedAt)
	}

	query := fmt.Sprintf(tools2.InsertBulkQuery, strings.Join(valueStrings, ","))

	_, err := r.db.Exec(query, valueArgs...)

	return err
}
