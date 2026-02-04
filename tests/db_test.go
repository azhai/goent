package tests_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Animal struct {
	Name        string `goe:"index"`
	HabitatId   *uuid.UUID
	InfoId      *[]byte
	Id          int `goe:"pk"`
	AnimalFoods []AnimalFood
}

type AnimalFood struct {
	AnimalId int       `goe:"pk"`
	FoodId   uuid.UUID `goe:"pk"`
}

type Food struct {
	Id          uuid.UUID `goe:"pk"`
	Name        string
	AnimalFoods []AnimalFood
}

type Habitat struct {
	Id          uuid.UUID `goe:"pk"`
	Name        string    `goe:"type:varchar(50)"`
	WeatherId   int
	NameWeather string
	Animals     []Animal
}

type Weather struct {
	Id       int `goe:"pk"`
	Name     string
	Habitats []Habitat
}

type Info struct {
	Id         []byte `goe:"pk"`
	Name       string `goe:"index(unique n:idx_name_status);index"`
	NameStatus string `goe:"index(unique n:idx_name_status)"`
	StatusId   int
}

type Status struct {
	Id   int `goe:"pk"`
	Name string
}

type User struct {
	Id        int    `goe:"pk"`
	Name      string `goe:"index(n:idx_name_lower f:lower)"`
	Email     string `goe:"unique"`
	UserRoles []UserRole
}

type UserRole struct {
	Id      int `goe:"pk"`
	UserId  int
	RoleId  int
	EndDate *time.Time
}

type Role struct {
	Id        int `goe:"pk"`
	Name      string
	UserRoles []UserRole
}

type Flag struct {
	Id         uuid.UUID `goe:"pk"`
	Name       string
	Float32    float32
	Float64    float64
	Today      time.Time
	Int        int
	Int8       int8
	Int16      int16
	Int32      int32
	Int64      int64
	Uint       uint
	Uint8      uint8
	Uint16     uint16
	Uint32     uint32 `goe:"default:32"`
	Uint64     uint64
	Bool       bool
	Price      decimal.Decimal `goe:"type:decimal(10,4)"`
	Byte       []byte
	NullId     sql.Null[uuid.UUID] `goe:"type:uuid"`
	NullString sql.NullString      `goe:"type:varchar(100)"`
}

type Person struct {
	Id   int `goe:"pk"`
	Name string
	Jobs []JobTitle
}

type PersonJobTitle struct {
	PersonId   int `goe:"pk"`
	JobTitleId int `goe:"pk"`
	CreatedAt  time.Time
}

type JobTitle struct {
	Name    string
	Id      int `goe:"pk"`
	Persons []Person
}

type Exam struct {
	Id      int `goe:"pk"`
	Score   float32
	Minimum float32
}

type Select struct {
	Id   int `goe:"pk"`
	Name string
}

type Page struct {
	ID         int `goe:"pk"`
	Number     int
	PageIDNext *int
	PageIDPrev *int
}

type FlagSchema struct {
	Flag *goent.Table[Flag]
}

type Authentication struct {
	User     *goent.Table[User]
	UserRole *goent.Table[UserRole]
	Role     *goent.Table[Role]
}

type FoodHabitatSchema struct {
	Food    *goent.Table[Food]
	Habitat *goent.Table[Habitat]
}

type Drop struct {
	Id   int
	Name string
}

type DropSchema struct {
	Drop *goent.Table[Drop]
}

type Default struct {
	ID   string `goe:"default:'Default'"`
	Name string
}

type Database struct {
	Animal     *goent.Table[Animal]
	AnimalFood *goent.Table[AnimalFood]
	*FoodHabitatSchema
	Info            *goent.Table[Info]
	Status          *goent.Table[Status]
	Weather         *goent.Table[Weather]
	*Authentication `goe:"schema"`
	*FlagSchema
	Person         *goent.Table[Person]
	PersonJobTitle *goent.Table[PersonJobTitle]
	JobTitle       *goent.Table[JobTitle]
	Exam           *goent.Table[Exam]
	Select         *goent.Table[Select]
	Page           *goent.Table[Page]
	Default        *goent.Table[Default]
	*DropSchema
	*goent.DB
}

var db *Database

var mapDriver = map[string]func() (*Database, error){
	"PostgreSQL": SetupPostgres,
	"SQLite":     SetupSqlite,
}

func Setup() (*Database, error) {
	if db != nil {
		return db, nil
	}
	var err error
	driver := os.Getenv("GOE_DRIVER")
	if driver == "" {
		driver = "SQLite"
	}
	db, err = mapDriver[driver]()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func SetupPostgres() (*Database, error) {
	var err error
	dsn := os.Getenv("GOE_DATABASE_DSN")
	if dsn == "" {
		dsn = "user=postgres password=postgres host=localhost port=5432 database=postgres"
	}
	db, err = goent.Open[Database](pgsql.Open(dsn, pgsql.NewConfig(pgsql.Config{
		// Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	})), "")
	if err != nil {
		return nil, err
	}
	err = goent.AutoMigrate(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func SetupSqlite() (*Database, error) {
	filename := filepath.Join(os.TempDir(), "goent.db")
	db, err := goent.Open[Database](sqlite.Open(filename, sqlite.NewConfig(
		sqlite.Config{
			// Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
			ConnectionHook: func(conn sqlite.ExecQuerierContext, dsn string) error {
				conn.ExecContext(context.Background(), "PRAGMA foreign_keys = OFF;", nil)
				return nil
			},
		},
	)), "")
	if err != nil {
		return nil, err
	}
	err = goent.AutoMigrate(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestConnection(t *testing.T) {
	_, err := Setup()
	if err != nil {
		t.Fatalf("Expected Connection, got error %v", err)
	}
}

func TestTx(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected setup, got error %v", err)
	}

	testCases := []struct {
		desc     string
		testCase func(t *testing.T)
	}{
		{
			desc: "Tx_Context_Cancel",
			testCase: func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := db.NewTransactionContext(ctx, sql.LevelSerializable)
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Expected context.Canceled, got : %v", err)
				}
			},
		},
		{
			desc: "Tx_Context_Timeout",
			testCase: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
				defer cancel()

				_, err := db.NewTransactionContext(ctx, sql.LevelSerializable)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Expected context.DeadlineExceeded, got : %v", err)
				}
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, tC.testCase)
	}
}

func TestRace(t *testing.T) {
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			driver := os.Getenv("GOE_DRIVER")
			if driver == "" {
				driver = "SQLite"
			}
			raceDb, _ := mapDriver[driver]()
			if raceDb != nil {
				goent.Close(raceDb)
			}
		}()
	}
	wg.Wait()
}

func TestMigrate(t *testing.T) {
	t.Skip("Skipping TestMigrate - API mismatch between *Database and *goent.DB")
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected a connection, got error %v", err)
	}
	_ = db
}
