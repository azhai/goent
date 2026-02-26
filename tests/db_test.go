package tests_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azhai/goent"
	// "github.com/azhai/goent-libpq"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Animal is an animal in the zoo.
type Animal struct {
	Name        string `goe:"index"`
	HabitatId   *uuid.UUID
	InfoId      *[]byte
	Id          int `goe:"pk"`
	AnimalFoods []AnimalFood
}

func (*Animal) TableName() string {
	return "animals"
}

// AnimalFood is the relationship between an animal and its food.
type AnimalFood struct {
	AnimalId int       `goe:"pk"`
	FoodId   uuid.UUID `goe:"pk"`
}

// Food is a type of food that an animal can eat.
type Food struct {
	Id          uuid.UUID `goe:"pk"`
	Name        string
	AnimalFoods []AnimalFood
}

// Habitat is a place where animals live.
type Habitat struct {
	Id          uuid.UUID `goe:"pk"`
	Name        string    `goe:"type:varchar(50)"`
	WeatherId   int
	NameWeather string
	Animals     []Animal
}

// Weather is a type of weather that a habitat can have.
type Weather struct {
	Id       int `goe:"pk"`
	Name     string
	Habitats []Habitat
}

// Info is additional information about an animal.
type Info struct {
	Id         []byte `goe:"pk"`
	Name       string `goe:"index(unique n:idx_name_status);index"`
	NameStatus string `goe:"index(unique n:idx_name_status)"`
	StatusId   int
}

// Status is a status of an animal.
type Status struct {
	ID   int64 `goe:"pk"`
	Name string
}

// ScanFields returns a slice of pointers to Status fields for database scanning.
func (t *Status) ScanFields() []any {
	return []any{
		&t.ID,
		&t.Name,
	}
}

// FetchStatus creates a FetchFunc for Status.
func FetchStatus() goent.FetchFunc {
	return func(target any) []any {
		return target.(*Status).ScanFields()
	}
}

// User is a user in the system.
type User struct {
	Id        int    `goe:"pk"`
	Name      string `goe:"index(n:idx_name_lower f:lower)"`
	Email     string `goe:"unique"`
	UserRoles []UserRole
}

// UserRole is the relationship between a user and a role.
type UserRole struct {
	Id      int `goe:"pk"`
	UserId  int `goe:"m2o"`
	RoleId  int `goe:"m2o"`
	EndDate *time.Time
}

// Role is a role in the system.
type Role struct {
	Id        int `goe:"pk"`
	Name      string
	UserRoles []UserRole
}

// Flag is a flag that can be set on an entity.
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

// PersonJobTitle is the relationship between a person and a job title.
type PersonJobTitle struct {
	PersonId   int `goe:"pk"`
	JobTitleId int `goe:"pk"`
	CreatedAt  time.Time
}

// JobTitle is a job title that a person can have.
type JobTitle struct {
	Name    string
	Id      int `goe:"pk"`
	Persons []Person
}

// Exam is an exam that a person can take.
type Exam struct {
	Id      int `goe:"pk"`
	Score   float32
	Minimum float32
}

// Select is a selection that a person can make.
type Select struct {
	Id   int `goe:"pk"`
	Name string
}

// Page is a page in a book.
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

// Drop is a type of drop that an animal can make.
type Drop struct {
	Id   int `goe:"pk"`
	Name string
}

type DropSchema struct {
	Drop *goent.Table[Drop]
}

// Default is a default value for a table.
type Default struct {
	ID   string `goe:"default:'Default'"`
	Name string
}

type AnimalSchema struct {
	Animal     *goent.Table[Animal]
	AnimalFood *goent.Table[AnimalFood]
}

type OtherSchema struct {
	Info           *goent.Table[Info]
	Status         *goent.Table[Status]
	Weather        *goent.Table[Weather]
	Person         *goent.Table[Person]
	PersonJobTitle *goent.Table[PersonJobTitle]
	JobTitle       *goent.Table[JobTitle]
	Exam           *goent.Table[Exam]
	Select         *goent.Table[Select]
	Page           *goent.Table[Page]
	Default        *goent.Table[Default]
}

type Database struct {
	AnimalSchema      `goe:"public"`
	OtherSchema       `goe:"public"`
	FoodHabitatSchema `goe:"food"`
	Authentication    `goe:"auth"`
	FlagSchema        `goe:"flag"`
	DropSchema        `goe:"drop"`
	*goent.DB
}

var db *Database

var mapDriver = map[string]func(dbDSN, logFile string) (*Database, error){
	// "libpq":      SetupLibPq,
	"pgsql":      SetupPgx,
	"postgres":   SetupPgx,
	"postgresql": SetupPgx,
	"sqlite":     SetupSqlite,
	"sqlite3":    SetupSqlite,
}

func TestMain(m *testing.M) {
	code := m.Run()
	if db != nil {
		if db.DriverName() == "PostgreSQL" {
			sql := `
			DROP TABLE IF EXISTS public.animals, public.person_job_title, public.person, public.job_title,
			public.weather, public.info, public.status, public.default, public.exam, public.page,
			public.select, public.animal_food, auth.user, auth.role, auth.user_role,
			food.food, food.habitat, flag.flag, drop.drop CASCADE;
			DROP SCHEMA IF EXISTS food, auth, flag, drop CASCADE;
			`
			_ = db.DB.RawExecContext(context.Background(), sql)
		}
		_ = goent.Close(db)
	}
	os.Exit(code)
}

func Setup() (*Database, error) {
	env := utils.NewEnvWithFile("../.env")
	dbType := env.GetStr("GOE_DRIVER", "sqlite")
	dbDSN := env.Get("GOE_DATABASE_DSN")
	logFile := env.Get("GOE_LOG_FILE")

	var err error
	dbType = strings.ToLower(dbType)
	db, err = mapDriver[dbType](dbDSN, logFile)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// func SetupLibPq(dbDSN, logFile string) (*Database, error) {
// 	if dbDSN == "" {
// 		dbDSN = "user=postgres password=postgres host=localhost port=5432 database=postgres sslmode=disable"
// 	}
// 	drv := libpq.Open(dbDSN, libpq.NewConfig(libpq.Config{}))
// 	return setupPostgres(drv, logFile)
// }

func SetupPgx(dbDSN, logFile string) (*Database, error) {
	if dbDSN == "" {
		dbDSN = "user=postgres password=postgres host=localhost port=5432 database=postgres sslmode=disable"
	}
	drv := pgsql.Open(dbDSN, pgsql.NewConfig(pgsql.Config{}))
	return setupPostgres(drv, logFile)
}

func setupPostgres(drv model.Driver, logFile string) (*Database, error) {
	var err error
	db, err = goent.Open[Database](drv, logFile)
	if err != nil {
		return nil, err
	}
	err = goent.AutoMigrate(db)
	if err != nil {
		return nil, err
	}
	sql := `
	TRUNCATE TABLE public.animals, public.person_job_title, public.person, public.job_title,
	public.weather, public.info, public.status, public.default, public.exam, public.page,
	public.select, public.animal_food, auth.user, auth.role, auth.user_role,
	food.food, food.habitat, flag.flag, drop.drop RESTART IDENTITY CASCADE;
	`
	err = db.DB.RawExecContext(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func SetupSqlite(dbDSN, logFile string) (*Database, error) {
	if dbDSN == "" {
		dbDSN = filepath.Join(os.TempDir(), "goent.db")
	}
	os.Remove(dbDSN)
	db = nil
	goent.ResetRegistry()

	cfg := sqlite.Config{
		ConnectionHook: func(conn sqlite.ExecQuerierContext, dsn string) error {
			conn.ExecContext(context.Background(), "PRAGMA foreign_keys = OFF;", nil)
			return nil
		},
	}
	if strings.ToLower(logFile) == "stdout" {
		cfg.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	var err error
	db, err = goent.Open[Database](sqlite.Open(dbDSN, sqlite.NewConfig(cfg)), "")
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
				driver = "sqlite"
			}
			driver = strings.ToLower(driver)
			var raceDb *Database
			var err error
			if driver == "sqlite" {
				filename := filepath.Join(os.TempDir(), "goent_race.db")
				raceDb, err = goent.Open[Database](sqlite.Open(filename, sqlite.NewConfig(
					sqlite.Config{})), "")
			} else {
				dsn := os.Getenv("GOE_DATABASE_DSN")
				if dsn == "" {
					dsn = "user=postgres password=postgres host=localhost port=5432 database=postgres"
				}
				raceDb, err = goent.Open[Database](pgsql.Open(dsn, pgsql.NewConfig(pgsql.Config{})), "")
			}
			if err == nil && raceDb != nil {
				goent.Close(raceDb)
			}
		}()
	}
	wg.Wait()
}

func TestMigrate(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Fatalf("Expected a connection, got error %v", err)
	}
	_ = db
}
