package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

type ToolsDB struct {
	dsn string
	*goent.DB
}

type DBConfig struct {
	DSN    string
	IsPg   bool
	Driver string
	DBType string
}

func NewEnvSafe() *utils.Environ {
	filename := ".env"
	if _, err := os.Stat(filename); err != nil {
		return &utils.Environ{}
	}
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	return utils.NewEnvWithFile(filename)
}

func IsPostgresDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

func isLikelyDSN(s string) bool {
	return strings.Contains(s, "://") ||
		strings.HasSuffix(s, ".db") ||
		strings.HasSuffix(s, ".sqlite")
}

func detectDriver(dsn string) DBConfig {
	isPg := IsPostgresDSN(dsn)
	driver := "sqlite"
	if isPg {
		driver = "pgsql"
	}
	return DBConfig{DSN: dsn, IsPg: isPg, Driver: driver, DBType: driver}
}

func resolveDriver(dsn, dbType string) DBConfig {
	if dbType == "pgsql" || dbType == "postgres" {
		return DBConfig{DSN: dsn, IsPg: true, Driver: "pgsql", DBType: dbType}
	}
	if dbType != "" {
		return DBConfig{DSN: dsn, IsPg: false, Driver: "sqlite", DBType: dbType}
	}
	return detectDriver(dsn)
}

func ParseDSNArgs(cliDSN string) (DBConfig, error) {
	if cliDSN != "" {
		return detectDriver(cliDSN), nil
	}

	env := NewEnvSafe()

	envDSN := os.Getenv("DB_DSN")
	envType := os.Getenv("DB_TYPE")
	if envDSN == "" {
		envDSN = env.Get("DB_DSN")
		envType = env.Get("DB_TYPE")
	}

	if envDSN == "" {
		envDSN = os.Getenv("GOE_DATABASE_DSN")
		envType = os.Getenv("GOE_DRIVER")
	}

	if envDSN == "" {
		return DBConfig{}, errors.New(
			"DSN is required: provide as argument, set DB_DSN/DB_TYPE or GOE_DATABASE_DSN/GOE_DRIVER env var, or add DB_DSN to .env file",
		)
	}

	return resolveDriver(envDSN, envType), nil
}

func ResolveLogFile() string {
	if _, ok := utils.GetEnvExists("DB_LOG_FILE"); ok {
		return os.Getenv("DB_LOG_FILE")
	}
	if val := os.Getenv("LOG_FILE"); val != "" {
		return val
	}
	if val := os.Getenv("GOE_LOG_FILE"); val != "" {
		return val
	}

	env := NewEnvSafe()
	if _, ok := env.Lookup("DB_LOG_FILE"); ok {
		return env.Get("DB_LOG_FILE")
	}
	return env.Get("LOG_FILE")
}

func OpenToolsDB(cfg DBConfig) (*ToolsDB, error) {
	var drv model.Driver
	if cfg.IsPg {
		drv = pgsql.OpenDSN(cfg.DSN)
	} else {
		_ = utils.MakeDirForFile(cfg.DSN)
		drv = sqlite.OpenDSN(cfg.DSN)
	}
	logFile := ResolveLogFile()
	tdb, err := goent.Open[ToolsDB](drv, logFile)
	if err != nil {
		return nil, err
	}
	tdb.dsn = cfg.DSN
	return tdb, nil
}

func DefaultDSN(dbType string) string {
	if dbType == "pgsql" || dbType == "postgres" {
		return "postgres://localhost:5432/test?sslmode=disable"
	}
	return "test.db"
}

func CloseDB(tdb *ToolsDB) error {
	return goent.Close(tdb)
}

func (tdb *ToolsDB) BeginTx(ctx context.Context) (model.Transaction, error) {
	return tdb.NewTransactionContext(ctx, sql.LevelDefault)
}

func (tdb *ToolsDB) DriverSQL() *sql.DB {
	driver := "sqlite"
	if IsPostgresDSN(tdb.dsn) {
		driver = "pgx"
	}
	db, _ := sql.Open(driver, tdb.dsn)
	return db
}

func (tdb *ToolsDB) RawQueryRowContext(ctx context.Context, rawSql string, args ...any) model.Row {
	conn := tdb.Driver().NewConnection()
	dc := tdb.Driver().GetDatabaseConfig()
	qr := model.CreateQuery(rawSql, args)
	row, _ := qr.WrapQueryRow(ctx, conn, dc)
	return row
}

func TxExec(ctx context.Context, tx model.Transaction, sqlStr string, args ...any) error {
	qr := model.CreateQuery(sqlStr, args)
	return tx.ExecContext(ctx, &qr)
}

func TxQuery(ctx context.Context, tx model.Transaction, sqlStr string, args ...any) (model.Rows, error) {
	qr := model.CreateQuery(sqlStr, args)
	return tx.QueryContext(ctx, &qr)
}

func TxQueryRow(ctx context.Context, tx model.Transaction, sqlStr string, args ...any) model.Row {
	qr := model.CreateQuery(sqlStr, args)
	return tx.QueryRowContext(ctx, &qr)
}

func q(name string) string {
	return `"` + name + `"`
}

func printDSNHelp() {
	fmt.Println()
	fmt.Println("DSN can be provided via (in order of priority):")
	fmt.Println("  1. Command line argument")
	fmt.Println("  2. DB_DSN (+ optional DB_TYPE) environment variable")
	fmt.Println("  3. GOE_DATABASE_DSN (+ optional GOE_DRIVER) environment variable")
	fmt.Println("  4. DB_DSN in .env file in current directory")
}
