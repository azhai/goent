package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/azhai/gobus/environ"
	"github.com/azhai/gobus/log"
	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
)

type Database struct {
	table string `goe:"-"`
	*goent.DB
}

func OpenDriverSQL(cfg DBConfig) *sql.DB {
	driver := "sqlite"
	if cfg.IsPg {
		driver = "pgx"
	}
	db, err := sql.Open(driver, cfg.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening driver SQL: %v\n", err)
		os.Exit(1)
	}
	if db.Ping() != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to database via driver SQL\n")
		os.Exit(1)
	}
	return db
}

func OpenDB(cfg DBConfig) (*Database, error) {
	var drv model.Driver
	if cfg.IsPg {
		drv = pgsql.OpenDSN(cfg.DSN)
	} else {
		_ = log.MakeDirForFile(cfg.DSN)
		drv = sqlite.OpenDSN(cfg.DSN)
	}
	db, err := goent.Open[Database](drv)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CloseDB(db *Database) error {
	return goent.Close(db)
}

type DBConfig struct {
	DSN    string
	IsPg   bool
	Driver string
	DBType string
}

func NewEnvSafe() *environ.Environ {
	filename := ".env"
	if _, err := os.Stat(filename); err != nil {
		return &environ.Environ{}
	}
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	return environ.NewEnvWithFile(filename)
}

func IsPostgresDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
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

func DefaultDSN(dbType string) string {
	if dbType == "pgsql" || dbType == "postgres" {
		return "postgres://localhost:5432/test?sslmode=disable"
	}
	return "test.db"
}

type TableWork struct {
	ctx   context.Context
	db    *goent.DB
	tx    model.Transaction
	ops   *goent.SchemaOps
	Table string
}

func NewTableWork(ctx context.Context, db *Database, table string) *TableWork {
	d := db.DB
	return &TableWork{ctx: ctx, db: d, ops: goent.NewSchemaOpsWithSchema(d, ""), Table: table}
}

func NewTableWorkWithSchema(ctx context.Context, db *Database, schema, table string) *TableWork {
	d := db.DB
	return &TableWork{ctx: ctx, db: d, ops: goent.NewSchemaOpsWithSchema(d, schema), Table: table}
}

func (w *TableWork) WithTx(tx model.Transaction) *TableWork {
	w.tx = tx
	return w
}

func (w *TableWork) SchemaOps() *goent.SchemaOps {
	return w.ops
}

func (w *TableWork) IsPg() bool {
	return w.ops.IsPg()
}

func (w *TableWork) BeginTx() (model.Transaction, error) {
	return w.db.NewTransactionContext(w.ctx, sql.LevelDefault)
}

func (w *TableWork) TxExec(sqlStr string, args ...any) error {
	qr := model.CreateQuery(sqlStr, args)
	return w.tx.ExecContext(w.ctx, &qr)
}

func quote(name string) string {
	return `"` + name + `"`
}
