package main

import (
	"fmt"
	"os"

	arg "github.com/alexflint/go-arg"
	"github.com/azhai/goent/utils"
)

type Args struct {
	DSN    string `arg:"positional" placeholder:"DSN" help:"Database connection string"`
	Schema string `arg:"-S,--schema" default:"public" help:"PostgreSQL schema name"`
}

func (Args) Description() string {
	return "Interactive SQL query tool for goent"
}

func (Args) Examples() []string {
	return []string{
		"goent-sql",
		"goent-sql 'postgres://user:pass@localhost/db?sslmode=disable'",
		"goent-sql -S my_schema 'postgres://...'",
		"goent-sql test.db",
	}
}

func resolveDSN(env *utils.Environ, cliDSN string) string {
	if cliDSN != "" {
		return cliDSN
	}
	if val := os.Getenv("DB_DSN"); val != "" {
		return val
	}
	if val := env.Get("DB_DSN"); val != "" {
		return val
	}
	if val := os.Getenv("GOE_DATABASE_DSN"); val != "" {
		return val
	}
	if val := env.Get("GOE_DATABASE_DSN"); val != "" {
		return val
	}
	return ""
}

func resolveDBType(env *utils.Environ, dsn string) string {
	if val := os.Getenv("DB_TYPE"); val != "" {
		return val
	}
	if val := env.Get("DB_TYPE"); val != "" {
		return val
	}
	if val := os.Getenv("GOE_DRIVER"); val != "" {
		return val
	}
	return ""
}

func main() {
	var args Args
	arg.MustParse(&args)

	env := NewEnvSafe()
	dsn := resolveDSN(env, args.DSN)
	dbType := resolveDBType(env, dsn)

	if dsn == "" {
		fmt.Fprintln(os.Stderr, "Error: DSN is required. Provide as argument or set DB_DSN/GOE_DATABASE_DSN env var.")
		os.Exit(1)
	}

	cfg, err := ToDBConfig(dsn, dbType)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	repl, err := NewREPL(cfg, args.Schema)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	repl.Run()
}
