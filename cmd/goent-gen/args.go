package main

import (
	"os"

	arg "github.com/alexflint/go-arg"
	"github.com/azhai/goent/utils"
)

type GenArgs struct {
	Fields  *GenFieldsArgs  `arg:"subcommand:fields"`
	Tables  *GenTablesArgs  `arg:"subcommand:tables"`
	Project *GenProjectArgs `arg:"subcommand:project"`
}

func (GenArgs) Description() string {
	return "Code generator for goent ORM"
}

type GenFieldsArgs struct {
	Force   bool   `arg:"--force" help:"Force overwrite existing files"`
	PkgPath string `arg:"positional" placeholder:"PACKAGE-PATH"`
}

func (GenFieldsArgs) Description() string {
	return "Generate field scanner methods for model structs"
}

func (GenFieldsArgs) Examples() []string {
	return []string{
		"goent-gen fields --force ./models",
	}
}

type GenTablesArgs struct {
	Force   bool   `arg:"--force" help:"Force overwrite existing files"`
	DSN     string `arg:"--dsn" help:"Database DSN (env: DB_DSN or GOE_DATABASE_DSN)"`
	Schema  string `arg:"--schema" default:"public" help:"Schema name to reverse"`
	Prefix  string `arg:"--prefix" help:"Table prefix filter (only reverse tables with this prefix)"`
	PkgPath string `arg:"positional" placeholder:"PACKAGE-PATH"`
}

func (GenTablesArgs) Description() string {
	return "Generate model structs from database schema"
}

func (GenTablesArgs) Examples() []string {
	return []string{
		"goent-gen tables --dsn 'postgres://user:pass@localhost/db?sslmode=disable' ./models",
		"goent-gen tables --schema public --prefix 't_' ./models",
	}
}

type GenProjectArgs struct {
	Force   bool   `arg:"--force" help:"Force overwrite existing files"`
	DSN     string `arg:"--dsn" help:"Database DSN (env: DB_DSN or GOE_DATABASE_DSN)"`
	PkgPath string `arg:"positional" placeholder:"PACKAGE-PATH"`
}

func (GenProjectArgs) Description() string {
	return "Generate database connection file for the project"
}

func (GenProjectArgs) Examples() []string {
	return []string{
		"goent-gen project --dsn 'postgres://user:pass@localhost/db?sslmode=disable' ./models",
	}
}

func Run() {
	var args GenArgs
	p := arg.MustParse(&args)

	env := NewEnvSafe()
	args.MergeConfigs(env)

	switch {
	case args.Fields != nil:
		runFields(args.Fields)
	case args.Tables != nil:
		runTables(args.Tables)
	case args.Project != nil:
		runProject(args.Project)
	default:
		p.Fail("expected one of: fields, tables, project")
	}
}

func (a *GenArgs) MergeConfigs(env *utils.Environ) {
	if a.Tables != nil && a.Tables.DSN == "" {
		a.Tables.DSN = resolveDSN(env)
	}
	if a.Project != nil && a.Project.DSN == "" {
		a.Project.DSN = resolveDSN(env)
	}
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

func resolveDSN(env *utils.Environ) string {
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
