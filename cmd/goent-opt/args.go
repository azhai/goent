package main

import (
	"errors"
	"os"

	arg "github.com/alexflint/go-arg"
	"github.com/azhai/goent/utils"
)

type OptArgs struct {
	DbExport   *DbExportArgs   `arg:"subcommand:db-export"`
	DbImport   *DbImportArgs   `arg:"subcommand:db-import"`
	IDCompact  *IDCompactArgs  `arg:"subcommand:id-compact"`
	PgOptimize *PgOptimizeArgs `arg:"subcommand:pg-optimize"`
}

func (OptArgs) Description() string {
	return "Database schema operations for goent"
}

type OptDSN struct {
	DSN    string   `arg:"positional" placeholder:"DSN"`
	Tables []string `arg:"positional" placeholder:"TABLES"`
}

type DbExportArgs struct {
	OptDSN
	All           bool   `arg:"-A,--all" help:"Export all tables"`
	Archive       bool   `arg:"-Z,--archive" help:"Export the whole database as a compressed archive using goent/backup"`
	Schema        string `arg:"-S,--schema" help:"PostgreSQL schema name"`
	StructureOnly bool   `arg:"-T,--structure-only" help:"Export only table structure (SQL), no data"`
	DataOnly      bool   `arg:"-D,--data-only" help:"Export only data, no table structure"`
	Dir           string `arg:"-O,--dir" default:"./export" help:"Output directory or archive file path when --archive is used"`
}

func (DbExportArgs) Description() string {
	return "Export table structure (SQL) and data (JSON Lines) to files" + DSNHelp()
}

func (DbExportArgs) Examples() []string {
	return []string{
		"goent-opt db-export 'postgres://user:pass@localhost/db?sslmode=disable'",
		"goent-opt db-export -A 'postgres://...'",
		"goent-opt db-export -O ./backup 'postgres://...' comment issue",
		"goent-opt db-export -T 'sqlite.db'",
	}
}

type DbImportArgs struct {
	OptDSN
	All           bool   `arg:"-A,--all" help:"Import all tables from directory"`
	Archive       bool   `arg:"-Z,--archive" help:"Import a compressed archive created with goent/backup"`
	Schema        string `arg:"-S,--schema" help:"PostgreSQL schema name"`
	StructureOnly bool   `arg:"-T,--structure-only" help:"Import only table structure (SQL), no data"`
	DataOnly      bool   `arg:"-D,--data-only" help:"Import only data, no table structure"`
	Truncate      bool   `arg:"--truncate" help:"Truncate tables before importing data"`
	Dir           string `arg:"-O,--dir" default:"./export" help:"Input directory or archive file path when --archive is used"`
}

func (DbImportArgs) Description() string {
	return "Import table structure (SQL) and data (JSON Lines) from files" + DSNHelp()
}

func (DbImportArgs) Examples() []string {
	return []string{
		"goent-opt db-import 'postgres://user:pass@localhost/db?sslmode=disable'",
		"goent-opt db-import -A 'postgres://...'",
		"goent-opt db-import -O ./backup 'postgres://...'",
		"goent-opt db-import --truncate 'postgres://...' comment",
	}
}

type IDCompactArgs struct {
	OptDSN
	Gap    int64 `arg:"--gap" default:"1024" help:"Minimum ID gap to trigger compaction (must be >= 1)"`
	DryRun bool  `arg:"--dry-run" help:"Preview changes without modifying data"`
}

func (IDCompactArgs) Description() string {
	return "Compact auto-increment ID gaps and reset sequences" + DSNHelp()
}

func (IDCompactArgs) Examples() []string {
	return []string{
		"goent-opt id-compact 'postgres://user:pass@localhost/db?sslmode=disable' comment issue",
		"goent-opt id-compact --gap 2048 --dry-run 'postgres://...' comment",
	}
}

type PgOptimizeArgs struct {
	OptDSN
	Rules  string `arg:"--rules" default:"optimized-rules.txt" help:"Path to rules file"`
	Init   string `arg:"--init" placeholder:"FILE" help:"Generate default rules file"`
	DryRun bool   `arg:"--dry-run" help:"Show recommendations only, don't execute SQL"`
}

func (PgOptimizeArgs) Description() string {
	return "Analyze PostgreSQL indexes and provide optimization recommendations" + DSNHelp()
}

func (PgOptimizeArgs) Examples() []string {
	return []string{
		"goent-opt pg-optimize --init optimized-rules.txt",
		"goent-opt pg-optimize 'postgres://user:pass@localhost/db?sslmode=disable'",
		"goent-opt pg-optimize --rules my-rules.txt --dry-run 'postgres://...'",
	}
}

func DSNHelp() string {
	return "\n\nDSN can be provided via (in order of priority):\n" +
		"  1. Command line positional argument\n" +
		"  2. DB_DSN (+ optional DB_TYPE) environment variable\n" +
		"  3. DB_DSN in .env file\n" +
		"  4. GOE_DATABASE_DSN (+ optional GOE_DRIVER) environment variable"
}

func Run() {
	var args OptArgs
	p := arg.MustParse(&args)

	env := NewEnvSafe()
	args.MergeConfigs(env)

	switch {
	case args.DbExport != nil:
		runExport(args.DbExport)
	case args.DbImport != nil:
		runImport(args.DbImport)
	case args.IDCompact != nil:
		runIDCompact(args.IDCompact)
	case args.PgOptimize != nil:
		runPgOptimize(args.PgOptimize)
	default:
		p.Fail("expected one of: id-compact, pg-optimize, db-export, db-import")
	}
}

func (a *OptArgs) MergeConfigs(env *utils.Environ) {
	var dsn *string
	switch {
	case a.DbExport != nil:
		dsn = &a.DbExport.DSN
	case a.DbImport != nil:
		dsn = &a.DbImport.DSN
	case a.IDCompact != nil:
		dsn = &a.IDCompact.DSN
	case a.PgOptimize != nil:
		dsn = &a.PgOptimize.DSN
	default:
		return
	}
	if *dsn == "" {
		*dsn = resolveDSN(env)
	}
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

func resolveDBType(env *utils.Environ) string {
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

func resolveLogFile(env *utils.Environ) string {
	if _, ok := utils.GetEnvExists("DB_LOG_FILE"); ok {
		return os.Getenv("DB_LOG_FILE")
	}
	if val := os.Getenv("LOG_FILE"); val != "" {
		return val
	}
	if val := os.Getenv("GOE_LOG_FILE"); val != "" {
		return val
	}
	if _, ok := env.Lookup("DB_LOG_FILE"); ok {
		return env.Get("DB_LOG_FILE")
	}
	return env.Get("LOG_FILE")
}

func ToDBConfig(dsn, dbType string) (DBConfig, error) {
	if dsn == "" {
		return DBConfig{}, errors.New(
			"DSN is required: provide as argument, set DB_DSN/DB_TYPE or " +
				"GOE_DATABASE_DSN/GOE_DRIVER env var, or add DB_DSN to .env file",
		)
	}
	return resolveDriver(dsn, dbType), nil
}
