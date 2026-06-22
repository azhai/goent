package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/azhai/goent"
	"github.com/azhai/goent/backup"
)

func runExport(args *DbExportArgs) {
	env := NewEnvSafe()
	dbType := resolveDBType(env)
	cfg, err := ToDBConfig(args.DSN, dbType)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}

	db, err := OpenDB(cfg)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer CloseDB(db)

	if args.Archive {
		runArchiveExport(db.DB, args)
		return
	}

	ctx := context.Background()

	tables := args.Tables
	if len(tables) == 0 || args.All {
		if args.Schema != "" {
			tables, err = goent.NewSchemaOps(db.DB).ListTablesInSchema(ctx, args.Schema)
		} else {
			tables, err = goent.NewSchemaOps(db.DB).ListTables(ctx)
		}
		if err != nil {
			fmt.Printf("Error listing tables: %v\n", err)
			os.Exit(1)
		}
	}

	if args.Schema != "" && !cfg.IsPg {
		fmt.Fprintln(os.Stderr, "Warning: --schema is only applicable for PostgreSQL, ignored")
	}

	if err := os.MkdirAll(args.Dir, 0755); err != nil {
		fmt.Printf("Error creating directory %s: %v\n", args.Dir, err)
		os.Exit(1)
	}

	fmt.Printf("Exporting to %s/\n", args.Dir)

	for _, table := range tables {
		fmt.Printf("  Exporting table: %s\n", table)

		if !args.DataOnly {
			w := NewTableWorkWithSchema(ctx, db, args.Schema, table)
			sqlStr, err := w.exportSchemaSQL()
			if err != nil {
				fmt.Printf("    Error exporting schema: %v\n", err)
				continue
			}
			schemaPath := filepath.Join(args.Dir, table+".sql")
			if err := os.WriteFile(schemaPath, []byte(sqlStr+"\n"), 0644); err != nil {
				fmt.Printf("    Error writing schema: %v\n", err)
				continue
			}
			fmt.Printf("    Schema -> %s\n", schemaPath)
		}

		if !args.StructureOnly {
			dataPath := filepath.Join(args.Dir, table+".data.jsonl")
			count, err := exportData(ctx, cfg, table, dataPath)
			if err != nil {
				fmt.Printf("    Error exporting data: %v\n", err)
				continue
			}
			fmt.Printf("    Data   -> %s (%d rows)\n", dataPath, count)
		}
	}

	fmt.Println("Export complete!")
}

func runArchiveExport(db *goent.DB, args *DbExportArgs) {
	ctx := context.Background()
	bc := backup.Config{
		Dir:    filepath.Dir(args.Dir),
		DSN:    args.DSN,
		Schema: args.Schema,
	}
	if bc.Schema == "" && strings.Contains(strings.ToLower(db.DriverName()), "postgres") {
		bc.Schema = "public"
	}
	if bc.Dir == "" || bc.Dir == "." {
		bc.Dir = "."
	}

	engine := backup.New(db, bc)
	res := engine.Full(ctx, "goent-export")
	if res.Err != nil {
		fmt.Fprintf(os.Stderr, "Archive export failed: %v\n", res.Err)
		os.Exit(1)
	}

	// Rename to the requested path if user provided an explicit archive path.
	if filepath.Ext(args.Dir) == ".gz" || filepath.Ext(args.Dir) == ".tar" {
		if err := os.Rename(res.Path, args.Dir); err != nil {
			fmt.Fprintf(os.Stderr, "Error renaming archive: %v\n", err)
			os.Exit(1)
		}
		res.Path = args.Dir
	}

	fmt.Printf("Archive exported to %s (%d bytes)\n", res.Path, res.Size)
}
