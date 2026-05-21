package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/azhai/goent"
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
