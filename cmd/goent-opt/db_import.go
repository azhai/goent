package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func runImport(args *DbImportArgs) {
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

	if args.Schema != "" && !cfg.IsPg {
		fmt.Fprintln(os.Stderr, "Warning: --schema is only applicable for PostgreSQL, ignored")
	}

	tables := args.Tables
	if len(tables) == 0 || args.All {
		entries, err := os.ReadDir(args.Dir)
		if err != nil {
			fmt.Printf("Error reading directory %s: %v\n", args.Dir, err)
			os.Exit(1)
		}
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".sql") {
				tables = append(tables, strings.TrimSuffix(name, ".sql"))
			}
		}
	}

	fmt.Printf("Importing from %s/\n", args.Dir)

	for _, table := range tables {
		fmt.Printf("  Importing table: %s\n", table)
		w := NewTableWork(ctx, db, table)

		if !args.DataOnly {
			schemaPath := findSchemaFile(args.Dir, table)
			if schemaPath == "" {
				fmt.Printf("    Warning: no schema file found for table %s\n", table)
			} else {
				if err := w.importSchema(schemaPath); err != nil {
					fmt.Printf("    Error importing schema: %v\n", err)
					continue
				}
				fmt.Printf("    Schema imported from %s\n", schemaPath)
			}
		}

		if !args.StructureOnly {
			if args.Truncate {
				if err := w.SchemaOps().TruncateTable(ctx, table); err != nil {
					fmt.Printf("    Warning: could not truncate: %v\n", err)
				} else {
					fmt.Printf("    Truncated table %s\n", table)
				}
			}

			dataPath := filepath.Join(args.Dir, table+".data.jsonl")
			count, err := w.importData(cfg, dataPath)
			if err != nil {
				fmt.Printf("    Error importing data: %v\n", err)
				continue
			}
			fmt.Printf("    Data imported from %s (%d rows)\n", dataPath, count)
		}
	}

	if NewTableWork(ctx, db, "").IsPg() {
		for _, table := range tables {
			NewTableWork(ctx, db, table).resetImportSequence()
		}
	}

	fmt.Println("Import complete!")
}

func findSchemaFile(dir, table string) string {
	sqlPath := filepath.Join(dir, table+".sql")
	if _, err := os.Stat(sqlPath); err == nil {
		return sqlPath
	}
	return ""
}
